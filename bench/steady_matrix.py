#!/usr/bin/env python3
"""
稳态梯度压测：固定时长窗口，开/关限流对照。

## 为什么不能用固定请求数

初版用 total = 1.2 × 并发，导致高并发档的测试窗口短得离谱：
  c=500  → 600 请求 / ~1.5s
  c=5000 → 6000 请求 / ~11s
同一台机器上只读接口的对照实验说明这有多失真：
  c=500 total=600   →  960 QPS（窗口 0.62s，几乎全是 ramp-up）
  c=500 total=10000 → 2128 QPS（窗口 4.7s，已进入稳态）
所以「限流开关对性能的影响」必须用固定时长，让每档都跑够时间。

## 为什么库存设 12000

库存如果太小（5000），c=500 关限流时会在 2.5 秒内售罄，窗口后段变成
「库存为 0 的快速失败路径」，QPS 曲线中途变形状，不是单一工作负载。
12000 保证 8 秒窗口内库存不是限制因素，测到的是限流本身的差异。

## 统计口径（与全部脚本一致）

  request_qps  = 全部响应 / 耗时（含 429）
  success_qps  = 业务成功 / 耗时（不含 429）
  order_throughput_qps = 实际落库订单 / (HTTP 窗口 + MQ 排空)
  429 永远不计入成功业务吞吐。

用法：python bench/steady_matrix.py --label on-unique
"""
import argparse
import json
import os
import subprocess
import threading
import time
from datetime import datetime

import gradient_bench as gb

STOCK = 12000
DURATION = 8          # 每档固定压 8 秒
DRAIN_TIMEOUT = 420   # 消费者只有 ~64 单/秒，12000 条要 3 分钟


def node_load_duration(concurrency, seconds):
    out = subprocess.run(
        ["node", "bench/load.js", str(concurrency), str(seconds), "seckill", "--duration"],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=1800)
    if out.returncode != 0:
        raise RuntimeError(f"node load failed: {out.stderr}")
    return json.loads(out.stdout.strip().splitlines()[-1])


def run_round(concurrency):
    gb.reset_state(STOCK)
    time.sleep(1)

    orders_before = int(gb.mysql("SELECT COUNT(*) FROM tb_voucher_order"))
    if orders_before != 0:
        print(f"[warn] 起始订单数={orders_before}", flush=True)

    # 压测窗口内持续采样 MQ 堆积，取峰值 —— 只看结束时刻会漏掉真正的堆积高点
    peak = {"ready": 0, "unacked": 0}
    stop = threading.Event()

    def sampler():
        while not stop.is_set():
            try:
                import httpx
                d = httpx.get("http://127.0.0.1:15672/api/queues/%2F/order.create.queue",
                              auth=("guest", "guest"), timeout=5).json()
                peak["ready"] = max(peak["ready"], d.get("messages", 0))
                peak["unacked"] = max(peak["unacked"], d.get("messages_unacknowledged", 0))
            except Exception:
                pass
            time.sleep(0.5)

    t = threading.Thread(target=sampler, daemon=True)
    t.start()

    t0 = time.time()
    res = node_load_duration(concurrency, DURATION)
    elapsed = time.time() - t0
    stop.set()
    t.join(timeout=5)

    # 排空：以 MySQL 订单数连续 5 次不变为准。
    # 不用 RabbitMQ 管理接口的 messages 字段做判据 —— 那是 5 秒刷新一次的缓存，
    # 实测刚灌完常读到 ready=0 的过期快照，会提前判定排空完毕。
    drain_start = time.time()
    last, stable, last_change = -1, 0, drain_start
    deadline = time.time() + DRAIN_TIMEOUT
    while time.time() < deadline:
        cnt = int(gb.mysql("SELECT COUNT(*) FROM tb_voucher_order"))
        if cnt == last:
            stable += 1
            if stable >= 5:
                break
        else:
            stable, last, last_change = 0, cnt, time.time()
        time.sleep(2)
    drain_sec = max(last_change - drain_start, 0)

    orders = int(gb.mysql("SELECT COUNT(*) FROM tb_voucher_order")) - orders_before
    cats = res["cats"]
    total = res["total"]
    success = cats.get("success", 0)
    limited = cats.get("rate_limited", 0)
    unavailable = cats.get("unavailable", 0)
    error = cats.get("error", 0)

    return {
        "target_concurrency": concurrency,
        "client": "node",
        "duration_sec": DURATION,
        "stock": STOCK,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "elapsed_sec": round(elapsed, 2),
        "drain_sec": round(drain_sec, 2),
        "http": {
            "total": total, "success_200": success,
            "biz_rejected_200": cats.get("biz_rejected", 0),
            "rate_limited_429": limited, "unavailable_503": unavailable, "error": error,
        },
        "rates": {
            "request_qps": round(total / elapsed, 2),
            "steady_qps": res.get("steady_qps"),
            "success_qps": round(success / elapsed, 2),
            "rate_limited_pct": round(limited / total * 100, 2) if total else 0,
            "error_pct": round((error + unavailable) / total * 100, 2) if total else 0,
        },
        "latency_ms": {"p50": res["p50"], "p99": res["p99"]},
        "per_sec": res.get("per_sec", []),
        "orders_persisted": orders,
        "order_throughput_qps": round(orders / (elapsed + drain_sec), 2) if elapsed + drain_sec else 0,
        "mq_peak": peak,
        "db_stock_after": gb.mysql(f"SELECT stock FROM tb_voucher WHERE id={gb.VOUCHER_ID}"),
        "redis_stock_after": gb.redis("GET", f"seckill:stock:{gb.VOUCHER_ID}"),
        "db_pool": gb.hikari_stats(None),
        "containers": gb.container_cpu(),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--sweep", default="500,1000,3000,5000")
    ap.add_argument("--warmup", type=int, default=3)
    args = ap.parse_args()

    os.makedirs(gb.RESULTS_DIR, exist_ok=True)
    concs = [int(x) for x in args.sweep.split(",")]

    # 预热必须打抢券链路：只读 GET 不触发 Lua / MQ 发布 / 事务，冷 JIT 下首档偏低 3 倍
    if args.warmup > 0:
        print(f"[warmup] {args.warmup} 轮抢券预热", flush=True)
        for _ in range(args.warmup):
            gb.reset_state(gb.INITIAL_STOCK)
            subprocess.run(["node", "bench/load.js", "200", "1000"],
                           capture_output=True, timeout=600)
        gb.reset_state(STOCK)
        time.sleep(2)

    reports = []
    for c in concs:
        print(f"\n=== {args.label} 并发={c} 时长={DURATION}s ===", flush=True)
        r = run_round(c)
        r["label"] = args.label
        reports.append(r)
        print(json.dumps({k: r[k] for k in
                          ("target_concurrency", "rates", "latency_ms",
                           "orders_persisted", "order_throughput_qps",
                           "drain_sec", "mq_peak")},
                         ensure_ascii=False), flush=True)

    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    out = os.path.join(gb.RESULTS_DIR, f"{args.label}_steady_{ts}.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(reports, f, ensure_ascii=False, indent=2)
    print(f"\n[written] {out}")


if __name__ == "__main__":
    main()
