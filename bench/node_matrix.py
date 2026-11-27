#!/usr/bin/env python3
"""
用 Node 客户端跑梯度压测，复用 gradient_bench 的重置/采集逻辑。

为什么需要它：Python 多进程客户端的 GIL 上限约 300-530 QPS，
用它测出的「服务端吞吐」其实是客户端吞吐（线程栈证明服务端 98% 线程空闲）。
Node 的原生异步 HTTP 能打出 ~1900 QPS，才接近服务端真实能力。

统计口径与 gradient_bench.py 完全一致：
  - 429 是限流拒绝，绝不计入成功
  - 订单吞吐以 MySQL 实际落库为准
  - 落库吞吐分母 = HTTP 耗时 + MQ 排空耗时
"""
import argparse
import json
import os
import subprocess
import time
from datetime import datetime

import gradient_bench as gb


def node_load(concurrency, total):
    out = subprocess.run(
        ["node", "bench/load.js", str(concurrency), str(total)],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=900)
    if out.returncode != 0:
        raise RuntimeError(f"node load failed: {out.stderr}")
    return json.loads(out.stdout.strip().splitlines()[-1])


def run_round(concurrency):
    gb.reset_state(gb.INITIAL_STOCK)
    time.sleep(1)

    before = gb.mq_stats()
    orders_before = int(gb.mysql("SELECT COUNT(*) FROM tb_voucher_order"))
    if orders_before != 0:
        print(f"[warn] 起始订单数={orders_before}，上一轮仍有在途消息", flush=True)

    # Node 的 total 取并发数的 1.2 倍，保证每档都是「一波打满后继续补」的持续压力
    total = int(concurrency * 1.2)
    t0 = time.time()
    res = node_load(concurrency, total)
    elapsed = time.time() - t0

    drain_start = time.time()
    last, stable, last_change = -1, 0, drain_start
    deadline = time.time() + 240
    while time.time() < deadline:
        cnt = int(gb.mysql("SELECT COUNT(*) FROM tb_voucher_order"))
        if cnt == last:
            stable += 1
            if stable >= 3:
                break
        else:
            stable, last, last_change = 0, cnt, time.time()
        time.sleep(1)
    drain_sec = max(last_change - drain_start, 0)

    orders = int(gb.mysql("SELECT COUNT(*) FROM tb_voucher_order")) - orders_before
    after = gb.mq_stats()
    cats = res["cats"]
    total_resp = res["total"]
    success = cats.get("success", 0)
    limited = cats.get("rate_limited", 0)
    unavailable = cats.get("unavailable", 0)
    error = cats.get("error", 0)

    return {
        "target_concurrency": concurrency,
        "actual_requests": total_resp,
        "client": "node",
        "initial_stock": gb.INITIAL_STOCK,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "elapsed_sec": round(elapsed, 2),
        "drain_sec": round(drain_sec, 2),
        "http": {
            "total": total_resp, "success_200": success,
            "biz_rejected_200": cats.get("biz_rejected", 0),
            "rate_limited_429": limited, "unavailable_503": unavailable, "error": error,
        },
        "rates": {
            "request_qps": round(total_resp / elapsed, 2),
            "success_qps": round(success / elapsed, 2),
            "rate_limited_pct": round(limited / total_resp * 100, 2) if total_resp else 0,
            "error_pct": round((error + unavailable) / total_resp * 100, 2) if total_resp else 0,
        },
        "latency_ms": {
            "p50": res["p50"], "p99": res["p99"],
        },
        "orders_persisted": orders,
        "order_throughput_qps": round(orders / (elapsed + drain_sec), 2) if elapsed + drain_sec else 0,
        "db_stock_after": gb.mysql(f"SELECT stock FROM tb_voucher WHERE id={gb.VOUCHER_ID}"),
        "redis_stock_after": gb.redis("GET", f"seckill:stock:{gb.VOUCHER_ID}"),
        "db_pool": gb.hikari_stats(None),
        "mq_before": before,
        "mq_after": after,
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

    # 预热用 GET 详情页，不产生订单
    if args.warmup > 0:
        print(f"[warmup] {args.warmup} 轮 GET 预热", flush=True)
        for _ in range(args.warmup):
            subprocess.run(["node", "bench/load.js", "200", "1000"],
                           capture_output=True, timeout=300)
        gb.reset_state(gb.INITIAL_STOCK)
        time.sleep(2)

    reports = []
    for c in concs:
        print(f"\n=== {args.label} 并发={c} (node) ===", flush=True)
        r = run_round(c)
        r["label"] = args.label
        reports.append(r)
        print(json.dumps({k: r[k] for k in
                          ("target_concurrency", "rates", "latency_ms",
                           "orders_persisted", "order_throughput_qps")},
                         ensure_ascii=False), flush=True)

    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    out = os.path.join(gb.RESULTS_DIR, f"{args.label}_node_{ts}.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(reports, f, ensure_ascii=False, indent=2)
    print(f"\n[written] {out}")


if __name__ == "__main__":
    main()
