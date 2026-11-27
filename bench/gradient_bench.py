#!/usr/bin/env python3
"""
梯度压测脚本 —— 令牌桶限流对照实验（多进程 + asyncio）。

## 为什么是多进程

本机实测（详见 bench/results 与 README 的瓶颈分析）：
  - 单进程 asyncio 客户端，无论目标并发 10 还是 500，吞吐都卡在 ~450 QPS；
  - 同一时刻服务端 java 进程 CPU 接近 0，说明服务端完全空闲；
  - 加到 8 个客户端进程后吞吐线性上升到 ~1365 QPS；
  - 16 个进程反而下降（进程调度开销超过收益）。
=> 单进程 Python 客户端的 GIL / 事件循环就是压测瓶颈，必须多进程。

## 统计口径（严格区分，绝不混淆）

  success_200       HTTP 200 且返回体含「抢券成功」  => 真正业务成功
  biz_rejected_200  HTTP 200 但抢光/重复/其它文案     => 业务拒绝（非限流）
  rate_limited_429  HTTP 429                          => 限流拒绝，绝不计入成功
  unavailable_503   HTTP 503                          => Redis 故障（Fail-Closed）
  error             超时/连接异常                      => 客户端或网络错误

  订单吞吐一律以 MySQL 实际落库行数为准，不信接口返回。

## 用法

  # 单档
  python bench/gradient_bench.py --label limit-on --concurrency 500 --workers 8

  # 梯度（500/1000/3000/5000）
  python bench/gradient_bench.py --label limit-on  --sweep 500,1000,3000,5000 --workers 8
  python bench/gradient_bench.py --label limit-off --sweep 500,1000,3000,5000 --workers 8 --limit-off
"""
import argparse
import asyncio
import json
import os
import statistics
import subprocess
import time
from datetime import datetime
from multiprocessing import Process, Queue

import httpx

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(BASE, "bench", "results")

VOUCHER_ID = 1
INITIAL_STOCK = 5000
BASE_URL = "http://127.0.0.1:8081/api/voucher/{}/seckill"


# ============================ 基础设施探测 ============================

def mysql(sql, db="voucher_engine"):
    cmd = ["docker", "exec", "ve-mysql", "mysql", "-uroot", "-proot", "-N", "-D", db, "-e", sql]
    out = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if out.returncode != 0:
        raise RuntimeError(f"mysql failed: {out.stderr}")
    return out.stdout.strip()


def redis(*args):
    cmd = ["docker", "exec", "ve-redis", "redis-cli"] + list(args)
    out = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    return out.stdout.strip()


def mq_stats():
    try:
        r = httpx.get("http://127.0.0.1:15672/api/queues/%2F",
                      auth=("guest", "guest"), timeout=5)
        queues = {q["name"]: q for q in r.json()}

        def info(name):
            q = queues.get(name, {})
            stats = q.get("message_stats") or {}
            return {
                "messages": q.get("messages", 0),
                "consumers": q.get("consumers", 0),
                "ack_rate": round((stats.get("ack_details") or {}).get("rate", 0), 2),
            }

        return {
            "order.create.queue": info("order.create.queue"),
            "order.dlq.queue": info("order.dlq.queue"),
            "order.timeout.dlq": info("order.timeout.dlq"),
        }
    except Exception as e:
        return {"error": str(e)}


def container_cpu():
    try:
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}",
             "ve-redis", "ve-mysql", "ve-mq"],
            capture_output=True, text=True, timeout=40)
        return [l for l in out.stdout.strip().splitlines() if l]
    except Exception as e:
        return [f"error: {e}"]


def app_cpu_sample(pid):
    """采样 java 进程 CPU 百分比（相对单核）"""
    try:
        ps = ("$p=Get-Process -Id {}; $t1=$p.TotalProcessorTime.TotalMilliseconds;"
              "Start-Sleep -Milliseconds 500; $p.Refresh();"
              "$t2=$p.TotalProcessorTime.TotalMilliseconds;"
              "[math]::Round(($t2-$t1)/500*100,1)").format(pid)
        out = subprocess.run(["powershell", "-NoProfile", "-Command", ps],
                             capture_output=True, text=True, timeout=30)
        return float(out.stdout.strip())
    except Exception:
        return None


def find_java_pid():
    try:
        ps = ("Get-Process java -ErrorAction SilentlyContinue |"
              " Sort-Object CPU -Descending | Select-Object -First 1 -ExpandProperty Id")
        out = subprocess.run(["powershell", "-NoProfile", "-Command", ps],
                             capture_output=True, text=True, timeout=30)
        return int(out.stdout.strip())
    except Exception:
        return None


def hikari_stats(pid):
    """
    连接池指标。项目未引入 actuator，这里用 jcmd 的 VM.native_memory 不可行，
    改为读取 Tomcat 线程数 + 通过 MySQL 侧 SHOW PROCESSLIST 观察活跃连接。
    """
    try:
        out = subprocess.run(
            ["docker", "exec", "ve-mysql", "mysql", "-uroot", "-proot", "-N", "-e",
             "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE DB='voucher_engine'"],
            capture_output=True, text=True, timeout=20)
        return {"mysql_active_connections": int(out.stdout.strip() or 0)}
    except Exception:
        return {}


def wait_mq_drained(timeout=90):
    """
    等待 MQ 里「可投递 + 在途未 ack」的消息全部消费完。

    为什么 purge_queue 不够：Spring AMQP 默认 prefetch=250，消费者手里可能
    已经握着 250 条「已投递但还没 ack」的消息。purge_queue 只能删掉队列里
    还没投递的 ready 消息，删不掉这些在途消息。如果紧接着 TRUNCATE 订单表，
    这些在途消息会在 truncate 之后才落库，导致本轮 orders_before 不为 0、
    落库数被少算（实测 500 档少算 98 单）。
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = httpx.get("http://127.0.0.1:15672/api/queues/%2F/order.create.queue",
                          auth=("guest", "guest"), timeout=5)
            q = r.json()
            ready = q.get("messages", 0)
            unacked = q.get("messages_unacknowledged", 0)
            if ready == 0 and unacked == 0:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    print("[warn] MQ 排空等待超时", flush=True)
    return False


def reset_state(stock):
    # 先清空队列，再等在途消息消费完，最后才 truncate —— 顺序不能反。
    for q in ["order.create.queue", "order.dlq.queue", "order.timeout.dlq",
              "order.retry.5s.queue", "order.retry.30s.queue"]:
        subprocess.run(["docker", "exec", "ve-mq", "rabbitmqctl", "purge_queue", q],
                       capture_output=True, timeout=30)
    wait_mq_drained()

    mysql(f"UPDATE tb_voucher SET stock={stock} WHERE id={VOUCHER_ID};"
          f"TRUNCATE TABLE tb_voucher_order; TRUNCATE TABLE tb_mq_consume_log;")
    redis("DEL", f"seckill:stock:{VOUCHER_ID}", f"seckill:order:{VOUCHER_ID}")
    redis("SET", f"seckill:stock:{VOUCHER_ID}", str(stock))
    keys = redis("--scan", "--pattern", "rate:seckill:*")
    if keys:
        redis("DEL", *keys.splitlines())


# ============================ 压测工作进程 ============================

async def _one(client, url, user_id, start_at):
    # 所有协程等到同一时刻再发，保证真正并发
    while time.time() < start_at:
        await asyncio.sleep(0.0005)
    t0 = time.perf_counter()
    try:
        r = await client.post(url, params={"userId": user_id})
        dt = (time.perf_counter() - t0) * 1000
        body = r.text or ""
        if r.status_code == 429:
            cat = "rate_limited"
        elif r.status_code == 503:
            cat = "unavailable"
        elif r.status_code == 200 and "抢券成功" in body:
            cat = "success"
        elif r.status_code == 200:
            cat = "biz_rejected"
        else:
            cat = "error"
        return cat, dt
    except Exception:
        return "error", (time.perf_counter() - t0) * 1000


def worker(worker_id, n, url, start_at, q, user_mode="unique", user_pool=100):
    """
    user_mode 决定 userId 的分配方式，直接影响哪一层限流会被触发：

      unique  : 每个请求一个全新 userId。用户桶容量 5 永不满，
                只有全局限流（1000 容量 / 400 per s）可能生效。
                这是「模拟大量真实用户各抢一次」的典型场景。

      same    : 所有请求用同一个 userId。第 6 次起必被用户桶拒绝，
                用来验证「单个用户狂刷」时限流是否真的挡住。
                这是「模拟恶意刷单」的场景，也是限流的主要价值所在。
    """
    warmup_url = url.replace("/seckill", "")

    def pick_user(i):
        if user_mode == "same":
            return 20_000_000
        if user_mode == "pool":
            return 20_000_000 + (i % user_pool)
        return 10_000_000 + worker_id * 100_000 + i

    async def run():
        limits = httpx.Limits(max_connections=n + 50, max_keepalive_connections=n + 50)
        async with httpx.AsyncClient(limits=limits, timeout=httpx.Timeout(120.0)) as client:
            await client.get(warmup_url)
            results = await asyncio.gather(*[
                _one(client, url, pick_user(i), start_at) for i in range(n)
            ])
        return results

    # 必须 try/finally 保证无论成败都回传结果。
    # 反例：只在成功路径 q.put —— 子进程一旦在预热或建连时抛异常，
    # 父进程就只能在 q.get 里干等到超时，整轮数据全部丢失（实测踩过两次）。
    try:
        results = asyncio.run(run())
    except Exception as e:
        print(f"[worker {worker_id}] 执行失败: {e}", flush=True)
        results = [("error", 0.0)] * n
    q.put(results)


def launch_workers(concurrency, workers, user_mode="unique", user_pool=100):
    """把总并发拆到多个进程，返回 (elapsed, [(cat, latency), ...], 实际请求数)"""
    per_worker = max(1, concurrency // workers)
    actual_total = per_worker * workers
    url = BASE_URL.format(VOUCHER_ID)
    q = Queue()
    start_at = time.time() + 4.0  # 留足所有进程启动+预热

    procs = [Process(target=worker, args=(w, per_worker, url, start_at, q, user_mode, user_pool))
             for w in range(workers)]
    for p in procs:
        p.start()

    # 必须先排空队列再 join。
    # 反例：先 join 再 get —— 子进程 q.put() 的整块结果若超过管道缓冲（Windows 上约 64KB），
    # put 会阻塞；父进程又在 join 里等子进程退出，两边互等，永久死锁。
    # 5000 并发 × 每个 (cat, latency) 元组远超 64KB，正是这个死锁点。
    results = []
    for _ in range(workers):
        try:
            results.extend(q.get(timeout=300))
        except Exception as e:
            print(f"[warn] 子进程结果读取超时: {e}", flush=True)
    for p in procs:
        p.join(timeout=30)
        if p.is_alive():
            p.terminate()
            p.join(timeout=5)

    # start_at 是 time.time() 的绝对时间戳，必须用同一条时间轴计算耗时，
    # 不能和 perf_counter 混用（perf_counter 是任意起点的相对值）。
    elapsed = max(time.time() - start_at, 0.001)

    return elapsed, results, actual_total


# ============================ 单轮 ============================

def run_round(concurrency, workers, user_mode="unique", user_pool=100):
    reset_state(INITIAL_STOCK)
    time.sleep(1)

    before = mq_stats()
    orders_before = int(mysql("SELECT COUNT(*) FROM tb_voucher_order"))
    # reset_state 后订单表应为空。不为空说明还有上一轮的在途消息在落库，
    # 本轮统计会被污染，必须暴露出来而不是悄悄算进去。
    if orders_before != 0:
        print(f"[warn] 起始订单数={orders_before}，上一轮仍有在途消息落库", flush=True)
    pid = find_java_pid()

    cpu_samples = []

    def sample_cpu():
        for _ in range(10):
            v = app_cpu_sample(pid) if pid else None
            if v is not None:
                cpu_samples.append(v)
            time.sleep(0.2)

    import threading
    sampler = threading.Thread(target=sample_cpu, daemon=True)
    sampler.start()

    elapsed, results, actual_total = launch_workers(concurrency, workers, user_mode, user_pool)
    sampler.join(timeout=5)
    drain_start = time.time()

    # 等 MQ 消费完：连续 3 次订单数不变视为稳定
    deadline = time.time() + 180
    last, stable = -1, 0
    last_change_at = drain_start
    while time.time() < deadline:
        cnt = int(mysql("SELECT COUNT(*) FROM tb_voucher_order"))
        if cnt == last:
            stable += 1
            if stable >= 3:
                break
        else:
            stable, last = 0, cnt
            last_change_at = time.time()
        time.sleep(1)

    # 消费排空耗时 = 最后一次看到订单数增长的时刻 - HTTP 阶段结束时刻。
    # 不把「确认稳定」的轮询时间算进去（那是测量开销，不是真实排空时间）。
    # 订单落库吞吐必须除以 (HTTP 耗时 + 排空耗时)：只除以 HTTP 耗时会把
    # 「还在 MQ 里排队、尚未落库」的部分提前算成已完成，属于高估。
    drain_sec = max(last_change_at - drain_start, 0)

    orders_after = int(mysql("SELECT COUNT(*) FROM tb_voucher_order"))
    after = mq_stats()

    cats, latencies = {}, []
    for cat, dt in results:
        cats[cat] = cats.get(cat, 0) + 1
        latencies.append(dt)
    latencies.sort()

    def pct(p):
        if not latencies:
            return 0
        return round(latencies[min(int(len(latencies) * p / 100), len(latencies) - 1)], 2)

    total = len(latencies)
    success = cats.get("success", 0)
    rate_limited = cats.get("rate_limited", 0)
    unavailable = cats.get("unavailable", 0)
    error = cats.get("error", 0)

    return {
        "target_concurrency": concurrency,
        "actual_requests": actual_total,
        "workers": workers,
        "user_mode": user_mode,
        "initial_stock": INITIAL_STOCK,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "elapsed_sec": round(elapsed, 2),
        "drain_sec": round(drain_sec, 2),
        "http": {
            "total": total,
            "success_200": success,
            "biz_rejected_200": cats.get("biz_rejected", 0),
            "rate_limited_429": rate_limited,
            "unavailable_503": unavailable,
            "error": error,
        },
        "rates": {
            "request_qps": round(total / elapsed, 2),
            "success_qps": round(success / elapsed, 2),
            "rate_limited_pct": round(rate_limited / total * 100, 2) if total else 0,
            "error_pct": round((error + unavailable) / total * 100, 2) if total else 0,
        },
        "latency_ms": {
            "p50": pct(50), "p95": pct(95), "p99": pct(99),
            "mean": round(statistics.mean(latencies), 2) if latencies else 0,
            "max": round(max(latencies), 2) if latencies else 0,
        },
        "orders_persisted": orders_after - orders_before,
        "db_stock_after": mysql(f"SELECT stock FROM tb_voucher WHERE id={VOUCHER_ID}"),
        "redis_stock_after": redis("GET", f"seckill:stock:{VOUCHER_ID}"),
        "app_cpu_pct": {
            "max": round(max(cpu_samples), 1) if cpu_samples else None,
            "avg": round(statistics.mean(cpu_samples), 1) if cpu_samples else None,
            "samples": len(cpu_samples),
        },
        "db_pool": hikari_stats(pid),
        "mq_before": before,
        "mq_after": after,
        "containers": container_cpu(),
    }


def warmup_jit(rounds=3, per_round=300):
    """
    JIT 预热。为什么必须有：

    Spring Boot 进程刚起来时方法还是解释执行，第一轮压测的 QPS 会明显偏低
    （实测同一档 500 并发：冷启动 195 QPS，预热后 636 QPS，差 3 倍）。
    如果每组的 500 档都跑在冷 JVM 上，开/关对照的基线就不可比。
    这里在正式梯度前先打几轮丢弃的流量，让热点方法完成 C2 编译。

    预热结果不计入任何统计，且每轮正式压测开始前都会 reset_state，
    所以不会污染订单数。
    """
    print(f"[warmup] JIT 预热 {rounds} 轮 × {per_round} 请求（结果丢弃）", flush=True)
    for i in range(rounds):
        reset_state(INITIAL_STOCK)
        launch_workers(per_round, 4, "unique")
        print(f"[warmup] 第 {i + 1}/{rounds} 轮完成", flush=True)
    time.sleep(2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="run")
    ap.add_argument("--concurrency", type=int, default=500)
    ap.add_argument("--sweep", default=None, help="逗号分隔梯度，如 500,1000,3000,5000")
    ap.add_argument("--workers", type=int, default=8, help="客户端进程数（实测 8 最佳）")
    ap.add_argument("--warmup", type=int, default=3, help="JIT 预热轮数，0 表示跳过")
    ap.add_argument("--limit-off", action="store_true")
    ap.add_argument("--user-mode", default="unique", choices=["unique", "same", "pool"],
                    help="unique=每请求新用户(测全局限流); same=同一用户狂刷(测用户级限流); pool=共享用户池")
    ap.add_argument("--user-pool", type=int, default=100)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    os.makedirs(RESULTS_DIR, exist_ok=True)
    concs = ([int(x) for x in args.sweep.split(",")] if args.sweep else [args.concurrency])

    if args.warmup > 0:
        warmup_jit(args.warmup)

    reports = []
    for c in concs:
        print(f"\n=== {args.label} 目标并发={c} 进程={args.workers} ===", flush=True)
        rep = run_round(c, args.workers, args.user_mode, args.user_pool)
        rep["label"] = args.label
        rep["limit_enabled"] = not args.limit_off
        reports.append(rep)
        print(json.dumps(rep, ensure_ascii=False, indent=2), flush=True)

    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    out = args.out or os.path.join(RESULTS_DIR, f"{args.label}_{ts}.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(reports, f, ensure_ascii=False, indent=2)
    print(f"\n[written] {out}")


if __name__ == "__main__":
    main()
