#!/usr/bin/env python3
"""汇总消费者扩容实验（固定桶数，对比不同消费者数/连接池容量）。

按 (池容量, 消费者数) 分组，每组给出均值与离散度。与 analyze_buckets.py 的区别：
那边按桶数分组，这里按「消费者数 × 池容量」分组，用于回答两个问题：
  1. 消费者从 4 加到 16，落库吞吐涨不涨？
  2. 16 消费者时池从 10 加到 20，吞吐是否回升（说明瓶颈在池而非数据库）？

用法：
  python bench/analyze_scale.py --dir bench/results/consumer-scale
  python bench/analyze_scale.py --dirs bench/results/consumer-scale,bench/results/consumer-scale-pool20
"""
import argparse
import glob
import json
import os
import statistics as st


def load_reports(dirs):
    out = []
    for d in dirs:
        for f in sorted(glob.glob(os.path.join(d, "**", "*.json"), recursive=True)):
            try:
                data = json.load(open(f, encoding="utf-8"))
            except Exception as e:
                print(f"[skip] {f}: {e}")
                continue
            for r in data.get("reports", []):
                r["_src"] = os.path.basename(f)
                r["_pool_effective"] = r.get("pool_size") or data.get("pool") or 10
                out.append(r)
    return out


def m(rs, k):
    vals = [r[k] for r in rs if r.get(k) is not None]
    if not vals:
        return (None, None)
    return (round(st.mean(vals), 2), round(st.pstdev(vals), 2))


def fmt(t, w=9, nd=2):
    if t[0] is None:
        return f"{'-':>{w}}"
    return f"{t[0]:>{w}.{nd}f}±{t[1]:.2f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dirs", default="bench/results/consumer-scale")
    ap.add_argument("--logs", default="bench/logs")
    ap.add_argument("--jfr", default=None,
                    help="JFR 目录；给出后按线程类别拆分取连接等待")
    ap.add_argument("--since", type=float, default=0.0)
    ap.add_argument("--until", type=float, default=0.0)
    args = ap.parse_args()
    dirs = [d.strip() for d in args.dirs.split(",") if d.strip()]

    reports = load_reports(dirs)
    if not reports:
        print("没有找到结果文件:", dirs)
        return

    groups = {}
    for r in reports:
        groups.setdefault((r["_pool_effective"], r["consumers"]), []).append(r)

    hdr = (f"{'池':>4}{'消费者':>6}{'轮':>4}{'注入s':>8}{'排空s':>8}{'落库单':>7}"
           f"{'落库TPS':>10}{'纯排空TPS':>10}{'行锁等待':>9}{'行锁ms':>9}"
           f"{'事务均值ms':>11}{'事务峰值ms':>10}{'池borrow':>9}{'池等待ms':>9}"
           f"{'池等待max':>10}{'MySQLms':>9}{'写MB':>7}{'写IOPS':>8}{'CPU峰值%':>9}{'连接峰值':>8}")
    print(hdr)
    print("-" * len(hdr))

    for key in sorted(groups):
        pool, cons = key
        rs = groups[key]
        print(f"{pool:>4}{cons:>6}{len(rs):>4}"
              f"{fmt(m(rs,'inject_sec'),8)}{fmt(m(rs,'drain_sec'),8)}"
              f"{fmt(m(rs,'orders_persisted'),7,0)}"
              f"{fmt(m(rs,'order_throughput_qps'),10)}"
              f"{fmt(m(rs,'drain_only_tps'),10)}"
              f"{fmt(m(rs,'row_lock_waits_delta'),9,0)}"
              f"{fmt(m(rs,'row_lock_time_ms_delta'),9,0)}"
              f"{fmt(m(rs,'tx_avg_ms'),11,3)}"
              f"{fmt(m(rs,'tx_max_ms'),10,1)}"
              f"{fmt(m(rs,'pool_borrow_n'),9,0)}"
              f"{fmt(m(rs,'pool_borrow_ms'),9,0)}"
              f"{fmt(m(rs,'pool_borrow_max_ms'),10,1)}"
              f"{fmt(m(rs,'mysql_cpu_ms'),9,0)}"
              f"{fmt(m(rs,'mysql_io_write_mb'),7,1)}"
              f"{fmt(m(rs,'mysql_io_write_ops'),8,0)}"
              f"{fmt(m(rs,'cpu_max_pct_all_cores'),9)}"
              f"{fmt(m(rs,'db_conns_peak'),8,0)}")

    print("\n语句级拆解（每轮均值）:")
    for key in sorted(groups):
        pool, cons = key
        rs = groups[key]
        parts = []
        for stmt_key in ("deduct_stock", "deduct_bucket", "insert_order",
                         "insert_order_bucket", "insert_consume_log"):
            cnts = [(r["stmt_stats"].get(stmt_key) or {}).get("count") for r in rs]
            cnts = [c for c in cnts if c is not None]
            avgs = [(r["stmt_stats"].get(stmt_key) or {}).get("avg_ms") for r in rs]
            avgs = [a for a in avgs if a is not None]
            locks = [(r["stmt_stats"].get(stmt_key) or {}).get("lock_ms") for r in rs]
            locks = [l for l in locks if l is not None]
            if cnts and avgs and locks:
                parts.append(f"{stmt_key}: n={st.mean(cnts):.0f} avg={st.mean(avgs):.3f}ms "
                             f"lock={st.mean(locks):.0f}ms")
        print(f"  池={pool} 消费者={cons}  " + " | ".join(parts))

    print("\n一致性 / 回退:")
    for key in sorted(groups):
        pool, cons = key
        rs = groups[key]
        ok = all(all(r["check"][k] for k in
                     ("no_oversell", "no_duplicate_orders", "no_lost_orders"))
                 for r in rs)
        fb = [r["fallback"]["fallback_orders_db"] for r in rs
              if r.get("fallback") and r["fallback"].get("fallback_orders_db") is not None]
        att = [r["fallback"]["attempts_per_order"] for r in rs
               if r.get("fallback") and r["fallback"].get("attempts_per_order") is not None]
        db_stock = [r["check"]["db_stock"] for r in rs]
        redis_stock = [r["check"]["redis_stock"] for r in rs]
        print(f"  池={pool} 消费者={cons}  一致性={'通过' if ok else '失败'}"
              f"  DB库存={sorted(set(db_stock))} Redis库存={sorted(set(redis_stock))}"
              f"  DB实测换桶单={fb}  换桶尝试/单={[round(a,4) for a in att]}")

    print("\n错误/重试/死信（日志）:")
    # -I 隔离模式下 sys.path 不含脚本目录，不能直接 import analyze_buckets
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "analyze_buckets", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                        "analyze_buckets.py"))
    ab = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ab)
    lg = ab.scan_logs(args.logs, args.since, args.until)
    print(f"  {lg['files']} 个文件 " + json.dumps(lg["counts"], ensure_ascii=False))

    if args.jfr:
        print("\n取连接等待的线程来源（按 JFR 文件，回答「谁在抢池」）:")
        for path in sorted(glob.glob(os.path.join(args.jfr, "*.jfr"))):
            if os.path.getsize(path) == 0:
                continue
            by = borrow_by_thread(path)
            if "error" in by:
                print(f"  {os.path.basename(path)}: {by['error']}")
                continue
            total = sum(v["n"] for v in by.values())
            parts = [f"{k}:{v['n']}次/{v['ms']:.0f}ms" for k, v in
                     sorted(by.items(), key=lambda kv: -kv[1]["ms"])]
            print(f"  {os.path.basename(path)}  总{total}次  " + "  ".join(parts))


def borrow_by_thread(path):
    """把 jdk.ThreadPark 里 ConcurrentBag.borrow 事件按线程类别聚合。

    为什么需要：池等待总量只能说明「池不够」，不能说明「谁在抢」。
    实测发现发布确认线程（rabbitConnectionFactoryN）和 MQ 消费线程各占一半，
    因为确认回调里要 UPDATE tb_local_message.markSent，也要借一条连接。
    只调大池等于同时给两条链路放水，定位到线程才知道该改哪一侧。
    """
    import re
    import subprocess
    out = subprocess.run(["jfr", "print", "--stack-depth", "8",
                          "--events", "jdk.ThreadPark", path],
                         capture_output=True, text=True, encoding="utf-8",
                         errors="replace")
    if out.returncode != 0:
        return {"error": out.stderr[:200]}
    unit = {"ns": 1e-6, "us": 1e-3, "ms": 1.0, "s": 1000.0, "m": 60000.0, "h": 3600000.0}

    def category(thread):
        if "RabbitListenerEndpointContainer" in thread:
            return "MQ消费线程"
        if "rabbitConnectionFactory" in thread:
            return "MQ发布确认线程"
        if "http-nio" in thread:
            return "HTTP线程"
        if "voucher-cache-rebuild" in thread:
            return "缓存重建线程"
        return "其它"

    agg = {}
    thread = None
    dur_ms = 0.0
    has_borrow = False
    for line in out.stdout.splitlines():
        s = line.strip()
        if s.startswith("jdk.ThreadPark"):
            thread, dur_ms, has_borrow = None, 0.0, False
            continue
        if s.startswith("eventThread"):
            thread = s.split("=", 1)[1].split("(")[0].strip().strip('"')
        elif s.startswith("duration"):
            m = re.match(r"duration = ([\d.]+) (ns|us|ms|s)", s)
            if m:
                dur_ms = float(m.group(1)) * unit[m.group(2)]
        elif "ConcurrentBag.borrow" in s:
            has_borrow = True
        elif s == "}" and has_borrow and thread:
            k = category(thread)
            rec = agg.setdefault(k, {"n": 0, "ms": 0.0})
            rec["n"] += 1
            rec["ms"] += dur_ms
            has_borrow = False
    return agg


if __name__ == "__main__":
    main()
