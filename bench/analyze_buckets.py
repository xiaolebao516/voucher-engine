#!/usr/bin/env python3
"""汇总分桶压测结果，并与 v3 基线（单库存行）对照。

用法：
  python bench/analyze_buckets.py [--dir bench/results/bucket-matrix]
"""
import argparse
import glob
import json
import os
import statistics as st


def load_reports(root):
    out = []
    for f in glob.glob(os.path.join(root, "**", "*.json"), recursive=True):
        try:
            d = json.load(open(f, encoding="utf-8"))
        except Exception:
            continue
        for r in d.get("reports", []):
            r["_file"] = f
            out.append(r)
    return out


def agg(rs):
    def m(k):
        vals = [r[k] for r in rs if r.get(k) is not None]
        return (round(st.mean(vals), 2), round(st.pstdev(vals), 2)) if vals else (None, None)

    deduct = {}
    for key in ("deduct_stock", "deduct_bucket"):
        cnts = [((r["stmt_stats"].get(key) or {}).get("count") or 0) for r in rs]
        locks = [((r["stmt_stats"].get(key) or {}).get("lock_ms") or 0.0) for r in rs]
        avgms = [((r["stmt_stats"].get(key) or {}).get("avg_ms") or 0.0) for r in rs]
        deduct[key] = {"count": round(st.mean(cnts), 0),
                       "avg_ms": round(st.mean(avgms), 3),
                       "lock_ms": round(st.mean(locks), 1)}
    return {
        "n": len(rs),
        "qps": m("order_throughput_qps"),
        "drain_only_tps": m("drain_only_tps"),
        "row_lock_waits": m("row_lock_waits_delta"),
        "row_lock_ms": m("row_lock_time_ms_delta"),
        "tx_avg_ms": m("tx_avg_ms"),
        "tx_max_ms": m("tx_max_ms"),
        "cpu": m("cpu_max_pct_all_cores"),
        "inject_sec": m("inject_sec"),
        "drain_sec": m("drain_sec"),
        "deduct": deduct,
        "fallback_ratio": m("fallback_attempts_per_order")
        if any(r.get("fallback") for r in rs) else (None, None),
        "consistent": all(
            all(r["check"][k] for k in ("no_oversell", "no_duplicate_orders", "no_lost_orders"))
            for r in rs),
    }


def scan_logs(log_dir, since_epoch, until_epoch=0.0):
    """统计本轮日志里的重试/死信/死锁/业务异常，作为「错误/重试/死信」证据。

    这些在正常压测中应为 0；不为 0 说明一致性有问题，必须报出来。
    mtime 落在 [since, until] 之外的文件跳过，否则审计探针等后续运行
    产生的日志会被算进压测窗口。
    """
    import re
    pats = {
        "retry_5s": "进入5秒延迟重试队列",
        "retry_30s": "再次异常，进入30秒延迟重试队列",
        "dlq": "转入死信队列",
        "max_retry": "达到最大重试次数",
        "deadlock": "Deadlock",
        "stock_exhausted": "库存不足",
        "duplicate_msg": "检测到重复消费消息",
        "duplicate_order": "用户重复下单",
    }
    counts = {k: 0 for k in pats}
    files = 0
    for f in glob.glob(os.path.join(log_dir, "app-*.log")):
        m = os.path.getmtime(f)
        if m < since_epoch or (until_epoch and m > until_epoch):
            continue
        files += 1
        txt = open(f, encoding="utf-8", errors="replace").read()
        for k, p in pats.items():
            counts[k] += txt.count(p)
    return {"files": files, "counts": counts}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="bench/results/bucket-matrix")
    ap.add_argument("--logs", default="bench/logs")
    ap.add_argument("--since", type=float, default=0.0,
                    help="只统计 mtime 晚于该 epoch 的日志；默认全部")
    ap.add_argument("--until", type=float, default=0.0,
                    help="只统计 mtime 早于该 epoch 的日志；0 表示不限")
    args = ap.parse_args()

    reports = load_reports(args.dir)
    if not reports:
        print("没有找到结果文件:", args.dir)
        return
    groups = {}
    for r in reports:
        groups.setdefault(r["buckets"], []).append(r)

    print(f"{'桶数':>5}{'轮':>4}{'QPS':>9}{'纯排空TPS':>11}{'行锁等待':>10}"
          f"{'行锁ms':>10}{'事务均值':>10}{'事务峰值':>10}{'CPU%':>8}{'一致性':>7}")
    for b in sorted(groups):
        a = agg(groups[b])
        f = lambda t: "  -  " if t[0] is None else f"{t[0]:.1f}"
        print(f"{b:>5}{a['n']:>4}{f(a['qps']):>9}{f(a['drain_only_tps']):>11}"
              f"{f(a['row_lock_waits']):>10}{f(a['row_lock_ms']):>10}"
              f"{f(a['tx_avg_ms']):>10}{f(a['tx_max_ms']):>10}{f(a['cpu']):>8}"
              f"{'通过' if a['consistent'] else '失败':>7}")

    print("\n扣减语句（均值）:")
    for b in sorted(groups):
        a = agg(groups[b])
        for key, v in a["deduct"].items():
            if v["count"]:
                print(f"  桶={b:<3}{key:<14} count={v['count']:>7.0f} avg={v['avg_ms']:>7.3f}ms "
                      f"lock={v['lock_ms']:>9.1f}ms")
        if a["fallback_ratio"][0] is not None:
            print(f"  桶={b:<3}换桶尝试/单 = {a['fallback_ratio'][0]}")

    print("\n桶间分布（每轮均值）:")
    for b in sorted(groups):
        rs = [r for r in groups[b] if r.get("bucket_dist")]
        if not rs:
            continue
        n = len(rs)
        nb = len(rs[0]["bucket_dist"])
        cols = []
        for i in range(nb):
            rem = round(sum(r["bucket_dist"][i]["remaining"] for r in rs) / n, 1)
            cnt = round(sum(r["bucket_dist"][i]["orders"] for r in rs) / n, 1)
            cols.append(f"b{i}:剩{rem}/单{cnt}")
        print(f"  桶={b:<3} " + "  ".join(cols))

    print("\n零回退判定（首选桶 floorMod(user,b) 且不回退时，各桶落单只能是 floor/ceil(总单数/b)）:")
    for b in sorted(groups):
        rs = [r for r in groups[b] if r.get("bucket_dist")]
        if not rs:
            continue
        verdicts = []
        for r in rs:
            dist = r["bucket_dist"]
            nb = len(dist)
            total = sum(d["orders"] for d in dist)
            lo, hi = total // nb, -(-total // nb)
            exact = all(lo <= d["orders"] <= hi for d in dist)
            fb = r.get("fallback") or {}
            verdicts.append((exact, fb.get("fallback_orders_db")))
        # 任一轮分布偏离 floor/ceil 即说明发生过换桶
        no_fb = all(v[0] for v in verdicts)
        db_fb = [v[1] for v in verdicts if v[1] is not None]
        print(f"  桶={b:<3} 分布符合零回退={no_fb}"
              + (f"  DB实测换桶单数={db_fb}" if db_fb else "  （该轮无 DB 实测字段）"))

    lg = scan_logs(args.logs, args.since, args.until)
    print(f"\n错误/重试/死信（{lg['files']} 个日志文件）: "
          + json.dumps(lg["counts"], ensure_ascii=False))


if __name__ == "__main__":
    main()
