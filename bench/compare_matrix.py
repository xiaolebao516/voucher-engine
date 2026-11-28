#!/usr/bin/env python3
"""
对比两轮 consumer_matrix 原始数据（默认 v2 基线 vs v3 本次）。

只做只读统计，不跑压测。重点回答：
  1. 消费者并发数提升时，排空 TPS 是否还有增益；
  2. deduct_stock 的行锁等待（lock_ms）与平均耗时是否随并发劣化；
  3. 事务均值/最大值的变化；
  4. 一致性校验是否全部通过。

用法：
  python bench/compare_matrix.py bench/results/consumer-matrix/v2/xxx.json \
                                  bench/results/consumer-matrix/v3/yyy.json
"""
import json
import sys
from statistics import mean


def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def agg(reports, c):
    rs = [r for r in reports if r["consumers"] == c]
    if not rs:
        return None
    def m(key):
        vals = [r[key] for r in rs if r.get(key) is not None]
        return round(mean(vals), 2) if vals else None
    def stmt(key, field):
        vals = [r["stmt_stats"][key][field] for r in rs
                if r.get("stmt_stats", {}).get(key) and r["stmt_stats"][key].get(field) is not None]
        return round(mean(vals), 3) if vals else None
    return {
        "reps": len(rs),
        "inject_sec": m("inject_sec"),
        "drain_sec": m("drain_sec"),
        "drain_tps": m("drain_tps"),
        "throughput_qps": m("order_throughput_qps"),
        "row_lock_waits": m("row_lock_waits_delta"),
        "row_lock_time_ms": m("row_lock_time_ms_delta"),
        "tx_avg_ms": m("tx_avg_ms"),
        "tx_max_ms": m("tx_max_ms"),
        "deduct_avg_ms": stmt("deduct_stock", "avg_ms"),
        "deduct_lock_ms": stmt("deduct_stock", "lock_ms"),
        "order_avg_ms": stmt("insert_order", "avg_ms"),
        "order_lock_ms": stmt("insert_order", "lock_ms"),
        "cpu_max": m("cpu_max_pct_all_cores"),
        "all_checks_pass": all(
            all(r["check"][k] for k in ("no_oversell", "no_duplicate_orders", "no_lost_orders"))
            for r in rs),
    }


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    a_path, b_path = sys.argv[1], sys.argv[2]
    A, B = load(a_path), load(b_path)
    cons = sorted({r["consumers"] for r in A["reports"]} | {r["consumers"] for r in B["reports"]})

    fields = [
        ("drain_sec", "排空s", "{:.2f}"),
        ("drain_tps", "排空TPS", "{:.2f}"),
        ("throughput_qps", "整体TPS", "{:.2f}"),
        ("row_lock_waits", "行锁等待次数", "{:.0f}"),
        ("row_lock_time_ms", "行锁等待ms", "{:.0f}"),
        ("deduct_avg_ms", "扣库存均值ms", "{:.3f}"),
        ("deduct_lock_ms", "扣库存锁等待ms", "{:.1f}"),
        ("order_avg_ms", "插订单均值ms", "{:.3f}"),
        ("tx_avg_ms", "事务均值ms", "{:.3f}"),
        ("tx_max_ms", "事务最大ms", "{:.1f}"),
        ("cpu_max", "CPU峰值%", "{:.2f}"),
    ]

    print(f"A(基线) = {a_path}")
    print(f"B(本次) = {b_path}\n")
    for c in cons:
        a, b = agg(A["reports"], c), agg(B["reports"], c)
        if not a or not b:
            continue
        print(f"===== 消费者 = {c}  (A {a['reps']} 轮 / B {b['reps']} 轮) =====")
        print(f"{'指标':<18}{'A':>12}{'B':>12}{'Δ%':>10}")
        for key, label, fmt in fields:
            av, bv = a.get(key), b.get(key)
            if av is None or bv is None:
                print(f"{label:<18}{'—':>12}{'—':>12}{'—':>10}")
                continue
            delta = (bv - av) / av * 100 if av else 0
            print(f"{label:<18}{fmt.format(av):>12}{fmt.format(bv):>12}{delta:>+9.1f}%")
        print(f"一致性: A={'通过' if a['all_checks_pass'] else '未通过'}  "
              f"B={'通过' if b['all_checks_pass'] else '未通过'}\n")


if __name__ == "__main__":
    main()
