#!/usr/bin/env python3
"""
汇总压测结果，输出对照表。

## 数据来源分三类，必须分开看

  bench/results/*_steady_*.json  —— Node 客户端，固定时长稳态（**唯一可信的性能数据**）
      每档压 8 秒，跳过前 2 秒 ramp-up 后取稳态 QPS。
      限流开关对照、服务端能力评估只认这组。

  bench/results/*_node_*.json    —— Node 客户端，固定请求数（**已废弃**）
      用 total = 1.2 × 并发，高并发档窗口只有 1-11 秒且被 ramp-up 主导。
      只读接口对照：同一 c=500 在 600 请求时 960 QPS，10000 请求时 2128 QPS。
      保留仅作为「测量窗口过短会低估 QPS」的反面教材。

  bench/results/py-client/*.json —— Python 多进程客户端（**客户端已饱和**）
      客户端自身在 ~300-530 QPS 就饱和，测出的「QPS」是客户端吞吐。
      保留只为验证限流逻辑在真实 HTTP 层的行为（429 比例、开关差异）。

## 统计口径（贯穿全部脚本）

  request_qps   = 全部 HTTP 响应 / 耗时       （含 429，不代表业务能力）
  steady_qps    = 跳过前 2 秒后的每秒请求数均值（排除 JIT/连接池 ramp-up）
  success_qps   = 业务成功响应 / 耗时         （不含 429）
  order_throughput_qps = MySQL 实际新增订单 / (HTTP 耗时 + MQ 排空耗时)
                   ← 唯一可信的「落库吞吐」，不信接口返回，也不把 429 算进去
"""
import glob
import json
import os

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(BASE, "bench", "results")
PY_DIR = os.path.join(RESULTS, "py-client")


def load(pattern):
    rows = []
    for f in sorted(glob.glob(pattern)):
        with open(f, encoding="utf-8") as fh:
            for r in json.load(fh):
                r["_file"] = os.path.basename(f)
                rows.append(r)
    return rows


def order_qps(r):
    if r.get("order_throughput_qps") is not None:
        return r["order_throughput_qps"]
    span = (r["elapsed_sec"] or 0.001) + r.get("drain_sec", 0)
    return r["orders_persisted"] / span if span else 0


def table(rows, title):
    if not rows:
        print(f"\n### {title}: 无数据")
        return
    print(f"\n### {title}")
    hdr = (f"{'组':<12}{'并发':>6}{'请求':>7}{'耗时s':>8}{'QPS':>9}{'稳态QPS':>9}"
           f"{'成功QPS':>9}{'P50ms':>9}{'P99ms':>9}{'429%':>7}{'错误%':>7}"
           f"{'落库单':>7}{'落库QPS':>9}")
    print(hdr)
    print("-" * len(hdr))
    for r in sorted(rows, key=lambda x: (x.get("label", ""), x["target_concurrency"])):
        total = r["http"]["total"]
        elapsed = r["elapsed_sec"] or 0.001
        steady = r["rates"].get("steady_qps")
        print(f"{r.get('label', '?'):<12}"
              f"{r['target_concurrency']:>6}"
              f"{total:>7}"
              f"{elapsed:>8.2f}"
              f"{r['rates']['request_qps']:>9.1f}"
              f"{(steady if steady is not None else 0):>9.1f}"
              f"{r['rates']['success_qps']:>9.1f}"
              f"{r['latency_ms']['p50']:>9.1f}"
              f"{r['latency_ms']['p99']:>9.1f}"
              f"{r['rates']['rate_limited_pct']:>7.2f}"
              f"{r['rates']['error_pct']:>7.2f}"
              f"{r['orders_persisted']:>7}"
              f"{order_qps(r):>9.2f}")


def compare_on_off(rows, title):
    on = {r["target_concurrency"]: r for r in rows if r.get("label") == "on-unique"}
    off = {r["target_concurrency"]: r for r in rows if r.get("label") == "off-unique"}
    if not (on and off):
        return
    print(f"\n### {title}")
    print(f"{'并发':>6}{'开稳态QPS':>12}{'关稳态QPS':>12}{'限流成本':>11}"
          f"{'开P99':>10}{'关P99':>10}")
    for c in sorted(set(on) & set(off)):
        a, b = on[c], off[c]
        qa = a["rates"].get("steady_qps") or a["rates"]["request_qps"]
        qb = b["rates"].get("steady_qps") or b["rates"]["request_qps"]
        cost = (1 - qa / qb) * 100 if qb else 0
        print(f"{c:>6}{qa:>12.1f}{qb:>12.1f}{cost:>10.1f}%"
              f"{a['latency_ms']['p99']:>10.1f}{b['latency_ms']['p99']:>10.1f}")


def mq_table(rows, title):
    if not rows:
        return
    has_peak = any("mq_peak" in r for r in rows)
    print(f"\n### {title}")
    if has_peak:
        print(f"{'组':<12}{'并发':>6}{'压测中峰值ready':>16}{'峰值unacked':>13}"
              f"{'落库单':>8}{'落库QPS':>9}")
        for r in sorted(rows, key=lambda x: (x.get("label", ""), x["target_concurrency"])):
            p = r.get("mq_peak", {})
            print(f"{r.get('label', '?'):<12}{r['target_concurrency']:>6}"
                  f"{p.get('ready', 0):>16}{p.get('unacked', 0):>13}"
                  f"{r['orders_persisted']:>8}{order_qps(r):>9.2f}")
    else:
        print(f"{'组':<12}{'并发':>6}{'结束时ready':>13}{'落库单':>8}{'落库QPS':>9}")
        for r in sorted(rows, key=lambda x: (x.get("label", ""), x["target_concurrency"])):
            q = (r.get("mq_after") or {}).get("order.create.queue", {})
            print(f"{r.get('label', '?'):<12}{r['target_concurrency']:>6}"
                  f"{q.get('messages', 0):>13}{r['orders_persisted']:>8}"
                  f"{order_qps(r):>9.2f}")


def main():
    steady = load(os.path.join(RESULTS, "*_steady_*.json"))
    node_fixed = load(os.path.join(RESULTS, "*_node_*.json"))
    py_rows = load(os.path.join(PY_DIR, "*.json"))

    table(steady, "Node 客户端 · 固定时长稳态（服务端真实能力）")
    table(node_fixed, "Node 客户端 · 固定请求数（已废弃：窗口过短，QPS 被低估）")
    table(py_rows, "Python 客户端（客户端已饱和，仅供限流行为参考）")

    compare_on_off(steady, "限流开关影响（稳态，unique 模式）")
    compare_on_off(node_fixed, "限流开关影响（固定请求数，已废弃）")

    mq_table(steady, "MQ 积压（稳态）")
    mq_table(py_rows, "MQ 积压（Python）")


if __name__ == "__main__":
    main()
