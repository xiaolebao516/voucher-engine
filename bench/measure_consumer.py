#!/usr/bin/env python3
"""
精确测量「订单落库吞吐上限」——即单个 MQ 消费者的真实处理速率。

## 为什么需要单独测

梯度压测里算出的 order_throughput_qps 混入了 HTTP 阶段的并发效应，且分母
包含不确定的排空等待。要回答「限流参数该设多少」，需要的是消费者在
**背压饱和**（队列里始终有货）状态下的稳定速率，这是一个只跟消费者代码
有关的常数。

## 方法

1. 清空状态，用 Node 客户端一次性灌入 N 条消息（N 远大于消费者的处理能力）。
2. 客户端停下后，队列里仍有大量积压 —— 此时消费者处于背压饱和状态。
3. 每 2 秒采样一次 MySQL 订单数，取「积压区间内」的斜率，排除首尾的
   预热/收尾区间，得到稳定吞吐。
4. 同时记录 ack_rate 作为交叉验证。

用法：python bench/measure_consumer.py [注入请求数，默认3000]
"""
import json
import subprocess
import sys
import time

sys.path.insert(0, "bench")
import gradient_bench as gb

Q = "order.create.queue"


def queue_state():
    import httpx
    d = httpx.get(f"http://127.0.0.1:15672/api/queues/%2F/{Q}",
                  auth=("guest", "guest"), timeout=5).json()
    s = d.get("message_stats") or {}
    return {
        "ready": d.get("messages", 0),
        "unacked": d.get("messages_unacknowledged", 0),
        "ack_rate": round((s.get("ack_details") or {}).get("rate", 0), 1),
        "deliver_rate": round((s.get("deliver_get_details") or {}).get("rate", 0), 1),
    }


def orders():
    return int(gb.mysql("SELECT COUNT(*) FROM tb_voucher_order"))


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 3000
    gb.reset_state(gb.INITIAL_STOCK)
    time.sleep(1)

    print(f"[inject] 灌入 {n} 条（并发 300）", flush=True)
    t0 = time.time()
    out = subprocess.run(["node", "bench/load.js", "300", str(n)],
                         capture_output=True, text=True, encoding="utf-8",
                         errors="replace", timeout=600)
    http_res = json.loads(out.stdout.strip().splitlines()[-1])
    http_sec = time.time() - t0
    print(f"[inject] 完成 HTTP {http_sec:.2f}s 结果={http_res}", flush=True)

    # 背压饱和区间采样。
    # 注意：RabbitMQ 管理接口的 messages/ready 是 5 秒刷新一次的缓存值，
    # 刚灌完就查常常读到「ready=0」这种过期快照。因此不能拿队列指标当
    # 结束条件，只能以 MySQL 订单数（直接真值）连续多次不变作为排空判据。
    samples = []
    deadline = time.time() + 400
    stable = 0
    while time.time() < deadline:
        q = queue_state()
        o = orders()
        samples.append((time.time(), o, q["ready"], q["unacked"], q["ack_rate"]))
        print(f"  t={time.time() - t0:6.1f}s orders={o:<5} ready={q['ready']:<5} "
              f"unacked={q['unacked']:<4} ack_rate={q['ack_rate']}", flush=True)
        if len(samples) > 1 and o == samples[-2][1]:
            stable += 1
            if stable >= 5:
                break
        else:
            stable = 0
        time.sleep(2)

    # 用订单数序列的斜率算吞吐：取「排空开始后」到「连续稳定」之间的区间，
    # 丢掉第一个样本（它可能落在 HTTP 阶段内，不是纯排空）。
    series = samples[1:]
    if len(series) >= 3:
        t_a, o_a = series[0][0], series[0][1]
        t_b, o_b = series[-1][0], series[-1][1]
        span = t_b - t_a
        ack_rates = [s[4] for s in samples if s[4] > 0]
        print(f"\n=== 排空区间 ===")
        print(f"  窗口 {span:.1f}s，落库 {o_b - o_a} 单")
        print(f"  落库吞吐 = {(o_b - o_a) / span if span else 0:.2f} 单/秒")
        if ack_rates:
            print(f"  broker ack_rate 峰值={max(ack_rates)} 均值={sum(ack_rates) / len(ack_rates):.1f}/s")
        print(f"  样本数 {len(series)}")
    else:
        print(f"\n[warn] 样本不足（{len(series)} 个），无法给出稳定吞吐")

    total = orders()
    span = samples[-1][0] - samples[0][0] if len(samples) > 1 else 0
    print(f"\n  本轮总落库 {total} 单，排空总耗时 {span:.1f}s，"
          f"平均 {total / span if span else 0:.2f} 单/秒")


if __name__ == "__main__":
    main()
