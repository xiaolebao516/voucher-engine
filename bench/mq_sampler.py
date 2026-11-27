#!/usr/bin/env python3
"""
独立的 MQ 积压采样器 —— 必须在**单独进程**里跑。

## 为什么不能放在压测脚本的线程里

初版把采样放在 steady_matrix.py 的 daemon 线程里，结果 mq_peak 全是 0。
原因有两层：
  1. RabbitMQ 管理接口的队列统计是 5 秒刷新一次的缓存，8 秒的 HTTP 窗口
     里最多只能拿到 1-2 个快照，且经常读到「还没刷新到积压」的旧值。
  2. 压测期间 CPU 被 node + java + 三个容器占满，同进程的 Python 线程
     抢不到 GIL，采样间隔实际被拉长到不可控。
放到独立进程 + 用 rabbitmqctl（走 Erlang CLI，不依赖管理插件缓存）才稳。

## 用法

  python bench/mq_sampler.py --out bench/results/mq_on_c500.jsonl --duration 120
  # 另开一个终端跑压测，结束后看 --out 文件里的峰值

  或者被 steady_matrix 以 subprocess 方式拉起。
"""
import argparse
import json
import subprocess
import time

QUEUE = "order.create.queue"


def sample():
    """返回 (ready, unacked)。rabbitmqctl 直接读 broker 内存状态，不走管理插件缓存。"""
    out = subprocess.run(
        ["docker", "exec", "ve-mq", "rabbitmqctl", "list_queues",
         "name", "messages", "messages_unacknowledged", "--quiet"],
        capture_output=True, text=True, timeout=20)
    for line in out.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) == 3 and parts[0] == QUEUE:
            try:
                return int(parts[1]), int(parts[2])
            except ValueError:
                return None
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--duration", type=float, default=120)
    ap.add_argument("--interval", type=float, default=1.0)
    args = ap.parse_args()

    end = time.time() + args.duration
    peak = {"ready": 0, "unacked": 0, "at": None}
    n = 0
    with open(args.out, "w", encoding="utf-8") as f:
        while time.time() < end:
            s = sample()
            if s is not None:
                ready, unacked = s
                n += 1
                f.write(json.dumps({"t": round(time.time(), 3),
                                    "ready": ready, "unacked": unacked}) + "\n")
                f.flush()
                if ready > peak["ready"]:
                    peak = {"ready": ready, "unacked": unacked,
                            "at": round(time.time(), 3)}
            time.sleep(args.interval)

    print(json.dumps({"samples": n, "peak": peak}, ensure_ascii=False))


if __name__ == "__main__":
    main()
