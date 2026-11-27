#!/usr/bin/env python3
"""
瓶颈定位实验：在压测进行中抓 java 线程栈，统计线程状态分布。

只做一件事：看请求线程到底卡在哪。
  - 卡在 redis 相关栈帧 => Redis/Lettuce 连接是瓶颈
  - 卡在 socketRead/await  => 等下游（DB/MQ/Redis）
  - 卡在业务代码          => CPU 计算
"""
import collections
import subprocess
import sys
import threading
import time

sys.path.insert(0, "bench")
import gradient_bench as gb


def java_pid():
    ps = ("Get-CimInstance Win32_Process -Filter \"Name='java.exe'\" |"
          " Where-Object { $_.CommandLine -like '*voucher-engine*.jar*' } |"
          " Select-Object -First 1 -ExpandProperty ProcessId")
    out = subprocess.run(["powershell", "-NoProfile", "-Command", ps],
                         capture_output=True, text=True)
    return int(out.stdout.strip())


def dump(pid):
    out = subprocess.run(["jstack", str(pid)], capture_output=True, text=True,
                         encoding="utf-8", errors="replace")
    return out.stdout


def classify(stack):
    """按线程栈里的关键字归类：线程此刻在等什么"""
    if "http-nio" not in stack:
        return None
    if "Redis" in stack or "Lettuce" in stack or "lettuce" in stack:
        return "redis"
    if "HikariPool" in stack or "jdbc" in stack.lower():
        return "mysql"
    if "RabbitTemplate" in stack or "amqp" in stack.lower():
        return "mq"
    if "socketRead" in stack or "socketWrite" in stack:
        return "socket-io"
    if "Thread.sleep" in stack or "park" in stack:
        return "parked"
    return "business"


def main():
    pid = java_pid()
    print("java pid =", pid)
    gb.reset_state(5000)

    stop = threading.Event()

    def sampler():
        counts = collections.Counter()
        n = 0
        while not stop.is_set():
            st = dump(pid)
            for block in st.split("\n\n"):
                c = classify(block)
                if c:
                    counts[c] += 1
            n += 1
            time.sleep(1.5)
        print(f"\n=== 采样 {n} 次，累计 http-nio 线程状态分布 ===")
        total = sum(counts.values())
        for k, v in counts.most_common():
            print(f"  {k:<12} {v:>5}  ({v / total * 100:.1f}%)")

    t = threading.Thread(target=sampler, daemon=True)
    t.start()

    print("--- 施加 2000 并发 ---")
    elapsed, results, total = gb.launch_workers(2000, 8, "unique")
    cats = collections.Counter(c for c, _ in results)
    print(f"完成 elapsed={elapsed:.2f}s 分类={dict(cats)}")
    time.sleep(2)
    stop.set()
    t.join(timeout=10)


if __name__ == "__main__":
    main()
