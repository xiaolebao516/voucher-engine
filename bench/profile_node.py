#!/usr/bin/env python3
"""
瓶颈定位：Node 客户端持续施压时，采样 java 线程栈 + 进程 CPU。

为什么要重写 profile_bottleneck.py：旧版用 Python 多进程客户端，那个客户端本身
只有 ~500 QPS，采样到的是「客户端不够快」而不是「服务端慢」。这里换成 Node
客户端（实测可打 ~1900 QPS 的只读接口），采到的阻塞点才属于服务端。

关键对比组（同一个进程、同一份预热状态下测）：
  detail  GET  /api/voucher/1        只读，代表 Tomcat + MySQL 读的上限
  seckill POST /api/voucher/1/seckill 抢券，额外走 限流Lua + 扣减Lua + MQ投递

两者 QPS 差多少，就说明抢券链路上多出来的那几步占了多少时间。
"""
import collections
import json
import subprocess
import sys
import threading
import time

sys.path.insert(0, "bench")
import gradient_bench as gb

APP_FILTER = ("Get-CimInstance Win32_Process -Filter \"Name='java.exe'\" |"
              " Where-Object { $_.CommandLine -like '*voucher-engine*.jar*' } |"
              " Select-Object -First 1 -ExpandProperty ProcessId")


def java_pid():
    out = subprocess.run(["powershell", "-NoProfile", "-Command", APP_FILTER],
                         capture_output=True, text=True)
    return int(out.stdout.strip())


def dump(pid):
    out = subprocess.run(["jstack", str(pid)], capture_output=True, text=True,
                         encoding="utf-8", errors="replace")
    return out.stdout


def classify(block):
    """判断一个 http-nio 线程此刻在等什么。

    顺序很重要：先匹配最具体的下游（redis/mysql/mq），再匹配通用阻塞。
    若先匹配 socketRead，所有下游等待都会被笼统归到 socket-io。
    """
    if "http-nio" not in block:
        return None
    if "Lettuce" in block or "Redis" in block or "RedisConnection" in block:
        return "redis"
    if "Hikari" in block or "jdbc" in block.lower() or "MybatisPlus" in block:
        return "mysql"
    if "amqp" in block.lower() or "Rabbit" in block:
        return "mq"
    if "TaskQueue.poll" in block or "SynchronousQueue" in block:
        return "idle-no-request"
    if "socketRead" in block or "socketWrite" in block:
        return "socket-io"
    if "Thread.sleep" in block or "park" in block:
        return "parked"
    return "business"


def cpu_pct(pid, seconds=1.0):
    ps = ("$p=Get-Process -Id {}; $t1=$p.TotalProcessorTime.TotalMilliseconds;"
          "Start-Sleep -Milliseconds {}; $p.Refresh();"
          "$t2=$p.TotalProcessorTime.TotalMilliseconds;"
          "$cores=(Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors;"
          "[math]::Round(($t2-$t1)/{}*100/$cores,1)").format(
        pid, int(seconds * 1000), seconds * 1000)
    out = subprocess.run(["powershell", "-NoProfile", "-Command", ps],
                         capture_output=True, text=True)
    try:
        return float(out.stdout.strip())
    except ValueError:
        return None


def run_with_sampling(label, mode, concurrency, total, pid):
    stop = threading.Event()
    counts = collections.Counter()
    samples = [0]
    cpu = []

    def sampler():
        while not stop.is_set():
            st = dump(pid)
            for block in st.split("\n\n"):
                c = classify(block)
                if c:
                    counts[c] += 1
            samples[0] += 1
            time.sleep(1.0)

    def cpu_sampler():
        while not stop.is_set():
            v = cpu_pct(pid)
            if v is not None:
                cpu.append(v)

    ts = [threading.Thread(target=sampler, daemon=True),
          threading.Thread(target=cpu_sampler, daemon=True)]
    for t in ts:
        t.start()

    out = subprocess.run(["node", "bench/load.js", str(concurrency), str(total), mode],
                         capture_output=True, text=True, encoding="utf-8",
                         errors="replace", timeout=1800)
    res = json.loads(out.stdout.strip().splitlines()[-1])

    time.sleep(1.5)
    stop.set()
    for t in ts:
        t.join(timeout=10)

    print(f"\n=== {label} 并发={concurrency} 请求={total} ===")
    print("  结果:", json.dumps(res, ensure_ascii=False))
    print(f"  java CPU(占全部核心%): max={max(cpu) if cpu else None} "
          f"avg={round(sum(cpu) / len(cpu), 1) if cpu else None} samples={len(cpu)}")
    total_t = sum(counts.values())
    print(f"  http-nio 线程状态（{samples[0]} 次采样，累计 {total_t} 个样本）:")
    for k, v in counts.most_common():
        print(f"    {k:<18} {v:>6}  ({v / total_t * 100:5.1f}%)")
    return res, counts


def main():
    pid = java_pid()
    print("java pid =", pid)

    # 预热必须打真实抢券链路：只读 GET 不会触发 Lua / MQ 发布 / 事务写入，
    # 冷 JIT 下第一档会偏低 2-3 倍（实测 195 vs 636 QPS）。
    print("[warmup] 抢券链路预热")
    for _ in range(4):
        gb.reset_state(gb.INITIAL_STOCK)
        subprocess.run(["node", "bench/load.js", "100", "600"],
                       capture_output=True, timeout=600)
    gb.reset_state(gb.INITIAL_STOCK)

    conc = int(sys.argv[1]) if len(sys.argv) > 1 else 200

    gb.reset_state(gb.INITIAL_STOCK)
    run_with_sampling("只读基线 detail", "detail", conc, conc * 25, pid)

    gb.reset_state(gb.INITIAL_STOCK)
    run_with_sampling("抢券 seckill", "seckill", conc, conc * 25, pid)


if __name__ == "__main__":
    main()
