#!/usr/bin/env python3
"""解析 JFR 里的 Hikari 连接池等待，输出取连接/归还连接的次数与累计耗时。

为什么用 JFR 而不是线程栈采样：
  jcmd Thread.print 每次都会触发 safepoint，压测中高频采样会拖慢被测应用，
  测出的吞吐不是真实值。JFR 是 JVM 内置的低开销采样（<1%），能按事件给出
  ThreadPark 的完整调用栈，据此区分 borrow（取不到连接）和 requite（归还）。

Hikari 的 ConcurrentBag 用 SynchronousQueue 交接连接：池里有空闲连接时
borrow 立刻拿到，拿不到才 park 在 SynchronousQueue$Transferer 上，栈里带
ConcurrentBag.borrow。因此 borrow 事件的累计时长就是「消费者被连接池卡住
的总时间」。requite 是归还时的短暂交接，不代表容量不足。

为什么解析文本而不是 JSON：
  同一份录制用 `jfr print --json` 会产出 270MB 且要跑几分钟；文本格式 11MB、
  1.5s 就能读完，解析逻辑也更简单。
"""
import re
import subprocess
import sys

_UNIT_MS = {"ns": 1e-6, "us": 1e-3, "ms": 1.0, "s": 1000.0, "m": 60000.0, "h": 3600000.0}


def _dur_to_ms(text):
    """把 '2.71 ms' / '1.00 s' / '999 ms' 转成毫秒。"""
    m = re.match(r"\s*(-?[\d.]+)\s*(ns|us|ms|s|m|h)\s*$", text or "")
    if not m:
        return 0.0
    return float(m.group(1)) * _UNIT_MS[m.group(2)]


def parse(path, stack_depth=64):
    out = subprocess.run(
        ["jfr", "print", "--stack-depth", str(stack_depth),
         "--events", "jdk.ThreadPark", path],
        capture_output=True, text=True, encoding="utf-8", errors="replace")
    if out.returncode != 0:
        return {"error": out.stderr[:300]}

    borrow, requite = [], []
    dur = 0.0
    in_block = False
    has_borrow = has_requite = False
    events = 0

    def flush():
        nonlocal dur, has_borrow, has_requite, events
        if not in_block:
            return
        events += 1
        if has_borrow:
            borrow.append(dur)
        elif has_requite:
            requite.append(dur)
        dur = 0.0
        has_borrow = has_requite = False

    for line in out.stdout.splitlines():
        s = line.strip()
        if s.startswith("jdk.ThreadPark"):
            flush()
            in_block = True
            continue
        if not in_block:
            continue
        if s.startswith("duration"):
            dur = _dur_to_ms(s.partition("=")[2])
        elif "ConcurrentBag.borrow" in s or "HikariPool.getConnection" in s:
            has_borrow = True
        elif "ConcurrentBag.requite" in s:
            has_requite = True
        elif s == "}":
            flush()
            in_block = False
    flush()

    return {
        "borrow_n": len(borrow),
        "borrow_ms": round(sum(borrow), 2),
        "borrow_max_ms": round(max(borrow), 2) if borrow else 0.0,
        "borrow_p50_ms": round(sorted(borrow)[len(borrow) // 2], 2) if borrow else 0.0,
        "requite_n": len(requite),
        "requite_ms": round(sum(requite), 2),
        "park_events": events,
    }


if __name__ == "__main__":
    import json
    print(json.dumps(parse(sys.argv[1]), ensure_ascii=False))
