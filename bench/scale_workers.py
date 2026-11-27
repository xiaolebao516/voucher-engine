#!/usr/bin/env python3
"""决定性实验：固定总并发，只改客户端进程数。

若 QPS 随进程数线性上升 => 瓶颈在客户端（Python GIL / 事件循环）
若 QPS 不随进程数变化    => 瓶颈在服务端
"""
import sys

sys.path.insert(0, "bench")
import gradient_bench as gb

CONC = 2000

print(f"固定并发={CONC}，改变客户端进程数")
print(f"{'进程数':>6}{'QPS':>10}{'P50ms':>10}{'P99ms':>10}{'耗时s':>8}")
for workers in (2, 4, 8, 12, 16):
    gb.reset_state(5000)
    elapsed, results, total = gb.launch_workers(CONC, workers, "unique")
    lats = sorted(dt for _, dt in results)
    p50 = lats[len(lats) // 2]
    p99 = lats[min(int(len(lats) * 0.99), len(lats) - 1)]
    print(f"{workers:>6}{total / elapsed:>10.1f}{p50:>10.1f}{p99:>10.1f}{elapsed:>8.2f}", flush=True)
