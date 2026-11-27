#!/usr/bin/env python3
"""一次性校验脚本：验证「HTTP 成功数 = Redis 扣减数 = DB 落库数」三条口径是否一致。

不复用 gradient_bench 的多进程（Windows 上动态导入无法 pickle），
这里用单进程 asyncio 打小流量，只为验证计数口径，不为测吞吐。
"""
import asyncio
import sys
import time

import httpx

sys.path.insert(0, "bench")
import gradient_bench as gb


async def burst(n, base_user):
    async with httpx.AsyncClient(timeout=60) as client:
        tasks = [client.post(gb.BASE_URL.format(1), params={"userId": base_user + i})
                 for i in range(n)]
        return await asyncio.gather(*tasks, return_exceptions=True)


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    print("--- reset ---")
    gb.reset_state(5000)
    print("db_stock", gb.mysql("SELECT stock FROM tb_voucher WHERE id=1"))
    print("redis_stock", gb.redis("GET", "seckill:stock:1"))
    print("orders", gb.mysql("SELECT COUNT(*) FROM tb_voucher_order"))

    print(f"--- {n} 个独立用户请求 ---")
    t0 = time.time()
    resp = asyncio.run(burst(n, 30_000_000))
    print("http 耗时", round(time.time() - t0, 2))

    cats = {}
    for r in resp:
        if isinstance(r, Exception):
            cats["exc"] = cats.get("exc", 0) + 1
        elif r.status_code == 429:
            cats["429"] = cats.get("429", 0) + 1
        elif "抢券成功" in (r.text or ""):
            cats["success"] = cats.get("success", 0) + 1
        else:
            cats["biz_rejected"] = cats.get("biz_rejected", 0) + 1
    print("分类", cats)

    print("--- 等 MQ 排空 ---")
    gb.wait_mq_drained(120)
    time.sleep(2)

    print("db_stock", gb.mysql("SELECT stock FROM tb_voucher WHERE id=1"))
    print("redis_stock", gb.redis("GET", "seckill:stock:1"))
    print("orders", gb.mysql("SELECT COUNT(*) FROM tb_voucher_order"))
    print("consume_log", gb.mysql("SELECT COUNT(*) FROM tb_mq_consume_log"))
    print("local_msg", gb.mysql("SELECT COUNT(*) FROM tb_local_message"))
    print("mq", gb.mq_stats()["order.create.queue"])
    print("delay_queue", gb.mq_stats().get("order.delay.queue", {}))


if __name__ == "__main__":
    main()
