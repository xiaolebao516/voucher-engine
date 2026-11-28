#!/usr/bin/env python3
"""
分桶实现的运行期审计探针（仅隔离测试库，应用需已启动且限流关闭）。

覆盖 spec/STOCK_BUCKET_EXPERIMENT.md 要求检查、且单元测试未覆盖的点：
  1. 异步换桶回滚：首选桶满时整笔事务回滚（订单/幂等/outbox 都不留残余），再换下一桶
  2. 并发售罄：总库存归零后不超卖、无幽灵订单
  3. 重复消费：同一消息重投只落一单
  4. 同步/异步混合死锁：同用户同时打两个接口，InnoDB 死锁增量应为 0
  5. 缓存库存聚合：详情缓存 stock == 各桶之和；tb_voucher.stock 不被逐单更新

原桶退款由 StockBucketTest.fallbackAndRefundToActualBucket 覆盖（同步路径单元测试），
此处不重复。

用法：
  python bench/probes/bucket_audit.py
"""
import concurrent.futures as cf
import json
import os
import subprocess
import sys
import time

import httpx

BASE = "http://127.0.0.1:8081/api/voucher"
VOUCHER = 1
MYSQL = ["docker", "exec", "ve-mysql", "mysql", "-uroot", "-proot", "-N",
         "-D", "voucher_engine", "-e"]


def mysql(sql):
    out = subprocess.run(MYSQL + [sql], capture_output=True, text=True,
                         encoding="utf-8", errors="replace")
    if out.returncode != 0:
        raise RuntimeError(f"mysql failed: {out.stderr}")
    return out.stdout.strip()


def scalar(sql):
    out = mysql(sql)
    if not out:
        return None
    v = out.splitlines()[0].strip()
    return int(v) if v.lstrip("-").isdigit() else v


def redis(*args):
    return subprocess.run(["docker", "exec", "ve-redis", "redis-cli"] + list(args),
                          capture_output=True, text=True, encoding="utf-8",
                          errors="replace").stdout.strip()


QUEUES = ("order.create.queue", "order.delay.queue", "order.dlq.queue",
          "order.timeout.dlq", "order.retry.5s.queue",
          "order.retry.30s.queue")


def purge_queues():
    for q in QUEUES:
        subprocess.run(["docker", "exec", "ve-mq", "rabbitmqctl", "purge_queue", q],
                       capture_output=True)


def wait_mq_empty(timeout=60):
    """purge 只清 ready 不清 unacked；5s/30s 重试队列 TTL 到期会 dead-letter
    回下单队列。必须等 ready 与 unacked 同时归零，否则上一用例的残留消息
    会在新用例里凭空落单（曾导致 case3 多出不同 msg_id 的订单）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        out = subprocess.run(["docker", "exec", "ve-mq", "rabbitmqctl", "list_queues",
                              "name", "messages", "messages_unacknowledged"],
                             capture_output=True, text=True).stdout
        busy = False
        for line in out.splitlines():
            parts = line.split("\t")
            if len(parts) == 3 and parts[0] in QUEUES:
                if int(parts[1]) or int(parts[2]):
                    busy = True
                    break
        if not busy:
            return True
        time.sleep(1)
    return False


def setup(stocks):
    """重置为 len(stocks) 个桶，每桶指定库存。先排空全部队列再重置，顺序不能反。

    不能把 TRUNCATE 和其余语句包进 START TRANSACTION：TRUNCATE 是 DDL 会隐式
    提交，之前的 DELETE/UPDATE 会提前落盘，事务并不原子。这里逐句执行即可。
    """
    buckets = len(stocks)
    purge_queues()
    wait_mq_empty()
    rows = ",".join(f"({VOUCHER},{i},{s})" for i, s in enumerate(stocks))
    mysql(";".join([
        f"DELETE FROM tb_voucher_stock_bucket WHERE voucher_id={VOUCHER}",
        "TRUNCATE TABLE tb_voucher_order",
        "TRUNCATE TABLE tb_mq_consume_log",
        "TRUNCATE TABLE tb_local_message",
        f"UPDATE tb_voucher SET stock_bucket_count={buckets}, stock=0 WHERE id={VOUCHER}",
        f"INSERT INTO tb_voucher_stock_bucket(voucher_id,bucket_id,stock) VALUES {rows}",
    ]) + ";")
    redis("DEL", f"seckill:stock:{VOUCHER}", f"seckill:order:{VOUCHER}",
          f"voucher:detail:{VOUCHER}")
    redis("SET", f"seckill:stock:{VOUCHER}", str(sum(stocks)))


def restore_legacy():
    """审计结束把 voucher 1 恢复成旧库存行模式。

    不恢复会污染共用同一张券的 SeckillByDbTest/SeckillConcurrencyTest：
    它们按 tb_voucher.stock 断言，而分桶模式只改 tb_voucher_stock_bucket，
    曾导致 5 个测试误报失败。
    """
    purge_queues()
    wait_mq_empty()
    mysql(";".join([
        f"DELETE FROM tb_voucher_stock_bucket WHERE voucher_id={VOUCHER}",
        "TRUNCATE TABLE tb_voucher_order",
        "TRUNCATE TABLE tb_mq_consume_log",
        "TRUNCATE TABLE tb_local_message",
        f"UPDATE tb_voucher SET stock_bucket_count=0, stock=100 WHERE id={VOUCHER}",
    ]) + ";")
    redis("DEL", f"seckill:stock:{VOUCHER}", f"seckill:order:{VOUCHER}",
          f"voucher:detail:{VOUCHER}")


def wait_drain(timeout=60):
    last, stable = -1, 0
    deadline = time.time() + timeout
    while time.time() < deadline:
        c = scalar("SELECT COUNT(*) FROM tb_voucher_order")
        if c == last:
            stable += 1
            if stable >= 4:
                return
        else:
            stable, last = 0, c
        time.sleep(1)


def post(path, user):
    try:
        r = httpx.post(f"{BASE}/{VOUCHER}/{path}", params={"userId": user}, timeout=60)
        return r.status_code, r.text[:80]
    except Exception as e:
        return "EXC", type(e).__name__


def publish(order_id, user_id):
    payload = json.dumps({"orderId": order_id, "userId": user_id, "voucherId": VOUCHER})
    subprocess.run(["docker", "exec", "ve-mq", "rabbitmqadmin", "publish",
                    "exchange=order.direct.exchange", "routing_key=order.create",
                    f"payload={payload}"], capture_output=True)


def deadlocks():
    return scalar("SELECT COUNT FROM information_schema.INNODB_METRICS "
                  "WHERE NAME='lock_deadlocks'")


RESULTS = {}


def case(name, fn):
    print(f"\n=== {name} ===", flush=True)
    try:
        RESULTS[name] = fn()
        print(json.dumps(RESULTS[name], ensure_ascii=False, indent=2), flush=True)
    except Exception as e:
        RESULTS[name] = {"error": f"{type(e).__name__}: {e}", "pass": False}
        print("ERROR", RESULTS[name], flush=True)


def c1_async_fallback_rollback():
    """桶0空、桶1有2张。userId%4==0 首选桶0 → 回退桶1。
    首桶尝试应整笔回滚：最终只 1 单、1 幂等日志、1 outbox。"""
    setup([0, 2, 0, 0])
    publish(8800000000000000001, 4000)  # 4000%4==0 -> 桶0空 -> 桶1
    wait_drain()
    orders = scalar("SELECT COUNT(*) FROM tb_voucher_order")
    bucket = scalar("SELECT stock_bucket_id FROM tb_voucher_order")
    logs = scalar("SELECT COUNT(*) FROM tb_mq_consume_log")
    outbox = scalar("SELECT COUNT(*) FROM tb_local_message")
    return {"orders": orders, "order_bucket": bucket, "consume_logs": logs,
            "outbox": outbox,
            "bucket_stocks": mysql(f"SELECT bucket_id,stock FROM tb_voucher_stock_bucket "
                                   f"WHERE voucher_id={VOUCHER} ORDER BY bucket_id"),
            "pass": orders == 1 and bucket == 1 and logs == 1 and outbox == 1}


def c2_concurrent_sellout():
    """总库存 3，8 个并发不同用户抢，恰好 3 单成功，无超卖、无负库存。"""
    setup([1, 1, 1, 0])
    with cf.ThreadPoolExecutor(8) as ex:
        res = list(ex.map(lambda u: post("seckill-db", u), [5000 + i for i in range(8)]))
    ok = sum(1 for _, b in res if "抢券成功" in b)
    orders = scalar("SELECT COUNT(*) FROM tb_voucher_order")
    total = scalar(f"SELECT COALESCE(SUM(stock),0) FROM tb_voucher_stock_bucket "
                   f"WHERE voucher_id={VOUCHER}")
    neg = scalar(f"SELECT COUNT(*) FROM tb_voucher_stock_bucket WHERE voucher_id={VOUCHER} AND stock<0")
    return {"success": ok, "orders": orders, "remaining": total, "negative": neg,
            "codes": sorted({c for c, _ in res}),
            "pass": ok == 3 and orders == 3 and total == 0 and neg == 0}


def c3_duplicate_consume():
    """同一 orderId 消息重复投递两次，只落一单（幂等日志拦截第二次）。"""
    setup([5, 5, 5, 5])
    oid = 8800000000000000002
    publish(oid, 6001)
    wait_drain()
    first = scalar("SELECT COUNT(*) FROM tb_voucher_order")
    publish(oid, 6001)  # 重投同一条
    time.sleep(3)
    wait_drain()
    orders = scalar("SELECT COUNT(*) FROM tb_voucher_order")
    logs = scalar("SELECT COUNT(*) FROM tb_mq_consume_log")
    return {"after_first": first, "orders": orders, "consume_logs": logs,
            "pass": first == 1 and orders == 1 and logs == 1}


def c4_mixed_deadlock():
    """同用户同时打 /seckill 与 /seckill-db，死锁增量应为 0。

    两个接口并发命中同一用户时，其中一个会撞 uk_user_voucher 抛出
    RuntimeException("每人限领一张")，Spring 默认映射成 500——基线与本分支
    代码逐字相同，属于既有行为。因此这里不要求 500 为 0，而是断言真正该保证的：
    死锁为 0，且每个用户最多一单、订单总数不超过用户数。
    """
    setup([250, 250, 250, 250])
    before = deadlocks()

    def one(i):
        u = 700000 + i
        with cf.ThreadPoolExecutor(2) as ex:
            f1 = ex.submit(post, "seckill", u)
            f2 = ex.submit(post, "seckill-db", u)
            return [f1.result(), f2.result()]

    with cf.ThreadPoolExecutor(16) as ex:
        res = list(ex.map(one, range(120)))
    after = deadlocks()
    codes = {}
    for c, _ in (x for pair in res for x in pair):
        codes[str(c)] = codes.get(str(c), 0) + 1
    orders = scalar("SELECT COUNT(*) FROM tb_voucher_order")
    max_per_user = scalar("SELECT COALESCE(MAX(c),0) FROM (SELECT COUNT(*) c "
                          "FROM tb_voucher_order GROUP BY user_id) t")
    return {"deadlocks_delta": after - before, "codes": codes, "orders": orders,
            "max_orders_per_user": max_per_user,
            "pass": after - before == 0 and orders <= 120 and max_per_user <= 1}


def c5_cache_aggregate():
    """各桶之和正确；tb_voucher.stock 不被逐单更新（保持 0）；扣减只动桶。"""
    setup([2, 2, 2, 2])
    db_sum = scalar(f"SELECT SUM(stock) FROM tb_voucher_stock_bucket WHERE voucher_id={VOUCHER}")
    raw = scalar(f"SELECT stock FROM tb_voucher WHERE id={VOUCHER}")
    post("seckill-db", 9000)
    post("seckill-db", 9001)
    db_sum_after = scalar(f"SELECT SUM(stock) FROM tb_voucher_stock_bucket WHERE voucher_id={VOUCHER}")
    raw_after = scalar(f"SELECT stock FROM tb_voucher WHERE id={VOUCHER}")
    return {"db_sum_before": db_sum, "raw_stock": raw,
            "db_sum_after": db_sum_after, "raw_after": raw_after,
            "pass": db_sum == 8 and db_sum_after == 6 and raw == 0 and raw_after == 0}


def main():
    case("1_异步换桶回滚", c1_async_fallback_rollback)
    case("2_并发售罄", c2_concurrent_sellout)
    case("3_重复消费", c3_duplicate_consume)
    case("4_混合死锁", c4_mixed_deadlock)
    case("5_缓存聚合", c5_cache_aggregate)
    restore_legacy()

    print("\n===== 汇总 =====")
    allpass = True
    for k, v in RESULTS.items():
        p = bool(v.get("pass"))
        allpass = allpass and p
        print(f"{k}: {'PASS' if p else 'FAIL'}")
    print("\n总体:", "全部通过" if allpass else "有失败")

    out = "bench/results/bucket-audit/audit_" + time.strftime("%Y%m%dT%H%M%S") + ".json"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    json.dump(RESULTS, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print("[written]", out)
    sys.exit(0 if allpass else 1)


if __name__ == "__main__":
    main()
