#!/usr/bin/env python3
"""
下单消费者并发对照实验：1 / 2 / 4 个消费者，相同环境、相同请求量、相同库存、限流关闭。

每组重复多次（默认 3）。每次：
  1. 重启应用，只改 seckill.order-consumer.concurrency，其余参数不动。
  2. reset_state：清队列、等在途消费完、truncate 订单/幂等/本地消息表、重置库存。
  3. 记录 MySQL 行锁等待、事务统计、连接数的基线快照。
  4. Node 客户端一次性灌入固定请求数（每请求一个新用户），期间采样 MQ 积压峰值、
     java 进程 CPU、MySQL 活跃连接。
  5. 以 MySQL 订单数连续稳定为排空判据，记录排空耗时。
  6. 落库后校验：无超卖（DB/Redis 库存=初始-订单数且=0）、无重复订单
     （distinct user=订单数）、无丢单（HTTP 成功数=落库数）。

统计口径与 bench/steady_matrix.py 一致：
  order_throughput_qps = 实际落库订单 / (HTTP 注入耗时 + MQ 排空耗时)
  另给出「纯排空区间」的斜率 TPS，即消费者在背压饱和下的稳态速率。

用法：
  python bench/consumer_matrix.py --consumers 1,2,4 --reps 3 --requests 3000
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime

sys.path.insert(0, "bench")
import gradient_bench as gb

JAR = "target/voucher-engine-1.0.0.jar"
PORT = 8081
Q = "order.create.queue"


# ============================ 应用生命周期 ============================

def _ps(cmd):
    out = subprocess.run(["powershell", "-NoProfile", "-Command", cmd],
                         capture_output=True, text=True)
    return out.stdout.strip()


def kill_app():
    _ps("Get-CimInstance Win32_Process -Filter \"Name='java.exe'\" |"
        " Where-Object { $_.CommandLine -like '*voucher-engine*.jar*' } |"
        " ForEach-Object { Stop-Process -Id $_.ProcessId -Force }")
    for _ in range(30):
        if not _port_alive():
            return
        time.sleep(1)


def _port_alive():
    try:
        import httpx
        r = httpx.get(f"http://127.0.0.1:{PORT}/api/voucher/1", timeout=1)
        return r.status_code == 200
    except Exception:
        return False


def start_app(consumers, log_path):
    kill_app()
    time.sleep(2)
    log = open(log_path, "w", encoding="utf-8")
    subprocess.Popen(
        ["java", "-jar", JAR,
         "--seckill.rate-limit.enabled=false",
         f"--seckill.order-consumer.concurrency={consumers}"],
        stdout=log, stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
    for i in range(90):
        if _port_alive():
            time.sleep(3)  # 等 consumer 容器完全注册
            return True
        time.sleep(1)
    raise RuntimeError(f"应用启动超时，见 {log_path}")


def app_pid():
    out = _ps("Get-CimInstance Win32_Process -Filter \"Name='java.exe'\" |"
              " Where-Object { $_.CommandLine -like '*voucher-engine*.jar*' } |"
              " Select-Object -First 1 -ExpandProperty ProcessId")
    return int(out) if out.strip().isdigit() else None


def app_cpu_pct(pid, ms=500):
    """java 进程 CPU，占全部逻辑核心的百分比。

    分母必须用 Stopwatch 实测的墙上时间，不能直接代入名义的 ms：
    PowerShell 的 Start-Sleep 会被调度拖长（实测 300ms 名义窗口可能睡 1.5s），
    用名义值当分母会把 CPU 高估数倍（出现过 >100% 的荒谬读数）。
    """
    ps = ("$sw=[Diagnostics.Stopwatch]::StartNew();"
          "$p=Get-Process -Id {}; $t1=$p.TotalProcessorTime.TotalMilliseconds;"
          "Start-Sleep -Milliseconds {}; $p.Refresh();"
          "$t2=$p.TotalProcessorTime.TotalMilliseconds; $sw.Stop();"
          "$el=[math]::Max($sw.Elapsed.TotalMilliseconds,1);"
          "$cores=(Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors;"
          "[math]::Round(($t2-$t1)/$el*100/$cores,2)").format(pid, ms)
    try:
        return float(_ps(ps))
    except ValueError:
        return None


# ============================ 指标快照 ============================

def mysql_scalar(sql):
    v = gb.mysql(sql).strip()
    return int(v) if v.lstrip("-").isdigit() else v


def status_var(name):
    v = gb.mysql(f"SELECT VARIABLE_VALUE FROM performance_schema.global_status "
                 f"WHERE VARIABLE_NAME='{name}'")
    return int(v)


def tx_summary():
    """performance_schema 事务汇总，只取读写事务（排除只读查询与调度任务）。

    TIMER_WAIT 单位为皮秒。
    """
    out = gb.mysql(
        "SELECT COUNT_READ_WRITE, SUM_TIMER_READ_WRITE, MAX_TIMER_READ_WRITE "
        "FROM performance_schema.events_transactions_summary_global_by_event_name")
    parts = out.split()
    return {"count": int(parts[0]), "sum_ns": int(parts[1]), "max_ns": int(parts[2])}


def reset_tx_summary():
    """清空事务汇总表，让 MAX_TIMER_READ_WRITE 变成「本轮内」的最大值。

    该表是全局累计的，MAX 列是 MySQL 实例生命周期内的高水位，不随轮次重置；
    之前几轮的死锁/锁等待探针留下的长事务会让后面所有轮次都读到同一个假值。
    清空后 base 快照为 0，after 快照的 MAX 即本轮真实最大值。
    """
    gb.mysql("TRUNCATE TABLE "
             "performance_schema.events_transactions_summary_global_by_event_name;")


# 关键 SQL 的 digest 指纹。DIGEST_TEXT 里值被替换成 ?，可以直接按前缀匹配。
KEY_STATEMENTS = {
    "deduct_stock": "UPDATE `tb_voucher` SET `stock` = `stock` - ? , `version` = `version` + ? WHERE `id` = ? AND `stock` > ?",
    "deduct_bucket": "UPDATE `tb_voucher_stock_bucket` SET `stock` = `stock` - ?",
    "insert_order": "INSERT INTO `tb_voucher_order`",
    # 分桶模式订单多一列 stock_bucket_id，与旧模板前缀相同、只在列清单尾部不同。
    # 必须用更长的前缀区分，否则 match_stmt 会选到历史计数最大的旧模板而恒为 0。
    "insert_order_bucket": "INSERT INTO `tb_voucher_order` ( `id` , `user_id` , `voucher_id` , `stock_bucket_id`",
    "insert_consume_log": "INSERT INTO `tb_mq_consume_log`",
    "insert_local_msg": "INSERT INTO `tb_local_message`",
}


def stmt_digest():
    """按 SQL 模板取语句级统计。

    比事务汇总更适合定位瓶颈：事务汇总把每条 autocommit 语句也算一个事务，
    订单事务和 afterCommit 里的 markSent 混在一起，均值被稀释。
    performance_schema 的 TIMER_WAIT / LOCK_TIME 单位都是皮秒。

    用 HEX(DIGEST) 作 key：DIGEST_TEXT 只是 SQL 模板的可读文本，不同模板的
    前 60 字符可能完全相同（例如 INSERT 时非空列不同，列清单的差异在更后面），
    若用截断文本作 key 会互相覆盖，导致整列统计变成 0。
    """
    out = gb.mysql(
        "SELECT HEX(DIGEST), ANY_VALUE(DIGEST_TEXT), SUM(COUNT_STAR), "
        "SUM(SUM_TIMER_WAIT), MAX(MAX_TIMER_WAIT), SUM(SUM_LOCK_TIME), "
        "SUM(SUM_ROWS_AFFECTED) "
        "FROM performance_schema.events_statements_summary_by_digest "
        "WHERE SCHEMA_NAME='voucher_engine' GROUP BY DIGEST")
    rows = {}
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) < 7:
            continue
        digest, text, cnt, sum_ns, max_ns, lock_ns, rows_aff = parts[:7]
        # MAX_TIMER_WAIT 是无符号列，MySQL 里存在下溢时会出现 2^64 量级的假值
        mx = int(max_ns)
        rows[digest] = {"text": text, "count": int(cnt), "sum_ns": int(sum_ns),
                        "max_ns": mx if mx < 2 ** 63 else None,
                        "lock_ns": int(lock_ns),
                        "rows_affected": int(rows_aff)}
    return rows


def match_stmt(snapshot_rows, key):
    """按 SQL 模板前缀取语句统计。

    同一条业务 SQL 可能有多条 digest：列组合不同（字段为 null 时 MyBatis-Plus
    会少拼列）或探针脚本手写的精简语句，都会各占一行。必须取 COUNT_STAR
    最大的主变体，否则会随机命中探针留下的个位数行。
    """
    # 用完整模板前缀匹配：不再截到 55 字符，否则形如 stock_bucket_id 的尾部区分点
    # 会被抹掉，导致分桶订单模板和旧订单模板互相串号。
    prefix = KEY_STATEMENTS[key]
    hits = [v for v in snapshot_rows.values() if v["text"].startswith(prefix)]
    if not hits:
        return None
    return max(hits, key=lambda v: v["count"])


def active_db_conns():
    return mysql_scalar(
        "SELECT COUNT(*) FROM information_schema.PROCESSLIST "
        "WHERE DB='voucher_engine'")


def busy_db_conns():
    """非 Sleep 连接数，近似 Hikari 正在被借出的连接数（项目未引 actuator，无池内指标）。"""
    return mysql_scalar(
        "SELECT COUNT(*) FROM information_schema.PROCESSLIST "
        "WHERE DB='voucher_engine' AND COMMAND<>'Sleep'")


def threads_running():
    return status_var("Threads_running")


def queue_depth():
    import httpx
    d = httpx.get(f"http://127.0.0.1:15672/api/queues/%2F/{Q}",
                  auth=("guest", "guest"), timeout=5).json()
    return d.get("messages", 0), d.get("messages_unacknowledged", 0), d.get("consumers", 0)


def snapshot():
    return {
        "row_lock_waits": status_var("Innodb_row_lock_waits"),
        "row_lock_time_ms": round(status_var("Innodb_row_lock_time"), 1),
        "tx": tx_summary(),
        "stmt": stmt_digest(),
        "max_used_conns": status_var("Max_used_connections"),
    }


# ============================ 单轮实验 ============================

def _drain_to_stable(timeout=300, need=5):
    """等 MySQL 订单数连续 need 次不变，返回最后变化时刻。"""
    last, stable, last_change = -1, 0, time.time()
    deadline = time.time() + timeout
    while time.time() < deadline:
        cnt = mysql_scalar("SELECT COUNT(*) FROM tb_voucher_order")
        if cnt == last:
            stable += 1
            if stable >= need:
                return last_change
        else:
            stable, last, last_change = 0, cnt, time.time()
        time.sleep(2)
    return last_change


def reset_bucket_state(stock, buckets):
    # 仅隔离压测库：在已排空、无外部流量时重置；原有 reset_state 本身会清理订单。
    gb.reset_state(stock)
    statements = ["START TRANSACTION", f"DELETE FROM tb_voucher_stock_bucket WHERE voucher_id={gb.VOUCHER_ID}",
                  f"UPDATE tb_voucher SET stock_bucket_count={buckets}, stock={0 if buckets else stock} WHERE id={gb.VOUCHER_ID}"]
    if buckets:
        rows = ','.join(f'({gb.VOUCHER_ID},{i},{stock // buckets + (i < stock % buckets)})' for i in range(buckets))
        statements.append("INSERT INTO tb_voucher_stock_bucket(voucher_id,bucket_id,stock) VALUES " + rows)
    statements.append("COMMIT")
    gb.mysql(';'.join(statements) + ';')


def run_once(consumers, requests, stock, buckets=0):
    log_path = f"bench/logs/app-c{consumers}-{datetime.now():%H%M%S}.log"
    start_app(consumers, log_path)
    pid = app_pid()

    # 冷 JIT 预热：每轮都重启了应用，热点方法还没被 C2 编译，直接测会让
    # 首轮排空 TPS 偏低。先灌一小批并等排空，结果丢弃，再开始正式测量。
    warm = max(200, min(500, requests // 4))
    reset_bucket_state(stock, buckets)
    subprocess.run(["node", "bench/load.js", "200", str(warm), "seckill"],
                   capture_output=True, timeout=600)
    _drain_to_stable(timeout=300)
    print(f"[warmup] 消费者={consumers} 预热 {warm} 单完成", flush=True)

    reset_bucket_state(stock, buckets)
    # reset_state 不清理 order.delay.queue：上一轮积压的延迟消息 15 分钟后会
    # 转入 order.timeout.dlq 并触发退库存、把订单状态改成 3，跨轮污染库存校验。
    # 这里主动清空延迟队列与本地消息表。
    subprocess.run(["docker", "exec", "ve-mq", "rabbitmqctl",
                    "purge_queue", "order.delay.queue"],
                   capture_output=True, timeout=30)
    gb.mysql("TRUNCATE TABLE tb_local_message;")
    time.sleep(1)

    q0 = queue_depth()
    if q0[0] != 0 or q0[1] != 0:
        print(f"[warn] 起始队列非空 ready={q0[0]} unacked={q0[1]}", flush=True)

    # 清空事务汇总，让本轮 tx_max_ms 是本轮真实最大值而非实例生命周期高水位。
    reset_tx_summary()
    base = snapshot()
    orders0 = mysql_scalar("SELECT COUNT(*) FROM tb_voucher_order")

    # ---- 采样器：MQ 峰值 + CPU + 连接池使用 ----
    peak = {"ready": 0, "unacked": 0, "cpu": 0.0, "conns": 0,
            "busy_conns": 0, "threads_running": 0}
    stop = threading.Event()

    def sampler():
        while not stop.is_set():
            try:
                r, u, _ = queue_depth()
                peak["ready"] = max(peak["ready"], r)
                peak["unacked"] = max(peak["unacked"], u)
            except Exception:
                pass
            try:
                c = app_cpu_pct(pid, 300)
                if c is not None:
                    peak["cpu"] = max(peak["cpu"], c)
            except Exception:
                pass
            try:
                peak["conns"] = max(peak["conns"], active_db_conns())
                peak["busy_conns"] = max(peak["busy_conns"], busy_db_conns())
                peak["threads_running"] = max(peak["threads_running"], threads_running())
            except Exception:
                pass

    th = threading.Thread(target=sampler, daemon=True)
    th.start()

    # ---- 注入 ----
    t0 = time.time()
    out = subprocess.run(["node", "bench/load.js", str(200), str(requests), "seckill"],
                         capture_output=True, text=True, encoding="utf-8",
                         errors="replace", timeout=1200)
    res = json.loads(out.stdout.strip().splitlines()[-1])
    inject_sec = time.time() - t0

    # ---- 排空：订单数连续 5 次不变 ----
    drain_start = time.time()
    orders_at_drain_start = mysql_scalar("SELECT COUNT(*) FROM tb_voucher_order")
    last, stable, last_change = -1, 0, drain_start
    deadline = time.time() + 600
    while time.time() < deadline:
        cnt = mysql_scalar("SELECT COUNT(*) FROM tb_voucher_order")
        if cnt == last:
            stable += 1
            if stable >= 5:
                break
        else:
            stable, last, last_change = 0, cnt, time.time()
        time.sleep(2)
    stop.set()
    th.join(timeout=10)
    drain_sec = max(last_change - drain_start, 0)
    orders = mysql_scalar("SELECT COUNT(*) FROM tb_voucher_order") - orders0
    # 纯消费者排空 TPS：只算注入结束后新增的落库量，剔除注入期已落库部分。
    drained_orders = mysql_scalar("SELECT COUNT(*) FROM tb_voucher_order") - orders_at_drain_start
    drain_only_tps = round(drained_orders / drain_sec, 2) if drain_sec else None

    # ---- 一致性校验 ----
    db_stock = mysql_scalar(f"SELECT COALESCE(SUM(stock),0) FROM tb_voucher_stock_bucket WHERE voucher_id={gb.VOUCHER_ID}") if buckets else mysql_scalar(f"SELECT stock FROM tb_voucher WHERE id={gb.VOUCHER_ID}")
    invalid_bucket_orders = mysql_scalar("SELECT COUNT(*) FROM tb_voucher_order o LEFT JOIN tb_voucher_stock_bucket b ON b.voucher_id=o.voucher_id AND b.bucket_id=o.stock_bucket_id WHERE o.stock_bucket_id IS NULL OR b.bucket_id IS NULL") if buckets else 0
    negative_buckets = mysql_scalar(f"SELECT COUNT(*) FROM tb_voucher_stock_bucket WHERE voucher_id={gb.VOUCHER_ID} AND stock<0") if buckets else 0
    bucket_rows = mysql_scalar(f"SELECT COUNT(*) FROM tb_voucher_stock_bucket WHERE voucher_id={gb.VOUCHER_ID}")
    redis_stock = int(gb.redis("GET", f"seckill:stock:{gb.VOUCHER_ID}"))
    distinct_users = mysql_scalar("SELECT COUNT(DISTINCT user_id) FROM tb_voucher_order")
    dup_pairs = mysql_scalar(
        "SELECT COUNT(*) FROM (SELECT user_id, voucher_id, COUNT(*) c "
        "FROM tb_voucher_order GROUP BY user_id, voucher_id HAVING c>1) t")
    http_success = res["cats"].get("success", 0)
    consume_log = mysql_scalar("SELECT COUNT(*) FROM tb_mq_consume_log")

    after = snapshot()

    def delta(a, b):
        return b - a

    tx_cnt = delta(base["tx"]["count"], after["tx"]["count"])
    tx_sum = delta(base["tx"]["sum_ns"], after["tx"]["sum_ns"])
    # performance_schema 的 TIMER_WAIT 单位是皮秒（picoseconds），换算成毫秒要除 1e9
    avg_tx_ms = round(tx_sum / tx_cnt / 1e9, 3) if tx_cnt else None
    max_tx_ms = round(after["tx"]["max_ns"] / 1e9, 1)
    rl_waits = delta(base["row_lock_waits"], after["row_lock_waits"])
    rl_time_ms = round(delta(base["row_lock_time_ms"], after["row_lock_time_ms"]), 1)

    # 语句级拆解：定位「时间到底花在扣库存还是插入订单」
    stmt_stats = {}
    for key in KEY_STATEMENTS:
        b = match_stmt(base["stmt"], key)
        a = match_stmt(after["stmt"], key)
        if b is None or a is None:
            stmt_stats[key] = None
            continue
        cnt = a["count"] - b["count"]
        stmt_stats[key] = {
            "count": cnt,
            "avg_ms": round((a["sum_ns"] - b["sum_ns"]) / cnt / 1e9, 3) if cnt else None,
            "lock_ms": round((a["lock_ns"] - b["lock_ns"]) / 1e9, 1),
        }

    # ---- 桶间库存分布与回退重试开销 ----
    # 每桶剩余量与每桶落单量，用于观察 floorMod 首选桶是否偏斜。
    bucket_dist = None
    fallback = None
    if buckets:
        dist_rows = gb.mysql(
            f"SELECT b.bucket_id, b.stock, COALESCE(o.cnt,0) FROM "
            f"(SELECT bucket_id, stock FROM tb_voucher_stock_bucket WHERE voucher_id={gb.VOUCHER_ID}) b "
            f"LEFT JOIN (SELECT stock_bucket_id, COUNT(*) cnt FROM tb_voucher_order "
            f"WHERE voucher_id={gb.VOUCHER_ID} GROUP BY stock_bucket_id) o "
            f"ON o.stock_bucket_id=b.bucket_id ORDER BY b.bucket_id").splitlines()
        bucket_dist = [{"bucket": int(r.split("\t")[0]), "remaining": int(r.split("\t")[1]),
                        "orders": int(r.split("\t")[2])} for r in dist_rows if r.strip()]
        # deduct_bucket 执行次数 / 落库单数 = 平均每单尝试桶次数。
        # 1.0 表示零回退；>1 即首选桶被撞空、整笔回滚换桶的开销。
        # 注意 performance_schema 在高并发下会少计，该比值只作趋势参考，
        # 精确回退量以 fallback_orders_db（DB 侧订单落桶与首选桶比对）为准。
        attempts = (stmt_stats.get("deduct_bucket") or {}).get("count") or 0
        fallback_orders_db = mysql_scalar(
            f"SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id={gb.VOUCHER_ID} "
            f"AND stock_bucket_id IS NOT NULL AND stock_bucket_id <> MOD(user_id, {buckets})")
        fallback = {
            "deduct_bucket_attempts": attempts,
            "orders": orders,
            "attempts_per_order": round(attempts / orders, 4) if orders else None,
            "deduct_bucket_lock_ms": (stmt_stats.get("deduct_bucket") or {}).get("lock_ms"),
            "fallback_orders_db": fallback_orders_db,
        }

    total_time = inject_sec + drain_sec
    report = {
        "consumers": consumers,
        "buckets": buckets,
        "requests": requests,
        "stock": stock,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "inject_sec": round(inject_sec, 2),
        "drain_sec": round(drain_sec, 2),
        "total_sec": round(total_time, 2),
        "http_cats": res["cats"],
        "http_success": http_success,
        "orders_persisted": orders,
        "order_throughput_qps": round(orders / total_time, 2) if total_time else 0,
        "drain_tps": round(orders / drain_sec, 2) if drain_sec else None,
        "drained_orders": drained_orders,
        "drain_only_tps": drain_only_tps,
        "mq_peak_ready": peak["ready"],
        "mq_peak_unacked": peak["unacked"],
        "cpu_max_pct_all_cores": round(peak["cpu"], 2),
        "db_conns_peak": peak["conns"],
        "db_busy_conns_peak": peak["busy_conns"],
        "mysql_threads_running_peak": peak["threads_running"],
        "row_lock_waits_delta": rl_waits,
        "row_lock_time_ms_delta": rl_time_ms,
        "stmt_stats": stmt_stats,
        "tx_count_delta": tx_cnt,
        "tx_avg_ms": avg_tx_ms,
        "tx_max_ms": max_tx_ms,
        "bucket_dist": bucket_dist,
        "fallback": fallback,
        # ---- 一致性校验 ----
        "check": {
            "db_stock": db_stock,
            "redis_stock": redis_stock,
            "bucket_rows": bucket_rows,
            "invalid_bucket_orders": invalid_bucket_orders,
            "negative_buckets": negative_buckets,
            "bucket_integrity": bucket_rows == buckets and invalid_bucket_orders == 0 and negative_buckets == 0,
            "distinct_users": distinct_users,
            "duplicate_pairs": dup_pairs,
            "consume_log_rows": consume_log,
            "expected_stock": stock - orders,
            "no_oversell": db_stock >= 0 and redis_stock >= 0
                           and db_stock == stock - orders and redis_stock == db_stock,
            "no_duplicate_orders": dup_pairs == 0 and distinct_users == orders,
            "no_lost_orders": http_success == orders and consume_log == orders,
        },
    }
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--consumers", default="1,2,4")
    ap.add_argument("--buckets", type=int, choices=[0, 1, 4, 8, 16], default=0,
                    help="0=旧库存行；1/4/8/16=分桶（仅隔离压测数据库）")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--requests", type=int, default=3000)
    ap.add_argument("--stock", type=int, default=None,
                    help="默认等于 requests，便于用库存归零做超卖校验")
    ap.add_argument("--out-dir", default="bench/results/consumer-matrix")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    stock = args.stock if args.stock is not None else args.requests
    groups = [int(x) for x in args.consumers.split(",")]

    all_reports = []
    for c in groups:
        for rep in range(1, args.reps + 1):
            print(f"\n=== 消费者={c} 第{rep}/{args.reps}次 请求={args.requests} 库存={stock} ===",
                  flush=True)
            r = run_once(c, args.requests, stock, args.buckets)
            r["rep"] = rep
            all_reports.append(r)
            print(json.dumps({k: r[k] for k in
                              ("consumers", "rep", "inject_sec", "drain_sec",
                               "orders_persisted", "order_throughput_qps", "drain_tps",
                               "drained_orders", "drain_only_tps",
                               "mq_peak_ready", "row_lock_waits_delta", "tx_avg_ms",
                               "cpu_max_pct_all_cores", "db_conns_peak",
                               "db_busy_conns_peak", "stmt_stats", "bucket_dist",
                               "fallback", "check")},
                             ensure_ascii=False), flush=True)

    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    out = os.path.join(args.out_dir, f"consumer_matrix_{ts}.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump({"stock": stock, "requests": args.requests, "buckets": args.buckets,
                   "reps": args.reps, "reports": all_reports}, f,
                  ensure_ascii=False, indent=2)
    print(f"\n[written] {out}")

    # 汇总
    print(f"\n{'消费者':>6}{'次数':>6}{'注入s':>9}{'排空s':>9}{'落库单':>8}"
          f"{'落库TPS':>10}{'排空TPS':>10}{'行锁等待':>10}{'事务均值ms':>12}"
          f"{'CPU峰值%':>10}{'连接峰值':>10}")
    for c in groups:
        rs = [r for r in all_reports if r["consumers"] == c]
        n = len(rs)
        avg = lambda k: round(sum(r[k] for r in rs) / n, 2) if n else 0
        print(f"{c:>6}{n:>6}{avg('inject_sec'):>9.2f}{avg('drain_sec'):>9.2f}"
              f"{avg('orders_persisted'):>8.0f}{avg('order_throughput_qps'):>10.2f}"
              f"{avg('drain_tps'):>10.2f}{avg('row_lock_waits_delta'):>10.0f}"
              f"{avg('tx_avg_ms'):>12.3f}{avg('cpu_max_pct_all_cores'):>10.2f}"
              f"{avg('db_conns_peak'):>10.0f}")

    bad = [r for r in all_reports
           if not all(r["check"][k] for k in
                      ("no_oversell", "no_duplicate_orders", "no_lost_orders"))]
    print(f"\n一致性校验：{'全部通过' if not bad else f'{len(bad)} 轮未通过'}")
    for r in bad:
        print("  FAIL", r["consumers"], "rep", r["rep"], r["check"])


if __name__ == "__main__":
    main()
