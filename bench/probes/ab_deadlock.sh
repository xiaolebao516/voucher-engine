#!/bin/sh
# 同一用户同时打 /seckill（异步）与 /seckill-db（同步）两个接口，
# 统计 InnoDB 死锁增量，验证两条路径加锁顺序是否一致。
# 用法: sh bench/probes/ab_deadlock.sh <jar> <label> [轮数]
JAR=$1; LABEL=$2; N=${3:-200}
cd "$(dirname "$0")/../.." || exit 1

powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='java.exe'\" | Where-Object { \$_.CommandLine -match 'lockorder|voucher-engine.*\.jar' } | ForEach-Object { Stop-Process -Id \$_.ProcessId -Force }" >/dev/null 2>&1
for i in $(seq 1 30); do
  curl -s -o /dev/null -m 1 "http://127.0.0.1:8081/api/voucher/1" 2>/dev/null || break
  sleep 1
done
sleep 1

docker exec ve-mysql mysql -uroot -proot -D voucher_engine -e "UPDATE tb_voucher SET stock=1000 WHERE id=1; TRUNCATE TABLE tb_voucher_order; TRUNCATE TABLE tb_mq_consume_log; TRUNCATE TABLE tb_local_message;" 2>/dev/null
docker exec ve-redis redis-cli SET "seckill:stock:1" 1000 >/dev/null 2>&1
docker exec ve-redis redis-cli DEL "seckill:order:1" >/dev/null 2>&1
for q in order.create.queue order.delay.queue order.dlq.queue order.timeout.dlq; do docker exec ve-mq rabbitmqctl purge_queue $q >/dev/null 2>&1; done
sleep 2

BEFORE=$(docker exec ve-mysql mysql -uroot -proot -N -e "SELECT COUNT FROM information_schema.INNODB_METRICS WHERE NAME='lock_deadlocks';" 2>/dev/null)
nohup java -jar "$JAR" --seckill.rate-limit.enabled=false --seckill.order-consumer.concurrency=4 > "bench/logs/ab-${LABEL}.log" 2>&1 &
for i in $(seq 1 90); do
  if curl -s -o /dev/null -m 1 -w "%{http_code}" "http://127.0.0.1:8081/api/voucher/1" 2>/dev/null | grep -q 200; then break; fi
  sleep 1
done
sleep 3
RUNNING=$(powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='java.exe'\" | Where-Object { \$_.CommandLine -match 'lockorder|voucher-engine.*\.jar' } | Select-Object -First 1 -ExpandProperty CommandLine" 2>/dev/null | tr -d '\r')
echo "[$LABEL] running=$(echo $RUNNING | grep -o '[^ ]*lockorder[^ ]*')"

python bench/probes/mixed_lockorder_probe.py $N
AFTER=$(docker exec ve-mysql mysql -uroot -proot -N -e "SELECT COUNT FROM information_schema.INNODB_METRICS WHERE NAME='lock_deadlocks';" 2>/dev/null)
echo "[$LABEL] deadlocks before=$BEFORE after=$AFTER delta=$((AFTER-BEFORE))"
