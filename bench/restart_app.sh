#!/usr/bin/env bash
# 重启应用并指定限流开关。压测对照实验需要切换 enabled 状态。
#
# 注意：只杀「jar 包」这个 java 进程，绝不能按内存大小杀所有 java ——
# 本机同时跑着 IntelliJ IDEA（也是 java，Xmx 2048m），按大小杀会把 IDE 一起干掉。
set -e
cd "$(dirname "$0")/.."

ENABLED="${1:-true}"
LOG="bench/logs/app-limit-${ENABLED}.log"
mkdir -p bench/logs

echo "[stop] 停止 voucher-engine jar 进程"
if [ -f bench/app.pid ]; then
  kill "$(cat bench/app.pid)" 2>/dev/null || true
  rm -f bench/app.pid
fi
# 精确匹配命令行里带 voucher-engine-*.jar 的 java 进程
powershell -NoProfile -Command "
  Get-CimInstance Win32_Process -Filter \"Name='java.exe'\" |
    Where-Object { \$_.CommandLine -like '*voucher-engine*.jar*' } |
    ForEach-Object { Stop-Process -Id \$_.ProcessId -Force }
" 2>/dev/null || true

for i in $(seq 1 30); do
  if ! curl -s -o /dev/null --max-time 1 http://127.0.0.1:8081/api/voucher/1; then break; fi
  sleep 1
done
sleep 2

echo "[start] 限流 enabled=${ENABLED}"
nohup java -jar target/voucher-engine-1.0.0.jar \
  --seckill.rate-limit.enabled="${ENABLED}" > "$LOG" 2>&1 &
echo $! > bench/app.pid
echo "PID=$(cat bench/app.pid) LOG=$LOG"

echo "[wait] 等待端口就绪"
for i in $(seq 1 60); do
  code=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8081/api/voucher/1 2>/dev/null || echo 000)
  if [ "$code" = "200" ]; then echo "ready after ${i}s"; exit 0; fi
  sleep 1
done
echo "启动超时"; tail -20 "$LOG"; exit 1
