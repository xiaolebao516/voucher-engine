#!/usr/bin/env bash
# 稳态梯度矩阵：限流开/关 × 500/1000/3000/5000 并发，每档固定 8 秒。
#
# 为什么重做：初版 node_matrix.py 用 total = 1.2 × 并发，高并发档窗口只有
# 1-11 秒且被 ramp-up 主导。只读接口对照显示同一 c=500 在 600 请求时测出
# 960 QPS、10000 请求时测出 2128 QPS —— 差 2.2 倍纯属测量窗口太短。
# 开关对照必须固定时长才能比出限流本身的成本。
set -u
cd "$(dirname "$0")/.."

LOCKFILE=bench/.matrix.lock
if ! mkdir "$LOCKFILE" 2>/dev/null; then
  echo "已有压测在运行（$LOCKFILE 存在）"; exit 1
fi
cleanup() { rmdir "$LOCKFILE" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

APP_COUNT=$(powershell -NoProfile -Command \
  "(Get-CimInstance Win32_Process -Filter \"Name='java.exe'\" | Where-Object { \$_.CommandLine -like '*voucher-engine*.jar*' } | Measure-Object).Count")
if [ "${APP_COUNT:-0}" -ne 1 ]; then
  echo "应用实例数=${APP_COUNT}，应为 1"; exit 1
fi

echo "=========== 限流开启 ==========="
bash bench/restart_app.sh true
python bench/steady_matrix.py --label on-unique --sweep 500,1000,3000,5000

echo "=========== 限流关闭 ==========="
bash bench/restart_app.sh false
python bench/steady_matrix.py --label off-unique --sweep 500,1000,3000,5000

echo "=========== 恢复默认（限流开启） ==========="
bash bench/restart_app.sh true
echo "[steady matrix done]"
