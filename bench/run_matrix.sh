#!/usr/bin/env bash
# 四组对照压测矩阵：限流 开/关 × 用户模式 unique/same，梯度 500/1000/3000/5000。
# 组间切换 enabled 需要重启应用，故串联执行，中途不人工介入。
set -u
cd "$(dirname "$0")/.."

SWEEP=500,1000,3000,5000
WORKERS=8
LOCKFILE=bench/.matrix.lock

# 互斥锁：压测会 TRUNCATE 表、清 Redis 库存、purge 队列。
# 一旦有两份 run_matrix.sh 同时跑（比如上一轮没杀干净），
# 它们会互相清掉对方的数据，所有统计全部失真 —— 实测踩过一次。
if ! mkdir "$LOCKFILE" 2>/dev/null; then
  echo "已有压测在运行（$LOCKFILE 存在）。如果确认没有，手动删掉该目录后重试。"
  exit 1
fi
cleanup() { rmdir "$LOCKFILE" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

# 校验只有一个应用实例：多实例会抢同一个 MQ 队列和库存，数据不可信。
APP_COUNT=$(powershell -NoProfile -Command \
  "(Get-CimInstance Win32_Process -Filter \"Name='java.exe'\" | Where-Object { \$_.CommandLine -like '*voucher-engine*.jar*' } | Measure-Object).Count")
if [ "${APP_COUNT:-0}" -ne 1 ]; then
  echo "应用实例数=${APP_COUNT}，应为 1。请先清理多余实例再压测。"
  exit 1
fi

run_group () {
  local label="$1" mode="$2"
  echo ""
  echo "############################################################"
  echo "## 组: ${label}  (user-mode=${mode})"
  echo "############################################################"
  python bench/gradient_bench.py --label "${label}" --sweep "${SWEEP}" \
    --workers "${WORKERS}" --user-mode "${mode}"
}

echo "=========== 限流开启 ==========="
bash bench/restart_app.sh true
run_group "on-unique" unique
run_group "on-same"   same

echo "=========== 限流关闭 ==========="
bash bench/restart_app.sh false
run_group "off-unique" unique
run_group "off-same"   same

echo "=========== 恢复默认配置（限流开启） ==========="
bash bench/restart_app.sh true
echo "[matrix done]"
ls -la bench/results/
