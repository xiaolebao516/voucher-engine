#!/usr/bin/env bash
# Node 客户端的四组对照矩阵。Node 能打出 ~1900 QPS，不会像 Python 客户端那样
# 在 ~500 QPS 就成为瓶颈，因此这组数据才反映服务端真实能力。
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

# same 模式用 Node 跑没意义（单用户必被业务规则挡住），
# 用户级限流已在 Python 组和 JUnit 里验证过，这里只补 unique 的开关对照。
echo "=========== 限流开启 ==========="
bash bench/restart_app.sh true
python bench/node_matrix.py --label on-unique --sweep 500,1000,3000,5000

echo "=========== 限流关闭 ==========="
bash bench/restart_app.sh false
python bench/node_matrix.py --label off-unique --sweep 500,1000,3000,5000

echo "=========== 恢复默认（限流开启） ==========="
bash bench/restart_app.sh true
echo "[node matrix done]"
