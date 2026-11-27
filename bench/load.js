// 独立压测客户端：用 Node.js 而非 Python。
//
// 为什么需要它：Python 客户端的 GIL 让「到底是服务端慢还是客户端慢」无法区分。
// Node 的单线程事件循环 + 原生异步 HTTP 是另一套实现，若 Node 能打出远高于
// Python 的 QPS，说明瓶颈在 Python 客户端；若两者接近，瓶颈才在服务端。
//
// 用法：
//   node bench/load.js <并发> <总请求数> [seckill|detail]
//   node bench/load.js <并发> <秒数> [seckill|detail] --duration
//
// 为什么需要 --duration 模式：
//   固定请求数会让高并发档位的测试窗口过短。实测同一个 c=500：
//     total=600  → 960 QPS   （窗口 0.62s，几乎全是 ramp-up）
//     total=10000 → 2128 QPS （窗口 4.7s，已进入稳态）
//   差 2.2 倍，全是测量窗口太短造成的假象。做限流开关对照必须用固定时长，
//   让每档都跑够时间进入稳态，否则比的是启动开销而不是限流开销。
const http = require('http');

const CONC = parseInt(process.argv[2] || '500', 10);
const AMOUNT = parseInt(process.argv[3] || '5000', 10);
const MODE = process.argv[4] || 'seckill';
const BY_DURATION = process.argv.includes('--duration');
const PORT = 8081;
const agent = new http.Agent({ keepAlive: true, maxSockets: CONC });

function one(userId, method = 'POST', path = `/api/voucher/1/seckill?userId=${userId}`) {
  return new Promise((resolve) => {
    const t0 = process.hrtime.bigint();
    const req = http.request(
      { host: '127.0.0.1', port: PORT, path, method, agent },
      (res) => {
        let body = '';
        res.on('data', (c) => (body += c));
        res.on('end', () => {
          const ms = Number(process.hrtime.bigint() - t0) / 1e6;
          let cat = 'error';
          if (res.statusCode === 429) cat = 'rate_limited';
          else if (res.statusCode === 503) cat = 'unavailable';
          else if (res.statusCode === 200 && MODE === 'detail') cat = 'success';
          else if (res.statusCode === 200 && body.includes('抢券成功')) cat = 'success';
          else if (res.statusCode === 200) cat = 'biz_rejected';
          resolve({ cat, ms });
        });
      }
    );
    req.on('error', () => resolve({ cat: 'error', ms: Number(process.hrtime.bigint() - t0) / 1e6 }));
    req.end();
  });
}

(async () => {
  // 预热：用 GET 详情页建立 keep-alive 连接。
  // 不能用 POST /seckill 预热 —— 那会产生真实订单，污染本轮统计（实测多出 200 单）。
  await Promise.all(Array.from({ length: Math.min(CONC, 200) }, (_, i) =>
    one(9_000_000 + i, 'GET', '/api/voucher/1')));

  let next = 0;
  const lats = [];
  const cats = {};
  const perSec = [];
  const t0 = process.hrtime.bigint();
  const deadline = BY_DURATION ? Date.now() + AMOUNT * 1000 : Infinity;

  // 每秒请求数时间线：用来把 ramp-up 区间和稳态区间分开看。
  const tick = setInterval(() => {
    perSec.push(next - (perSec.total || 0));
    perSec.total = next;
  }, 1000);

  async function lane(offset) {
    while (true) {
      if (BY_DURATION ? Date.now() >= deadline : next >= AMOUNT) return;
      const i = next++;
      const uid = 50_000_000 + offset * 1_000_000 + i;
      const r = MODE === 'detail'
        ? await one(uid, 'GET', '/api/voucher/1')
        : await one(uid);
      lats.push(r.ms);
      cats[r.cat] = (cats[r.cat] || 0) + 1;
    }
  }

  await Promise.all(Array.from({ length: CONC }, (_, k) => lane(k)));
  clearInterval(tick);

  const elapsed = Number(process.hrtime.bigint() - t0) / 1e9;
  lats.sort((a, b) => a - b);
  const pct = (p) => lats[Math.min(Math.floor(lats.length * p / 100), lats.length - 1)];
  console.log(JSON.stringify({
    concurrency: CONC, total: lats.length, elapsed: +elapsed.toFixed(2),
    qps: +(lats.length / elapsed).toFixed(1),
    p50: +pct(50).toFixed(1), p99: +pct(99).toFixed(1),
    // 稳态 QPS：丢掉前 2 秒的 ramp-up，取剩余区间的平均
    steady_qps: perSec.length > 4
      ? +(perSec.slice(2).reduce((a, b) => a + b, 0) / (perSec.length - 2)).toFixed(1)
      : +(lats.length / elapsed).toFixed(1),
    per_sec: perSec,
    cats,
  }));
  agent.destroy();
})();
