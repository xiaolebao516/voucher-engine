import httpx, concurrent.futures as cf, time, sys

BASE = "http://127.0.0.1:8081/api/voucher/1"
USER_BASE = 700_000

def hit(path, user):
    try:
        r = httpx.post(f"{BASE}/{path}", params={"userId": user}, timeout=60)
        return (path, r.status_code, (r.text or "")[:60])
    except Exception as e:
        return (path, "EXC", type(e).__name__)

def roundtrip(i):
    # 同一用户同时打异步和同步接口：两条路径都操作同一 (user, voucher)
    u = USER_BASE + i
    with cf.ThreadPoolExecutor(2) as ex:
        f1 = ex.submit(hit, "seckill", u)
        f2 = ex.submit(hit, "seckill-db", u)
        return [f1.result(), f2.result()]

N = int(sys.argv[1]) if len(sys.argv) > 1 else 300
t0 = time.time()
res = []
with cf.ThreadPoolExecutor(16) as ex:
    for r in ex.map(roundtrip, range(N)):
        res.extend(r)
print(f"requests={len(res)} elapsed={time.time()-t0:.1f}s")
from collections import Counter
print("status:", Counter((p, s) for p, s, _ in res))
