-- KEYS[1]: seckill:stock:{voucherId}
-- KEYS[2]: seckill:comp:{orderId}  (幂等占位键)
-- ARGV[1]: 占位键过期秒数，例如 86400

-- 1. 库存 Key 不存在（券已下架被删），直接跳过，禁止复活
if (redis.call('EXISTS', KEYS[1]) == 0) then
    return 2
end

-- 2. 抢幂等标记：若该 orderId 已补偿过，直接跳过防重复加库存
if (not redis.call('SET', KEYS[2], '1', 'NX', 'EX', ARGV[1])) then
    return 1
end

-- 3. 原子回补 1 个库存
redis.call('INCR', KEYS[1])
return 0