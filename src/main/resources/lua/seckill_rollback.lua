-- KEYS[1]: 库存 key, 例如 seckill:stock:1
-- KEYS[2]: 已购用户集合 key, 例如 seckill:order:1
-- ARGV[1]: 用户 ID

-- 如果用户不在集合里，直接退出，确认在集合里才继续，防止多扣库存导致少卖
if (redis.call('sismember', KEYS[2], ARGV[1]) == 0) then
    return 0
end

-- 移除用户抢券记录，补偿库存
redis.call('srem', KEYS[2], ARGV[1])
redis.call('incr', KEYS[1])
return 1