-- KEYS[1]: 库存的 Key，例如 "seckill:stock:1"
-- KEYS[2]: 已抢购用户集合的 Key，例如 "seckill:order:1"
-- ARGV[1]: 当前抢券的用户 ID，例如 "1001"

-- 1. 取出库存并转成数字，如果为空或 <= 0，返回 1（代表库存不足）
local stock = tonumber(redis.call('get', KEYS[1]))
if (stock == nil or stock <= 0) then
    return 1
end

-- 2. 判断当前用户 ID 是否已经在 Set 集合里，返回 1 表示已存在，则脚本返回 2（代表重复下单）
if (redis.call('sismember', KEYS[2], ARGV[1]) == 1) then
    return 2
end

-- 3. 校验全通过！原子扣减库存 1，并把用户 ID 塞进 Set 集合
redis.call('incrby', KEYS[1], -1)
redis.call('sadd', KEYS[2], ARGV[1])

-- 4. 返回 0 代表抢券成功
return 0