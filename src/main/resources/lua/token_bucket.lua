-- KEYS[1]: 用户令牌桶
-- KEYS[2]: 全局令牌桶
-- ARGV[1]: 用户桶容量
-- ARGV[2]: 用户补充速率
-- ARGV[3]: 全局桶容量
-- ARGV[4]: 全局补充速率

local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000
          + math.floor(tonumber(time[2]) / 1000)

local function tryAcquire(key, capacity, rate)
    local tokens = tonumber(redis.call('HGET', key, 'tokens'))
    local lastTime = tonumber(redis.call('HGET', key, 'lastTime'))

    if tokens == nil or lastTime == nil then
        tokens = capacity
        lastTime = now
    end

    local currentTime = math.max(now, lastTime)

    tokens = math.min(capacity,
        tokens + (currentTime - lastTime) / 1000 * rate)

    local allowed = false
    if tokens >= 1 then
        tokens = tokens - 1
        allowed = true
    end

    redis.call('HSET', key,
        'tokens', tokens,
        'lastTime', currentTime)

    redis.call('EXPIRE', key,
        math.ceil(capacity / rate) + 1)

    return allowed
end

-- 用户级限流
if not tryAcquire(KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2])) then
    return 0
end

-- 全局限流
if not tryAcquire(KEYS[2], tonumber(ARGV[3]), tonumber(ARGV[4])) then
    return 1
end

return 2