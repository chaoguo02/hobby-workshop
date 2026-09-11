local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]

local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId
local admittedKey = 'seckill:admitted:' .. voucherId
local closedKey = 'seckill:closed:' .. voucherId

if redis.call('hexists', closedKey, orderId) == 1 then
    return 0
end

if redis.call('exists', stockKey) == 1 then
    redis.call('incrby', stockKey, 1)
end
redis.call('srem', orderKey, userId)
redis.call('hdel', admittedKey, orderId)
redis.call('hset', closedKey, orderId, 1)

local stockTtl = redis.call('ttl', stockKey)
if stockTtl > 0 then
    redis.call('expire', closedKey, stockTtl)
else
    -- 库存 key 过期或尚未初始化时也要限制幂等标记寿命，避免永久占用内存。
    redis.call('expire', closedKey, 604800)
end
return 1
