-- 1. 参数列表
-- 优惠券id
local voucherId = ARGV[1]
-- 用户id
local userId = ARGV[2]
-- 订单id
local orderId = ARGV[3]

-- 2.数据key
-- 库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 订单key
local orderKey = 'seckill:order:' .. voucherId

-- 3.脚本业务
-- 库存 key 不存在（活动未初始化或已过期被清理）时 redis.call('get') 返回 false，
-- 直接 tonumber 会得到 nil 并让 `nil <= 0` 抛错，因此先判 false。
local stock = redis.call('get', stockKey)
if (stock == false or tonumber(stock) <= 0) then
    return 1
end
if(redis.call('sismember', orderKey, userId) == 1) then
    return 2
end

redis.call('incrby', stockKey, -1)
redis.call('sadd', orderKey, userId)
-- 记录已准入的 orderId -> userId，作为 MySQL 补单的唯一依据（Redis 为真值，不回滚）
-- 与扣库存、SADD 同脚本原子完成，保证「准入必可被补齐」
redis.call('hset', 'seckill:admitted:' .. voucherId, orderId, userId)
-- 让一人一单集合跟随库存 key 的生命周期过期，避免该集合随用户数无限增长。
-- 库存 key 在活动结束（endTime + 缓冲）后过期，orderKey 在此之后不再被写入，故 TTL 只会随活动临近而缩短。
local stockTtl = redis.call('ttl', stockKey)
if (stockTtl > 0) then
    redis.call('expire', orderKey, stockTtl)
end
return 0