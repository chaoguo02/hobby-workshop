-- 令牌桶限流：原子完成「按时间补充令牌 -> 判断 -> 扣减」
-- KEYS[1] 桶 key，例如 rate:limit:sms:send:13800000000
-- ARGV[1] capacity  桶容量，决定允许的瞬时突发次数
-- ARGV[2] rate      每秒补充的令牌数，决定长期平均速率
-- ARGV[3] now       当前毫秒时间戳（应用传入，多实例口径一致）
-- ARGV[4] requested 本次消耗的令牌数，通常为 1
-- 返回 { allowed, remaining }：allowed=1 放行 / 0 拒绝；remaining 为剩余令牌数（向下取整）

local key       = KEYS[1]
local capacity  = tonumber(ARGV[1])
local rate      = tonumber(ARGV[2])
local now       = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

if capacity == nil or rate == nil or requested == nil
        or capacity <= 0 or rate <= 0 or requested <= 0 then
    return { 0, 0 }                       -- 配置非法，fail closed
end

local bucket = redis.call('hmget', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts     = tonumber(bucket[2])

if tokens == nil then
    tokens = capacity                     -- 首次访问：桶是满的，允许一段突发
    ts = now
end

-- 按流逝时间线性补充；max(0, ...) 防止时钟回拨时凭空多出令牌
local elapsed = math.max(0, now - ts) / 1000.0
tokens = math.min(capacity, tokens + elapsed * rate)

local allowed = 0
if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
end

redis.call('hset', key, 'tokens', tokens, 'ts', now)
-- 桶装满所需时间 + 60s 缓冲，空闲后自动回收，避免 key 无限增长
redis.call('expire', key, math.ceil(capacity / rate) + 60)

return { allowed, math.floor(tokens) }
