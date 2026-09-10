# 项目待优化清单

## 一、缓存治理体系

### 1.1 缓存更新一致性
- [ ] `ShopServiceImpl.update()` 先更新数据库再删除缓存，如果**删缓存失败**，下次请求读到旧数据（缓存和数据库不一致）
- [ ] 解决方案：延迟双删（先删缓存 → 更新 DB → 延时再删缓存）或订阅 binlog（Canal）同步删除

### 1.2 缓存击穿——互斥锁方案
- [ ] `queryWithMutex()` 中获取锁失败后 `Thread.sleep(50)` 递归重试，递归深度可能导致栈溢出
- [ ] 应改为 while 循环 + 超时机制（如最多重试 10 次）
- [ ] 锁的 key 粒度过粗：所有请求抢同一把锁，应细化到具体业务 key

### 1.3 缓存击穿——逻辑过期方案
- [ ] `queryWithLogicalExpire()` 中获取锁成功后，没有 **DoubleCheck**——获取锁的间隙可能有其他线程已经重建了缓存，应二次检查缓存是否已过期再重建
- [ ] 锁的超时时间写死 10 秒，如果缓存重建耗时超过 10 秒，锁自动释放会导致多个线程同时重建。应结合 Redisson WatchDog 自动续期
- [ ] 缓存重建线程池用 `Executors.newFixedThreadPool(10)`，底层无界队列有 OOM 风险。应改为 `ThreadPoolExecutor` + 有界队列 + DiscardPolicy

### 1.4 缓存穿透
- [ ] 目前只用了空值缓存，没有考虑**布隆过滤器**（Bloom Filter）。空值缓存的问题是：空值本身也有过期时间，如果大量不存在的 key 被攻击，Redis 中存了大量空值 key，浪费内存
- [ ] 优化方案：布隆过滤器前置拦截（请求先查布隆过滤器，不存在直接返回，不查 Redis 也不查 DB）

### 1.5 GEO 附近搜索优化
- [ ] `ShopServiceImpl.queryShopByType()` 中使用 `GEOSEARCH` 一次查 5000 米内所有商铺，然后在内存中做分页截取。如果商铺数量很大，网络传输和内存消耗都不小
- [ ] 应考虑 Redis GEO 的分页能力或业务层做二次过滤

---

## 二、秒杀执行链路

### 2.1 异步队列可靠性
- [ ] 当前使用 **JVM BlockingQueue**（`ArrayBlockingQueue`），应用重启或宕机时队列中所有订单丢失
- [ ] 消费者 `handleVoucherOrder()` 处理失败后只 `log.error`，消息直接丢失，没有重试机制
- [ ] 解决方案：启用项目中的 Kafka 版本（`VoucherOrderServiceImpl_kafka.java`），消息持久化 + 自动重投 + at-least-once 语义

### 2.2 Kafka 消费者端一致性
- [ ] 消费者缺少**幂等性检查**（`selectById` 判断订单是否已存在），Kafka 重投消息时会导致重复扣库存
- [ ] **事务提交与消息 ACK 时序未保障**：没有使用 `TransactionSynchronizationManager.afterCommit()`，无法保证 MySQL 事务提交后才 ACK
- [ ] 消费失败时直接 `throw e`，没有设置**重试次数上限**和**死信队列**，无限重试可能导致消息积压

### 2.3 库存扣减顺序
- [ ] `createVoucherOrder()` 中先扣库存（UPDATE）后写订单（INSERT），**热点行锁持有时间较长**
- [ ] 应调整为先 INSERT 后 UPDATE，并在 UPDATE 前加轻量 SELECT 预检查，缩短行锁持有时间

### 2.4 Lua 脚本
- [ ] Lua 脚本中 `xadd` 发送 Redis Stream 消息，但当前版本用的是 JVM BlockingQueue，两者不一致。应以 Kafka 为准
- [ ] Lua 脚本没有**库存预热检查**——如果 Redis 中没有库存 key，`get` 返回 nil，`tonumber(nil) <= 0` 会异常

### 2.5 补偿机制
- [ ] 缺少**定时对账**：Redis 已购集合（`seckill:order:{voucherId}`）与 MySQL 订单表没有定期对比
- [ ] 没有**库存回滚**机制：Redis 预扣成功但 MySQL 落库失败时，Redis 库存不会主动还原

### 2.6 分布式锁粒度
- [ ] 锁的粒度：`lock:order:{userId}` 是按用户粒度加锁，但如果同一用户同时抢两个不同的优惠券，会互相阻塞
- [ ] 应改为 `lock:order:{userId}:{voucherId}`，按用户 + 优惠券双重维度

### 2.7 全局唯一 ID 生成器
- [ ] 当前 `RedisIdWorker` 用 Redis INCR + 时间戳拼接，每天一个 key 支持 42 亿 ID
- [ ] 高可用风险：Redis 宕机时 ID 生成不可用。解决方案：预分配号段模式（本地缓存一段 ID，用完再取）
- [ ] 可扩展：对比雪花算法，了解时钟回拨问题的三种解决方案（等待回拨、续用序列号、预留回拨位）

---

## 三、反薅羊毛风控体系

> 反薅羊毛不等于限流。限流控制的是"量"（每秒不超过 N 次），反薅羊毛判断的是"人"（这个用户是真人还是脚本）。两者目的不同、手段不同、的层级不同。

### 3.1 分层说明

| 层级 | 手段 | 目标 | 拦截位置 |
|---|---|---|---|
| **限流** | Nginx `limit_req` + Redis 计数器 | 控制请求频率，防止打挂后端 | Nginx / 网关层 |
| **风控** | 行为画像 + Lua 实时评分 | 区分真人 vs 脚本，保护库存不被黄牛抢走 | Lua 脚本内 |
| **验证码安全** | 频控 + 错误锁定 | 防暴力破解验证码 | 登录接口 |

**限流是"让系统不被打死"，风控是"让羊毛党买不到"——前者保系统，后者保业务。**

### 3.2 限流层
- [ ] Nginx 层：`limit_req_zone $binary_remote_addr` 限制单 IP 每秒请求数
- [ ] 接口层：自定义 `@RateLimit` 注解 + 拦截器，限制单用户每秒秒杀调用不超过 5 次
- [ ] Redis 分布式限流：滑动窗口计数器，跨实例共享限流状态

### 3.3 风控层——行为画像评分
- [ ] 在 Redis 中维护用户行为特征（滚动窗口 1 小时）：
  - 请求频率（1 秒内请求次数）
  - User-Agent 指纹（正常用户的 UA 会变化，脚本固定不变）
  - 操作路径（是否从首页浏览过来，还是直接调接口）
- [ ] Lua 脚本中增加风控评分校验：

```lua
-- 增强后的 seckill.lua（风控前置过滤）
local userId = ARGV[2]
local riskKey = 'risk:user:' .. userId

-- 查行为分
local requestCount = redis.call('hget', riskKey, 'requestCount')
if requestCount and tonumber(requestCount) > 10 then
    return 3  -- 疑似脚本，拒绝
end

-- 原有库存和重复校验不变
if(tonumber(redis.call('get', stockKey)) <= 0) then return 1 end
if(redis.call('sismember', orderKey, userId) == 1) then return 2 end

redis.call('incrby', stockKey, -1)
redis.call('sadd', orderKey, userId)
redis.call('xadd', 'stream.orders', '*', ... )
return 0
```

- [ ] 风控事件异步埋点：订单创建成功后，在 Kafka 消费者中异步记录用户行为特征，不阻塞主链路

### 3.4 验证码安全（已有，从登录模块迁移）
- [ ] 发送验证码接口缺少频控：同一手机号 60 秒内不能重复发送
- [ ] 同一 IP 每天发送上限未限制
- [ ] 验证码输入错误 5 次后未做临时锁定（防暴力破解）

---

## 四、关单与支付链路

> 当前订单创建后 status=1（未支付），没有后续处理。不支付的订单永远不关闭，库存永远不释放。需要补全订单的生命周期。

### 4.1 订单状态机完整设计

```
                   ┌──────────┐
                   │ 创建订单  │   status=1（未支付）
                   └────┬─────┘
                        │
              ┌─────────┼──────────┐
              │         │          │
              ▼         ▼          ▼
         ┌────────┐ ┌──────┐ ┌──────────┐
         │ 超时关单 │ │ 支付  │ │ 转赠     │
         │status=4 │ │ 中   │ │status=5  │
         └────────┘ └──┬───┘ └──────────┘
                       │                   
                       ▼                   
                  ┌──────────┐             
                  │ 已支付    │             
                  │ status=2 │             
                  └────┬─────┘             
                       │                   
                       ▼                   
                  ┌──────────┐             
                  │ 已核销    │             
                  │ status=3 │             
                  └──────────┘             

补充链路：
status=4（已取消）→ 释放库存
status=5（退款中）→ 恢复库存
status=6（已退款）
```

### 4.2 新增数据库变更

```sql
-- 订单表已有字段，无需新增
-- tb_voucher_order 已有：id, user_id, voucher_id, status, pay_type,
--                       create_time, pay_time, use_time, refund_time, update_time

-- ① 支付流水表（新增，记录每笔支付的完整链路）
CREATE TABLE tb_payment_record (
    id              BIGINT PRIMARY KEY,
    order_id        BIGINT NOT NULL,           -- 关联订单 ID
    user_id         BIGINT NOT NULL,
    amount          BIGINT NOT NULL,           -- 支付金额（分）
    channel         VARCHAR(32) NOT NULL,      -- 支付渠道：alipay / wechat
    trade_no        VARCHAR(128),              -- 支付渠道的交易号
    status          TINYINT DEFAULT 1,         -- 1=支付中 2=支付成功 3=支付失败 4=已退款
    notify_data     TEXT,                      -- 支付回调的原始数据
    created_at      DATETIME,
    updated_at      DATETIME,
    INDEX idx_order (order_id),
    INDEX idx_trade_no (trade_no)
);

-- ② 关单日志表（新增，记录关单操作）
CREATE TABLE tb_order_close_log (
    id              BIGINT PRIMARY KEY,
    order_id        BIGINT NOT NULL,
    close_type      VARCHAR(32) NOT NULL,      -- TIMEOUT / USER_CANCEL / ADMIN
    close_reason    VARCHAR(256),
    stock_restored  TINYINT DEFAULT 0,         -- 库存是否已回滚
    created_at      DATETIME,
    INDEX idx_order (order_id)
);
```

### 4.3 超时关单方案

> 方案选型对比：

| 方案 | 精度 | 可靠性 | 复杂度 |
|---|---|---|---|
| 定时任务扫表 | 分钟级 | ✅ 可靠 | 低 |
| 延迟消息（Redis ZSet） | 秒级 | ⚠️ Redis 宕机丢消息 | 中 |
| RabbitMQ 延迟队列 | 毫秒级 | ✅ 可靠 | 高（需引入 MQ） |

**推荐方案：分层保证——Redis 延迟消息作为主力 + 定时任务扫表作为兜底。**

```java
// 主力：Redis ZSet 实现秒级延迟关单
public void sendDelayClose(Long orderId, long delaySeconds) {
    String key = "delay:close:order";
    // score = 期望执行的时间戳
    double score = (System.currentTimeMillis() + delaySeconds * 1000);
    stringRedisTemplate.opsForZSet().add(key, orderId.toString(), score);
}

// 后台轮询：每秒检查哪些订单到期了
@Scheduled(fixedDelay = 1000)
public void processTimeoutOrders() {
    String key = "delay:close:order";
    double now = System.currentTimeMillis();
    
    // 取出所有已到期的订单
    Set<String> expiredOrders = stringRedisTemplate.opsForZSet()
        .rangeByScore(key, 0, now);
    
    for (String orderId : expiredOrders) {
        // 尝试关单
        boolean success = closeOrder(Long.valueOf(orderId), "TIMEOUT");
        if (success) {
            // 关单成功，从延迟队列移除
            stringRedisTemplate.opsForZSet().remove(key, orderId);
        }
    }
}

// 兜底：定时任务每 5 分钟扫表，防止 Redis 中丢消息
@Scheduled(fixedDelay = 300000)
public void dbScanTimeoutOrders() {
    // SELECT * FROM tb_voucher_order
    // WHERE status = 1 AND create_time < NOW() - 15 MINUTE
    // AND id NOT IN (最近已处理的订单)
    List<VoucherOrder> timeoutOrders = orderMapper.selectTimeoutOrders(15);
    for (VoucherOrder order : timeoutOrders) {
        closeOrder(order.getId(), "TIMEOUT_DB_SCAN");
    }
}

// 关单核心逻辑（带库存回滚）
@Transactional
public void closeOrder(Long orderId, String closeType) {
    VoucherOrder order = orderMapper.selectById(orderId);
    if (order == null || order.getStatus() != 1) {
        return;  // 已支付或已取消，跳过
    }
    
    // 1. 关单（乐观锁，防止并发）
    int updated = orderMapper.updateStatus(orderId, 1, 4);
    if (updated == 0) return;  // 被其他线程处理了
    
    // 2. 回滚库存
    seckillVoucherService.update()
        .setSql("stock = stock + 1")
        .eq("voucher_id", order.getVoucherId())
        .update();
    
    // 3. 记录关单日志
    orderCloseLogMapper.insert(new OrderCloseLog(orderId, closeType, "超时未支付"));
}
```

### 4.4 支付对接

> 支付对接的核心是**异步回调**——用户支付后，支付渠道异步通知后端，后端修改订单状态并通知用户。

```
下单 → 用户选择支付 → 调支付渠道 → 用户支付 → 支付渠道异步回调
                                                      │
                                                      ▼
                                              校验签名 + 修改订单 status=2
                                                      │
                                                      ▼
                                              通知用户：支付成功
```

```java
// 支付回调接口（接收支付宝/微信的异步通知）
@PostMapping("/payment/notify")
public String handlePaymentNotify(@RequestBody String notifyData) {
    // 1. 验签（验证确实是支付渠道发的，不是伪造的）
    if (!payService.verifySign(notifyData)) {
        return "fail";  // 验签失败，支付渠道会重试
    }
    
    // 2. 解析回调数据
    PaymentNotify notify = payService.parseNotify(notifyData);
    
    // 3. 幂等性处理（同一个回调可能发多次）
    PaymentRecord record = paymentMapper.selectByTradeNo(notify.getTradeNo());
    if (record != null && record.getStatus() == 2) {
        return "success";  // 已处理过，直接返回
    }
    
    // 4. 修改订单状态
    orderMapper.updateStatus(notify.getOrderId(), 1, 2);
    paymentMapper.insert(new PaymentRecord(notify));
    
    return "success";  // 返回 success 告知支付渠道不要再发了
}
```

### 4.5 隐患与解决措施

| 隐患 | 场景 | 解决措施 |
|---|---|---|
| **重复回调** | 支付渠道因网络原因发送了两次回调 | 幂等性：`payment_record.trade_no` 唯一索引，重复插入失败 |
| **超时关单和支付并发** | 用户刚好在第 15 分钟支付，同时关单任务也在执行 | 乐观锁 `UPDATE status=4 WHERE id=? AND status=1`，一个成功另一个失败。支付成功的优先，关单失败自动跳过 |
| **库存回滚失败** | 关单成功但 `UPDATE stock + 1` 失败 | 库存回滚和关单在同一个事务中，失败一起回滚 |
| **回调丢了** | 支付渠道回调没到，用户支付了但系统不知道 | 主动查询兜底：定时任务扫 `status=1 AND create_time > 15min` 的订单，主动调支付渠道查状态 |
| **退款后库存恢复** | 已支付的订单退款，库存要不要恢复？ | 看券的类型——有时效性的券（过期不退款）不恢复库存；通用券退款后恢复库存 |
| **虚假回调攻击** | 恶意用户伪造支付回调通知 | 必须验签：支付宝/微信的 SDK 会验证签名和通知来源，伪造的回调无法通过验签 |

---

## 五、智能客服与 Agent 推荐

### 5.1 架构总览

```
用户消息
    │
    ▼
┌────────────────────────────┐
│  ① 消息预处理               │
│    敏感词过滤 / 限流 / 归档  │
└───────────┬────────────────┘
            │
            ▼
┌────────────────────────────┐
│  ② RAG 知识召回             │  ← 技术点：RAG
│    关键词匹配 + 向量检索      │
│    本地知识库优先，不走 LLM   │
└───────────┬────────────────┘
            │
            ▼
┌────────────────────────────┐
│  ③ 工作流引擎（推荐场景）     │  ← 技术点：Workflow
│    固定步骤 + LLM 推理       │
│    Step1→2→3→4→5 不可跳步   │
└───────────┬────────────────┘
            │
            ▼
┌────────────────────────────┐
│  ④ LLM + Tool Calling      │  ← 技术点：MC P + Tool Calling
│    工具注册 / 调用链编排     │
│    结果缓存 / 反注入        │
└───────────┬────────────────┘
            │
            ▼
        回复用户
```

### 5.2 RAG 知识库
- [ ] 新增 `tb_knowledge_base` 表：存储 FAQ（问题 + 答案 + 关键词 + 分类）
- [ ] 用户输入 → 关键词匹配 → 召回 TOP 3 相关知识 → 注入 Prompt
- [ ] 知识库匹配命中时可直接返回模板答案，不调 LLM（降本增效）

### 5.3 MCP Tool 清单
- [ ] `query_nearby_shops`：附近商铺搜索（复用 Redis GEO）
- [ ] `get_shop_detail`：商铺详情（复用 MySQL）
- [ ] `get_shop_vouchers`：优惠券和秒杀活动（复用秒杀系统）
- [ ] `get_user_location`：用户位置获取
- [ ] `geocode_address`：文字地址解析为经纬度

### 5.4 推荐工作流

```
Step 1: 意图理解         ← LLM 提取偏好（类型、预算、口味）
Step 2: 获取位置         ← 代码（查用户缓存 / 调地址解析）
Step 3: 附近搜索         ← 代码（调 Redis GEO 查商铺）
Step 4: 智能筛选         ← LLM 按用户偏好排序 TOP 3
Step 5: 查优惠券         ← 代码（调秒杀/优惠接口）
Step 6: 生成推荐         ← LLM 综合生成推荐文案
```

- [ ] 每一步有独立的输入输出日志，形成**调用链追踪**
- [ ] 纯数据步骤（2、3、5）不调 LLM，减少延迟和成本
- [ ] 整个工作流最大 5 步循环，防止死循环

### 5.5 隐患与解决措施

| 隐患 | 场景 | 解决措施 |
|---|---|---|
| **LLM 幻觉** | 推荐了不存在的优惠或乱编信息 | 知识库召回在前，关键数据必须来自 Tool 返回，LLM 只负责组织和润色文案 |
| **响应延迟** | 多 Tool 调用累计 3~5 秒 | 不依赖的 Tool（详情+优惠）并行调用；Tool 结果缓存 30 秒 |
| **调用链不可控** | LLM 自己决定调工具的顺序，可能跳步 | 推荐场景固定走工作流，LLM 不自主决定工具调用顺序 |
| **Prompt 注入** | 用户通过对话让 LLM 执行越权操作 | 写操作必须有二次确认；MCP Tool 权限由后端控制 |
| **上下文过长** | 聊了多轮后 Token 超限 | 只保留最近 10 轮；超出后用 LLM 总结压缩 |

---

## 六、附录：其他遗留问题

### 6.1 登录认证
- [ ] 登出接口未实现（`UserController.logout()` 返回"功能未完成"）
- [ ] Token 刷新策略：登录接口不应该刷新 token 有效期，只在有业务操作的请求时才续期
- [ ] 没有 token 黑名单机制（用户主动退出后 token 在有效期内仍可用）

### 6.2 ThreadLocal 使用
- [ ] `VoucherOrderServiceImpl` 中的 `proxy` 成员变量未加 `volatile`，存在**可见性问题**——生产者线程赋值后，消费者线程可能一直看到 null
- [ ] 建议改用方法局部变量传递，或在每次使用时从 `AopContext.currentProxy()` 获取

### 6.3 代码维护
- [ ] 多个 VoucherOrderServiceImpl 版本（`_sync`、`_kafka`、`_stream`、`_fsync`）共存，应确定一个正式版本，删除冗余文件
- [ ] `VoucherOrderConsumer.java` 在 `service/impl/` 和 `utils/` 各有一个，且内容不一致，需清理

### 6.4 分布式锁备选方案
- [ ] `SimpleRedisLock` 没有 WatchDog 自动续期机制，锁超时后业务未完成会释放导致并发问题。当前已通过 Redisson 解决，但 `SimpleRedisLock` 作为备选方案未完善
