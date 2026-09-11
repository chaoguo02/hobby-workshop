# 秒杀完整链路

> 面向「从抢购到核销/关单」的全过程说明，包含数据流、状态机、兜底机制与可观测性。
> 相关源码路径见文末。

## 0. 总览

**Redis 做准入（挡住超卖和重复），本地消息表(outbox) 做可靠投递，Kafka 做削峰，消费者做落库。**
**Redis 是准入真值且永不回滚，MySQL 只向它单向收敛；每一笔已准入的资格最终都会有订单。**

```
用户 ──POST /voucher-order/seckill/{voucherId}──▶ VoucherOrderController
  ① seckill.lua（原子：查库存 + 一人一单 → 扣 Redis 库存 + 记录准入）
  ② 请求线程写 outbox(READY) ──┐
                              │ ③ Relay 每 500ms 扫 → 单事务批量抢占(PROCESSING) → 异步发 Kafka
                              ▼
                       Kafka: voucher-orders (3 分区)
                              │ ④ Consumer(并发 3, 手动 ack) → createVoucherOrder 事务
                              ▼
             MySQL: 插订单(status=1 待支付) + 扣库存 + outbox=COMPLETED（同一事务）
                              │ ⑤ 支付 / 取消 / 900s 超时关单
                              ▼
              待核销(2) → 线下核销(3)；或 关单(7→4) 释放名额（MySQL/Redis 各 +1）
```

## 1. 逐阶段

### ① 入口与原子准入（`seckill.lua`）

`POST /voucher-order/seckill/{voucherId}`，**需要登录**——`/voucher-order/**` 不在
`LoginInterceptor` 白名单里（白名单只有 `/voucher/**`，两者是不同路径段）。

请求线程先用雪花算法生成 `orderId`，再执行 Lua：

| 步骤 | 命令 | 作用 |
|---|---|---|
| 查库存 | `get seckill:stock:{v}` | key 不存在或 ≤0 → `return 1` |
| 一人一单 | `sismember seckill:order:{v} userId` | 已在集合 → `return 2` |
| 扣库存 | `incrby seckill:stock:{v} -1` | |
| 记录用户 | `sadd seckill:order:{v} userId` | |
| **记录准入** | `hset seckill:admitted:{v} orderId userId` | **orderId 与「谁被准入」一起原子落 Redis** |

返回：`0` 成功 / `1` 库存不足 / `2` 不能重复下单 / `null` 服务繁忙（Redis 异常，未产生任何准入）。

整个脚本原子执行，因此**不会超卖、不会一人多单**；并且「已准入」这件事在 Redis 里可反查，
这是后面所有兜底机制的前提。

### ② 写 outbox（请求线程，快路径）

Lua 成功后，请求线程 `INSERT tb_seckill_message (status=READY)`。
**这一步失败也照样返回成功**——Redis 已是真值，恢复任务会依据 `seckill:admitted` 把 outbox 行补回来。

接口返回 `orderId` 字符串（18 位雪花 ID 超出 `Number.MAX_SAFE_INTEGER`，按数字下发前端会丢精度）。

### ③ Relay 投递（`SeckillMessageRelay`，`fixedDelay=500ms`）

1. 扫 `status IN (READY, FAILED) AND (next_retry_time IS NULL OR 到期)`，`limit 200`；
2. **单事务批量条件 UPDATE** 抢占为 `PROCESSING`（`claimBatch` 用 `TransactionTemplate`。
   多实例并发时后到者在行锁释放后条件不成立，影响行数为 0，仍然只有抢到的那台会发送）；
3. 异步 `kafkaTemplate.send(topic, key=orderId, order)`，不阻塞 relay 线程，单轮可派发整批；
4. ACK 回调 → 置 `SENT`；失败 → `releaseForRetry`：`retry+1` + 退避 + 置回 `READY`（常规段）
   或 `FAILED`（告警段）并设置 `next_retry_time`。

> key 用 `orderId` 而不是 `voucherId`：订单之间无序依赖（主键 + 唯一键 + 一人一单已保证），
> 打散后同一张热门券的消息能分布到 3 个分区并行消费。

改异步投递 + `claimBatch` 的收益（实测 100 条突发）：drain 3030ms(33/s) → 2451ms → **1618ms(61/s)**。
剩余串行点是同一张券的 MySQL 库存行锁，单券天生热点。

### ④ 消费落库（`VoucherOrderConsumer`，3 并发、手动 ack）

- 订单已存在 → 置 `COMPLETED` + ack（幂等重放）；
- 否则 `createVoucherOrder` **单事务**做三件事：
  1. 插订单（`status=1 待支付`）；
  2. 条件扣 MySQL 库存（`UPDATE ... WHERE stock>0`），失败抛异常回滚整笔；
  3. outbox 置 `COMPLETED`——**与订单、库存同一事务提交**，所以 `COMPLETED` 是链路终点。
- `DuplicateKeyException` 分两种，不能一律当成功：
  - 本 `orderId` 确实已存在 → 幂等重放，置 `COMPLETED` + ack；
  - 撞的是 `(user_id, voucher_id)` 唯一键但本 `orderId` 没落库 → **不能当成功**，抛出重试。

消费失败按 `SeekToCurrentErrorHandler` 每 1s 重试、共 4 次，仍失败进 DLT
`voucher-orders.DLT`（recoverer 按源分区号写，所以 DLT 必须与主 topic 同为 3 分区）。

到这里「秒杀完成」= 订单落库、`status=1 待支付`。

### ⑤ 支付 / 取消 / 超时关单

- `POST /voucher-order/{id}/pay?payType=1..3`：CAS `1 → 2 待核销`，
  **截止时间判定写在 SQL 条件里**（`create_time > NOW() - INTERVAL 900 SECOND`），
  0 行时重读真值区分「幂等重付 / 关单赢了 / 超时」。
- `POST /voucher-order/{id}/cancel`，或 900s 超时由 `VoucherOrderCloseTask`（每 5s 扫、批量 100）触发：
  1. `markClosing`：CAS `1 → 7(关单中)`，**同事务** MySQL `stock = stock + 1`（0 行则抛异常整体回滚）；
  2. 提交后补偿 Redis：`close-voucher-order.lua` 用 `seckill:closed:{v}` 哈希做幂等，
     执行 `incrby stock` + `srem seckill:order` + `hdel admitted`；
  3. DB CAS `7 → 4 已取消`。
- 补偿失败就留在 `7`，`close_retry` 指数退避（5s 起、封顶 3600s）反复重试，并写 WARN 事件 + 打 ERROR 日志。

配置：`workshop.order.payment-timeout-seconds:900` / `close-scan-delay-ms:5000` / `close-batch-size:100`。

## 2. 两套状态机（勿混用）

| 投递状态 `tb_seckill_message` | 含义 | 订单状态 `tb_voucher_order` | 含义 |
|---|---|---|---|
| 0 READY | 待投递 | 1 | 待支付 |
| 1 PROCESSING | relay 已抢占 | 2 | 待核销 |
| 2 SENT | 已到 Kafka | 3 | 已核销 |
| 3 COMPLETED | **订单已落库（链路终点）** | 4 | 已取消 |
| 4 FAILED | 重试告警段，**非终态**，仍会自动重投 | 7 | 关单中 |

> `outbox COMPLETED` 只说明订单落过库，之后仍可能被取消。管理后台把两者并列展示，
> 不要用投递状态代表订单状态。

## 3. 兜底与对账（自动化闭环，不依赖人工）

| 定时任务 | 周期 | 职责 |
|---|---|---|
| `SeckillMessageRelay` | 500ms | 重投 READY，含 FAILED 慢速通道 |
| `SeckillMessageRecoveryTask` | 30s | HSCAN `seckill:admitted:*`：缺 outbox 行的补插 READY；已 COMPLETED 的 HDEL 防膨胀 |
| `SeckillMessageReconcileTask` | 30s | PROCESSING/SENT 卡 >60s：retry<3 重置 READY 快退避；≥3 转 FAILED 慢退避 |
| `SeckillStockReconcileTask` | 60s | 核对恒等式，只告警不改数 |
| `VoucherOrderCloseTask` | 5s | 过期关单 + 关单补偿重试 |

**退避策略**集中在 `SeckillRetryPolicy`，relay 与 ReconcileTask 共用，避免阈值漂移：

- 常规段（`retry < 3`）：1、2、4 … 秒，封顶 300s；
- 告警段（`retry >= 3`）：5、10、20、40 分钟，封顶 1h；
- **告警段没有「放弃」终点**——`FAILED` 只是变慢 + 告警，后台仍持续自动重试。
  人工重放是加速手段，不是唯一恢复路径。

**核心不变量**：每一笔已准入的资格最终都会自动落库（at-least-once + 多层幂等）。
**隐含前提**：Redis 必须开启持久化——`seckill:admitted` 是可信根，丢了第 1 层兜底就无从补起。

### 幂等的五道闸

1. Lua `sismember seckill:order`（一人一单）；
2. DB `uk(user_id, voucher_id)` 唯一键（最终闸门，允许复用已取消行并换新订单号）；
3. 消费者落库前的存在性检查；
4. relay 抢占的条件 UPDATE（多实例防重复投递）；
5. 关单的 `seckill:closed` 哈希（防重复释放名额）。

## 4. 库存恒等式

```
MySQL库存 = Redis库存 + 未落库准入数
```

「未落库准入数」= 该券下 outbox 状态非 `COMPLETED` 的消息数（`COMPLETED` 与订单落库同事务）。
关单会让 MySQL 与 Redis **各 +1**、outbox 仍是 `COMPLETED`，所以恒等式不变。

`SeckillStockReconcileTask` 每 60s 核对，漂移只告警不自动改数。漂移来源包括 Redis 扣了
MySQL 没扣（消息卡住/FAILED），重放后自动消除。

## 5. 可观测性

- **时间线** `tb_seckill_message_event`：append-only，best-effort（写失败只告警、可开关），
  埋点覆盖 `REDIS_ADMIT → OUTBOX_WRITTEN → RELAY_CLAIMED → KAFKA_SENT → CONSUMER_RECEIVED
  → ORDER_COMMITTED → ORDER_CLOSE_REQUESTED → ORDER_CLOSED`，以及异常/兜底/人工重放。
  业务成功事件必须在**事务提交后**写（用独立连接），否则回滚会留下假事件。
- `GET /monitor/seckill`：原始快照（outbox 各状态计数、最老 READY/FAILED 时长、完成延迟、
  Kafka lag、DLT 堆积、关单积压）。
- `GET /monitor/seckill/pipeline`：中文阶段漏斗 + 近 20 笔订单（**订单状态 + 投递阶段并列**）。
- `GET /monitor/seckill/timeline/{orderId}`：单笔全过程还原（阶段中文名 + 相对耗时 + 级别）。
- `front/.../admin.html`：漏斗、时间线弹窗、DLT 标红、一键重放 FAILED、关单告警。
- 日志告警：outbox `failedCount`、关单 `retrying>0`，定时 30s 打 ERROR。

## 6. 关键设计取舍

- **削峰**：入口只有一次 Lua + 一次 outbox INSERT；扣 MySQL 库存、写订单等重活交给消费者异步做。
- **不丢单**：先落本地消息表再发 MQ（事务性 outbox）；发送失败可重投；Redis 准入记录是永远可反查的真值。
- **不重复**：Lua 一人一单 + DB 唯一键 + 消费幂等 + relay 条件抢占 + 关单幂等哈希。
- **DB 先于 Redis 恢复库存**：关单先在同一事务里恢复 MySQL 库存，再补偿 Redis。
  这样即使 Redis 永久不可用，MySQL 也不会少库存；短暂出现的 `MySQL > Redis` 正是对账任务要抓的漂移。
- **可解释**：每笔都能拉出从抢购到关单的完整时间线，问题可定位到具体阶段。

## 7. 实测数据（`.runtime/*.jtl` + JMeter Summariser）

- 修复 Lua 脚本每次热读 fat jar 的锁竞争后：**981/s、P95 200ms、peak 1634 QPS、0 错误**
  （对照修复前 288/s、P95 6869ms）。
- 1000 用户抢 200 张券：成功 200 / `库存不足` 800 / 重复下单 0；MySQL 与 Redis 库存双双到 0；
  订单 200 且用户去重后 200；outbox 200 行全部 `COMPLETED`；全库负库存 0。
- 同用户 200 并发：成功 1 / `不能重复下单` 199，库存只减 1。
- 200 笔超时关单：全部 `close_retry=0`，MySQL 库存精确 +200（无重复恢复），`closing` 残留 0，对账无漂移。

## 8. 相关文件

| 关注点 | 路径 |
|---|---|
| 准入脚本 | `src/main/resources/seckill.lua` |
| 关单补偿脚本 | `src/main/resources/close-voucher-order.lua` |
| 秒杀入口 | `src/main/java/com/hmdp/service/impl/VoucherOrderServiceImpl_kafka.java` |
| 投递 relay | `src/main/java/com/hmdp/utils/SeckillMessageRelay.java` |
| 消费者 | `src/main/java/com/hmdp/utils/VoucherOrderConsumer.java` |
| 退避策略 | `src/main/java/com/hmdp/utils/SeckillRetryPolicy.java` |
| 兜底任务 | `SeckillMessageRecoveryTask.java` / `SeckillMessageReconcileTask.java` / `SeckillStockReconcileTask.java` |
| 支付与关单 | `src/main/java/com/hmdp/service/VoucherOrderLifecycleService.java` / `VoucherOrderCloseTask.java` |
| Kafka / DLT 配置 | `src/main/java/com/hmdp/config/KafkaConsumerConfig.java` |
| 可观测性 | `src/main/java/com/hmdp/monitor/SeckillStatsService.java` / `SeckillEventLogger.java` / `SeckillTimelineService.java` |
| 管理端点 | `src/main/java/com/hmdp/controller/SeckillMonitorController.java` |
| 管理后台 | `front/nginx-1.18.0/html/hmdp/admin.html` |
| 建表脚本 | `src/main/resources/db/hmdp.sql` / `db/voucher_order_close.sql` |
