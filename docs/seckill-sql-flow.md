# 秒杀全流程真实 SQL（复习用）

> 配套 [秒杀完整链路](./seckill-chain.md) 与 [支付/关单/核销链路](./order-lifecycle.md)。
> 本文按**代码执行顺序**列出每一步真正落到 MySQL 的语句，方便对着代码逐条复习。
> 行号对应当前版本；代码改动后如需精确定位，按方法名搜索即可。

## 0. 涉及的表与字段

| 表 | 关键字段 | 说明 |
|---|---|---|
| `tb_seckill_message` | `order_id`(PK), `user_id`, `voucher_id`, `status`, `retry`, `next_retry_time`, `create_time`, `update_time` | 本地消息表（outbox）：0 READY / 1 PROCESSING / 2 SENT / 3 COMPLETED / 4 FAILED |
| `tb_voucher_order` | `id`(PK), `user_id`, `voucher_id`, `pay_type`, `status`, `create_time`, `pay_time`, `use_time`, `refund_time`, `update_time`, `close_retry`, `close_next_retry_time`, `close_last_error`, `close_reason` | 订单：1 待支付 / 2 待核销 / 3 已核销 / 4 已取消 / 7 关闭处理中 |
| `tb_seckill_voucher` | `voucher_id`(PK), `stock`, `begin_time`, `end_time`, `update_time` | 秒杀券库存 |
| `tb_seckill_message_event` | `id`(自增), `order_id`, `user_id`, `voucher_id`, `stage`, `level`, `detail`, `create_time` | append-only 时间线事件（仅观测） |

> MyBatis-Plus 的 `insert` 只写非 null 字段，`create_time`/`update_time` 走 DB 的
> `DEFAULT CURRENT_TIMESTAMP`，所以下面的 INSERT 字段数比表少。

## A. 请求线程（`VoucherOrderServiceImpl_kafka.java:101`）

```sql
-- A1 写 outbox，status=0 READY
INSERT INTO tb_seckill_message (order_id, user_id, voucher_id, status, retry)
VALUES (?, ?, ?, 0, 0);

-- A2 时间线事件（SeckillEventLogger.record，每个阶段都会插一条）
INSERT INTO tb_seckill_message_event (order_id, user_id, voucher_id, stage, level, detail)
VALUES (?, ?, ?, 'REDIS_ADMIT' / 'OUTBOX_WRITTEN', 'INFO', ?);
```

写 outbox 失败也照样返回成功：Redis 的 `seckill:admitted` 是准入真值，恢复任务会补（见 D1）。

## B. Relay 投递（`SeckillMessageRelay`）

```sql
-- B1 扫描候选（:53），走 idx_status_next_retry_create
SELECT order_id, user_id, voucher_id, status, retry, next_retry_time, create_time, update_time
FROM tb_seckill_message
WHERE status IN (0, 4)
  AND (next_retry_time IS NULL OR next_retry_time <= NOW())
ORDER BY create_time ASC
LIMIT 200;

-- B2 单事务批量抢占（:93）：0/4 → 1；影响行数=1 才去发 Kafka，多实例只有一个抢到
UPDATE tb_seckill_message
SET status = 1
WHERE order_id = ?
  AND status IN (0, 4);

-- B3 发送成功回调（:127）：1 → 2 SENT
UPDATE tb_seckill_message
SET status = 2
WHERE order_id = ?
  AND status = 1;

-- B4 发送失败退避（releaseForRetry :157）：1 → 0 READY（retry<3）或 4 FAILED（retry≥3）
UPDATE tb_seckill_message
SET status = ?,                                  -- 0 或 4
    retry = retry + 1,
    next_retry_time = DATE_ADD(NOW(), INTERVAL ? SECOND)
WHERE order_id = ?
  AND status = 1;
```

> Kafka 的 key 是 `orderId`（`SeckillMessageRelay.java:118`），订单间无序依赖，打散后同一热门券可分布到多分区并行消费。

## C. 消费者落库（`VoucherOrderConsumer.java:30`）

```sql
-- C1 幂等预检（:37 selectById）；已存在则 markCompleted + ack
SELECT * FROM tb_voucher_order WHERE id = ?;
```

`createVoucherOrder`（`VoucherOrderServiceImpl_kafka.java:193`）**单事务**内：

```sql
-- C2 查该用户对该券是否有「已取消(4)」旧行，用于复用（:201）
SELECT * FROM tb_voucher_order
WHERE user_id = ? AND voucher_id = ? AND status = 4
LIMIT 1;

-- C3a 没旧行 → 插订单，status=1 待支付（:208）；pay_type 未传，DB 默认 1
INSERT INTO tb_voucher_order (id, user_id, voucher_id, status)
VALUES (?, ?, ?, 1);

-- C3b 有旧行 → 复用该行、换新 orderId 并重置字段（:211）
UPDATE tb_voucher_order
SET id = ?, status = 1, create_time = ?,
    pay_time = NULL, use_time = NULL, refund_time = NULL,
    close_retry = 0, close_next_retry_time = NULL, close_last_error = NULL, close_reason = NULL,
    update_time = ?
WHERE id = ? AND status = 4;

-- C4 条件扣 MySQL 库存（:233）：0 行则抛异常，整事务回滚
UPDATE tb_seckill_voucher
SET stock = stock - 1
WHERE voucher_id = ?
  AND stock > 0;

-- C5 outbox 与订单、库存同事务置终态（:246）：→ 3 COMPLETED
UPDATE tb_seckill_message
SET status = 3
WHERE order_id = ?;

-- C6 事务提交后才写成功事件（consumer :51）
INSERT INTO tb_seckill_message_event (order_id, user_id, voucher_id, stage, level, detail)
VALUES (?, ?, ?, 'ORDER_COMMITTED', 'INFO', ?);
```

`COMPLETED` 是链路终点；`SENT` 只代表消息到了 Kafka。

## D. 兜底任务

```sql
-- D1 恢复任务：查这些 orderId 在 outbox 是否已存在（SeckillMessageRecoveryTask :123）
SELECT * FROM tb_seckill_message WHERE order_id IN (?, ?, ...);

-- D1b 缺失的补插 READY（:137）；并发撞主键由 DuplicateKeyException 忽略
INSERT INTO tb_seckill_message (order_id, user_id, voucher_id, status, retry)
VALUES (?, ?, ?, 0, 0);

-- D2 对账任务：扫卡住超过 60s 的 PROCESSING/SENT（SeckillMessageReconcileTask :40）
SELECT * FROM tb_seckill_message
WHERE status IN (1, 2)
  AND update_time < DATE_SUB(NOW(), INTERVAL 60 SECOND)
ORDER BY update_time ASC
LIMIT 100;

-- D2a 未到阈值（retry<3）：重置 READY（:51，WHERE 带原状态防并发重复改）
UPDATE tb_seckill_message
SET status = 0, retry = retry + 1,
    next_retry_time = DATE_ADD(NOW(), INTERVAL ? SECOND)
WHERE order_id = ? AND status = ?;              -- 原状态 1 或 2

-- D2b 到阈值：转 FAILED 慢速通道（:69）
UPDATE tb_seckill_message
SET status = 4, retry = retry + 1,
    next_retry_time = DATE_ADD(NOW(), INTERVAL ? SECOND)
WHERE order_id = ? AND status = ?;

-- D3 人工重放（SeckillMessageReplayService :35）：只动 FAILED，retry 归零、清退避
UPDATE tb_seckill_message
SET status = 0, retry = 0, next_retry_time = NULL
WHERE order_id = ? AND status = 4;

-- D4 时间线事件按天清理（SeckillEventLogger :120）
DELETE FROM tb_seckill_message_event WHERE create_time < ?;
```

## E. 支付 / 关单 / 核销

```sql
-- E1 支付（VoucherOrderLifecycleService :79）：CAS 1→2，截止时间写在条件里（DB 时钟）
SELECT * FROM tb_voucher_order WHERE id = ?;
UPDATE tb_voucher_order
SET status = 2, pay_type = ?, pay_time = NOW(), update_time = NOW()
WHERE id = ? AND user_id = ? AND status = 1
  AND create_time > DATE_SUB(NOW(), INTERVAL 900 SECOND);

-- E2 关单任务扫描超时单（VoucherOrderCloseTask :33）
SELECT * FROM tb_voucher_order
WHERE status = 1 AND create_time <= DATE_SUB(NOW(), INTERVAL 900 SECOND)
ORDER BY create_time ASC LIMIT 100;

-- E2a 受理：CAS 1→7（:180）；用户取消不带最后那行截止条件
UPDATE tb_voucher_order
SET status = 7, close_reason = ?, close_retry = 0,
    close_next_retry_time = NOW(), close_last_error = NULL, update_time = NOW()
WHERE id = ? AND status = 1
  AND create_time <= DATE_SUB(NOW(), INTERVAL 900 SECOND);

-- E2b 同事务恢复 MySQL 库存（:195）：0 行抛异常整体回滚
UPDATE tb_seckill_voucher SET stock = stock + 1 WHERE voucher_id = ?;

-- E2c Redis 补偿成功 → CAS 7→4（:151）
UPDATE tb_voucher_order
SET status = 4, close_next_retry_time = NULL, close_last_error = NULL, update_time = NOW()
WHERE id = ? AND status = 7;

-- E2d 补偿失败 → 保留 7、退避重试（:233）
UPDATE tb_voucher_order
SET close_last_error = ?, close_retry = close_retry + 1,
    close_next_retry_time = DATE_ADD(NOW(), INTERVAL ? SECOND), update_time = NOW()
WHERE id = ? AND status = 7;

-- E2e 任务扫待重试的关单（:47）
SELECT * FROM tb_voucher_order
WHERE status = 7 AND (close_next_retry_time IS NULL OR close_next_retry_time <= NOW())
ORDER BY close_next_retry_time ASC, update_time ASC LIMIT 100;

-- E3 核销（VoucherOrderServiceImpl_kafka :163）：CAS 2→3
UPDATE tb_voucher_order
SET status = 3, use_time = ?, update_time = ?
WHERE id = ? AND status = 2;
```

## 状态迁移总表（只看 status 列）

| 表 | 迁移 | 触发代码 |
|---|---|---|
| `tb_seckill_message` | `INSERT` → 0 | 请求线程 `:101` |
| | 0/4 → 1 | Relay `claimBatch` `:93` |
| | 1 → 2 | 发送成功回调 `:127` |
| | 1 → 0 / 4 | `releaseForRetry` `:157` |
| | 1/2 → 0 或 4 | Reconcile `:51` / `:69` |
| | 4 → 0 | 人工重放 `:35` |
| | **→ 3 COMPLETED** | 消费者同事务 `:246` |
| `tb_voucher_order` | `INSERT` → 1 | 消费者 `:208` / 复用 `:211` |
| | 1 → 2 | 支付 `:79` |
| | 1 → 7 | 关单受理 `:180` |
| | 7 → 4 | 补偿成功 `:151` |
| | 2 → 3 | 核销 `:163` |

## 一条主线记住幂等

**所有状态迁移都是 `WHERE 主键 = ? AND status = 旧状态` 的条件 UPDATE。**
并发、多实例、重复投递下只会有一个赢家；输的一方影响行数为 0，再重读真值决策。
这就是「不超卖、不一人多单、不重复落库、不重复释放名额」的实现基础。

## 相关文件

| 关注点 | 路径 |
|---|---|
| 秒杀入口 + 写 outbox + 落库 | `src/main/java/com/hmdp/service/impl/VoucherOrderServiceImpl_kafka.java` |
| 准入脚本 | `src/main/resources/seckill.lua` |
| 投递 relay | `src/main/java/com/hmdp/utils/SeckillMessageRelay.java` |
| 消费者 | `src/main/java/com/hmdp/utils/VoucherOrderConsumer.java` |
| 恢复 / 对账 / 重放 | `SeckillMessageRecoveryTask.java` / `SeckillMessageReconcileTask.java` / `SeckillMessageReplayService.java` |
| 退避策略 | `src/main/java/com/hmdp/utils/SeckillRetryPolicy.java` |
| 时间线事件 | `src/main/java/com/hmdp/monitor/SeckillEventLogger.java` |
| 支付 / 关单 / 补偿 | `src/main/java/com/hmdp/service/VoucherOrderLifecycleService.java` |
| 关单任务 | `src/main/java/com/hmdp/utils/VoucherOrderCloseTask.java` |
| 关单补偿脚本 | `src/main/resources/close-voucher-order.lua` |
| 建表脚本 | `src/main/resources/db/hmdp.sql` / `db/voucher_order_close.sql` |
