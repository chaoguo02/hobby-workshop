# 支付 / 关单 / 核销完整链路

> 承接[秒杀完整链路](./seckill-chain.md)。起点是**订单已写入 MySQL、MySQL 库存已扣减、
> outbox 置 `COMPLETED`** 的那一刻——秒杀准入到此结束，之后进入订单生命周期。
> 相关源码路径见文末。

## 0. 总览

**支付用 CAS 抢占，关单用 CAS 抢占，两者争的是同一个 `status=1`；谁先改到谁赢，输的一方重读真值再回答用户。**
**关单先在同一事务里恢复 MySQL 库存，提交后再幂等补偿 Redis；补偿失败就停在「关闭处理中」自动重试。**

```
outbox=COMPLETED（订单已落库，status=1 待支付）
        │
        │  POST /voucher-order/{id}/pay?payType=1..3
        │        └─ CAS 1→2（支付截止时间写在 SQL 条件里）
        │              └─▶ 待核销(2) ──工作人员核销 CAS 2→3──▶ 已核销(3)
        │
        └─ POST /voucher-order/{id}/cancel  或  900s 支付超时（CloseTask 每 5s 扫）
                 │
            CAS 1→7（关单中）+ 同一事务 MySQL stock + 1
                 │ 事务提交后
            close-voucher-order.lua：Redis stock + 1、srem 一人一单、
                                     hdel seckill:admitted、写 seckill:closed 幂等标记
                 │
            CAS 7→4 已取消（名额释放完毕）；失败则留在 7，close_retry 退避 5s..3600s 重试
```

## 1. 起点：`outbox=COMPLETED` 意味着什么

`VoucherOrderConsumer.createVoucherOrder` 在**同一个事务**里做了三件事：

1. `INSERT tb_voucher_order`，`status=1 待支付`（若该用户对该券已有 `已取消(4)` 的旧行，则复用该行、换成新 `orderId` 并重置字段）；
2. 条件扣 MySQL 库存 `UPDATE ... SET stock=stock-1 WHERE stock>0`，0 行则抛异常整笔回滚；
3. `UPDATE tb_seckill_message SET status=COMPLETED`。

所以 `COMPLETED` 只是「订单落过库」的凭证，**不代表订单当前状态**——它之后随时可能被支付或取消。
管理后台把 outbox 投递状态和 `tb_voucher_order.status` 分开列，就是防止用前者冒充后者。

从这一刻起，Redis 库存、MySQL 库存、outbox 三者已经对齐：
`MySQL库存 = Redis库存 + 非COMPLETED消息数`，而该 orderId 已 `COMPLETED`，两边各扣了 1。

## 2. ① 支付（`VoucherOrderLifecycleService.payMyOrder`）

接口 `POST /voucher-order/{id}/pay?payType=1..3`，`payType` 只接受 1/2/3。

**核心是一条带条件的 CAS UPDATE**（`VoucherOrderLifecycleService.java:79`）：

```sql
UPDATE tb_voucher_order
   SET status = 2 待核销, pay_type = ?, pay_time = NOW(), update_time = NOW()
 WHERE id = ? AND user_id = ? AND status = 1
   AND create_time > NOW() - INTERVAL 900 SECOND   -- 截止时间写进同一条 SQL
```

三个设计点：

- **截止时间放在 SQL 条件里**，而不是先 `SELECT` 判断再 `UPDATE`。避免「校验通过 → 期间被关单 → 仍然更新成功」的竞态。
- **用数据库时钟 `NOW()`**，多实例部署时应用机器时钟不一致也不会误判。
- **`user_id` 一起做条件**，越权改不了别人的单。

`updated == 0` 时**不能直接报失败**，必须重读数据库真值区分四种情况（`VoucherOrderLifecycleService.java:88`）：

| 重读后的 `status` | 判定 | 返回给用户 |
|---|---|---|
| 2 待核销 / 3 已核销 | 重复点击支付，幂等 | 成功（按当前订单展示） |
| 7 关单中 / 4 已取消 | 关单赢了这次竞争 | 失败：「订单已由关单流程锁定，本次支付未生效」 |
| 1 待支付 | 状态没变，「0 行」是因为支付时限已过 | 失败：「支付时限已过，订单正在关闭」 |
| 其它 / 查不到 | 结果未确认 | 失败：「支付结果未确认，请刷新订单状态」 |

成功时**再 `selectById` 一次**返回数据库真值，不拿内存对象拼结果（`VoucherOrderLifecycleService.java:105`）。

## 3. ② 关单（用户取消 / 支付超时）

两个入口，走**同一套** `markClosing`：

- 用户主动：`POST /voucher-order/{id}/cancel` → `cancelMyOrder`，`close_reason=USER_CANCEL`，不要求已超时；
- 支付超时：`VoucherOrderCloseTask`（每 5s 扫一次，批量 100）→ `closeExpiredOrder`，`close_reason=PAYMENT_TIMEOUT`，CAS 里额外要求 `create_time <= NOW() - 900s`。

### 3.1 关单受理：CAS `1 → 7` + 同事务恢复 MySQL 库存

`markClosing`（`VoucherOrderLifecycleService.java:179`）在一个事务里：

1. `UPDATE ... SET status=7, close_reason=?, close_retry=0 WHERE id=? AND status=1`；0 行直接返回 `false`（状态已被支付或别的关单改过）；
2. `UPDATE tb_seckill_voucher SET stock = stock + 1 WHERE voucher_id=?`；0 行则抛异常**回滚整笔**（不能出现「订单关了但库存没还」）。

**为什么先恢复 MySQL 再补偿 Redis**：Redis 是准入真值、MySQL 向它收敛。先落 MySQL 侧，即使 Redis 随后永久不可用，MySQL 也不会少库存；
短暂出现的 `MySQL > Redis` 正是库存对账任务要抓的漂移，重试补偿后自动消除。反过来做，若先加 Redis 再崩，MySQL 会长期少一个名额且无补偿路径。

### 3.2 事务提交后补偿 Redis（`close-voucher-order.lua`）

受理事务提交后（`afterCommit`，`VoucherOrderLifecycleService.java:211`）执行 Lua，一次原子完成四件事：

| 命令 | 作用 |
|---|---|
| `hexists seckill:closed:{v} orderId` | 已补偿过 → `return 0`（幂等出口） |
| `if exists stockKey then incrby stockKey 1` | 库存 key 还在才还名额（活动已结束就不还） |
| `srem seckill:order:{v} userId` | 移除一人一单标记，用户可重新抢 |
| `hdel seckill:admitted:{v} orderId` | 清准入记录，恢复任务不会再补 outbox |
| `hset seckill:closed:{v} orderId 1` + TTL | 记录「已补偿」幂等标记，TTL 跟随库存 key（无库存 key 时兜底 7 天） |

因为 Lua 原子且 `seckill:closed` 是幂等闸，**重复执行不会重复加库存**。Redis 返回 `1` 表示首次补偿，`0` 表示此前已补过。

补偿成功后 DB CAS `7 → 4 已取消`（`VoucherOrderLifecycleService.java:151`），到此名额完全释放。

### 3.3 补偿失败：停在 7 自动重试

Redis 不可用/超时 → 抛异常 → `recordCompensationFailure`：订单**保留 `status=7`**，
`close_retry + 1`，`close_next_retry_time = NOW() + 退避`，退避从 5s 起倍增至封顶 3600s。

`VoucherOrderCloseTask` 每 5s 除了扫超时订单，还会扫 `status=7 AND close_next_retry_time <= NOW()` 的订单重试补偿。
所以「关单最终一定完成」不依赖人工。

## 4. ③ 核销（简单）

核销码就是**订单号本身**（`VoucherOrder.id` 用 `@JsonSerialize(ToStringSerializer)` 按字符串下发，18 位雪花 ID 在 JS 里不丢精度）；
工作人员可带 `WS-` 前缀输入，后端 `parseVerificationCode` 会剥掉前缀并校验纯数字。

两个端点都在 `/monitor/**` 下，即**登录 + `X-Monitor-Token` 管理员令牌双重保护**（工作人员端）：

- `GET /monitor/workshop-orders/{code}`：按码查订单（`queryWorkshopOrderForRedeem`）；
- `POST /monitor/workshop-orders/{code}/redeem`：核销（`redeemWorkshopOrder`）。

核销就是一条 CAS（`VoucherOrderServiceImpl_kafka.java:163`）：

```sql
UPDATE tb_voucher_order
   SET status = 3 已核销, use_time = ?, update_time = ?
 WHERE id = ? AND status = 2 待核销
```

0 行则返回「订单状态已变化，请刷新后重试」；已核销的重复提交在读阶段就被拦下返回「该预约已经核销，请勿重复操作」，
并发重复提交由 CAS 保证只成功一次。核销**不改库存**（名额在支付时就已消耗），所以没有跨存储一致性问题。

用户侧查询是 `GET /voucher-order/me`（`queryMyOrders`），按 `create_time` 倒序返回本人全部订单，
并给待支付订单补一个**不落库**的 `paymentDeadline` 字段供前端倒计时。

## 5. 两套状态机（对照）

| `tb_seckill_message` 投递状态 | 含义 | `tb_voucher_order.status` | 含义 |
|---|---|---|---|
| 0 READY | 待投递 | 1 | 待支付 |
| 1 PROCESSING | relay 已抢占 | 2 | 待核销 |
| 2 SENT | 已到 Kafka | 3 | 已核销 |
| 3 COMPLETED | **订单已落库**（秒杀链路终点） | 4 | 已取消（名额已释放） |
| 4 FAILED | 慢速重试中，非终态 | 7 | 关闭处理中（待 Redis 补偿） |

订单生命周期：

```
         支付 CAS 1→2                 核销 CAS 2→3
待支付(1) ──────────▶ 待核销(2) ──────────────▶ 已核销(3)   终态
   │
   │ 取消/超时 CAS 1→7（同事务 MySQL 库存+1）
   ▼
关闭处理中(7) ── Redis 补偿成功 CAS 7→4 ──▶ 已取消(4)       终态
   ▲
   └── 补偿失败保留 7，close_retry 退避重试
```

> `COMPLETED` 与「订单状态」必须分开看：一笔订单在 `COMPLETED` 之后可以变成 2、3、4、7 中的任何一个。

## 6. 竞态与边界（重点）

| 场景 | 系统如何处理 | 用户看到 |
|---|---|---|
| **关单最后一秒支付** | 支付与关单都是 `WHERE status=1` 的 CAS，行锁串行化，**恰好一个成功**。支付赢 → `status=2`，关单 CAS 0 行跳过；关单赢 → `status=7`，支付 CAS 0 行后重读真值 | 支付赢：支付成功；关单赢：明确「本次支付未生效」，不会出现「付了又被关」 |
| 支付刚好卡在 900s 边界 | 截止判定在 SQL 条件里用 `NOW()` 比较，支付 `create_time > NOW()-900` 与关单 `create_time <= NOW()-900` 互补；即便边界上两条语句的 `NOW()` 差几毫秒，仍由 `status=1` 的 CAS 决定唯一赢家 | 有且仅有一个结果 |
| 用户重复点支付 | 第一次 CAS 成功；后续读阶段就识别为 2/3 直接返回成功；并发时第二个 CAS 0 行、重读为 2 也返回成功 | 幂等成功，不重复处理 |
| 用户重复点取消 | 已是 4/7 直接返回成功；并发时只有一个 CAS 1→7 成功，另一个 0 行返回「状态已变化」 | 取消一次生效 |
| 取消后重新抢同一张券 | 关单 `srem` 了一人一单、`hdel` 了准入，用户可再抢；DB 侧复用那条 `已取消(4)` 的旧行，换成**新 orderId**（`VoucherOrderServiceImpl_kafka.java:201`） | 能重新抢，且受唯一键约束仍是一人一单 |
| 关单补偿时 Redis 挂了 | DB 已在事务里恢复库存、订单停在 7；退避重试直到 Redis 恢复；`close_retry` 与 `close_last_error` 留痕 | 订单显示「关闭处理中」，不会丢名额也不会重复加 |
| 补偿重复执行（CloseTask 重试 + 用户重试叠加） | `seckill:closed` 哈希幂等，Lua 直接返回 0，不加库存 | 库存只释放一次 |
| 库存 key 已过期（活动结束） | Lua 里 `exists stockKey` 为假则**不**加 Redis 库存，但 MySQL 已 +1 | 活动已结束，Redis 无键可卖，MySQL 库存多出属预期 |
| 关单补偿失败期间 Redis 恢复后业务继续抢购 | 重试补偿时 `incrby` 会把名额还回；若期间已被别人买走，Redis 库存绝对值仍正确（只是顺序不同） | 库存总量不超卖 |
| 核销重复提交 / 并发 | CAS `2→3` 只成功一次，其余 0 行或读阶段拦截 | 只核销一次 |
| 越权支付/取消/核销 | 支付与取消在 CAS 条件里带 `user_id`；核销端点要求管理员令牌 | 404/失败，改不了别人的单 |

**唯一需要外部前提的**：Redis 补偿要能最终连上。长期不可用会积压 `status=7`（这正是 `orderClose.retrying` 告警要抓的）。
即便如此，因为 MySQL 库存已经在关单事务里恢复，**不会出现少卖名额**，只是 Redis 侧多卖风险窗口被冻结在补偿前的水平。

## 7. 库存恒等式在关单下的变化

```
MySQL库存 = Redis库存 + 未落库准入数
「未落库准入数」= 该券下 outbox 状态非 COMPLETED 的消息数
```

关单时 MySQL 与 Redis **各 +1**、outbox 仍 `COMPLETED`，所以恒等式两边同增，**不变**。
只有补偿尚未完成（MySQL +1 了、Redis 还没 +1）的窗口里，恒等式会短暂成立为 `MySQL = Redis + 1`，
即 `MySQL > Redis` 的漂移——这正是 `SeckillStockReconcileTask` 每 60s 要抓的、且**补偿完成会自动消除**的漂移。

## 8. 可观测性

- 时间线阶段（`tb_seckill_message_event`，按 orderId 还原）：
  `⑦ ORDER_CLOSE_REQUESTED`（关单受理，含用户取消/支付超时）→ `⑧ ORDER_CLOSED`（补偿完成、名额已释放）；
  补偿失败写 `⑧ ORDER_CLOSE_FAILED`（WARN）。
  支付本身不新增阶段码，通过订单状态从 1 变 2 体现。
- `GET /monitor/seckill` 与 `/seckill/pipeline` 里的 `orderClose`：`expiredPending`（已超时未关）、
  `closing`（停在 7）、`retrying`（7 且 retry>0）、`maxRetry`。
- **告警**：`SeckillStatsService` 每 30s 打快照，`retrying > 0` 时打 ERROR
  「关单补偿积压告警：N 笔订单停在 CLOSING 反复重试」。
- 管理后台 `admin.html` 的近期订单表**并列展示订单状态与投递阶段**，关单告警区标红。

## 9. 相关文件

| 关注点 | 路径 |
|---|---|
| 支付 / 取消 / 关单受理 / Redis 补偿 | `src/main/java/com/hmdp/service/VoucherOrderLifecycleService.java` |
| 超时扫描与补偿重试 | `src/main/java/com/hmdp/utils/VoucherOrderCloseTask.java` |
| 关单补偿脚本 | `src/main/resources/close-voucher-order.lua` |
| 订单落库（秒杀终点，含取消后复用行） | `src/main/java/com/hmdp/service/impl/VoucherOrderServiceImpl_kafka.java` |
| 消费与状态推进 | `src/main/java/com/hmdp/utils/VoucherOrderConsumer.java` |
| 订单实体与状态常量 | `src/main/java/com/hmdp/entity/VoucherOrder.java` |
| 用户端支付/取消接口 | `src/main/java/com/hmdp/controller/VoucherOrderController.java` |
| 核销与管理接口 | `src/main/java/com/hmdp/controller/SeckillMonitorController.java` |
| 关单监控与告警 | `src/main/java/com/hmdp/monitor/SeckillStatsService.java` |
| 时间线阶段名 | `src/main/java/com/hmdp/monitor/SeckillTimelineService.java` |
| 建表脚本 | `src/main/resources/db/voucher_order_close.sql` / `db/hmdp.sql` |
| 配置项 | `src/main/resources/application.yaml`（`workshop.order.*`） |
