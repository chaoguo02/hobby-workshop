# 面试问答稿（1 分钟口述版）

> 基于本项目（秒杀 + 支付 / 关单 / 核销 + 多级缓存）整理。
> 每题控制在 1 分钟左右口述篇幅，建议对着关键数字练到能自然说出口。
> 第 32、33 题不在本项目范围内，已标注处理方式，**不要把两个项目混着讲**。

## 目录

- 项目与技术总览：1–7
- 秒杀与订单生命周期：8–13
- 全局唯一 ID：14–16
- Kafka：17–25
- Lua 与一致性：26–27
- 缓存三兄弟：28–30
- 压测与限流：31、35、36
- 其他常见题：32–34
- 补充题（做了但没被问到）：37–46
- 补充专题：MySQL 行锁与系统设计：47–51

---

## 1. STAR 法则介绍项目

**S**：项目是一个本地生活服务平台，我负责其中的交易链路——秒杀下单、支付、关单、核销，核心难点是瞬时高并发下的库存不超卖和订单不丢。

**T**：我要保证「Redis 准入、MySQL 落库、Kafka 投递」三段在并发、重复投递、进程崩溃下最终一致。

**A**：用 Lua 原子完成库存扣减和一人一单，本地消息表做可靠投递，Kafka 削峰，消费者单事务落库，再补恢复、对账、关单三组定时任务自动兜底。

**R**：压测 1000 用户抢 200 张券，成功 200、库存不足 800、重复下单 0；MySQL 与 Redis 库存双双到 0，无负库存；修复脚本热读 fat jar 的锁竞争后吞吐从 288/s 提升到 981/s，P95 从 6869ms 降到 200ms。

---

## 2. 项目里使用线程池了吗

用了，但不是为了并发编排。主要有四处：Tomcat 请求线程池；HikariCP 数据库连接池；Kafka 消费者每实例 3 个消费线程（`concurrency: 3`，与 3 个分区对齐）；以及缓存重建用的自建 `ThreadPoolExecutor`——有界队列 256、daemon 命名线程、自定义拒绝策略记录并拒绝，之前用的是 `newFixedThreadPool`，无界队列在慢源下会把内存打爆。定时任务跑在 Spring 的单线程调度器上。没有用 `CompletableFuture` 做业务并行。

---

## 3. 项目里用了哪些设计模式

几个比较实在的：

- **策略模式**：`SeckillRetryPolicy` 把快/慢两段退避集中定义，relay 和对账任务共用，避免阈值漂移；缓存也有穿透/互斥/逻辑过期三套策略。
- **责任链**：拦截器链，刷新 token 的拦截器和登录校验拦截器按 order 串起来。
- **模板方法**：`ServiceImpl`、`TransactionTemplate`。
- **工厂**：`ConcurrentKafkaListenerContainerFactory`。
- **建造者**：`QueryWrapper / UpdateWrapper` 链式拼条件。
- **代理**：`@Transactional` 声明式事务。
- **观察者/回调**：`TransactionSynchronizationAdapter` 的 `afterCommit`，在事务提交后才做缓存失效和关单补偿。

---

## 4. 登录认证是如何实现的

token 存 Redis。登录成功时生成随机 token，把用户信息以 Hash 存到 `login:token:{token}`，设置 TTL，token 返回前端，之后请求都带 `authorization` 头。

服务端用两个拦截器：`RefreshTokenInterceptor` 对所有路径生效，从请求头拿 token、查 Redis 的 Hash、转成 `UserDTO` 存进 `ThreadLocal`，并把 TTL 重置一次实现滑动续期；`LoginInterceptor` 只对需要登录的路径生效，它不碰 Redis，只看 `ThreadLocal` 里有没有用户，没有就返回 401。哪些路径免登录统一配在 `excludePathPatterns` 里。

---

## 5. 为什么不用 JWT

主要是**可控性**。JWT 是无状态的，签发出去就收不回来——用户登出、改密码、被封号、后台强制下线都做不到，只能等过期，或者再维护一个黑名单，那就又变成有状态了。我们 token 存 Redis，删 key 就能立刻踢人。

另外 JWT payload 是明文且随请求变大，密钥轮换也麻烦，而续期还得额外搞 refresh token。我们的流量规模完全能承受一次 Redis 查询，无状态的性能优势用不上，所以选了 Redis token。

---

## 6. 为什么要将用户信息存入 ThreadLocal

主要是**避免参数层层传递**。Controller、Service、ServiceImpl 深层都要拿当前用户，如果不用 ThreadLocal，每个方法签名都得挂一个 `UserDTO`。ThreadLocal 提供请求级上下文，拦截器写一次，任意深处 `UserHolder.getUser()` 直接取。

关键是**必须在 `afterCompletion` 里 remove**——Tomcat 线程是池化复用的，不清理的话下一个游客请求会读到上一个用户的身份，造成越权。另外要注意 ThreadLocal 不跨线程，用 `@Async` 或线程池时得显式传参或用 `TaskDecorator`。

---

## 7. SpringMVC 的流程

请求进来先到 `DispatcherServlet`，它拿 `HandlerMapping` 找到对应的 Handler 和拦截器链，然后交给 `HandlerAdapter` 执行。

执行前先按 order 调各拦截器的 `preHandle`——我们的 token 刷新和登录校验就在这一步。然后适配器调用 Controller 方法，参数由 `HandlerMethodArgumentResolver` 解析，返回值由 `HandlerMethodReturnValueHandler` 处理：带 `@ResponseBody` 的走 `HttpMessageConverter` 直接写 JSON，否则解析成 `ModelAndView` 交给 `ViewResolver` 渲染。最后逆序执行 `postHandle` 和 `afterCompletion`，我们的 `UserHolder.remove()` 就在 `afterCompletion` 里。

---

## 8. 秒杀下单到支付完成的完整流程

入口 `POST /voucher-order/seckill/{id}`，先生成 orderId，执行 Lua 原子做三件事：查库存、一人一单、扣 Redis 库存并把 orderId 和 userId 写进 `seckill:admitted`。然后请求线程写一条 outbox 记录，返回 orderId——**这一步失败也返回成功**，因为 Redis 才是准入真值。

Relay 每 500ms 批量抢占 outbox 并发到 Kafka，消费者单事务里插订单、条件扣 MySQL 库存、把 outbox 置 `COMPLETED`，然后手动 ack。之后是订单生命周期：支付 CAS `1→2 待核销`，核销 CAS `2→3`；或者用户取消/900 秒超时走关单 `1→7`，同事务恢复 MySQL 库存，提交后跑 Lua 补 Redis，再 CAS `7→4`。

---

## 9. 如果数据库的订单没有生成怎么办

Redis 的 `seckill:admitted` 记录了「哪个 orderId 被准入」，这是可信根。恢复任务每 30 秒用 HSCAN 扫所有 admitted 记录，拿 orderId 批量查 outbox 表，缺行的就补插一条 READY，重新走正常投递链路；已经 `COMPLETED` 的就把 admitted 里的 field 删掉防止无限膨胀。

所以不管是请求线程写 outbox 失败，还是消费者落库前进程崩溃，最终都会被补回来。隐含前提是 Redis 必须开持久化，否则这条兜底路径本身也没了。

---

## 10. Redis 库存扣减了但数据库库存没扣减怎么办

这是正常会出现的中间态，不是 bug。因为 Redis 是准入真值、MySQL 单向向它收敛，所以设计上允许「Redis 已扣、MySQL 还没扣」。

outbox 里 `status` 非 `COMPLETED` 的消息数就是「已准入但未落库」的数量，恒等式是 `MySQL 库存 = Redis 库存 + 未落库准入数`。这些消息要么被 relay 重投、要么被对账任务从卡死状态重置后重投，落库时在一个事务里扣 MySQL 库存，恒等式自动恢复。对账任务每 60 秒核对，只告警不改数。

---

## 11. 用户支付时对应的数据库记录没有生成怎么办

我们的接口流程保证了不会出现「支付时订单不存在」——秒杀接口返回给用户的只是 orderId，前端不认为订单已生效，要先查到订单才展示支付入口。

实现上：秒杀成功后前端轮询订单状态，查到 `status=1 待支付` 才出现支付按钮；如果还没落库，展示的是「订单生成中」。就算用户在极端情况下直接调支付接口，`payMyOrder` 第一件事是 `selectById`，查不到直接返回「预约订单不存在」，不会误改任何数据。这也是我们把「抢购成功」和「订单生成成功」在 UI 上分成两个状态的原因。

---

## 12. 订单超时如何自动关闭，几种方案优缺点怎么优化

我们用**定时任务扫描**：`VoucherOrderCloseTask` 每 5 秒扫一次，每次批量 100，条件把「`status=1` 且 `create_time <= NOW()-900s`」写进 SQL，用数据库时钟，多实例也不会重复处理。

对比其他方案：

- **Redis 过期事件**：实现最简但有坑——事件不可靠、不保证送达、Redis 重启就丢，不能用于交易。
- **延迟队列**（RocketMQ 延迟消息、RabbitMQ TTL+死信）：时效准、不用轮询，但要引入额外组件和运维成本。
- **Redis ZSet 时间轮**：自己实现延时队列，精度好、无需扫描，但需处理重启恢复和消息去重。

优化方向：`idx_status_create_time` 索引保证扫描高效；批量 + 条件 UPDATE 减少锁竞争；关单补偿失败停在 7 状态、带指数退避自动重试，所以「关单最终一定完成」不依赖人工。规模再大可以把扫描按 voucherId 或时间分片，或换时间轮减少空扫。

---

## 13. 支付回调和超时关闭出现并发怎么办

**两条路争的是同一个 `status=1`，靠 CAS 决出唯一赢家。**

支付是 `UPDATE ... SET status=2 WHERE id=? AND user_id=? AND status=1 AND create_time > NOW()-900s`，截止判断也写在同一条 SQL 里；关单是 `UPDATE ... SET status=7 WHERE id=? AND status=1 AND create_time <= NOW()-900s`。行锁把两条语句串行化，恰好一个影响行数为 1。

支付 0 行时**不能直接报失败**，要重读数据库真值区分四种情况：状态已是 2/3 就是重复点击，幂等返回成功；是 7/4 说明关单赢了，明确告诉用户「本次支付未生效」；还是 1 说明是超时。这样不会出现「付了又被关」，两边也各只有一种确定结果。实测 200 笔超时关单，`close_retry` 全为 0，MySQL 库存精确 +200。

---

## 14. 全局唯一 ID 生成

我们用的是**基于 Redis 自增的全局 ID**：`timestamp << 32 | 序列号`。序列号来自 Redis 的 `INCR icr:order:{日期}`，高 32 位是相对某个基准时间戳的秒数，低 32 位是当天自增序号，拼接成一个 long。

这样 ID 全局唯一、趋势递增（对 B+ 树索引友好），而且不依赖数据库自增，多实例下天然安全。因为 ID 是随秒数递增、同秒内由 Redis 保证唯一，所以不需要机器号。

注意这个方案**不是雪花算法**——雪花是本地生成、靠机器位区分，我们是靠 Redis 集中发号。

---

## 15. 雪花算法是什么

雪花算法是 Twitter 提出的分布式 ID 方案，用 64 位 long：1 位符号位固定为 0，41 位毫秒时间戳（约能用 69 年），10 位机器 ID（支持 1024 个节点），12 位同毫秒内序列号（单节点每毫秒 4096 个）。

特点是不依赖任何外部组件、本地生成、性能极高，且整体趋势递增。因为靠机器位来区分不同节点，理论上不需要协调，是分布式 ID 的经典方案。

---

## 16. 雪花算法存在的问题

三个主要问题：

- **时钟回拨**：机器时间往回跳可能生成重复 ID，需要检测回拨并等待或抛错。
- **机器 ID 分配**：10 位机器位必须保证集群内唯一，动态扩缩容时得有个统一分配机制，否则冲突。
- **精度问题**：64 位 long 超过 JavaScript 的 `Number.MAX_SAFE_INTEGER`（2^53），直接按数字下发前端会丢精度。我们项目里所有订单 ID 都强制序列化成字符串下发，就是为了这个。

另外它是趋势递增不是严格递增，某些依赖严格有序的场景要注意。

---

## 17. Kafka 如何保证可靠性

分三段防：

- **发送端**：`acks=all` 等所有 ISR 副本确认，开启重试，并打开幂等生产者（`enable.idempotence`，注意在 Spring Boot 2.3 里必须写在 `properties` 下，否则被忽略）。
- **Broker 端**：靠副本机制，配合 `min.insync.replicas` 保证写入至少落到多个副本。
- **消费端**：手动 ack，业务成功才提交位点，失败由 `SeekToCurrentErrorHandler` 每 1 秒重试、共 4 次，仍失败进死信队列。

更关键的是上游还有一层保险：我们不是直接发 Kafka，而是先写本地消息表（outbox），relay 抢占后再投递，发送失败能重投，进程崩溃后能扫描补投——**这是保证不丢消息的关键**。

---

## 18. Kafka 消息堆积怎么处理

分两个层面。

**提高消费能力**：topic 从 1 个分区扩到 3 个分区，消费者 `concurrency` 同步改成 3，并行消费；key 从 voucherId 改成 orderId——因为订单之间无序依赖，按 orderId 打散能让同一张热门券的消息分布到多个分区并行处理，而不是全挤在一个分区。

**提高投递效率**：relay 改成异步发送不阻塞、`fixedDelay` 降到 500ms、抢占从逐条 autocommit 改成单事务批量 `claimBatch`——逐条提交每条都要一次 redo fsync，实测 100 条从 933ms 降到 172ms。整体 100 条突发从 3030ms 降到 1618ms。

剩余瓶颈是同一张券的 MySQL 库存行锁，这是单券天生热点，再往上要库存分片或合并批量扣减。

---

## 19. Kafka 如何保证幂等性

两处。

**生产端**开 `enable.idempotence=true`，配合 `acks=all`，Broker 用 PID + 序列号去重，防止生产者重试导致单分区内重复写入。

**消费端**是重点，因为 at-least-once 下重复消费不可避免，业务必须自己幂等：我们以 orderId 做主键，消费前先 `getById(orderId)` 查订单是否已存在，存在就只补 `COMPLETED` 状态然后 ack；并发情况下靠主键冲突兜底，捕获 `DuplicateKeyException` 后再查一次，确实存在就按幂等重放处理。所有状态推进也都是带状态条件的 UPDATE，重复执行影响行数为 0。

---

## 20. Kafka 如何保证顺序消费

Kafka 只保证**同一分区内**有序，所以关键是 key 的选择：相同 key 的消息会落到同一分区。如果要保序，就把 key 设成业务实体 ID，并让消费者单线程消费该分区。

但**我们这个场景故意不保序**：key 用的是 orderId 而不是 voucherId，因为订单之间是独立的——主键唯一、`uk(user_id,voucher_id)` 唯一、Redis 一人一单，三重保证下订单之间没有顺序依赖。打散之后同一张热门券的消息能分布到多个分区并行消费，吞吐明显更高。

所以「顺序」要用在业务真正需要的地方，我们这里不需要。

---

## 21. Kafka 消费时如何保证扣减库存与写订单的一致性

把三个动作放进**同一个本地事务**：插入订单（或者复用已取消的旧行并换新 orderId）、条件扣减 MySQL 库存 `UPDATE ... SET stock=stock-1 WHERE voucher_id=? AND stock>0`、把 outbox 置 `COMPLETED`。库存扣减影响行数为 0 时直接抛异常，整个事务回滚，不会出现「扣了库存没订单」或「有订单没扣库存」。

`COMPLETED` 跟订单落库同事务提交，所以它才是链路终点——`SENT` 只代表消息到了 Kafka，不代表订单落了库。日志和监控里也一直把这两个状态分开看。

---

## 22. 先扣库存还是先写订单，为什么

我们**先写订单，再扣库存**。原因有三个：

1. 订单的 INSERT 是主键幂等闸，重复投递在这里就会抛 `DuplicateKeyException`，天然挡住后续动作，避免多余操作；
2. 一人一单的 `uk` 冲突也在这一步暴露，语义最清晰；
3. 库存扣减是「条件校验」，`WHERE stock>0` 影响行数为 0 就抛异常回滚，放在最后抛最干净。

需要说明的是，**同一个事务内两种顺序的原子性是一样的**，都能回滚，顺序主要影响的是锁竞争和错误暴露点。我们的选择是让幂等和业务约束尽量靠前暴露。

---

## 23. 如何保证高并发下库存不超卖

**两道防线。**

第一道是 Redis，用 Lua 脚本把「查库存 + 判断一人一单 + 扣库存 + 记录准入」合并成一次原子执行，中间没有任何其他命令能插队，所以不可能超卖。

第二道是 MySQL，扣库存时不是读出来再算，而是 `UPDATE ... SET stock=stock-1 WHERE voucher_id=? AND stock>0`，把判断和更新放在同一条 SQL 里，靠行锁保证并发安全，影响行数为 0 就回滚。即使 Redis 那层出了问题，MySQL 也守得住。

实测 1000 个用户抢 200 张券：成功 200、库存不足 800、重复下单 0，MySQL 和 Redis 库存双双到 0，全库负库存为 0。

---

## 24. 如何保证高并发场景下一人一单

**三层。**

1. Redis 的 `sismember seckill:order:{voucherId} userId` + `sadd`，写在 Lua 里和库存扣减同一次原子执行，并发下只会有一个成功。
2. 数据库唯一键 `uk(user_id, voucher_id)`，这是最终闸门，绕过 Redis 也挡得住。
3. 消费端幂等，重复投递不会落第二笔。

有个细节：用户取消订单后要能重新抢，所以我们不能简单靠唯一键拒绝——关单时 Redis 侧 `srem` 掉一人一单标记，DB 侧则复用那条已取消的旧行、换上新 orderId，`(user_id, voucher_id)` 不变所以唯一键始终不冲突。

实测同一个用户 200 并发抢：成功 1 次，其余 199 次全部返回「不能重复下单」，库存只减了 1。

---

## 25. Kafka 异步落库的整体流程

入口 Lua 成功后，请求线程写一条 outbox 记录 `status=READY` 就返回 orderId。Relay 每 500ms 扫 `READY/FAILED` 且退避到期的消息，在一个事务里批量抢占为 `PROCESSING`（多实例并发只有一个抢到），然后异步发 Kafka，key 用 orderId。发送成功回调把状态置 `SENT`，失败按退避策略置回 `READY` 或慢速通道 `FAILED`。

消费者 3 个并发、手动 ack，先查订单是否存在做幂等预检，不存在就单事务插订单、扣库存、把 outbox 置 `COMPLETED`，然后 ack。异常重试耗尽进 `voucher-orders.DLT`。

再往外还有兜底：恢复任务补缺失的 outbox，对账任务重置卡死的消息，人工重放只是加速手段。

---

## 26. 为什么要用 Lua 脚本

为了**原子性**。秒杀准入要做「查库存、判断一人一单、扣库存、记录准入」四件事，如果分成四次 Redis 调用，两次调用之间会有其他请求插进来，就会出现超卖或者一人多单。Lua 脚本在 Redis 里是单线程原子执行的，中间不会被插入其他命令，这四步变成了一个不可分割的操作。

顺带说一个性能收获：脚本加载方式我们踩过坑。最初用 `setLocation(ClassPathResource)`，`DefaultRedisScript` 每次执行都会重新读 fat jar 里的脚本文件、重算 SHA1，读取要过 `JarFile` 的全局锁，把并发全部串行化，吞吐被硬顶在 420/s。改成静态初始化时读一次、用 `setScriptText` 常驻内存后，SHA1 只算一次，吞吐提到 981/s，P95 从 6869ms 降到 200ms。

---

## 27. Redis 和 MySQL 中的数据如何保持一致

核心原则是**单向收敛**：Redis 是准入真值且永不回滚，MySQL 向 Redis 收敛。

库存上的恒等式是 `MySQL 库存 = Redis 库存 + 未落库准入数`，其中未落库准入数就是该券下 outbox 状态非 `COMPLETED` 的消息数。正常投递时两边各扣 1，恒等式成立；关单时 MySQL 和 Redis 各加 1，恒等式两边同增，依然成立。

补偿顺序上坚持**先恢复 MySQL 再补偿 Redis**：关单先在事务里把 MySQL 库存加回来，提交后再跑 Lua 补 Redis。这样即使 Redis 永久不可用，MySQL 也不会少库存，只会短暂出现 `MySQL > Redis` 的漂移——这正是对账任务要抓的、且补偿完成后会自动消除。

对账任务每 60 秒核对一次，**只告警不自动改数**，避免自动改数掩盖真实问题。

---

## 28. 缓存穿透如何解决

穿透是查一个**数据库里也不存在**的数据，缓存永远不命中，请求全打到数据库。

我们用的是**空值哨兵**：查不到就写一个空字符串 `""` 进缓存，TTL 设短一些（2 分钟），这样同一个恶意 ID 的重复请求会被缓存挡住。判断上统一用 `json != null` 来区分「key 不存在」和「key 存在但值为空」。

没有用布隆过滤器，因为我们的店铺数据量不大，而且布隆有误判、删除困难、还需要额外维护同步，性价比不高。另外在入口做了参数校验，非法 ID 直接在 Controller 层挡掉，不进入查缓存流程。

---

## 29. 缓存击穿如何解决

击穿是某个**热点 key 突然过期**，大量并发同时回源打数据库。

我们实现了两种策略：

- **互斥锁重建**：只让一个线程去查库重建，其他线程短暂等待后重试。锁用 `SETNX` 加 TTL，释放时用 Lua 比对 value 再删（`if get==ARGV then del`），避免误删别人的锁；获取失败时用有上限的循环加重试，而不是递归，防止栈溢出。
- **逻辑过期**：key 不设物理过期，value 里带逻辑过期时间，过期后由一个线程异步重建，其他线程先返回旧数据，这样不会有请求阻塞。

两个细节：重建前要 Double Check，异步重建的异常要记录不能无声吞掉；第一次查缓存未命中时，同步回源并返回真实数据，而不是写完缓存返回 null。

---

## 30. 缓存雪崩如何解决

雪崩是**大量 key 在同一时刻集体失效**，或者 Redis 直接宕机，流量全压到数据库。三个措施：

1. **TTL 加随机抖动**，在基础过期时间上浮动 ±10%，避免同一批 key 同时过期；
2. **多级缓存**，加了 L1 的 Caffeine 本地缓存（1 万个容量、TTL 30 秒），Redis 出问题时本地还能兜一段，而且 L1 命中完全不碰 Redis；
3. **逻辑过期**，热点数据不设物理过期，从根本上避免集体失效。

另外重建缓存用的线程池改成了有界队列 + 拒绝策略，防止故障时无界堆积把内存打爆。

---

## 31. 项目做过压测吗

做过，用 JMeter，参数化线程数和循环数，也做了持续并发的对照。几个关键结果：

修复 Lua 脚本热读 jar 的锁竞争后，**10000 次请求 10 秒完成，981/s，平均 57ms，P95 200ms，峰值 1634 QPS，0 错误**，修复前是 288/s、P95 6869ms。持续高并发下峰值到过 4338 QPS，但 P95 升到约 700ms，瓶颈是每笔准入一次同步 MySQL 插入加上消费者堆积竞争。

功能验证上做了三组专项：1000 用户抢 200 张券、同一用户 200 并发、200 笔超时关单。结果都是全绿的——订单数等于去重用户数、MySQL 和 Redis 库存精确对得上、全库无负库存、outbox 全部 `COMPLETED`、无订单卡在中间态。

---

## 32. 智能客服是怎么实现的

**这题不在当前这个项目里**，我们的交易链路没有客服模块。

如果简历或另一个项目里写了，回答框架可以是这样：客服本质是「意图识别 + 知识检索 + 生成」，用 RAG——把 FAQ、订单规则、售后政策向量化存进向量库，用户问题先做意图分类，再检索 Top-K 相关片段，拼进 prompt 交给大模型生成回复；与业务系统打通的部分用 Function Calling，比如「查订单状态」这类问题让模型调用真实接口而不是编造。

如果面试官问的是本项目，建议直接说明「客服不在我负责的模块」，然后把话题引回交易链路，不要硬答。

---

## 33. 项目里用了 MCP 吗，用了 Skill 吗

**这个项目没有**——MCP 和 Skill 属于 AI Agent 那一侧，我们做的是交易后端。

如果面试官问的是我参与的另一个 Deep Research Agent 项目，那可以答：知识检索是通过 MCP Tool 接入的，把图书、论文等外部知识服务统一封装成 Agent 可调用的工具，适配层统一了请求参数和返回结构（转成 `KnowledgeChunk`），并把外部错误映射成统一错误码，让 Agent 能区分「查不到」和「查询失败」。Skill 是把高频工作流封装成可复用能力。

**建议：如果本项目没有，就明确说不属于这个项目，不要把两个项目混着讲**，容易被追问穿。

---

## 34. 订单生成失败了怎么跟用户表达

我们的接口语义是：秒杀接口返回 orderId **只代表 Redis 准入成功**，不代表订单已生成。所以前端拿到 orderId 后展示的是「抢购成功，订单生成中」，然后轮询查询订单；查到 `status=1 待支付` 才展示支付入口。

这个设计的原因是我们允许 outbox 写失败也返回成功——因为 Redis 是准入真值，恢复任务会补。所以正常情况下用户几秒内就能看到订单，短时的「生成中」是预期行为，而不是错误。

**这里有个可以改进的点**：如果消息最终进了死信、长时间没有落库，用户会一直卡在「生成中」。更完善的做法是给任务加一个超时上限，超过就明确提示「订单生成失败，名额已释放，请重试」，并给客服入口，而不是让用户无限等待。

---

## 35. 限流算法有哪些

常见四类：

- **固定窗口**：按时间窗口计数，简单但有临界问题，窗口边界可能放过两倍流量；
- **滑动窗口**：用 Redis ZSet 记录请求时间戳，精确但内存开销大；
- **漏桶**：请求先入桶按固定速率流出，能削峰但无法应对突发；
- **令牌桶**：按速率发令牌，请求拿令牌才放行，允许一定突发，是最常用的。

工程上还有基于响应时间的自适应限流（如 Sentinel 的 BBR）。

我们项目本身**没有单独做应用层限流**，因为秒杀场景的库存数量本身就是上限，Lua 原子准入会挡掉超出的请求，一人一单也限制了单用户刷量。如果要加，我倾向于在网关层用 Nginx 的 `limit_req` 做粗粒度兜底，再在应用层用 Redis + Lua 实现按用户的令牌桶。

---

## 36. 你们是如何限流的

说实话**没有做传统限流**，而是用**业务漏斗**天然限流：第一层每人只能抢一单（`sismember` 挡住重复请求），第二层库存只有 N 张，Lua 里 `库存 <= 0` 直接返回「库存不足」，所以真正进入落库链路的请求量天然被压在库存数量以内。这也是这个场景不需要额外限流的原因——**限流的上限就是库存**。

如果未来要加，我会分两处：入口用 Nginx `limit_req` 做 IP 级粗限流，注意反代后 `remoteAddr` 恒为 127.0.0.1，必须取 `X-Forwarded-For`；应用层用 Redis + Lua 做**按 userId** 的令牌桶，因为秒杀场景下按 IP 限流会误伤同一 NAT 下的正常用户，而且挡不住换 IP 的脚本。真正需要关注的是单券的 MySQL 库存行锁，那是物理串行点。

---

# 补充题（项目做了但没被问到）

## 37. 消息丢失怎么防

分三段：生产端 `acks=all` + 重试 + 幂等生产者；broker 端多副本和 `min.insync.replicas`；消费端手动 ack、成功才提交位点。

但我们真正的保险是**本地消息表**：先写 DB 再发 MQ，发送失败重投，进程崩溃后扫描补投，所以即使某一段丢了消息，也能从 outbox 里捞回来。另外 Redis 的 `seckill:admitted` 是最后一道——它记录每笔准入，就算 outbox 也丢了还能补。

---

## 38. 分布式锁怎么实现，为什么最后不用 Redisson

两种做法我都写过。基础版是 `SET key value NX EX` 加锁，value 存一个随机 token，释放时用 Lua 比对 token 再删，避免误删别人的锁；配上重试和过期时间。生产级要考虑可重入、看门狗自动续期、锁等待和公平性，这些 Redisson 都封装好了。

但我们项目**最后移除了分布式锁**——因为我们不靠锁来防超卖，而是把「判断 + 扣减」合并进 Lua 原子执行，再加 MySQL 条件扣减，从机制上就不需要锁。少一个组件就少一份故障面，锁的续期、误删、锁超时这些问题都不存在了。**能用原子操作解决的，就不要引入锁。**

---

## 39. 接口幂等性是怎么保证的

统一原则是**所有状态迁移都是 `WHERE 主键=? AND status=旧状态` 的条件 UPDATE**，重复执行影响行数为 0，输的一方重读真值再决策。

具体到各环节：秒杀准入靠 Lua 的 `sismember`；订单落库靠 orderId 主键加消费前存在性预检；支付靠 `status=1` 的 CAS，重复点击读到已支付就幂等返回成功；关单补偿靠 `seckill:closed` 哈希标记，重复执行 Lua 直接返回 0；人工重放也带状态条件。

---

## 40. 缓存与数据库的一致性怎么保证

**更新数据库后删缓存，而不是更新缓存**（更新缓存在并发下容易写进旧值）。删除时机放在事务的 `afterCommit` 里，用 `TransactionSynchronizationAdapter` 注册，避免「事务还没提交就先删了缓存，读请求又把旧值刷回来」。

多实例下还要处理本地缓存：删完 Redis 后通过 Pub/Sub 广播，其他实例清掉自己的 L1。L1 是最终一致，脏读窗口上界就是 L1 的 TTL（30 秒）。

延迟双删也是常见方案，但会引入不确定的睡眠时间，我们没采用。缓存一致性没有 100% 的方案，只能缩短窗口。

---

## 41. 为什么不用布隆过滤器解决穿透

布隆能高效判断「一定不存在」，但有几个成本：

- **有误判率**，判断存在的其实可能不存在，对于要求准确的场景要额外兜底；
- **不支持删除**（可以用计数布隆，但更复杂），而我们的数据会变；
- **需要维护同步**，新增数据要写、删除数据要处理，多了一层不一致风险。

我们数据量不大，**空值哨兵 + 短 TTL** 已经够用，实现简单、没有额外组件。选型上我还是倾向于够用就好。

---

## 42. Redis 持久化怎么选

RDB 是定时快照，文件小、恢复快，但两次快照之间的数据会丢；AOF 是追加日志，可以做到秒级不丢（`everysec`），但文件大、恢复慢。生产上一般是**两者都开**，重启优先用 AOF 恢复，AOF 定期重写控制体积。

**我们项目对这一点特别敏感**：`seckill:admitted` 是兜底机制的「可信根」，如果 Redis 丢了这部分数据，恢复任务就无从补起，已准入的订单会永久丢失。所以 Redis 必须开启持久化，我在文档里也把这条写成了明确的隐含前提。

---

## 43. @Transactional 什么情况下会失效

常见几种：

- **自调用**：同一个类里 A 方法直接调 B 方法，不经过代理，B 上的 `@Transactional` 完全不生效；
- **方法不是 public**，Spring 的代理拦不到；
- **异常被吞掉**，或者抛的是受检异常而没配 `rollbackFor`（默认只回滚 `RuntimeException` 和 `Error`）；
- **多线程**，事务上下文不跨线程；
- **数据库引擎不支持事务**。

我们项目里有一条刻意的约束：**Kafka 消费者的方法上不能加 `@Transactional`**，因为业务事务必须落在 `createVoucherOrder` 里面，消费者外层负责的是 ack 和事件记录。事件记录还必须等事务提交后再写，用独立连接，否则回滚时会留下假的成功事件。

---

## 44. 项目的可观测性是怎么做的

三层。

**时间线**：`tb_seckill_message_event` 是 append-only 的事件表，每笔订单从 `REDIS_ADMIT`、`OUTBOX_WRITTEN`、`RELAY_CLAIMED`、`KAFKA_SENT`、`CONSUMER_RECEIVED`、`ORDER_COMMITTED` 到关单阶段全记录，通过时间线接口能还原单笔订单的完整经过，带相对耗时。

**指标**：outbox 各状态计数、最老 READY/FAILED 时长、端到端延迟、Kafka consumer lag、DLT 堆积量、关单积压。

**告警**：每 30 秒打一次快照，`failedCount > 0` 或关单重试数大于 0 就打 ERROR 日志。管理后台把这些画成漏斗和近 20 笔订单的流转表，一键重放失败消息。

---

## 45. 项目中遇到最难的问题是什么

是**吞吐被硬顶在 420/s**。现象很怪：加并发没用，Tomcat 线程大量空闲，CPU 才 0-4%，jstack 在压测中抓也看不到明显热点。

最后定位到 Lua 脚本的加载方式——`DefaultRedisScript.setLocation(ClassPathResource)` 每次执行都会 `isModified()` 检查并重新读 fat jar 里的脚本文件来算 SHA1，读 jar 要过 `JarFile` 的全局锁，Windows 上还叠加文件时间戳检查。**所有请求被这一把锁串行化了**，吞吐约等于 1/(每请求读 jar 的耗时)。

改法很简单：类静态初始化时把脚本读成字符串一次，用 `setScriptText` 让脚本常驻内存，SHA1 只算一次。吞吐从 288/s 提到 981/s，P95 从 6869ms 降到 200ms。

这个问题的收获是：**性能问题不一定要靠加机器和调参，先看清楚有没有隐蔽的全局锁**；而且 jstack 必须在压测进行中抓，跑完再抓全是 idle。

---

## 46. 项目单机，如果部署多实例会有什么影响

我们的服务本身是**无状态**的，真值都在 Redis 和 MySQL，所以多实例安全。逐个看：全局 ID 来自 Redis `INCR`，不会冲突；秒杀准入是 Lua 原子，多实例天然互斥；outbox 投递用条件 UPDATE 抢占，只有一个实例能抢到；消费者用同一个 group，分区自动分配；关单和支付都是带状态的 CAS。

要注意的是四点：

1. **每个实例都会跑所有 `@Scheduled` 定时任务**，N 个实例就是 N 倍扫描，虽然靠条件 UPDATE 保证正确，但浪费资源，规模大了要上 ShedLock 或选举；
2. **Kafka 分区数成了并发上限**，3 个分区配 3 个实例刚好，实例再多就有消费者空跑；
3. **本地缓存 L1 需要跨实例失效**，目前靠 Pub/Sub 广播；
4. **连接池和线程池都要乘以实例数**，数据库和外部依赖要能承受。还有 Nginx 上游要改成一个实例组。

---

# 补充专题：MySQL 行锁与系统设计

## 47. MySQL 的行锁是怎么回事，为什么条件 UPDATE 能防并发

**一句话：InnoDB 的行锁是加在索引记录上的，不是加在数据行上的；条件 UPDATE 靠行锁把并发串行化，靠当前读拿到最新值。**

锁的类型：

| 锁 | 作用 |
|---|---|
| Record Lock（记录锁） | 锁住一条索引记录 |
| Gap Lock（间隙锁） | 锁住索引记录之间的间隙，阻止插入 |
| Next-Key Lock（临键锁） | 记录锁 + 间隙锁，左开右闭区间 `(a, b]` |
| Insert Intention Lock | 插入意向锁，插入时对间隙加，与 Gap Lock 冲突 |

加锁范围取决于**索引和隔离级别**：

- **唯一索引等值命中** → 退化为 Record Lock，只锁这一行；
- **唯一索引等值未命中** → 加 Gap Lock；
- **非唯一索引或范围查询** → Next-Key Lock，会锁住一段区间；
- **RR（默认）才有 Gap Lock**，RC 下基本只有记录锁。

最关键的一条：**如果 WHERE 条件没走索引，InnoDB 会扫描全表并锁住所有扫到的记录（含间隙），表现为表锁。** 所以线上更新语句必须走索引，否则一条 UPDATE 能把整张表锁住。

**快照读 vs 当前读**：普通 `SELECT` 走 MVCC 是快照读，不加锁；`UPDATE / DELETE / INSERT / SELECT ... FOR UPDATE` 是当前读，会加锁并且**读到的是最新已提交版本**。

回到秒杀场景：

```sql
UPDATE tb_seckill_voucher SET stock = stock - 1 WHERE voucher_id = ? AND stock > 0;
```

`voucher_id` 是主键，等值命中 → 只加 Record Lock 锁这一行。并发时 T1 先拿到锁把 100 改成 99，T2 在锁上等待；T1 提交释放锁后，**T2 重新读取最新值 99**，再判断 `stock > 0`，成立才改成 98。这就是「条件 UPDATE 不会超卖」的原理——判断和更新在同一条语句里，中间没有窗口。如果 voucher_id 没索引，这条语句会锁全表，秒杀直接退化成串行。

状态迁移同理：`UPDATE tb_voucher_order SET status = 2 WHERE id = ? AND status = 1`，`id` 是主键，锁单行，CAS 恰好一个成功。

三个实践要点：

1. **行锁持有到事务提交才释放**，所以事务要短，千万不要在事务里做 RPC、HTTP 或批量计算。
2. **死锁**来自加锁顺序不一致（不同事务以不同顺序更新多行）。InnoDB 有死锁检测，会回滚代价小的事务；业务上要统一加锁顺序，或按主键排序后批量更新。
3. **排查手段**：`SHOW ENGINE INNODB STATUS` 看 `LATEST DETECTED DEADLOCK`；`performance_schema.data_locks / data_lock_waits` 看当前锁等待；`innodb_lock_wait_timeout` 控制等待超时。

---

## 48. 如何设计一个秒杀系统（系统设计题）

**先说不变量：不超卖、不少卖、不一人多单、不丢单。** 然后分层解决。

### 第一层：客户端与网关

- 秒杀开始前**按钮置灰**，用倒计时对齐服务端时间，避免无效请求；
- 加答题、验证码、滑块，把脚本和真实用户分开；
- 静态资源 CDN 化，页面静态化，别让前端把压力打到应用；
- 网关层限流（Nginx `limit_req`）+ 黑名单，注意反代后要取 `X-Forwarded-For`。

### 第二层：Redis 做准入（核心）

活动开始前把库存预热到 Redis，并设 TTL。请求进来执行 Lua 原子做四件事：

```
1. get 库存 → 不存在或 <= 0 直接返回「库存不足」
2. sismember 一人一单集合 → 已在里面返回「不能重复下单」
3. incrby 库存 -1
4. sadd 一人一单集合 + hset 准入记录（orderId → userId）
```

**为什么要记准入**：这条记录是「谁被批准了」的真值，后续所有兜底都靠它反查。
**为什么要 Lua**：四步必须原子，否则并发下会超卖或多单。

### 第三层：异步削峰（本地消息表 + MQ）

- 请求线程只做「Lua + 写一条 outbox 记录」，**几百毫秒内返回**，不碰 MySQL 库存；
- 本地消息表（outbox）保证不丢：先落库、再投 MQ，发送失败可重投，进程崩溃可扫描补投；
- Relay 定时扫描，单事务批量抢占后异步发 Kafka；
- Kafka key 用 orderId 而不是 voucherId——订单间无依赖，打散后可多分区并行。

### 第四层：MySQL 最终落地

消费者单事务完成：插订单 → 条件扣库存 `WHERE stock > 0` → outbox 置终态。任何一步失败整笔回滚。
数据库侧还靠两个约束做最终闸门：主键（订单幂等）和 `uk(user_id, voucher_id)`（一人一单）。

### 第五层：兜底与可观测

- **恢复任务**：依 Redis 准入记录补缺失的 outbox；
- **对账任务**：重置卡在中间态超时的消息，超阈值转慢速通道；
- **关单任务**：超时未支付自动取消并回补库存；
- **时间线**：每笔订单从准入到落库全记录，能定位卡在哪一步。

### 进阶优化（面试加分）

- **库存分片**：把一个券的库存拆成 N 份到 N 个 key，请求按 userId hash 打到不同 key，把单 key 热点摊开；代价是可能「总量还有但某个分片空了」，需要允许跨分片重试或做二次分配。
- **热点隔离**：秒杀独立部署一组服务 + 独立 Redis，不和其他业务抢资源。
- **批量扣减**：把 N 次 `stock = stock - 1` 合并成一次 `stock = stock - N`，减少行锁竞争。
- **服务端时间对齐**：所有超时判断用数据库时钟 `NOW()`，不用应用机器时钟。

一句话总结：**入口用 Lua 原子把并发挡在 Redis，中间用消息队列削峰，落库用事务 + 唯一键 + 条件更新兜住正确性，最后用一组定时任务保证最终一致。**

---

## 49. 如何设计一个图文上传 + 点赞的系统

### 表设计

**帖子主表**

```sql
CREATE TABLE tb_post (
  id            BIGINT       NOT NULL COMMENT '帖子ID',
  user_id       BIGINT       NOT NULL COMMENT '作者',
  title         VARCHAR(128) NULL,
  content       TEXT         NULL,
  cover_url     VARCHAR(255) NULL COMMENT '封面图',
  image_count   INT          NOT NULL DEFAULT 0 COMMENT '图片数（冗余，列表页免查子表）',
  status        TINYINT      NOT NULL DEFAULT 1 COMMENT '1 已发布/2 审核中/3 已删除/4 草稿',
  like_count    INT          NOT NULL DEFAULT 0 COMMENT '点赞数（冗余计数）',
  comment_count INT          NOT NULL DEFAULT 0,
  view_count    INT          NOT NULL DEFAULT 0,
  create_time   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  INDEX idx_user_status_create (user_id, status, create_time),  -- 某人的帖子列表
  INDEX idx_status_create (status, create_time)                 -- 全站时间流
);
```

**图片子表**（多图用子表，不要塞 JSON）

```sql
CREATE TABLE tb_post_image (
  id          BIGINT       NOT NULL AUTO_INCREMENT,
  post_id     BIGINT       NOT NULL,
  url         VARCHAR(255) NOT NULL,
  sort        TINYINT      NOT NULL DEFAULT 0 COMMENT '顺序',
  create_time DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_post_sort (post_id, sort),   -- 重复提交同序号时幂等，不会插出两张
  INDEX idx_post (post_id)
);
```

> 为什么拆子表而不是 `images JSON`：图片要单独替换、单独审核、按顺序展示；JSON 无法建索引，也很难做「第 2 张图」这种操作。缺点是列表页要额外查一次，所以主表冗余 `image_count` 和 `cover_url`。

**点赞表**（核心）

```sql
CREATE TABLE tb_like (
  id          BIGINT   NOT NULL AUTO_INCREMENT,
  user_id     BIGINT   NOT NULL COMMENT '点赞人',
  biz_type    TINYINT  NOT NULL COMMENT '1 帖子/2 评论/3 视频',
  biz_id      BIGINT   NOT NULL COMMENT '被点赞对象ID',
  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_user_biz (user_id, biz_type, biz_id),   -- 一人一赞的最终闸门
  INDEX idx_biz_create (biz_type, biz_id, create_time)  -- 某对象的点赞列表（按时间）
);
```

三个设计决策：

1. **`uk_user_biz(user_id, biz_type, biz_id)`** 是防重复点赞的最终闸门——对齐秒杀里的 `uk(user_id, voucher_id)`。重复点赞靠它抛 `DuplicateKeyException` / `INSERT IGNORE` 挡住，不靠「先查再插」。
2. **带 `biz_type`** 而不是每种对象建一张表，一张表统一管帖子和评论的点赞。
3. **`idx_biz_create(biz_type, biz_id, create_time)`** 是给「谁点赞了这条 + 按时间倒序」用的，注意列顺序：等值列在前，排序列在后。

### 点赞的写流程与幂等

```sql
-- 点赞（同一事务）
INSERT INTO tb_like (user_id, biz_type, biz_id) VALUES (?, 1, ?);           -- 撞 uk 即已赞
UPDATE tb_post SET like_count = like_count + 1 WHERE id = ?;                -- 冗余计数

-- 取消点赞（同一事务）
DELETE FROM tb_like WHERE user_id=? AND biz_type=1 AND biz_id=?;            -- 影响行数=1 才算真取消
UPDATE tb_post SET like_count = GREATEST(like_count - 1, 0) WHERE id = ?;   -- 下界保护
```

要点：
- **计数必须用 `like_count = like_count + 1` 这种原子写法**，不能 `SELECT` 出来加一再写回，否则并发下会丢计数；
- 取消点赞时判断 `DELETE` 的影响行数，**0 行说明本来就没赞过，不要减计数**；
- 同一条 UPDATE 走主键，是 Record Lock 单行锁。

### 高并发下的两个热点及优化

**热点一：大 V 的一条动态被几百万次点赞**，`tb_post` 那一行 `like_count` 成了行锁热点，而且单行更新有 QPS 上限。
优化：**Redis 计数 + 异步合并落库**。点赞先 `INCR like:count:{postId}` 并写一条消息，消费者把 N 次 +1 合并成一次 `like_count = like_count + N` 写回 MySQL。读的时候优先读 Redis。进一步可以分片计数（`tb_like_count(post_id, shard, cnt)`，读时 SUM）。

**热点二：判断「我有没有赞过」不能每次查 DB。**
优化：用 Redis 存点赞关系。用户 ID 连续且量大时用 **Bitmap**（`SETBIT like:post:{id} userId 1`，offset 直接就是 userId，极省内存），否则用 **Set**。DB 只做最终一致的落库，查询时先查 Redis、未命中再回源 DB 并回填。

**第三个问题：点赞列表（谁赞了）不可能全量展示。**
只展示前 N 个 + 「共 X 人赞过」，全量列表分页走 `idx_biz_create`。

---

## 50. 点赞系统常见 SQL 实例（含多跳查询）

以第 49 节的表结构为例。**面试让你现场写 SQL，先确认表结构和索引，再说思路，最后写语句。**

### ① 判断我是否点赞过某帖

```sql
SELECT 1 FROM tb_like
 WHERE user_id = 10 AND biz_type = 1 AND biz_id = 100
 LIMIT 1;
```

走 `uk_user_biz` 前缀，等值命中，`LIMIT 1` 找到即返回。
（生产上优先查 Redis `SISMEMBER` 或 `GETBIT`，未命中再回源 DB。）

### ② 某帖的点赞用户列表（按时间倒序、分页）

```sql
SELECT user_id, create_time
  FROM tb_like
 WHERE biz_type = 1 AND biz_id = 100
 ORDER BY create_time DESC
 LIMIT 20;
```

走 `idx_biz_create(biz_type, biz_id, create_time)`：前两列等值定位，第三列直接满足排序，**没有 filesort**。
如果再加 `AND create_time < :lastCreateTime` 就是游标分页（见第 51 节），比 `LIMIT 100000, 20` 好得多。

### ③ 我点赞过的帖子（我赞过的列表）

```sql
SELECT p.id, p.title, p.cover_url, p.user_id, p.like_count, l.create_time AS liked_at
  FROM tb_like l
  JOIN tb_post p ON p.id = l.biz_id AND p.status = 1
 WHERE l.user_id = 10 AND l.biz_type = 1
 ORDER BY l.create_time DESC
 LIMIT 20;
```

驱动表是 `tb_like`：`uk_user_biz` 前缀 `(user_id, biz_type)` 等值定位 + `create_time` 排序（注意此时排序用不到索引，因为 `uk` 的第三列是 `biz_id`；如果按点赞时间排得很频繁，可以再加 `idx_user_type_create(user_id, biz_type, create_time)`）。
`tb_post` 用主键回表。

### ④ 我点赞过的帖子的作者（去重）

```sql
SELECT DISTINCT p.user_id
  FROM tb_like l
  JOIN tb_post p ON p.id = l.biz_id
 WHERE l.user_id = 10 AND l.biz_type = 1;
```

### ⑤ 【多跳】我点赞过的帖子的作者，他们点赞过的帖子有哪些

这是面试常考的「三步跳」，思路先拆开：

```
我 → 我点赞过的帖子(l1) → 这些帖子的作者(p1) → 这些作者点赞过的帖子(l2) → 帖子详情(p2)
```

```sql
SELECT p2.id,
       p2.title,
       p2.user_id      AS author_id,
       p2.like_count,
       p2.create_time,
       COUNT(DISTINCT p1.user_id) AS hit_authors   -- 有多少个我喜欢的作者都赞过它
  FROM tb_like l1
  JOIN tb_post p1 ON p1.id = l1.biz_id
  JOIN tb_like l2 ON l2.user_id = p1.user_id AND l2.biz_type = 1
  JOIN tb_post p2 ON p2.id = l2.biz_id AND p2.status = 1
 WHERE l1.user_id = :me
   AND l1.biz_type = 1
   AND p1.user_id <> :me          -- 排除自己赞自己的情况
   AND p2.user_id <> :me          -- 排除自己发的
 GROUP BY p2.id, p2.title, p2.user_id, p2.like_count, p2.create_time
 ORDER BY hit_authors DESC, p2.create_time DESC
 LIMIT 20;
```

索引走的路径：

- `l1`：`uk_user_biz` 前缀 `(user_id, biz_type)` 等值定位 —— 驱动表，行数最少；
- `p1`：主键回表；
- `l2`：`uk_user_biz` 的 `user_id` 前缀（等值）—— 索引范围扫描，注意**不能**用 `(user_id, biz_type)` 复合前缀优化排序，但定位足够；
- `p2`：主键回表 + `status = 1` 过滤。

`GROUP BY p2.id` 的作用是**去重**：多个作者赞了同一条帖子时只出现一次，同时 `COUNT(DISTINCT p1.user_id)` 给出「命中几个作者」，可以作为推荐排序权重。

**但要提醒面试官（这是加分点）**：这条 SQL 是实时多跳，`l2` 的量可能非常大，**生产上一般不会这么查**。真实做法是：

- 离线或近线算好「相似作者 / 推荐内容」写进推荐表或缓存，接口只读一张宽表；
- 或者先取 TopN 作者（子查询限流），再用 `IN` 拉候选，避免大表 JOIN 爆炸：

```sql
-- 先取我点赞过的作者（限量）
WITH authors AS (
  SELECT DISTINCT p.user_id
    FROM tb_like l JOIN tb_post p ON p.id = l.biz_id
   WHERE l.user_id = :me AND l.biz_type = 1 AND p.user_id <> :me
   LIMIT 50
)
SELECT p2.*
  FROM authors a
  JOIN tb_like l2 ON l2.user_id = a.user_id AND l2.biz_type = 1
  JOIN tb_post p2 ON p2.id = l2.biz_id AND p2.status = 1
 GROUP BY p2.id
 ORDER BY COUNT(*) DESC
 LIMIT 20;
```

**先说「限制作者数量、走中间结果表」再写 JOIN，而不是一上来写四表 JOIN，这是区分有没有生产经验的地方。**

### ⑥ 我自己的帖子列表

```sql
SELECT * FROM tb_post
 WHERE user_id = 10 AND status = 1
 ORDER BY create_time DESC
 LIMIT 20;
```

走 `idx_user_status_create(user_id, status, create_time)`：等值 + 等值 + 排序，完全命中，无 filesort。

### ⑦ 全站最新/热门

```sql
-- 最新（走 idx_status_create）
SELECT * FROM tb_post WHERE status = 1 ORDER BY create_time DESC LIMIT 20;

-- 热门（like_count 排序，用不上时间索引，需要单独的索引或离线榜单）
SELECT * FROM tb_post WHERE status = 1 ORDER BY like_count DESC, id DESC LIMIT 20;
```

`like_count` 排序**没有合适索引**，数据量大时是全表排序——所以热门榜一般是离线算好写进 Redis ZSet（`ZREVRANGE hot:posts 0 19`），而不是实时 `ORDER BY like_count`。

---

## 51. 索引设计原则与慢 SQL 排查

### 建索引的原则

1. **联合索引遵守最左前缀**：`(user_id, status, create_time)` 能服务 `user_id`、`user_id+status`、`user_id+status+create_time` 三种查询，但单独查 `status` 用不上。
2. **等值列在前、排序列在后**：`WHERE a=? AND b=? ORDER BY c` 对应 `(a, b, c)`，这样排序能直接走索引，避免 filesort。
3. **区分度高的列放前面**：区分度 = `COUNT(DISTINCT col) / COUNT(*)`。性别、状态这类低区分度列单独建索引没意义（回表比全表扫还慢）。
4. **尽量用覆盖索引**：如果查询的列都在索引里，就不用回表。`EXPLAIN` 的 Extra 会显示 `Using index`。
5. **不要建太多索引**：每个索引都增加写入成本和存储，更新频繁的表要克制。
6. **长字符串用前缀索引**：`INDEX(url(32))`，但要接受一定的过滤精度损失。

### 索引失效的常见场景

- 对索引列**做函数运算或表达式**：`WHERE DATE(create_time) = '2026-01-01'` → 改成范围 `>= ... AND < ...`；
- **隐式类型转换**：`varchar` 列传数字、或 `bigint` 列传字符串，都会退化成全表扫；
- **前导模糊匹配**：`LIKE '%关键词'` 用不上索引，`LIKE '关键词%'` 可以；
- **`OR` 连接的列有一侧没索引**，整体就失效；
- **不满足最左前缀**；
- `IS NULL` / `!=` / `NOT IN` 通常选择性差，优化器可能放弃索引。

### EXPLAIN 看什么

| 列 | 关注点 |
|---|---|
| `type` | 效率排序：`system > const > eq_ref > ref > range > index > ALL`，出现 `ALL` 或 `index`（全索引扫描）就要警惕 |
| `key` | 实际用了哪个索引，为 `NULL` 说明没走索引 |
| `rows` | 预估扫描行数，越大越危险 |
| `filtered` | 过滤后剩余比例，太低说明索引选择性差 |
| `Extra` | `Using filesort`（额外排序）、`Using temporary`（临时表）、`Using index`（覆盖索引，好）、`Using index condition`（索引下推，好） |

### 深分页怎么优化

```sql
-- 慢：要扫描并丢弃前 100000 行
SELECT * FROM tb_post WHERE status = 1 ORDER BY id DESC LIMIT 100000, 20;

-- 快：游标分页，记住上一页最后一条的 id
SELECT * FROM tb_post WHERE status = 1 AND id < :lastId ORDER BY id DESC LIMIT 20;
```

游标分页把「偏移量」变成「范围条件」，走索引直接定位。缺点是只能顺序翻页、不能跳页；产品上可以做成「上一页/下一页」而不是数字页码。

### 慢查询排查流程

1. 开 `slow_query_log`，设 `long_query_time`（比如 1s），观察一段时间；
2. 用 `pt-query-digest` 或 `mysqldumpslow` 聚合，找到耗时占比最高的 SQL；
3. 对目标 SQL `EXPLAIN`，看 type / rows / Extra；
4. 补索引或改写 SQL；改完再 `EXPLAIN` 对比，注意**索引不是越多越好**，也要评估写入成本；
5. 如果加了索引仍然慢，考虑：数据量是否该归档/分表、是否该走缓存或离线预计算、是否 SQL 本身写法有问题（比如大表 JOIN、`SELECT *` 回表太多）。
