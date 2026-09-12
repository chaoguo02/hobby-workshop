# 科学导航 · 图书馆智能 Deep Research Agent 设计文档

> 适用范围：科研场景下的长时研究任务系统。核心是把「一次 Deep Research」
> 建模为一个**长时、多阶段、可中断、依赖外部知识源的有状态任务**，并保证
> 它在多实例、重复投递、进程崩溃、外部服务抖动下都能最终完成。
>
> 本文是可直接迁移到新项目的设计骨架。表名/字段名/包名可按需替换，但
> **状态机、幂等键、执行租约**这三样建议不要省——它们决定系统是「demo」还是「能落地」。

---

## 0. 需求与技术的对应关系

| 需求 | 本质问题 | 主要落点 |
|---|---|---|
| ① Research 任务异步执行 | 任务太长，不能同步阻塞；且消息可能重复/丢失 | 第 3.3 节 outbox + 第 5 节执行模型 |
| ② SubAgent 并行检索优化 | 多源串行太慢；但并行会放大资源与失败面 | 第 6 节 |
| ③ Research 阶段状态恢复 | 中断后已完成步骤重复执行，浪费成本 | 第 3.2 节产物表 + 第 5.3 节主循环 |
| ④ MCP 知识检索接入 | 外部服务异构、不稳定、有配额 | 第 10 节适配层 |

**关键认知：①②③ 不能各自实现。** 它们共用同一个「任务执行模型」：
Kafka 负责投递，租约负责防双跑，阶段产物负责恢复。④ 是这之上的外部依赖层。

---

## 1. 核心不变量

先定不变量，其余设计都是它的实现：

1. 每个已受理的 `taskId` 最终都会到达终态，**不依赖人工介入**。
2. 任一时刻，同一 `taskId` **至多有一个执行者**（靠执行租约保证）。
3. 同一 `(task_id, stage, sub_key)` 的有效产物只被写入一次；重入时读已有产物。
4. 每个阶段可安全重入，重入不产生额外副作用（尤其是外部检索的配额与费用）。
5. 所有状态推进都是 `WHERE 主键 = ? AND status = 旧状态` 的条件 UPDATE，
   并发/多实例/重复投递下只有一个影响行数 = 1 的赢家。

---

## 2. 领域模型与状态机

### 2.1 任务级状态 `task.status`

```
0 PENDING ──▶ 1 RUNNING ──┬──▶ 2 SUCCEEDED          终态
                          ├──▶ 5 PARTIAL_SUCCEEDED  终态（部分知识源失败，仍产出报告）
                          ├──▶ 3 FAILED             终态
                          └──▶ 4 CANCELLED          终态
```

**终态只有 2 / 3 / 4 / 5 四个。** `FAILED` 必须是真终态（有重试次数上限），
不像纯投递链路可以无限重试——因为这里每次重试都在烧 LLM token 和外部配额。

### 2.2 阶段 `task.phase`（与状态分离，勿混用）

```
0 DECOMPOSE 问题拆解
1 RETRIEVE  多路检索（含 N 个子查询）
2 SUMMARIZE 内容总结
3 GENERATE  报告生成
```

> **状态回答「任务活着还是死了」，阶段回答「跑到哪一步了」。**
> 两个字段分开存、分开给前端展示。把「投递/执行状态」当成「业务状态」展示，
> 是长任务系统里最常见的状态错位 bug。

### 2.3 任务状态迁移总表

| 迁移 | 触发 | 判定条件 |
|---|---|---|
| `INSERT` → 0 PENDING | 创建任务 | 与 outbox 行同事务写入 |
| 0 → 1 RUNNING | 消费者抢租约成功 | `lease` 空闲或已过期，条件 UPDATE 影响行数 = 1 |
| 1 → 2 SUCCEEDED | 四阶段全部完成且无降级 | — |
| 1 → 5 PARTIAL | 完成但存在失败/降级知识源 | `degraded = 1` |
| 1 → 0 PENDING | 可重试异常，退避后重投 | `retry < MAX_RETRY` |
| 1 → 3 FAILED | 不可重试异常，或重试超上限 | 真终态，需告警 |
| 0 / 1 → 4 CANCELLED | 用户取消 | CAS，仅当未到终态 |
| 3 → 0 PENDING | 人工重放 | `retry = 0`、清退避（加速手段，非唯一恢复路径） |

### 2.4 阶段状态迁移

| 迁移 | 触发 |
|---|---|
| `INSERT` → 0 PENDING | 任务创建时预置全部阶段，或首次进入时惰性创建 |
| 0 → 1 RUNNING | 开始执行该阶段 |
| 1 → 2 DONE | **产物与状态同一事务写入** |
| 1 → 3 FAILED | 该阶段不可恢复失败 |
| 3 → 0 PENDING | 任务重试时重置 |

---

## 3. 表结构

### 3.1 `tb_research_task` — 任务主表

```sql
CREATE TABLE tb_research_task (
  id                BIGINT        NOT NULL COMMENT '任务ID（外部生成，幂等键）',
  user_id           BIGINT        NOT NULL COMMENT '发起用户',
  question          TEXT          NOT NULL COMMENT '研究问题',
  status            TINYINT       NOT NULL DEFAULT 0 COMMENT '0 PENDING/1 RUNNING/2 SUCCEEDED/3 FAILED/4 CANCELLED/5 PARTIAL',
  phase             TINYINT       NOT NULL DEFAULT 0 COMMENT '0 拆解/1 检索/2 总结/3 生成',
  progress          TINYINT       NOT NULL DEFAULT 0 COMMENT '0-100，给前端展示',
  -- 执行租约（多实例安全的核心）
  lease_owner       VARCHAR(64)   NULL COMMENT '持有该任务的实例标识',
  lease_expire_time DATETIME      NULL COMMENT '租约到期时间',
  heartbeat_time    DATETIME      NULL COMMENT '最近一次续租时间',
  -- 重试
  retry             INT           NOT NULL DEFAULT 0 COMMENT '已重试次数',
  next_retry_time   DATETIME      NULL COMMENT '下次可重试时间（退避用）',
  error_code        VARCHAR(64)   NULL COMMENT '统一错误码',
  error_msg         VARCHAR(500)  NULL COMMENT '最近一次异常信息',
  -- 结果与成本
  degraded          TINYINT       NOT NULL DEFAULT 0 COMMENT '是否有知识源降级',
  failed_sources    VARCHAR(500)  NULL COMMENT '降级/失败的知识源清单（JSON）',
  token_used        INT           NOT NULL DEFAULT 0 COMMENT '累计 token 消耗（成本控制）',
  pipeline_version  INT           NOT NULL DEFAULT 1 COMMENT 'prompt/模型/流程版本，决定产物可否复用',
  create_time       DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time       DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  finish_time       DATETIME      NULL,
  PRIMARY KEY (id),
  INDEX idx_status_lease (status, lease_expire_time),        -- 恢复任务扫描僵尸租约
  INDEX idx_status_next_retry (status, next_retry_time),     -- 重试扫描
  INDEX idx_user_create (user_id, create_time)               -- 用户任务列表
);
```

### 3.2 `tb_research_stage` — 阶段产物（恢复的载体）

```sql
CREATE TABLE tb_research_stage (
  id            BIGINT        NOT NULL AUTO_INCREMENT,
  task_id       BIGINT        NOT NULL,
  stage         TINYINT       NOT NULL COMMENT '0 拆解/1 检索/2 总结/3 生成',
  sub_key       VARCHAR(64)   NOT NULL DEFAULT '' COMMENT '子任务键：检索阶段=子查询hash；其余阶段=""',
  status        TINYINT       NOT NULL DEFAULT 0 COMMENT '0 PENDING/1 RUNNING/2 DONE/3 FAILED',
  artifact      MEDIUMTEXT    NULL COMMENT '产物 JSON（大产物改存对象存储）',
  artifact_ref  VARCHAR(255)  NULL COMMENT '对象存储 key（artifact 过大时使用）',
  content_hash  CHAR(32)      NULL COMMENT '产物指纹，校验完整性/去重',
  duration_ms   INT           NULL COMMENT '该阶段耗时',
  error_code    VARCHAR(64)   NULL,
  create_time   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_task_stage_sub (task_id, stage, sub_key),    -- 阶段级幂等闸门
  INDEX idx_task_stage (task_id, stage)
);
```

**三个必须遵守的点**：

1. **产物与状态同事务写**。禁止「先 `UPDATE status = DONE`，再写产物」，否则中间崩溃就留下假完成。
2. **恢复判据是「产物存在且完整」，不是只看 status**。可用 `content_hash` 校验。
3. **检索阶段的 `sub_key` 必须细到单个子查询**。这是唯一有外部副作用（配额/费用）的
   阶段，粒度太粗会导致重试时把已成功的子查询重查一遍。

### 3.3 `tb_research_dispatch` — 投递 outbox

```sql
CREATE TABLE tb_research_dispatch (
  task_id         BIGINT   NOT NULL COMMENT '同时作为 outbox 主键',
  status          TINYINT  NOT NULL DEFAULT 0 COMMENT '0 READY/1 PROCESSING/2 SENT/3 COMPLETED/4 FAILED',
  retry           INT      NOT NULL DEFAULT 0,
  next_retry_time DATETIME NULL,
  create_time     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (task_id),
  INDEX idx_status_next_retry_create (status, next_retry_time, create_time)
);
```

事务性 outbox 的价值：**先落库再发 MQ**。发送失败可重投，进程崩溃后扫描任务表能补投。
`FAILED` 只是「退避更长的慢速通道」，不是终态，relay 仍会持续扫它。

### 3.4 `tb_research_event` — 时间线（append-only，仅观测）

```sql
CREATE TABLE tb_research_event (
  id          BIGINT        NOT NULL AUTO_INCREMENT,
  task_id     BIGINT        NOT NULL,
  stage       TINYINT       NULL,
  level       VARCHAR(8)    NOT NULL DEFAULT 'INFO' COMMENT 'INFO/WARN/ERROR',
  code        VARCHAR(64)   NOT NULL COMMENT '事件码，见 9.3',
  detail      VARCHAR(1000) NULL,
  create_time DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  INDEX idx_task_id (task_id, id)
);
```

**业务成功事件必须在事务提交后写**（用独立连接），否则回滚会留下假事件。
建议按天清理（保留 3~7 天）。

---

## 4. 接口设计

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/research/tasks` | 创建任务。**必须支持 `Idempotency-Key` 请求头**，重复提交返回同一 taskId |
| GET | `/research/tasks/{id}` | 状态 + 阶段 + progress + degraded + failedSources |
| GET | `/research/tasks/{id}/timeline` | 时间线还原（阶段中文名 + 相对耗时） |
| GET | `/research/tasks/{id}/report` | 最终报告 |
| POST | `/research/tasks/{id}/cancel` | 取消（CAS 到 CANCELLED） |
| POST | `/research/tasks/{id}/retry` | 人工重放（FAILED → PENDING，retry 归零） |
| GET | `/research/tasks?page=&size=` | 本人任务列表 |

创建任务的事务：

```
BEGIN
  INSERT tb_research_task (status = PENDING)
  INSERT tb_research_dispatch (status = READY)   -- 与任务同事务
COMMIT
-- 提交后由 relay 投递 Kafka
```

> 接口返回 taskId 后，**即使 outbox 写入失败也不影响正确性**：任务表里已有
> PENDING 任务，补偿任务会依据它把 outbox 补回来。这与「先扣 Redis 再落库」是同一个思路。

**状态查询接口的返回示例**（前端倒计时/进度条直接消费）：

```json
{
  "taskId": "1845...",
  "status": 1, "statusName": "执行中",
  "phase": 1,  "phaseName": "检索中",
  "progress": 45,
  "degraded": true, "failedSources": ["arxiv"],
  "tokenUsed": 12840,
  "createTime": "...", "updateTime": "..."
}
```

---

## 5. 执行模型（①②③ 的核心）

### 5.1 三层角色

```
Relay (每 500ms)
  扫 tb_research_dispatch READY/FAILED → 单事务批量抢占 PROCESSING → 异步发 Kafka
        │
        ▼
Kafka  topic: research-tasks  (N 分区, key = taskId)
        │    key 用 taskId：同一任务落同一分区，保证任务内事件有序
        ▼
Consumer（调度角色）
  只做「抢租约 + 交给线程池」，不在消费线程里执行任务
        │
        ▼
Executor 线程池（检索池 / LLM 池分离）
        │
        ▼
LeaseRenewalScheduler（每 15s 续租所有自己持有的任务）
```

**为什么 consumer 不直接执行任务**：
- 单个任务可能跑几十分钟，占满 `@KafkaListener` 线程 → 分区数变成并发上限；
- 长任务期间发生 rebalance，会触发重复消费 → 必须靠租约兜住；
- 消费线程应尽快 ack 返回，把耗时工作移出 Kafka 生命周期。

### 5.2 执行租约（多实例安全的关键）

```sql
-- 抢租约：只有租约空闲、已过期、或本来就是自己时才能抢到
UPDATE tb_research_task
   SET lease_owner        = :instanceId,
       lease_expire_time  = DATE_ADD(NOW(), INTERVAL 60 SECOND),
       heartbeat_time     = NOW(),
       status             = 1,
       update_time        = NOW()
 WHERE id = :taskId
   AND status IN (0, 1)
   AND (lease_owner IS NULL
        OR lease_expire_time < NOW()
        OR lease_owner = :instanceId);
-- 影响行数 = 1 才获得执行权
```

```sql
-- 续租：只续自己的租约
UPDATE tb_research_task
   SET lease_expire_time = DATE_ADD(NOW(), INTERVAL 60 SECOND),
       heartbeat_time    = NOW()
 WHERE id = :taskId AND lease_owner = :instanceId;

-- 释放：只释放自己的租约
UPDATE tb_research_task
   SET lease_owner = NULL, lease_expire_time = NULL
 WHERE id = :taskId AND lease_owner = :instanceId;
```

性质：

- 实例崩溃 → 租约 60s 后自动过期 → 恢复任务接管，**不会永久卡死**；
- 实例只是慢 → 持续续租，别人抢不走；
- **双跑被彻底排除**，这是 ① 和 ③ 共同的基石。

> 续租用**单例调度器**扫描「`lease_owner = 自己` 的任务」批量续租，
> 而不是每个任务起一个心跳线程——后者任务一多就线程爆炸。

### 5.3 `runTask` 主循环（完整伪代码）

```java
void runTask(long taskId) {
    if (!tryAcquireLease(taskId)) {          // 5.2 的条件 UPDATE
        return;                              // 有别人在跑，直接退出
    }
    try {
        for (Stage stage : STAGES) {         // DECOMPOSE → RETRIEVE → SUMMARIZE → GENERATE
            if (isCancelled(taskId)) {
                markCancelled(taskId);
                return;
            }

            // ── 恢复的入口：产物存在且完整则跳过重算 ──
            if (isStageDone(taskId, stage)) {
                event(taskId, stage, "STAGE_SKIPPED", "复用已有产物，跳过");
                continue;
            }

            markStageRunning(taskId, stage);
            event(taskId, stage, "STAGE_START", null);

            StageResult result = executeStage(taskId, stage);   // 内部并行 + 超时 + 部分失败聚合
            if (result.isFatal()) {
                throw new FatalException(result.errorCode(), result.errorMsg());
            }

            // 产物与阶段状态同一事务写
            saveStageArtifactAndMarkDone(taskId, stage, result);
            event(taskId, stage, "STAGE_DONE", "耗时 " + result.durationMs() + "ms");
            heartbeat(taskId);                                   // 阶段边界续租
        }

        boolean degraded = hasFailedSources(taskId);
        markSucceeded(taskId, degraded);                         // degraded → PARTIAL(5)
        event(taskId, null, "TASK_FINISHED", null);

    } catch (RetryableException e) {
        scheduleRetry(taskId, e);                                // status → PENDING，退避
    } catch (FatalException e) {
        markFailed(taskId, e);                                   // 真终态 + 告警
    } catch (Exception e) {
        // 未知异常按可重试处理，但要计入 retry，防止死循环
        scheduleRetry(taskId, e);
    } finally {
        releaseLease(taskId);                                    // 只释放自己的
    }
}
```

### 5.4 阶段与恢复的对应关系

| 阶段 | 产物内容 | 重入代价 | 恢复策略 |
|---|---|---|---|
| DECOMPOSE | 子问题列表 | 低（纯 LLM 计算） | 可整段重跑 |
| RETRIEVE | 每个子查询的 `KnowledgeChunk[]` | **高（外部配额 + 费用）** | 按 `sub_key` 粒度跳过已完成子查询 |
| SUMMARIZE | 总结文本 | 中（LLM token） | 整段重跑 |
| GENERATE | 最终报告 | 中高（LLM token） | 整段重跑，但计入 token 预算 |

**结论**：恢复设计的重点在 RETRIEVE——它必须做到「子查询级」的幂等，
否则「恢复」只是把重复计算从阶段级缩小到子查询级，钱照烧。

### 5.5 恢复任务（僵尸租约扫描）

```sql
-- 扫描租约过期但仍处于 RUNNING 的任务
SELECT * FROM tb_research_task
 WHERE status = 1 AND lease_expire_time < NOW()
 ORDER BY lease_expire_time ASC
 LIMIT 100;
```

对每条：重置为 PENDING（`lease_owner = NULL`）并重投 outbox。
因为 `runTask` 开头会先检查阶段产物，接管者会从断点续跑，**不会重头再来**。

### 5.6 重试与退避（必须有上限）

```
常规段（retry < 3）：1s, 2s, 4s ... 封顶 300s
告警段（retry >= 3）：5min, 10min, 20min ... 封顶 1h
硬上限：MAX_RETRY（例如 8）→ 超过直接 FAILED 终态并告警
```

> 与纯投递链路不同，**这里必须有「放弃」终点**，否则任务会无限重试、
> 无限消耗外部配额与 token。FAILED 之后靠人工重放（`retry` 归零）恢复。

---

## 6. SubAgent 并行检索

### 6.1 线程池配置（两个池必须隔离）

```java
// 检索池：IO 密集（外部 MCP / HTTP）
retrievalPool = new ThreadPoolExecutor(
        16, 32, 60L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(256),                 // 有界！防 OOM
        namedDaemonFactory("retrieval-"),
        new ThreadPoolExecutor.CallerRunsPolicy());    // 或自定义：记录 + 跳过该源

// LLM 池：长调用，与检索隔离，避免慢 LLM 饿死检索
llmPool = new ThreadPoolExecutor(
        8, 16, 60L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(64),
        namedDaemonFactory("llm-"),
        rejectPolicy);
```

要点：
- **必须有界队列 + 明确的拒绝策略**。无界队列在慢源场景下会把内存打爆。
- **检索与 LLM 分池**。合并成一个池，几个慢 LLM 调用就能占满线程，检索全被饿死。
- 线程命名 + daemon，便于 jstack 排查。

### 6.2 并行聚合模板

```java
List<CompletableFuture<SourceResult>> futures = sources.stream()
        .map(src -> CompletableFuture
                .supplyAsync(() -> callMcp(src, query), retrievalPool)
                // 失败转占位，避免 allOf 因个别异常整体失败
                .handle((ok, ex) -> ex == null
                        ? SourceResult.ok(src, ok)
                        : SourceResult.fail(src, classify(ex)))
                // 每个 future 独立超时
                .orTimeout(10, TimeUnit.SECONDS))
        .collect(Collectors.toList());

List<SourceResult> results = futures.stream()
        .map(CompletableFuture::join)
        .collect(Collectors.toList());
```

三个容易踩的坑：

1. **每个 future 必须自带超时**。`allOf` 只要有一个 future 不完成就永远不完成，
   一个卡死的源会拖垮整个任务。
   - Java 9+：`orTimeout` / `completeOnTimeout`；
   - **Java 8 没有这两个方法**，需用 `ScheduledExecutorService` 延时
     `completeExceptionally` 手动实现。
2. **失败要转成占位对象**（`SourceResult.fail`），不能直接抛异常，否则聚合阶段爆炸。
3. **结果要去重 + 排序 + 截断**：按 `url`/`docId` 去重 → 按 score 排序 → 按 token 预算截断。

### 6.3 上下文传递

`UserHolder`（ThreadLocal）、traceId、MDC 都不跨线程。两种做法：

- 显式把 userId / traceId 当参数传下去（推荐，最直白）；
- 用 `TaskDecorator` 在提交时快照 ThreadLocal、执行前恢复、执行后清理。

不处理的后果：异步线程里取用户为空、日志 traceId 丢失、跨线程事务上下文失效。

### 6.4 外部配额与线程池的关系

**线程池大小 ≠ 允许的外部并发数。** 每个知识源单独一个 `Semaphore(quota)`，
超配额时阻塞或降级。否则 16 个检索线程同时打同一个源，直接触发第三方限流。

---

## 7. 失败矩阵

| 场景 | 分类 | 处理 | 用户可见结果 |
|---|---|---|---|
| LLM 超时 / 5xx | 可重试 | 退避重试 | 任务继续，进度不变 |
| 参数非法 / prompt 渲染失败 | 不可重试 | 直接 FAILED | 明确失败原因 |
| 单个知识源不可用 | 可降级 | 标记该源失败，其余继续 | 报告标 `degraded` + 缺失源清单 |
| 全部知识源失败 | 可重试 | 退避重试，超上限 FAILED | 失败 |
| 执行实例宕机 | 可恢复 | 租约过期 → 恢复任务接管 → 从断点续跑 | 无感知 |
| 重复消费同一 taskId | 幂等 | 租约 + 阶段幂等挡住 | 无感知 |
| token 超预算 | 不可重试 | 停止生成，返回已完成部分 | PARTIAL + 超预算提示 |
| 用户取消 | 终态 | 阶段边界检查标志，CAS → CANCELLED | 已取消 |
| 阶段产物写入中途崩溃 | 可恢复 | 产物与状态同事务，读到的要么全有要么全无 | 无感知 |

**设计要点**：错误必须分类，不能一律重试。`参数非法` 重试是纯浪费；
`外部源不可用` 若整体重试而不降级，会拖慢所有任务。

---

## 8. 并发与背压（多层必须对齐）

| 层 | 控制手段 | 不对齐的后果 |
|---|---|---|
| 任务入口 | 单用户进行中任务上限 + 全局队列上限，超限直接拒绝 | 无限接单 → 线程池/DB 雪崩 |
| Kafka | 分区数 = 每实例消费并发上限 | 并发上不去，或消费者空跑 |
| 线程池 | 有界队列 + 拒绝策略 | 慢源打爆内存 |
| 外部 API | 每源信号量 / 令牌桶 | 被第三方限流或封禁 |
| DB 连接池 | 连接数 ≥ 各业务池并发之和 | 拿不到连接，请求超时 |

**背压必须有明确策略**：队列满了是「拒绝新任务」还是「排队等待」？
长任务系统建议**拒绝 + 明确提示**，而不是无限排队造成假象。

---

## 9. 可观测性

### 9.1 指标

- 各状态任务数、PENDING 最老等待时长（积压）
- 阶段耗时 p50/p95、端到端耗时分布
- 各知识源成功率 / 超时率 / 平均命中数 / 调用次数
- 单任务与全局 token 消耗（成本）
- 线程池 `activeCount / queueSize / poolSize / rejectedCount`
- Kafka lag、DLT 堆积、**租约接管次数**

### 9.2 告警

- FAILED 率突增
- 某知识源成功率骤降（熔断前兆）
- 租约接管次数异常增多（实例频繁挂 / 任务频繁超时）
- 单任务 token 超预算
- PENDING 积压超阈值

### 9.3 每任务时间线事件码

```
TASK_CREATED → DISPATCHED → LEASE_ACQUIRED
  → STAGE_START / STAGE_DONE / STAGE_SKIPPED  （每个阶段各一对）
  → RETRIEVAL_SOURCE_FAILED                   （WARN）
  → RETRIEVAL_DEGRADED                        （WARN）
  → REPORT_GENERATED
  → TASK_FINISHED / TASK_FAILED / TASK_CANCELLED
  → LEASE_TAKEOVER                            （WARN，被别的实例接管）
  → MANUAL_REPLAY                             （人工重放）
```

时间线要带**相对耗时（offsetMs）**，这是排障时唯一能回答「卡在哪一步」的东西。
全部通过 `GET /research/tasks/{id}/timeline` 还原。

---

## 10. MCP 知识检索接入

### 10.1 分层结构

```
Agent 层
   │  只依赖内部抽象，不接触 MCP 原始 JSON
   ▼
KnowledgeChunk { title, content, source, url, docId, score, publishedAt, metadata }
   ▲
KnowledgeSource（适配接口）
   ├─ ArxivAdapter     ┐
   ├─ LibraryAdapter   ├─ 各自把 MCP 原始返回映射成 KnowledgeChunk，
   └─ PaperAdapter     ┘   把外部错误映射成统一错误码
   ▼
McpClient（stdio / SSE / HTTP）  ← 鉴权、超时、熔断、限流、缓存都在这一层
```

**不要直接把 MCP 原始 JSON 透传给 Agent**：外部 schema 一变，整条链路崩。
适配层是唯一允许知道外部格式的地方。

### 10.2 适配接口

```java
public interface KnowledgeSource {
    String name();
    /** 检索；失败时抛 KnowledgeSourceException（带统一错误码） */
    List<KnowledgeChunk> search(KnowledgeQuery query) throws KnowledgeSourceException;
}

public class KnowledgeQuery {
    String normalizedQuery;   // 已归一化（小写、去停用词），用于缓存 key
    int topK;
    Map<String, String> filters;   // 年份、学科等
    Duration timeout;
}

public class KnowledgeChunk {
    String title;
    String content;           // 已截断/摘要，控制 token
    String source;            // 来源标识
    String url;
    String docId;             // 用于跨源去重
    Double score;
    String publishedAt;
    Map<String, Object> metadata;
}
```

### 10.3 统一错误码（Agent 据此决策）

| 码 | 含义 | Agent 动作 |
|---|---|---|
| `NO_RESULT` | 查询成功但无结果 | 换关键词 / 换源，**不算失败** |
| `TIMEOUT` | 超时 | 重试或跳过该源 |
| `RATE_LIMITED` | 被限流 | 退避或跳过 |
| `UNAUTHORIZED` | 鉴权失败 | 不可重试，立即告警 |
| `UNAVAILABLE` | 服务不可用 | 触发熔断 + 降级 |
| `BAD_REQUEST` | 参数不合法 | 不可重试，修适配器 |

**`NO_RESULT` 与 `TIMEOUT` 必须可区分**：前者是正常业务结果，后者是故障。
混成一个异常，Agent 就只能盲目重试。

### 10.4 其余必备能力

- **缓存**：`hash(source + normalizedQuery)` → 结果，带 TTL。多子任务重复查询很常见。
- **熔断**：单源连续失败 N 次 → 短路一段时间，直接返回降级结果。
- **token 预算**：检索回的原文通常超长，先截断/摘要再进 prompt；
  并用 `task.token_used` 做全局预算控制。
- **鉴权与密钥**：MCP token 走配置/密钥管理，禁止硬编码。
- **安全**：
  - 若 tool 支持传入任意 URL 抓取，需防 **SSRF**；
  - 外部文本进 prompt 前要做**提示词注入防护**（检索结果里可能藏指令）。

---

## 11. 配置项清单

```yaml
research:
  task:
    max-retry: 8                     # 硬上限，超过转 FAILED
    lease-seconds: 60                # 租约时长
    heartbeat-interval-ms: 15000     # 续租周期（应 < lease 的 1/3）
    stage-timeout-ms: 600000         # 单阶段超时
    zombie-scan-delay-ms: 30000      # 僵尸租约扫描周期
  retrieval:
    pool-core: 16
    pool-max: 32
    pool-queue: 256
    per-source-timeout-ms: 10000
    max-tokens-per-task: 200000
  dispatch:
    relay-delay-ms: 500
    relay-batch-size: 200
  mcp:
    sources: [ arxiv, library, paper ]
    cache-ttl-seconds: 86400
    circuit-breaker-threshold: 5
```

---

## 12. 落地里程碑

| 阶段 | 交付 | 验收标准 |
|---|---|---|
| M1 | 四阶段同步跑通，产物落库 | 单任务能产出报告 |
| M2 | Kafka 异步 + 任务状态 + outbox 补投 | 杀进程重启，任务不丢、自动补齐 |
| M3 | 并行检索 + 部分失败聚合 | 单源挂掉任务仍成功且标 `degraded` |
| M4 | checkpoint + 执行租约 + 僵尸恢复 | **杀掉执行实例，恢复后从断点续跑，不重复检索** |
| M5 | MCP 多源接入 + 限流/熔断/缓存 | 单源限流不影响整体 |
| M6 | 可观测 + 压测 + 故障注入 | 有指标、有告警、有压测报告 |

> **M4 是整个项目的分水岭。** 没有租约和 checkpoint，①②③ 只是「异步 + 并行」的 demo；
> 有了它，才真正满足「长任务后台可靠运行」的要求。

---

## 13. 验收清单（逐条自检）

- [ ] 杀掉执行中的实例，重启后任务能从断点续跑，**已完成阶段不重算**（查事件表确认有 `STAGE_SKIPPED`）
- [ ] 同一 taskId 重复投递，任务只被一个实例执行（查 `lease_owner`、跑两实例观察）
- [ ] 单知识源不可用，任务仍能产出报告并标 `degraded`
- [ ] 所有知识源不可用，任务按退避重试，超上限转 FAILED 并告警
- [ ] 用户取消后，长时间运行的任务确实在阶段边界停下（不是卡着不停）
- [ ] token 超预算时任务停止生成并返回 PARTIAL
- [ ] 线程池队列满时按策略拒绝，不 OOM、不长时间阻塞
- [ ] 每个任务都能拉出完整时间线，能定位到具体阶段
- [ ] 失败任务可人工重放，且重放不会重复消耗已成功的检索
- [ ] DB 连接池、线程池、Kafka 并发、外部配额四层数字对得上

---

## 附录 A. 参考实现映射（来自已落地的秒杀系统）

本项目 ①②③ 的机制与一个已验证的秒杀系统高度同构，可直接借鉴其实现：

| 秒杀系统组件 | 本项目对应 | 复用点 |
|---|---|---|
| 本地消息表 `tb_seckill_message` | `tb_research_dispatch` | 事务性 outbox |
| Relay（批量抢占 + 异步投递 + 退避） | Kafka 投递层 | `claimBatch` 条件 UPDATE、`releaseForRetry` |
| ReconcileTask（扫卡死消息重置） | 僵尸租约扫描 | 「超时 → 重置重投」的思路 |
| Consumer 幂等预检 | 抢租约 + 阶段幂等 | 「先查存在再写」 |
| 统一退避策略类 | `ResearchRetryPolicy` | 快/慢两段退避集中定义，避免阈值漂移 |
| 事件表 + 时间线接口 | `tb_research_event` | 全过程可解释 |
| 有界线程池 + 拒绝策略 | 检索池 / LLM 池 | 防 OOM、防饿死 |
| 条件 UPDATE 状态机 | 任务/阶段状态推进 | 并发与重复投递下的唯一赢家 |

**差异点（本项目必须额外做的）**：
1. 秒杀是「无终点自动重试」，本项目必须**有重试上限并转 FAILED**（成本约束）。
2. 秒杀无「执行租约」，本项目因任务超长，**必须有租约 + 心跳**。
3. 秒杀无「阶段产物」，本项目**必须有 checkpoint**，且检索阶段要细到子查询粒度。
