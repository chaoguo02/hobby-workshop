# 兴趣工作坊预约系统

面向兴趣课程与线下工作坊的高并发预约系统，包含用户登录、工作坊浏览、限量名额抢购、模拟支付、预约核销、超时关单和后台监控。系统使用 Kafka 异步创建订单，并通过 Caffeine + Redis 二级缓存承载热点详情访问。

## 核心链路

### 限量工作坊预约

1. 用户抢购工作坊名额，Lua 在 Redis 中原子判断库存和一人一单并预扣库存。
2. MySQL 本地消息表记录待投递预约，Relay 将消息发送到 `voucher-orders`。
3. Kafka 消费者在同一事务中创建待支付订单、扣减 MySQL 库存并完成消息状态。
4. `order_id` 主键和 `(user_id, voucher_id)` 唯一索引负责消费幂等和一人一单兜底。
5. 用户在支付期限内点击支付，订单从“待支付”变为“待核销”；工作人员到店核销后变为“已核销”。

消息失败会重试并进入 DLT；监控入口为 `GET /monitor/seckill`。

### 超时关单

待支付订单默认保留名额 15 分钟。用户主动取消或支付超时后，订单先进入“关闭处理中”，在 MySQL 事务中恢复库存，再通过幂等 Lua 恢复 Redis 库存并删除用户预约标记，全部完成后才进入“已取消”。Redis 补偿失败会记录错误并指数退避重试。

```text
待支付 ──点击支付──> 待核销 ──到店核销──> 已核销
   └──取消/超时──> 关闭处理中 ──补偿完成──> 已取消
```

支付为项目内模拟流程，不接入第三方支付平台。

### 二级缓存

- 查询：`Caffeine L1 -> Redis L2 -> MySQL`。
- 热点 Key 使用逻辑过期和异步重建。
- 空值缓存防穿透，随机 TTL 防雪崩。
- 更新时先提交数据库，再删除缓存。
- 缓存失效事件与数据库更新同事务保存；删除失败由 Relay 重试。
- Redis Pub/Sub 通知其他实例清理本地 Caffeine。
- Redis 异常时限制数据库并发回源，避免压垮 MySQL。

热点工作坊详情预热默认关闭。当前底层仍复用 `Shop` 数据模型，所以环境变量暂时保留原名：

```text
CACHE_PREWARM_ENABLED=true
CACHE_PREWARM_SHOP_IDS=1,2,10
```

缓存监控入口为 `GET /monitor/cache`。

## 环境要求

| 服务 | 版本/地址 |
| --- | --- |
| 后端 JDK | JDK 8 |
| Maven | 3.6+ |
| MySQL | `127.0.0.1:3306/hmdp` |
| Redis | `127.0.0.1:6379`，需要 6.2+ |
| Kafka | `localhost:9092` |
| 后端 | `http://localhost:8081` |
| 前端 | `http://localhost:8080` |

本机 Kafka 使用 JDK 17，后端使用 JDK 8，分别在不同 PowerShell 窗口中启动。

## 第一次运行

### 1. 初始化 MySQL

启动 MySQL 后进入客户端：

```powershell
& "D:\SoftwareDownload\MySQL\MySQL Server 8.0\bin\mysql.exe" -u root -p
```

首次初始化执行：

```sql
CREATE DATABASE IF NOT EXISTS hmdp DEFAULT CHARACTER SET utf8mb4;
USE hmdp;
SOURCE D:/StudyProjects/ProjectBench/hobby-workshop/src/main/resources/db/hmdp.sql;
```

如果数据库已经初始化，只需在 MySQL 客户端执行增量脚本，不要重复执行包含 `DROP TABLE` 的完整 `hmdp.sql`：

```sql
USE hmdp;
SOURCE D:/StudyProjects/ProjectBench/hobby-workshop/src/main/resources/db/cache_invalidation.sql;
SOURCE D:/StudyProjects/ProjectBench/hobby-workshop/src/main/resources/db/voucher_order_close.sql;
```

### 2. 配置本地环境变量

在项目根目录创建不会提交到 Git 的 `.env`：

```text
DB_USERNAME=root
DB_PASSWORD=你的MySQL密码
JAVA_HOME=D:\SoftwareDownload\JDKVersion\jdk8
MONITOR_ADMIN_TOKEN=自定义随机令牌
```

### 3. 启动 Redis

```powershell
Set-Location "D:\SoftwareDownload\Redis-6.2.10-Windows-x64-msys2-with-Service\Redis-6.2.10-Windows-x64-msys2-with-Service"
.\redis-server.exe .\redis.conf
```

验证：

```powershell
.\redis-cli.exe ping
```

返回 `PONG` 即正常。

### 4. 启动 Kafka

PowerShell A：

```powershell
$env:JAVA_HOME = "D:\SoftwareDownload\JDKVersion\jdk17"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
Set-Location "D:\SoftwareDownload\kafka"
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

PowerShell B：

```powershell
$env:JAVA_HOME = "D:\SoftwareDownload\JDKVersion\jdk17"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
Set-Location "D:\SoftwareDownload\kafka"
.\bin\windows\kafka-server-start.bat .\config\server.properties --override log.dirs=D:/tmp/kafka-logs-hmdp
```

首次创建 Topic：

```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --if-not-exists --topic voucher-orders --partitions 1 --replication-factor 1
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --if-not-exists --topic voucher-orders.DLT --partitions 1 --replication-factor 1
```

### 5. 启动后端

项目根目录执行：

```powershell
powershell -ExecutionPolicy Bypass -File .\run.ps1
```

看到 `Started HmDianPingApplication` 后验证：

```powershell
Invoke-RestMethod http://localhost:8081/shop-type/list
```

构建但不启动：

```powershell
mvn clean package -DskipTests
```

### 6. 启动前端

```powershell
Set-Location "D:\StudyProjects\ProjectBench\hobby-workshop\front\nginx-1.18.0"
.\nginx.exe -t
.\nginx.exe
```

浏览器访问 `http://localhost:8080`。停止 Nginx：

```powershell
.\nginx.exe -s quit
```

## 管理接口

`/monitor/**` 需要同时携带登录 token 和管理员令牌：

```powershell
$headers = @{
  authorization = "登录token"
  "X-Monitor-Token" = "MONITOR_ADMIN_TOKEN"
}
Invoke-RestMethod http://localhost:8081/monitor/cache -Headers $headers
```

## 常见问题

- Kafka 持续报告 `localhost:9092` 连接失败：Kafka broker 没有启动或启动后退出。
- `Communications link failure`：检查 MySQL 服务、`hmdp` 数据库和 `.env` 密码。
- Redis/Redisson 连接失败：确认 Redis 正在监听 `6379`，密码配置保持一致。
- JDK 17/21 编译 Lombok 报错：后端切换到 JDK 8，并用 `mvn -version` 确认。
- 端口 `8081` 被占用：启动时传入 `-Dspring-boot.run.arguments=--server.port=8082`。

## 测试说明

部分旧测试会连接真实 MySQL/Redis 并写入大量数据，不要把完整 `mvn test` 当作普通健康检查。日常使用：

```powershell
mvn clean package -DskipTests
mvn -Dtest=CacheClientTest test
```

## 目录

```text
src/main/java/com/hmdp/         后端代码
src/main/resources/             配置、SQL 和 Lua
front/nginx-1.18.0/             前端与 Windows Nginx
run.ps1                         本地启动脚本
pom.xml                         Maven 配置
```
