# 黑马点评后端（hm-dianping）

这是一个基于 Spring Boot 的点评类项目，包含用户登录、商铺查询、探店笔记、关注、签到、优惠券与秒杀下单等功能。`front/nginx-1.18.0` 中已经包含 Windows 版 Nginx、前端页面和静态资源。

## 技术栈

- Java 8
- Spring Boot 2.3.12.RELEASE
- Maven
- MySQL + MyBatis-Plus
- Redis + Lettuce + Redisson
- Kafka

默认服务地址如下：

| 服务 | 默认地址 | 是否必需 |
| --- | --- | --- |
| 应用 | `http://localhost:8081` | 是 |
| MySQL | `127.0.0.1:3306`，数据库 `hmdp` | 是 |
| Redis | `127.0.0.1:6379`，当前配置无密码 | 是 |
| Kafka | `localhost:9092` | 是，当前秒杀链路使用 Kafka |

## 启动前准备

建议准备以下环境：

1. **JDK 8**
   
   `pom.xml` 的目标版本是 Java 8。当前依赖中的旧版 Lombok 与 JDK 21 不兼容，使用 JDK 21 编译会出现 `JCTree$JCImport.qualid` 相关错误，因此优先使用 JDK 8。

2. **Maven 3.6+**
   
   首次构建需要访问 Maven Central 下载依赖。

3. **MySQL 5.7**
   
   SQL 文件来源于 MySQL 5.6，项目使用 MySQL Connector/J 5.1.47。MySQL 5.7 是最省事的本地选择。使用 MySQL 8 时可能还需要处理认证插件或连接参数兼容问题。

4. **Redis**
   
   Redis 必须监听 `6379`。当前 Redisson 地址在 `RedissonConfig.java` 中写死为 `redis://localhost:6379`，并且没有设置密码。

5. **Kafka**
   
   Kafka 必须监听 `9092`。当前秒杀接口使用 `voucher-orders` topic 投递订单，消费失败后进入 `voucher-orders.DLT`。

本机已安装的 Kafka 是 3.9.0。Kafka 进程请使用 JDK 17（或 JDK 21），后端项目使用 JDK 8；分别在不同终端中设置 `JAVA_HOME`，互不影响。

确认本地 Java 和 Maven：

```powershell
java -version
mvn -version
```

两条命令显示的 Java 都应该是 1.8。如果 Windows 上安装了多个 JDK，可以为当前 PowerShell 临时切换：

```powershell
$env:JAVA_HOME = "C:\path\to\jdk8"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
java -version
mvn -version
```

本机项目后端可直接使用：

```powershell
$env:JAVA_HOME = "D:\SoftwareDownload\JDKVersion\jdk8"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
```

在 IntelliJ IDEA 中还需要设置：

- `File > Project Structure > Project SDK` 选择 `D:\SoftwareDownload\JDKVersion\jdk8`
- `Settings > Build Tools > Maven > Runner > JRE` 选择同一个 JDK 8
- 重新加载 Maven 项目后，在 IDEA Terminal 中执行 `mvn -version` 再确认一次

## 1. 初始化 MySQL

本机 MySQL 服务名是 `MySQL80`。使用**管理员 PowerShell**启动并检查：

```powershell
Start-Service MySQL80
Get-Service MySQL80
```

如果状态为 `Running`，进入 MySQL 客户端：

```powershell
& "D:\SoftwareDownload\MySQL\MySQL Server 8.0\bin\mysql.exe" -u root -p
```

输入 MySQL root 密码后执行：

```sql
CREATE DATABASE IF NOT EXISTS hmdp
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;
USE hmdp;
SOURCE D:/StudyProjects/ProjectBench/hobby-workshop/src/main/resources/db/hmdp.sql;
```

如果项目不在上述目录，请将 `SOURCE` 后面的路径替换为本机 SQL 文件的绝对路径，并使用 `/` 作为路径分隔符。

默认数据库配置位于 `src/main/resources/application.yaml`：

```yaml
spring:
  datasource:
    url: jdbc:mysql://127.0.0.1:3306/hmdp?useSSL=false&serverTimezone=UTC
    username: ${DB_USERNAME:root}
    password: ${DB_PASSWORD}
```

密码不再写进配置文件，启动前通过环境变量提供（`DB_USERNAME` 可省略，默认 `root`）：

```powershell
$env:DB_PASSWORD = "你的密码"
mvn spring-boot:run
```

也可以只对单次启动生效：

```powershell
mvn spring-boot:run "-Dspring-boot.run.arguments=--spring.datasource.username=root --spring.datasource.password=你的密码"
```

## 2. 启动 Redis

项目需要 Redis 6.2 或更高版本，因为附近商铺查询使用了 Redis 6.2 新增的 `GEOSEARCH`。不要使用 `D:\SoftwareDownload\Redis6\bin` 中实际版本为 6.0.6 的程序。

本机已经安装 Redis 6.2.10，新开一个 PowerShell 执行：

```powershell
Set-Location "D:\SoftwareDownload\Redis-6.2.10-Windows-x64-msys2-with-Service\Redis-6.2.10-Windows-x64-msys2-with-Service"
.\redis-server.exe .\redis.conf
```

保持该窗口打开，再开一个 PowerShell 检查：

```powershell
& "D:\SoftwareDownload\Redis-6.2.10-Windows-x64-msys2-with-Service\Redis-6.2.10-Windows-x64-msys2-with-Service\redis-cli.exe" -h 127.0.0.1 -p 6379 ping
```

预期返回：

```text
PONG
```

当前 Redis 6.2.10 使用无密码配置，`application.yaml` 中的密码也保持注释。`RedissonConfig` 已调整为读取 `spring.redis` 的主机、端口和可选密码；如果以后启用密码，需要同时修改 Redis 配置和 `application.yaml`。

## 3. 启动 Kafka

确保 Kafka broker 可从 `localhost:9092` 访问。若 broker 禁止自动创建 topic，请手动创建：

本机 Kafka 3.9.0 使用现有 ZooKeeper 配置启动。先新开 PowerShell A，使用 JDK 17 启动 ZooKeeper：

```powershell
$env:JAVA_HOME = "D:\SoftwareDownload\JDKVersion\jdk17"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
Set-Location "D:\SoftwareDownload\kafka"
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

保持窗口 A 打开，再新开 PowerShell B，启动 Kafka：

```powershell
$env:JAVA_HOME = "D:\SoftwareDownload\JDKVersion\jdk17"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
Set-Location "D:\SoftwareDownload\kafka"
.\bin\windows\kafka-server-start.bat .\config\server.properties --override log.dirs=D:/tmp/kafka-logs-hmdp
```

这里通过 `--override` 使用独立的 `D:/tmp/kafka-logs-hmdp` 日志目录，避免本机旧目录 `D:/tmp/kafka-logs` 中被 Windows 占用的索引文件导致 Broker 退出。ZooKeeper 数据目录为 `D:/tmp/zookeeper`；程序会在需要时创建这些目录。

```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --if-not-exists --topic voucher-orders --partitions 1 --replication-factor 1
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --if-not-exists --topic voucher-orders.DLT --partitions 1 --replication-factor 1
```

Linux/macOS 使用 `kafka-topics.sh`。`voucher-orders.DLT` 的分区数不能少于主 topic。

## 4. 构建并启动后端

新开 PowerShell C，在项目根目录切换至 JDK 8，然后执行：

```powershell
$env:JAVA_HOME = "D:\SoftwareDownload\JDKVersion\jdk8"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
Set-Location "D:\StudyProjects\ProjectBench\hobby-workshop"
mvn -version
mvn clean package -DskipTests
mvn spring-boot:run
```

或者运行打包产物：

```powershell
java -jar target/hm-dianping-0.0.1-SNAPSHOT.jar
```

看到类似下面的日志代表 Web 服务已启动：

```text
Started HmDianPingApplication
```

然后访问一个无需登录的接口进行检查：

```powershell
Invoke-RestMethod http://localhost:8081/shop-type/list
```

## 5. 启动前端 Nginx

新开 PowerShell D：

```powershell
Set-Location "D:\StudyProjects\ProjectBench\hobby-workshop\front\nginx-1.18.0"
.\nginx.exe -t
.\nginx.exe
```

浏览器访问：

```text
http://localhost:8080
```

Nginx 已配置为将 `/api/*` 转发到后端 `127.0.0.1:8081`，不需要再改前端的 `common.js`。

停止 Nginx：

```powershell
Set-Location "D:\StudyProjects\ProjectBench\hobby-workshop\front\nginx-1.18.0"
.\nginx.exe -s quit
```

## 当前构建状态

`KafkaConsumerConfig` 的泛型编译问题已修正，项目已在 JDK 8 下通过：

```powershell
mvn clean package -DskipTests
```

构建产物为 `target/hm-dianping-0.0.1-SNAPSHOT.jar`。Maven 仍会提示 `spring-boot-starter-data-redis` 重复声明；它目前不阻止构建，后续可以再合并 `pom.xml` 中的两处 Redis starter 配置。

## 功能使用提示

### 登录

项目没有接入真实短信服务。请求验证码后，验证码会以 DEBUG 日志输出，并保存到 Redis。

```powershell
Invoke-RestMethod -Method Post "http://localhost:8081/user/code?phone=13800138000"
```

从应用日志取得验证码后登录：

```powershell
$body = @{ phone = "13800138000"; code = "日志中的六位验证码" } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://localhost:8081/user/login -ContentType "application/json" -Body $body
```

返回的 token 应通过请求头 `authorization` 传给需要登录的接口。

### 秒杀

新增秒杀券接口 `POST /voucher/seckill` 会同时写入 MySQL 秒杀信息和 Redis 的 `seckill:stock:{voucherId}` 库存。下单接口 `POST /voucher-order/seckill/{id}` 还会向 Kafka 的 `voucher-orders` topic 发送订单消息。

仓库 SQL 只提供普通券演示数据，没有可直接下单的秒杀券，因此测试秒杀前需要先新增一张秒杀券。

### 图片上传

图片上传目录已经调整为项目内的：

```text
front/nginx-1.18.0/html/hmdp/imgs
```

请始终从项目根目录启动后端，否则相对路径会指向错误位置。

## 测试注意事项

不要把 `mvn test` 当作普通健康检查。当前测试类会连接真实 MySQL/Redis，并包含以下有副作用的操作：

- 并发生成约 30,000 个 Redis ID
- 向 Redis 写入店铺 GEO 数据
- 写入约 1,000,000 次 HyperLogLog 样本
- 在项目根目录生成或覆盖 `Tokens.txt`

日常只验证编译和打包时使用：

```powershell
mvn clean package -DskipTests
```

## 常见问题

### `Communications link failure`

MySQL 未启动、`hmdp` 数据库未创建，或者 `application.yaml` 中的账号密码不正确。

### `Unable to connect to Redis` / Redisson 连接失败

确认 Redis 在 `localhost:6379` 运行。若启用了密码，需要同时配置 Spring Redis 与 Redisson。

### Kafka 持续打印连接失败

确认 broker 在 `localhost:9092` 运行，并检查 Kafka 的 advertised listeners 是否能被应用所在机器解析和访问。

### 8081 端口被占用

临时换端口启动：

```powershell
mvn spring-boot:run "-Dspring-boot.run.arguments=--server.port=8082"
```

### 使用 JDK 17/21 编译时报 Lombok/Javac 错误

切回 JDK 8，并再次确认 `mvn -version` 显示的 Java 版本也是 1.8。只看 `java -version` 不足以确认 Maven 使用了哪个 JDK。

## 目录结构

```text
src/main/java/com/hmdp/        Java 业务代码
src/main/resources/            应用配置、Mapper XML、Lua 脚本
src/main/resources/db/hmdp.sql 数据库结构与演示数据
src/test/                      集成测试（依赖真实基础设施）
front/nginx-1.18.0/            Windows Nginx、前端页面与图片
pom.xml                        Maven 依赖与构建配置
```
