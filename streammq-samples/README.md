# StreamMQ Samples

本目录包含 StreamMQ 各核心功能的示例项目，可直接运行验证。

## 前置条件

- JDK 21+
- Docker（运行 Redis）
- Maven 3.9+

## 快速启动 Redis

在项目根目录执行：

```bash
cd streammq-samples
docker-compose up -d
```

## 示例项目一览

| 项目 | 功能 | 启动类 |
|------|------|--------|
| `streammq-sample-quickstart` | 同步发送 + 并发消费 | `QuickstartApplication` |
| `streammq-sample-orderly` | 顺序消息消费 | `OrderlyApplication` |
| `streammq-sample-transaction` | 事务消息 | `TransactionApplication` |
| `streammq-sample-delay` | 延时消息（18 级 + 自定义毫秒） | `DelayApplication` |
| `streammq-sample-dlq` | 死信队列消费 | `DlqApplication` |

## 运行示例

每个示例均为独立 Spring Boot 应用，以 quickstart 为例：

```bash
cd streammq-sample-quickstart
mvn spring-boot:run
```

示例启动后自动发送消息并消费，观察控制台日志输出。

## 配置说明

所有示例使用默认 Redis 连接：`localhost:6379`，可通过 `application.yml` 修改：

```yaml
spring:
  redis:
    host: localhost
    port: 6379
```

## 停止环境

```bash
docker-compose down
```
