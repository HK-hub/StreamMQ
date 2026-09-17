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
| `streammq-sample-quickstart` | 同步发送 + 并发消费 | `QuickStartApplication` |
| `streammq-sample-orderly` | 顺序消息消费 | `OrderlySampleApplication` |
| `streammq-sample-transaction` | 事务消息 | `TransactionSampleApplication` |
| `streammq-sample-delay` | 延时消息（18 级 + 自定义毫秒） | `DelaySampleApplication` |
| `streammq-sample-dlq` | 死信队列消费 | `DlqSampleApplication` |
| `streammq-sample-interceptor` | Producer / Consumer 拦截器 | `InterceptorSampleApplication` |
| `streammq-sample-diagnostics` | 诊断画像与慢消费 | `DiagnosticsApplication` |
| `streammq-sample-tracing` | OpenTelemetry 链路追踪 | `TracingApplication` |

## 运行示例

每个示例均为独立 Spring Boot 应用，以 quickstart 为例：

```bash
# 0. 先在仓库根目录把 StreamMQ 构件安装到本地仓库
#    示例通过 BOM 以无版本号方式依赖 io.github.streammq:streammq-spring-boot-starter；
#    在 0.1.2 正式发布到 Maven Central 之前（或本地开发调试时），必须先 install 否则依赖无法解析；
#    也可只构建 starter 及其上游模块：mvn -q -DskipTests -pl streammq-spring-boot-starter -am install
mvn -q -DskipTests install

cd streammq-samples/streammq-sample-quickstart
mvn spring-boot:run
```

示例启动后注册消费者并等待消息；消息发送由示例内 `ApplicationRunner` 自动触发（部分示例需在 IT 或外部发送方中触发），观察控制台日志输出。

## 配置说明

所有示例使用默认 Redis 连接：`localhost:6379`，可通过各示例的 `application.yml` 修改。Redisson Spring Boot Starter 依据 `spring.data.redis.*` 构建客户端：

```yaml
spring:
  data:
    redis:
      host: 127.0.0.1
      port: 6379
      database: 0
      # password: ${REDIS_PASSWORD:}   # 需要认证时打开
```

> **注意**：`redisson.singleServerConfig.*` 在本 starter 中**没有属性绑定**（只绑定 `spring.redis.redisson.config` / `spring.redis.redisson.file`），写了不会生效。集群 / 哨兵等高级拓扑请通过 `spring.redis.redisson.config` 提供 Redisson 原生配置。

## 停止环境

```bash
docker-compose down
```
