# StreamMQ Samples

本目录包含 StreamMQ 各核心功能的示例项目，可在本仓库内构建运行验证。

## 前置条件

- JDK 21+
- Docker（运行 Redis；也可用本地 redis-server）
- Maven 3.9+

## 快速启动 Redis

在项目根目录执行：

```bash
cd streammq-samples
docker-compose up -d
```

> **注意：`docker-compose.yml` 与 CONTRIBUTING 的 `docker run` 示例互斥，二选一。**
> 二者容器名同为 `streammq-redis`、端口同为 `6379:6379`，同时使用会因名称/端口冲突启动失败。
> 若已在 CONTRIBUTING 流程中执行过 `docker run -d --name streammq-redis -p 6379:6379 redis:7.2`，
> 请直接复用该容器，不要再执行 `docker-compose up -d`。

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

示例 POM 的 parent 指向本仓库 reactor（`streammq-parent` → `streammq-samples`），因此**必须在本仓库内构建**：
它们不是可以从 Maven Central 独立拉取依赖的「独立 Spring Boot 应用」，而是随仓库一同构建、运行的示例工程。

```bash
# 0. 先在仓库根目录把 StreamMQ 构件安装到本地仓库
#    示例通过 BOM 以无版本号方式依赖 io.github.streammq:streammq-spring-boot-starter；
#    在 0.1.2 正式发布到 Maven Central 之前（或本地开发调试时），必须先 install 否则依赖无法解析；
#    若要运行 diagnostics / tracing 两个示例，请安装整个 reactor——它们依赖
#    streammq-diagnostics / streammq-tracing-opentelemetry 这两个【不随 0.1.2 发布】的模块（不在 BOM 中，示例以 ${project.version} 直接引用）：
mvn -q -DskipTests install

# 只跑普通示例（quickstart / orderly / transaction / delay / dlq / interceptor）时，也可只装 starter 及其上游模块：
# mvn -q -DskipTests -pl streammq-bom,streammq-spring-boot-starter -am install

cd streammq-samples/streammq-sample-quickstart
mvn spring-boot:run
```

示例启动后注册消费者并等待消息；消息发送由示例内 `ApplicationRunner` 自动触发（部分示例需在 IT 或外部发送方中触发），观察控制台日志输出。

### 两个示例的特殊说明

- **`streammq-sample-diagnostics`**：监听 **8081** 端口（`server.port: 8081`，避免与其它示例的 8080 冲突），
  诊断端点在应用主端口的 `/streammq/diagnostics/**`（不受 Actuator `management.endpoints.web.exposure.*` 治理）。
  它需要 `streammq-diagnostics` 模块，且 `application.yml` 中已开启 `streammq.diagnostics.enabled=true`、
  `streammq.trace.enabled=true` + `storage=redis`。
- **`streammq-sample-tracing`**：开启 `streammq.tracing.otel.enabled=true`，但**只有配置了
  `streammq.tracing.otel.otlp-endpoint`（如 `http://localhost:4317`）才会真正导出 Span**；
  未配置端点时为 no-op 实例（不导出任何数据，仅打印一条 INFO 日志），需要自行启动 OTLP Collector/Jaeger 才能看到链路。

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
cd streammq-samples
docker-compose down
```

> 若 Redis 是用 `docker run` 启动的（CONTRIBUTING 流程），则用 `docker stop streammq-redis` 停止。
