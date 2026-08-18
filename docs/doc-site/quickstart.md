# 快速开始

本指南将带你用 5 分钟完成 StreamMQ 的接入，从依赖引入到消息收发的完整闭环。

---

## 环境要求

在开始之前，请确认你的开发环境满足以下要求：

| 组件 | 最低版本 | 推荐版本 | 说明 |
|------|----------|----------|------|
| JDK | 21 | 21+ | StreamMQ 基于 Java 21 构建，使用了虚拟线程等新特性 |
| Maven | 3.9 | 3.9+ | 用于依赖管理与构建 |
| Redis | 7.2 | 7.2+ | 底层依赖 Redis Stream，建议 7.2+ |
| Spring Boot | 3.3 | 3.3.5 | 与 Spring Boot 3.3.x 深度集成 |

> 💡 可通过 `java -version`、`mvn -version`、`redis-server --version` 检查本地环境。

---

## Step 1：添加 Maven 依赖

在项目 `pom.xml` 中引入 StreamMQ BOM 与 Starter。BOM 统一管理所有 StreamMQ 模块版本，无需逐个指定：

```xml
<dependencyManagement>
    <dependencies>
        <!-- StreamMQ BOM：统一管理版本 -->
        <dependency>
            <groupId>io.github.streammq</groupId>
            <artifactId>streammq-bom</artifactId>
            <version>0.1.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <!-- StreamMQ Spring Boot Starter：自动装配 + 核心模块 + Redisson 适配 -->
    <dependency>
        <groupId>io.github.streammq</groupId>
        <artifactId>streammq-spring-boot-starter</artifactId>
    </dependency>

    <!-- Redisson Spring Boot Starter：提供 RedissonClient Bean -->
    <dependency>
        <groupId>org.redisson</groupId>
        <artifactId>redisson-spring-boot-starter</artifactId>
    </dependency>
</dependencies>
```

> ℹ️ `streammq-spring-boot-starter` 已传递依赖 `streammq-core` 与 `streammq-redisson`，无需重复引入。
> Redisson 版本由其官方 Starter 管理，建议使用与 Spring Boot 3.3.x 兼容的 3.34.1。

---

## Step 2：配置 application.yml

在 `src/main/resources/application.yml` 中配置 StreamMQ 与 Redisson：

```yaml
spring:
  application:
    name: streammq-demo

streammq:
  enabled: true              # 启用 StreamMQ（默认 true）
  namespace: streammq        # 全局命名空间，用于隔离不同环境（如 dev/test/prod）
  producer:
    group: default-producer
    send-message-timeout: 3000   # 发送超时（毫秒）

# Redisson 配置（单机示例，生产环境请使用集群配置）
redisson:
  singleServerConfig:
    address: "redis://127.0.0.1:6379"
    database: 0
```

### 关键配置说明

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `streammq.enabled` | `true` | 是否启用 StreamMQ 自动装配 |
| `streammq.namespace` | `""` | 全局命名空间，用于 Redis Key 隔离 |
| `streammq.producer.group` | `default-producer-group` | 默认生产者组名 |
| `streammq.producer.send-message-timeout` | `3000` | 发送超时毫秒数 |
| `redisson.*` | - | Redisson 原生配置，参考 Redisson 文档 |

> ⚠️ 生产环境请使用 Redis Sentinel 或 Cluster 配置，并设置独立命名空间隔离环境。

---

## Step 3：启用 StreamMQ

在 Spring Boot 启动类上添加 `@EnableStreamMQ` 注解，显式声明启用 StreamMQ：

```java
package io.github.streammq.demo;

import io.github.streammq.core.annotation.EnableStreamMQ;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableStreamMQ
public class DemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
```

`@EnableStreamMQ` 支持的可选属性：

| 属性 | 默认值 | 说明 |
|------|--------|------|
| `mode` | `MODE_STANDARD` | 启用模式，`MODE_LITE` 为轻量模式（不启用 Actuator 指标） |
| `tracingEnabled` | `false` | 是否全局启用消息追踪 |
| `scanBasePackages` | `{}` | 自定义扫描包路径（默认使用启动类所在包） |

> ℹ️ 即使不添加 `@EnableStreamMQ`，当 `streammq-spring-boot-starter` 在 classpath 时，Spring Boot 也会通过 `AutoConfiguration.imports` 自动装配核心组件。该注解主要用于显式标记与配置属性载体。

---

## Step 4：发送消息

通过 `StreamMessageTemplate` 发送消息。它是对齐 `RocketMQTemplate` 体验的核心生产者 API：

```java
package io.github.streammq.demo;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private final StreamMessageTemplate template;

    // 构造器注入（推荐）
    public OrderService(StreamMessageTemplate template) {
        this.template = template;
    }

    public SendResult sendOrder(String orderId, String content) {
        Message<String> message = MessageBuilder.<String>withTopic("order-topic")
                .tag("created")              // 设置 Tag，用于消息过滤
                .keys(orderId)               // 设置业务键，用于幂等与查询
                .body(content)               // 设置消息体
                .withUserProperty("traceId", "t-001")  // 自定义用户属性
                .build();

        // 同步发送：等待 Redis 返回，失败抛 StreamMQException
        SendResult result = template.syncSend(message);
        System.out.println("消息发送成功: " + result.getMessageId());
        return result;
    }
}
```

### 发送方式一览

| 方法 | 说明 | 适用场景 |
|------|------|----------|
| `syncSend(message)` | 同步发送，等待返回 | 大部分业务场景（默认） |
| `syncSend(message, timeoutMillis)` | 同步发送，指定超时 | 需要自定义超时 |
| `syncSend(message, timeoutMillis, retryTimes)` | 同步发送，指定超时与重试 | 高可靠场景 |
| `asyncSend(message)` | 异步发送，返回 `CompletableFuture` | 高吞吐场景 |
| `asyncSend(message, callback)` | 异步发送，回调通知 | 高吞吐场景 |
| `sendOneway(message)` | 单向发送，不等待响应 | 日志、监控等允许丢失的场景 |
| `syncSendBatch(batch)` | 批量发送 | 批量投递场景 |
| `executeInTransaction(message, callback)` | 事务消息 | 需要最终一致性的场景 |

---

## Step 5：消费消息

使用 `@StreamMQConsumer` 注解声明式定义消费者，实现 `StreamMessageConcurrentlyConsumer` 接口：

```java
package io.github.streammq.demo;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import org.springframework.stereotype.Component;

@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-consumer-group")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {

    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) throws Exception {
        System.out.println("收到订单消息:");
        System.out.println("  - Keys:    " + message.getKeys());
        System.out.println("  - Tag:     " + message.getTag());
        System.out.println("  - Body:    " + message.getBody());
        System.out.println("  - 重试次数: " + context.reconsumeTimes());
        System.out.println("  - traceId: " + message.getUserProperty("traceId"));

        try {
            // 业务处理
            processOrder(message.getKeys(), message.getBody());
            return ConsumeAction.SUCCESS;           // 消费成功，自动 ACK
        } catch (Exception e) {
            // 消费失败，按 RetryPolicy 重试
            return ConsumeAction.RECONSUME_LATER;
            // 或指定延迟重试：return ConsumeAction.defer(Duration.ofSeconds(30));
        }
    }

    private void processOrder(String orderId, String content) {
        // 处理订单业务逻辑
    }
}
```

### ConsumeAction 返回值语义

| 返回值 | 说明 |
|--------|------|
| `ConsumeAction.SUCCESS` | 消费成功，自动 ACK，从 PEL 移除 |
| `ConsumeAction.RECONSUME_LATER` | 消费失败，按 `RetryPolicy` 计算延迟后重投 |
| `ConsumeAction.defer(Duration)` | 消费失败，按指定延迟重投（覆盖 RetryPolicy） |

> ⚠️ 抛出 `RuntimeException` 等价于返回 `RECONSUME_LATER`。框架不提供手动 ACK/nack 调用，避免双模式冲突。

### `@StreamMQConsumer` 常用属性

| 属性 | 默认值 | 说明 |
|------|--------|------|
| `topic` | （必填） | 消费的主题 |
| `consumerGroup` | （必填） | 消费者组名 |
| `consumeMode` | `CLUSTERING` | 消费模式：`CLUSTERING` 集群 / `BROADCASTING` 广播 |
| `messageModel` | `CONCURRENT` | 消息模型：`CONCURRENT` 并发 / `ORDERLY` 顺序 |
| `consumeThreadMin` | `1` | 最小消费线程数 |
| `consumeThreadMax` | `64` | 最大消费线程数 |
| `maxReconsumeTimes` | `16` | 最大重试次数 |
| `consumeTimeout` | `30000` | 单条消息消费超时（毫秒） |
| `selectorType` | `TAG` | 过滤类型：`TAG` / `SQL92` |
| `selectorExpression` | `"*"` | 过滤表达式，`*` 表示接收全部 |
| `shardCount` | `4` | 顺序消费分片数（仅 `ORDERLY` 生效） |
| `dlqMode` | `false` | 是否为 DLQ 死信消费者 |
| `pullBatchSize` | `32` | 单次拉取批量大小 |

---

## Step 6：通过 REST 接口测试

为了让发送与消费形成完整闭环，可添加一个 REST Controller 触发消息发送（REST 触发接口并非必需——也可通过发送方示例或测试触发消息发送）：

```java
package io.github.streammq.demo;

import io.github.streammq.core.message.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping("/order")
    public String createOrder(@RequestParam String orderId,
                              @RequestParam String content) {
        SendResult result = orderService.sendOrder(orderId, content);
        return "订单消息已发送，messageId=" + result.getMessageId();
    }
}
```

### 运行测试

1. **启动 Redis**：确保 `redis://127.0.0.1:6379` 可访问。

2. **启动应用**：运行 `DemoApplication.main()`，观察控制台日志：

   ```
   StreamMQ auto-configured: namespace=streammq
   StreamMQ consumer registered: topic=order-topic, group=order-consumer-group
   ```

3. **发送消息**：使用 curl 或 Postman 发送请求：

   ```bash
   curl -X POST "http://localhost:8080/order?orderId=123&content=test"
   ```

   预期返回：
   ```
   订单消息已发送，messageId=123-0-1700000000000-0
   ```

4. **观察消费**：消费者控制台将输出收到的消息内容。

---

## Step 7：下一步

完成基础接入后，推荐通过官方示例工程学习高级特性：

| 示例 | 路径 | 说明 |
|------|------|------|
| **快速开始** | [`streammq-samples/streammq-sample-quickstart`](https://github.com/streammq/streammq/tree/main/streammq-samples/streammq-sample-quickstart) | 本文对应的完整示例 |
| **事务消息** | [`streammq-samples/streammq-sample-transaction`](https://github.com/streammq/streammq/tree/main/streammq-samples/streammq-sample-transaction) | 半消息 + 本地事务 + 事务回查 |
| **延时消息** | [`streammq-samples/streammq-sample-delay`](https://github.com/streammq/streammq/tree/main/streammq-samples/streammq-sample-delay) | 18 级延时 + 任意毫秒延时 |
| **顺序消息** | [`streammq-samples/streammq-sample-orderly`](https://github.com/streammq/streammq/tree/main/streammq-samples/streammq-sample-orderly) | ShardingKey 分片顺序消费 |
| **死信队列** | [`streammq-samples/streammq-sample-dlq`](https://github.com/streammq/streammq/tree/main/streammq-samples/streammq-sample-dlq) | DLQ 死信消费与处理 |
| **拦截器** | [`streammq-samples/streammq-sample-interceptor`](https://github.com/streammq/streammq/tree/main/streammq-samples/streammq-sample-interceptor) | Producer / Consumer 拦截器 |

### 推荐阅读

- [核心特性](features.md) —— 全部特性的详解与代码示例
- [核心概念](concepts.md) —— Topic / ConsumerGroup / 消费模型等关键术语
- [配置参考](configuration.md) —— 全部配置项说明
- [API 文档](api.md) —— 完整 API 参考

---

## 故障排查

### 1. 启动报错：找不到 StreamMQ 自动装配

**现象**：`StreamMQAutoConfiguration` 未生效，`StreamMessageTemplate` Bean 不存在。

**排查**：
- 确认 `streammq-spring-boot-starter` 已在依赖中（`mvn dependency:tree | grep streammq`）。
- 确认 `streammq.enabled` 未被设置为 `false`。
- 确认 `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` 资源文件存在。

### 2. 连接 Redis 失败

**现象**：`RedisConnectionException: Unable to connect to Redis server`。

**排查**：
- 确认 Redis 服务已启动：`redis-cli ping` 应返回 `PONG`。
- 确认 `redisson.singleServerConfig.address` 配置正确，格式为 `redis://host:port`。
- 检查防火墙、网络连通性。

### 3. 消息发送超时

**现象**：`ProducerTimeoutException: Send message timeout`。

**排查**：
- 调大 `streammq.producer.send-message-timeout`（默认 3000ms）。
- 检查 Redis 负载与网络延迟。
- 确认 Redis 未执行 `BGSAVE` 等阻塞操作。

### 4. 消费者收不到消息

**现象**：发送成功但消费者无日志输出。

**排查**：
- 确认 `@StreamMQConsumer` 的 `topic` 与发送方一致。
- 确认 `consumerGroup` 唯一，未被其他应用占用相同组名导致消息被抢消费。
- 确认消费者类被 Spring 扫描到（`@Component` 注解 + 包路径在扫描范围内）。
- 检查 `selectorExpression` 是否过滤掉了消息（默认 `*` 接收全部）。
- 确认 `enable` 属性为 `true`（默认）。

### 5. 消费者反复重试

**现象**：消息反复消费，`reconsumeTimes` 持续增长。

**排查**：
- 检查 `onMessage` 是否抛出异常或返回 `RECONSUME_LATER`。
- 检查 `maxReconsumeTimes` 配置，达到上限后消息进入 DLQ。
- 确认业务逻辑是否存在 Bug（如空指针、数据库锁冲突）。

### 6. Redis 内存持续增长

**现象**：Redis 内存占用持续上升。

**排查**：
- Stream 默认不限制长度，建议设置 `streamMaxLen` 或 `stream.stream-max-len` 限制每个 Topic 的 Stream 长度。
- 检查是否有未 ACK 的消息堆积在 PEL（Pending Entries List）。
- 检查 DLQ Stream 是否有大量未消费的死信。

### 7. 获取更多帮助

- 查看 [FAQ](faq.md)
- 提交 Issue：https://github.com/streammq/streammq/issues
- 查阅 [API 文档](api.md) 与 [配置参考](configuration.md)

---

*下一步 → [核心特性](features.md)*
