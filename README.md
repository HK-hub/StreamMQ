# StreamMQ

> 基于 Redis Stream + Redisson 的高性能消息中间件 SDK，让 Redis 成为你的消息总线。

![License](https://img.shields.io/badge/License-MIT-blue.svg)
![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.x-green.svg)
![Redisson](https://img.shields.io/badge/Redisson-3.34.x-red.svg)
![Version](https://img.shields.io/badge/Version-0.1.0--SNAPSHOT-lightgrey.svg)

---

## 目录

- [项目介绍](#项目介绍)
- [快速开始](#快速开始)
- [核心概念](#核心概念)
- [使用指南](#使用指南)
- [模块结构](#模块结构)
- [可观测性](#可观测性)
- [文档导航](#文档导航)
- [路线图](#路线图)
- [贡献与许可](#贡献与许可)

---

## 项目介绍

### 什么是 StreamMQ

StreamMQ 是一款基于 **Redis Stream** 与 **Redisson** 构建的开源消息中间件 SDK，以 MIT 协议发布。它将 Redis Stream 的原生能力封装为一套类似 RocketMQ 的、面向业务开发者友好的消息 API，让你在无需引入重量级 MQ（如 RocketMQ / Kafka 集群）的前提下，获得：

- 注解驱动的声明式消费
- 类似 `RocketMQTemplate` 的 `StreamMessageTemplate` 编程模型
- 事务消息、延时消息、顺序消息、批量发送等高级特性
- 与 Spring Boot 3 深度集成的自动装配能力

如果你已经在使用 Redis，又需要一个轻量、可靠、易用的消息中间件，StreamMQ 是你的理想选择。

### 核心特性

| 序号 | 特性              | 说明                                                                 |
| ---- | ----------------- | -------------------------------------------------------------------- |
| 1    | **注解驱动**      | `@StreamMQConsumer` 声明式定义消费者                                  |
| 2    | **Template 编程** | `StreamMessageTemplate` 统一发送入口，同步/异步/事务/批量一站式调用    |
| 3    | **4 种消费模型**  | 集群消费 / 广播消费 / 顺序消费 / DLQ 死信消费                        |
| 4    | **事务消息**      | 半消息 + 本地事务 + 事务回查机制，保证最终一致性                    |
| 5    | **延时消息**      | 内置 18 级延时等级，亦可自定义任意延时毫秒                            |
| 6    | **顺序消息**      | 基于 ShardingKey 的分片顺序消费，保证同一分区内严格有序              |
| 7    | **批量发送**      | `BatchMessage` 批量投递，充分利用 Pipeline 提升吞吐                 |
| 8    | **可观测性**      | Micrometer 指标 / MDC 结构化日志 / Trace 集成 / Actuator 健康检查     |
| 9    | **背压控制**      | InflightQueue 拉取-处理解耦，防止内存溢出                           |
| 10   | **消费超时取消**  | 支持消费超时自动取消并进入重试队列                                   |

### 与同类产品对比

| 能力             | StreamMQ                | Redisson RStream       | Spring Data Redis Stream | RocketMQ         | Kafka            |
| ---------------- | ----------------------- | ---------------------- | ------------------------ | ---------------- | ---------------- |
| 底层存储         | Redis Stream            | Redis Stream           | Redis Stream             | NameServer+Broker | Broker+ZK/KRaft  |
| 部署复杂度       | **低（仅 Redis）**      | 低（仅 Redis）         | 低（仅 Redis）           | 高（独立集群）    | 高（独立集群）    |
| 注解声明式消费   | **支持**                | 不支持                 | 部分支持                 | 支持              | 不支持            |
| Template 编程    | **支持**                | 不支持                 | 不支持                   | 支持              | 不支持            |
| 事务消息         | **支持**                | 不支持                 | 不支持                   | 支持              | 不支持            |
| 延时消息         | **支持（18 级+任意）**  | 不支持                 | 不支持                   | 支持（18 级）     | 不支持            |
| 顺序消息         | **支持**                | 不支持                 | 不支持                   | 支持              | 支持（分区内）    |
| 背压控制         | **支持**                | 不支持                 | 不支持                   | 支持              | 支持              |
| Spring Boot 集成 | **深度集成**            | 一般                   | 一般                     | 一般（第三方）     | 一般（第三方）    |
| 学习成本         | **低**                  | 中                     | 中                       | 中                | 中                |
| 适用规模         | 中小规模                | 中小规模               | 中小规模                 | 大规模            | 超大规模          |

### 适用场景

- 已有 Redis 基础设施，希望复用为消息总线
- 中小规模业务（单集群日消息量 < 1 亿）
- 需要事务消息 / 延时消息 / 顺序消息能力但不想引入独立 MQ 集群
- 微服务架构下基于 Spring Boot 3 的轻量级异步通信
- 电商订单状态流转、支付回调、库存扣减、通知推送等业务场景

### 反场景（不建议使用）

- 超大规模流式数据处理（单集群日消息量 > 1 亿）—— 建议使用 Kafka
- 对消息吞吐要求极高且可容忍少量丢失 —— 建议使用 Kafka
- 需要复杂路由规则（topic 通配符、多级路由）—— 建议使用 RabbitMQ
- 已有成熟 MQ 集群且无 Redis 资源 —— 直接复用现有 MQ

---

## 快速开始

### 环境要求

| 组件    | 最低版本 | 推荐版本 |
| ------- | ------- | -------- |
| JDK     | 21      | 21       |
| Maven   | 3.9     | 3.9+     |
| Redis   | 7.2     | 7.2+     |
| Spring Boot | 3.3 | 3.3.5   |

### 1. 引入 Maven 依赖

在你的 Spring Boot 3 项目 `pom.xml` 中引入 BOM 与 Starter：

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.github.streammq</groupId>
            <artifactId>streammq-bom</artifactId>
            <version>0.1.0-SNAPSHOT</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <dependency>
        <groupId>io.github.streammq</groupId>
        <artifactId>streammq-spring-boot-starter</artifactId>
    </dependency>

    <dependency>
        <groupId>org.redisson</groupId>
        <artifactId>redisson-spring-boot-starter</artifactId>
    </dependency>
</dependencies>
```

### 2. 配置 application.yml

```yaml
spring:
  application:
    name: streammq-demo

streammq:
  enabled: true
  namespace: streammq

redisson:
  singleServerConfig:
    address: "redis://127.0.0.1:6379"
    database: 0
```

### 3. 发送消息

```java
@Component
public class OrderService {
    private final StreamMessageTemplate template;

    public OrderService(StreamMessageTemplate template) {
        this.template = template;
    }

    public SendResult sendOrder(String orderId, String content) {
        Message<String> message = MessageBuilder.<String>withTopic("order-topic")
                .tag("created")
                .keys(orderId)
                .body(content)
                .build();
        return template.syncSend(message);
    }
}
```

### 4. 消费消息

```java
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-consumer-group")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {
    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        System.out.println("收到订单：" + message.getKeys() + ", 内容：" + message.getBody());
        return ConsumeAction.SUCCESS;
    }
}
```

### 5. 启用

在启动类添加 `@EnableStreamMQ`：

```java
@SpringBootApplication
@EnableStreamMQ
public class DemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
```

---

## 核心概念

| 概念           | 英文            | 说明                                                          |
| -------------- | --------------- | ------------------------------------------------------------- |
| 主题           | Topic           | 消息的逻辑分类，对应 Redis Stream 的一个 key                  |
| 消费组         | ConsumerGroup   | 一组消费者的逻辑标识，同一组内集群消费，跨组广播              |
| 消息           | Message         | 消息载体，包含 topic/tag/keys/shardingKey/properties/body 等 |
| 消费结果       | ConsumeAction   | 消费返回值：`SUCCESS` / `RECONSUME_LATER` / `defer(Duration)` |
| 顺序消费动作   | OrderlyAction   | 顺序消费返回值：`SUCCESS` / `SUSPEND_CURRENT_QUEUE_A_MOMENT`  |
| 分片键         | ShardingKey     | 顺序消息的分片依据，相同 key 的消息路由到同一分片             |
| 延时等级       | DelayLevel      | 18 级延时（1s/5s/10s/30s/1m/2m/3m/4m/5m/6m/7m/8m/9m/10m/20m/30m/1h/2h） |
| 半消息         | Half Message    | 事务消息的中间态，本地事务提交前对消费者不可见                |
| 事务回查       | Transaction Check | 事务状态不确定时，Broker 回查生产者的本地事务状态           |
| 死信队列       | DLQ             | 消费重试耗尽后的消息队列，用于人工干预                        |
| 命名空间       | Namespace       | 全局前缀，用于多租户/多环境隔离                                |

---

## 使用指南

### 注解清单

| 注解                            | 作用域       | 用途                                 |
| ------------------------------- | ------------ | ------------------------------------ |
| `@EnableStreamMQ`               | 类（启动类） | 开启 StreamMQ 自动装配              |
| `@StreamMQConsumer`             | 类           | 声明消费者（并发/顺序/DLQ）         |
| `@StreamMQTransactionConsumer`  | 类           | 声明事务消息回查器                   |
| `@StreamMQDlqConsumer`          | 类           | 声明死信队列消费者                   |

### StreamMQConsumer 属性表

| 属性             | 类型              | 默认值 | 说明                         |
| ---------------- | ----------------- | ------ | ---------------------------- |
| `topic`          | String            | -      | 监听的主题（必填）           |
| `consumerGroup`  | String            | -      | 消费组名（必填）             |
| `messageModel`   | MessageModel      | CONCURRENT | 消费模型：CONCURRENT / ORDERLY |
| `consumeMode`    | ConsumeMode       | CLUSTERING | 消费模式：CLUSTERING / BROADCASTING |
| `consumeThreadMin` | int             | 1      | 最小消费线程数               |
| `consumeThreadMax` | int             | 64     | 最大消费线程数               |
| `maxReconsumeTimes` | int            | 16     | 最大重试次数                 |
| `consumeTimeout` | long              | 30000  | 消费超时毫秒数               |
| `pullBatchSize`  | int               | 32     | 单次拉取批量                 |
| `selectorExpression` | String       | "*"    | Tag 过滤表达式               |
| `shardCount`     | int               | 4      | 顺序消费分片数               |
| `dlqMode`        | boolean           | false  | 是否为 DLQ 消费者            |

### StreamMessageTemplate API

```java
public interface StreamMessageTemplate {
    <T> SendResult syncSend(Message<T> message);
    <T> SendResult syncSend(Message<T> message, long timeoutMillis);
    <T> SendResult syncSend(Message<T> message, long timeoutMillis, int retryTimes);

    <T> CompletableFuture<SendResult> asyncSend(Message<T> message);
    <T> void asyncSend(Message<T> message, SendCallback callback);
    <T> void asyncSend(Message<T> message, SendCallback callback, long timeoutMillis);

    <T> void sendOneway(Message<T> message);

    <T> List<SendResult> syncSendBatch(BatchMessage<T> batch);

    <T> SendResult executeInTransaction(Message<T> message, TransactionCallback<T> callback);
}
```

### 消息构建

```java
Message<String> message = MessageBuilder.<String>withTopic("order-topic")
        .tag("created")
        .keys("order-123")
        .shardingKey("user-456")
        .body("{\"orderId\":123}")
        .userProperty("traceId", "t-001")
        .delayLevel(DelayLevel.LEVEL_6)
        .build();
```

### 消费示例

#### 并发消费

```java
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {
    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        processOrder(message.getBody());
        return ConsumeAction.SUCCESS;
    }
}
```

#### 顺序消费

```java
@Component
@StreamMQConsumer(
    topic = "order-topic", 
    consumerGroup = "order-orderly-group",
    messageModel = MessageModel.ORDERLY,
    shardCount = 8
)
public class OrderOrderlyConsumer implements StreamMessageOrderlyConsumer<String> {
    @Override
    public OrderlyAction onMessage(Message<String> message, ConsumeOrderlyContext context) {
        processOrder(message.getBody());
        return OrderlyAction.SUCCESS;
    }
}
```

#### DLQ 消费

```java
@Component
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    dlqMode = true
)
public class OrderDlqConsumer implements StreamMessageConcurrentlyConsumer<String> {
    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        handleDeadLetter(message);
        return ConsumeAction.SUCCESS;
    }
}
```

### 事务消息

```java
@Component
public class TransactionService {
    private final StreamMessageTemplate template;

    public TransactionService(StreamMessageTemplate template) {
        this.template = template;
    }

    public SendResult sendTransactionMessage(String content) {
        Message<String> message = MessageBuilder.<String>withTopic("tx-topic")
                .body(content)
                .build();

        TransactionCallback<String> callback = (msg, ctx) -> {
            try {
                executeLocalTransaction(msg.getBody());
                return LocalTransactionState.COMMIT_MESSAGE;
            } catch (Exception e) {
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }
        };

        return template.executeInTransaction(message, callback);
    }
}
```

事务回查器：

```java
@Component
@StreamMQTransactionConsumer(transactionGroup = "default-tx-group")
public class TransactionChecker implements TransactionChecker<String> {
    @Override
    public LocalTransactionState check(Message<String> message, TransactionContext context) {
        return checkLocalTransactionStatus(context.getTransactionId());
    }
}
```

### 延时消息

```java
// 固定延时级别（18级）
Message<String> fixedDelay = MessageBuilder.<String>withTopic("delay-topic")
        .body("content")
        .delayLevel(DelayLevel.LEVEL_9)
        .build();

// 任意延时毫秒
Message<String> customDelay = MessageBuilder.<String>withTopic("delay-topic")
        .body("content")
        .delayTimeMillis(15 * 60 * 1000L)
        .build();
```

### 配置项

```yaml
streammq:
  enabled: true
  namespace: streammq
  producer:
    group: default-producer-group
    send-timeout: 3000
    retry-times: 2
  consumer:
    consume-thread-min: 1
    consume-thread-max: 64
    pull-batch-size: 32
    max-reconsume-times: 16
    consume-timeout: 30000
  transaction:
    check-interval-ms: 60000
    max-check-times: 15
  dlq:
    enabled: true
```

---

## 模块结构

| 模块                              | 说明                                       | 选择建议                         |
| --------------------------------- | ------------------------------------------ | -------------------------------- |
| `streammq-bom`                    | BOM，统一版本管理                          | 外部用户 import 即可             |
| `streammq-parent`                 | Parent POM，统一插件与依赖管理             | 内部模块继承                     |
| `streammq-core`                   | 核心库，定义 API 接口与默认实现             | 所有模块的基础依赖               |
| `streammq-redisson-adapter`       | Redisson 适配器，基于 Redis Stream 实现     | 需要存储后端时引入               |
| `streammq-spring-boot-starter`    | Spring Boot 3 Starter，自动装配             | Spring Boot 用户必选             |
| `streammq-native`                 | 原生 API，无 Spring 依赖                  | 非 Spring 项目使用               |
| `streammq-kafka-compat`           | Kafka 兼容层，平滑迁移                     | 从 Kafka 迁移时使用              |
| `streammq-amqp-compat`            | AMQP 兼容层，平滑迁移                     | 从 RabbitMQ 迁移时使用           |
| `streammq-test`                   | 测试工具，提供嵌入式测试支持              | 测试场景使用                     |

---

## 可观测性

### 指标（Micrometer）

| 指标名                              | 类型      | 说明                     |
| ----------------------------------- | --------- | ------------------------ |
| `streammq.producer.send.total`      | Counter   | 发送消息总数             |
| `streammq.producer.send.success`    | Counter   | 发送成功数               |
| `streammq.producer.send.failed`     | Counter   | 发送失败数               |
| `streammq.producer.send.duration`   | Timer     | 发送耗时分布             |
| `streammq.consumer.consume.total`   | Counter   | 消费消息总数             |
| `streammq.consumer.consume.duration`| Timer     | 消费耗时分布             |
| `streammq.consumer.retry.total`     | Counter   | 重试消息数               |
| `streammq.consumer.dlq.total`        | Counter   | 进入死信队列数           |

### Prometheus 集成

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
  metrics:
    tags:
      application: ${spring.application.name}
```

---

## 文档导航

| 文档                            | 内容                           |
| ------------------------------- | ------------------------------ |
| [01-PRD.md](docs/01-PRD.md)     | 产品需求文档                   |
| [02-architecture.md](docs/02-architecture.md) | 架构设计文档 |
| [03-functional-design.md](docs/03-functional-design.md) | 功能设计文档 |
| [04-detailed-design.md](docs/04-detailed-design.md) | 详细设计文档 |
| [05-review-report.md](docs/05-review-report.md) | 骨架阶段审查修复报告 |
| [06-implementation-review.md](docs/06-implementation-review.md) | 实现审查报告 |
| [07-completeness-analysis.md](docs/07-completeness-analysis.md) | 功能完整性分析报告 |

---

## 路线图

### v0.1.0 — MVP（当前阶段）
- 核心发送/消费链路
- 注解驱动声明式消费
- 集群消费 + 广播消费
- `StreamMessageTemplate` 同步/异步发送
- Spring Boot 3 自动装配

### v0.2.0 — 高级特性
- 事务消息（半消息 + 回查）
- 延时消息（18 级 + 任意延时）
- 顺序消息（ShardingKey 分片）
- 批量发送
- 死信队列

### v1.0.0 — GA
- Micrometer 指标完善
- Trace 链路集成
- Actuator 健康检查
- Kafka / AMQP 兼容层
- 完整文档与示例
- 性能基准测试与调优

---

## 贡献与许可

### 贡献流程

1. 提交 Issue 描述问题或建议
2. Fork 仓库并创建特性分支
3. 遵循 Google Java Style 规范
4. 提交信息遵循 Conventional Commits
5. 保证编译零警告
6. 新增功能需配套测试用例
7. 发起 Pull Request

### MIT 许可证

本项目基于 [MIT License](LICENSE) 开源。

---

*StreamMQ · 让 Redis 成为你的消息总线。*