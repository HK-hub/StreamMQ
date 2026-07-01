# StreamMQ

> 基于 Redis Stream + Redisson 的高性能消息中间件 SDK，让 Redis 成为你的消息总线。

![License](https://img.shields.io/badge/License-MIT-blue.svg)
![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.x-green.svg)
![Redisson](https://img.shields.io/badge/Redisson-3.34.x-red.svg)
![Version](https://img.shields.io/badge/Version-0.1.0--SNAPSHOT-lightgrey.svg)
![Build Status](https://img.shields.io/badge/Build-passing-brightgreen.svg)

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
- 类似 `RocketMQTemplate` 的 `StreamMqTemplate` 编程模型
- 事务消息、延时消息、顺序消息、批量发送等高级特性
- 与 Spring Boot 3 深度集成的自动装配能力

如果你已经在使用 Redis，又需要一个轻量、可靠、易用的消息中间件，StreamMQ 是你的理想选择。

### 核心特性

| 序号 | 特性              | 说明                                                                 |
| ---- | ----------------- | -------------------------------------------------------------------- |
| 1    | **注解驱动**      | `@StreamMqListener` / `@StreamMqProducer` 声明式定义消费者与生产者   |
| 2    | **Template 编程** | `StreamMqTemplate` 统一发送入口，同步/异步/批量一站式调用             |
| 3    | **4 种消费模型**  | 集群消费 / 广播消费 / 顺序消费 / 事务回查消费                        |
| 4    | **事务消息**      | 半消息 + 本地事务 + 事务回查机制，保证最终一致性                    |
| 5    | **延时消息**      | 内置 18 级延时等级，亦可自定义延时级别                                |
| 6    | **顺序消息**      | 基于 ShardingKey 的分片顺序消费，保证同一分区内严格有序              |
| 7    | **批量发送**      | `BatchMessage` 批量投递，充分利用 Pipeline 提升吞吐                 |
| 8    | **可观测性**      | Micrometer 指标 / 结构化日志 / Trace 集成 / Actuator 健康检查        |

### 与同类产品对比

| 能力             | StreamMQ                | Redisson RStream       | Spring Data Redis Stream | RocketMQ         | Kafka            |
| ---------------- | ----------------------- | ---------------------- | ------------------------ | ---------------- | ---------------- |
| 底层存储         | Redis Stream            | Redis Stream           | Redis Stream             | NameServer+Broker | Broker+ZK/KRaft  |
| 部署复杂度       | **低（仅 Redis）**      | 低（仅 Redis）         | 低（仅 Redis）           | 高（独立集群）    | 高（独立集群）    |
| 注解声明式消费   | **支持**                | 不支持                 | 部分支持                 | 支持              | 不支持            |
| Template 编程    | **支持**                | 不支持                 | 不支持                   | 支持              | 不支持            |
| 事务消息         | **支持**                | 不支持                 | 不支持                   | 支持              | 不支持            |
| 延时消息         | **支持（18 级）**       | 不支持                 | 不支持                   | 支持（18 级）     | 不支持            |
| 顺序消息         | **支持**                | 不支持                 | 不支持                   | 支持              | 支持（分区内）    |
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
    <!-- StreamMQ Spring Boot Starter（自动装配） -->
    <dependency>
        <groupId>io.github.streammq</groupId>
        <artifactId>streammq-spring-boot-starter</artifactId>
    </dependency>

    <!-- Redisson Spring Boot Starter（提供 RedissonClient） -->
    <dependency>
        <groupId>org.redisson</groupId>
        <artifactId>redisson-spring-boot-starter</artifactId>
    </dependency>
</dependencies>
```

### 2. 配置 application.yml

```yaml
streammq:
  namespace: streammq          # 全局命名空间，隔离不同业务
  redisson:
    config: redisson.yml       # Redisson 配置文件路径

spring:
  application:
    name: streammq-demo
```

`redisson.yml` 示例：

```yaml
singleServerConfig:
  address: "redis://127.0.0.1:6379"
  database: 0
```

### 3. 发送消息（5 行）

```java
@Service
public class OrderService {
    private final StreamMqTemplate template;

    public OrderService(StreamMqTemplate template) {
        this.template = template;
    }

    public void sendOrder(Order order) {
        template.syncSend("order-topic", order);
    }
}
```

### 4. 消费消息（5 行）

```java
@Component
@StreamMqListener(topic = "order-topic", consumerGroup = "order-consumer-group")
public class OrderConsumer implements StreamMqListener<Order> {
    @Override
    public void onMessage(Order order, ConsumerContext context) {
        System.out.println("收到订单：" + order.getId());
    }
}
```

### 5. 启用

在启动类添加 `@EnableStreamMq`：

```java
@SpringBootApplication
@EnableStreamMq
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
| 消息           | Message         | 消息载体，包含 header / body / shardingKey / delayLevel 等    |
| 消费结果       | Action          | 消费返回值：`CONSUME_SUCCESS` / `RECONSUME_LATER` / `SUSPEND` |
| 分片键         | ShardingKey     | 顺序消息的分片依据，相同 key 的消息路由到同一分片             |
| 延时等级       | DelayLevel      | 18 级延时（1s/5s/10s/30s/1m/2m/3m/4m/5m/6m/7m/8m/9m/10m/20m/30m/1h/2h） |
| 半消息         | Half Message    | 事务消息的中间态，本地事务提交前对消费者不可见                |
| 事务回查       | Transaction Check | 事务状态不确定时，Broker 回查生产者的本地事务状态           |
| 命名空间       | Namespace       | 全局前缀，用于多租户/多环境隔离                                |

---

## 使用指南

### 注解清单

StreamMQ 提供 5 个核心注解：

| 注解                            | 作用域       | 用途                                 |
| ------------------------------- | ------------ | ------------------------------------ |
| `@EnableStreamMq`               | 类（启动类） | 开启 StreamMQ 自动装配              |
| `@StreamMqProducer`            | 类           | 声明生产者 Bean                      |
| `@StreamMqListener`            | 类           | 声明集群消费监听器                   |
| `@StreamMqOrderlyListener`     | 类           | 声明顺序消费监听器                   |
| `@StreamMqTransactionListener` | 类           | 声明事务消息监听器（含回查）         |

#### `@StreamMqListener` 属性表

| 属性             | 类型              | 默认值 | 说明                         |
| ---------------- | ----------------- | ------ | ---------------------------- |
| `topic`          | String            | -      | 监听的主题（必填）           |
| `consumerGroup`  | String            | -      | 消费组名（必填）             |
| `consumeMode`   | ConsumeMode       | CLUSTER | 消费模式：CLUSTER / BROADCAST |
| `acknowledgeMode`| AcknowledgeMode   | AUTO   | 确认模式：AUTO / MANUAL      |
| `consumeThread`  | int               | 20     | 消费线程数                   |
| `maxRetryTimes`  | int               | 16     | 最大重试次数                 |
| `delayLevel`     | DelayLevel        | -      | 重试延时等级                  |

### StreamMqTemplate API

`StreamMqTemplate` 是统一的发送入口，核心方法：

```java
public interface StreamMqTemplate {

    // 同步发送
    SendResult syncSend(String topic, Message message);

    // 异步发送
    void asyncSend(String topic, Message message, SendCallback callback);

    // 单向发送（不等待应答）
    void sendOneway(String topic, Message message);

    // 批量发送
    SendResult syncSendBatch(String topic, BatchMessage batch);

    // 顺序发送
    SendResult syncSendOrderly(String topic, Message message);

    // 延时发送
    SendResult syncSendDelay(String topic, Message message, DelayLevel level);

    // 事务发送
    SendResult sendMessageInTransaction(TransactionContext context);
}
```

#### 消息构建

```java
Message message = MessageBuilder.withPayload(order)
        .setShardingKey(order.getUserId())     // 顺序消息分片键
        .setDelayLevel(DelayLevel.LEVEL_6)     // 延时 2 分钟
        .addHeader("bizType", "order")
        .build();

SendResult result = template.syncSend("order-topic", message);
```

### 4 种 Listener

#### 1. StreamMqListener（集群消费）

最常用的消费模型，同一消费组内每条消息只被一个实例消费。

```java
@Component
@StreamMqListener(topic = "order-topic", consumerGroup = "order-group")
public class OrderListener implements StreamMqListener<Order> {
    @Override
    public void onMessage(Order order, ConsumerContext context) {
        // 业务处理
        processOrder(order);
        // 默认自动确认；如需手动确认，使用 AckListener
    }
}
```

#### 2. StreamMqAckListener（手动确认）

消费失败时控制重试或死信。

```java
@Component
@StreamMqListener(topic = "order-topic", consumerGroup = "order-group",
                  acknowledgeMode = AcknowledgeMode.MANUAL)
public class OrderAckListener implements StreamMqAckListener<Order> {
    @Override
    public void onMessage(Order order, ConsumerContext context, Acknowledgment ack) {
        try {
            processOrder(order);
            ack.acknowledge();              // 确认成功
        } catch (Exception e) {
            ack.acknowledgeLater();          // 稍后重试
        }
    }
}
```

#### 3. StreamMqOrderlyListener（顺序消费）

基于 ShardingKey 保证同一分片内严格顺序消费。

```java
@Component
@StreamMqOrderlyListener(topic = "order-topic", consumerGroup = "order-orderly-group")
public class OrderOrderlyListener implements StreamMqOrderlyListener<Order> {
    @Override
    public void onMessage(Order order, OrderlyContext context) {
        // 相同 userId 的订单会被严格按发送顺序消费
        processOrder(order);
    }
}
```

#### 4. StreamMqTransactionListener（事务消息）

实现半消息 + 本地事务 + 回查机制。

```java
@Component
@StreamMqTransactionListener
public class OrderTransactionListener implements StreamMqTransactionListener<Order> {

    @Override
    public LocalTransactionState executeLocalTransaction(Order order, TransactionContext context) {
        // 执行本地事务（如写库）
        try {
            saveOrder(order);
            return LocalTransactionState.COMMIT;
        } catch (Exception e) {
            return LocalTransactionState.ROLLBACK;
        }
    }

    @Override
    public LocalTransactionState checkLocalTransaction(Order order) {
        // 事务状态回查：返回本地事务是否已提交
        return orderRepository.existsById(order.getId())
                ? LocalTransactionState.COMMIT
                : LocalTransactionState.ROLLBACK;
    }
}
```

### 高级特性

#### 事务消息

```java
TransactionContext context = TransactionContext.builder()
        .withTopic("order-topic")
        .withPayload(order)
        .build();

SendResult result = template.sendMessageInTransaction(context);
// 半消息发送成功后，自动执行本地事务监听器的 executeLocalTransaction
```

#### 延时消息

```java
Message message = MessageBuilder.withPayload(payment)
        .setDelayLevel(DelayLevel.LEVEL_9)   // 延时 5 分钟
        .build();
template.syncSendDelay("payment-topic", message, DelayLevel.LEVEL_9);
```

#### 顺序消息

```java
Message message = MessageBuilder.withPayload(order)
        .setShardingKey(order.getUserId())     // 按 userId 分片保证顺序
        .build();
template.syncSendOrderly("order-topic", message);
```

#### 批量发送

```java
BatchMessage batch = BatchMessage.builder()
        .withTopic("order-topic")
        .addMessage(msg1)
        .addMessage(msg2)
        .addMessage(msg3)
        .build();
template.syncSendBatch("order-topic", batch);
```

### 配置项

完整 `application.yml` 示例：

```yaml
streammq:
  namespace: streammq                    # 全局命名空间（前缀）
  producer:
    default-topic: default-topic          # 默认主题
    send-timeout: 3000                    # 发送超时（毫秒）
    retry-times: 2                        # 同步发送重试次数
    enable-batch: true                    # 启用批量发送
    batch-max-size: 1000                  # 批量最大消息数
  consumer:
    consume-thread-min: 20                # 最小消费线程
    consume-thread-max: 64                # 最大消费线程
    pull-batch-size: 100                  # 单次拉取消息数
    ack-timeout: 30000                    # ACK 超时（毫秒）
    max-reconsume-times: 16               # 最大重试次数
  transaction:
    check-thread-pool-size: 16           # 事务回查线程池大小
    check-max-times: 15                  # 最大回查次数
  redisson:
    config: redisson.yml                  # Redisson 配置文件路径

spring:
  application:
    name: streammq-app
  # Redisson 也可直接在 application.yml 中配置
  redis:
    host: 127.0.0.1
    port: 6379
```

### SPI 扩展机制

StreamMQ 通过 SPI（Service Provider Interface）提供 8 个扩展点，允许用户在不修改核心代码的前提下定制行为：

| SPI 接口                | 作用                               | 默认实现                |
| ----------------------- | ---------------------------------- | ----------------------- |
| `MessageSerializer`     | 消息序列化/反序列化                | `JacksonMessageSerializer` |
| `MessageConverter`      | 消息体与业务对象转换               | 默认反射转换            |
| `ProducerInterceptor`   | 生产者拦截器（发送前后钩子）       | 无                      |
| `ConsumerInterceptor`   | 消费者拦截器（消费前后钩子）       | 无                      |
| `RetryPolicy`           | 重试策略（间隔/次数/退避算法）     | 默认延时等级重试        |
| `RebalanceStrategy`     | 消费者重平衡策略                   | 平均分片                |
| `TraceCollector`        | Trace 上下文采集                   | Micrometer Tracing      |
| `ManagementAuthenticator` | 管理接口鉴权                     | 无鉴权                  |

#### 自定义序列化器示例

```java
public class ProtobufMessageSerializer implements MessageSerializer {
    @Override
    public byte[] serialize(Message message) {
        // 自定义序列化逻辑
        return ProtobufUtil.toByteArray(message);
    }

    @Override
    public Message deserialize(byte[] bytes) {
        return ProtobufUtil.parseFrom(bytes);
    }
}
```

通过 Spring Bean 注册即可生效：

```java
@Bean
public MessageSerializer messageSerializer() {
    return new ProtobufMessageSerializer();
}
```

#### 自定义重试策略示例

```java
@Bean
public RetryPolicy retryPolicy() {
    return RetryPolicy.builder()
            .maxAttempts(5)
            .initialDelay(Duration.ofSeconds(1))
            .multiplier(2.0)          // 指数退避
            .maxDelay(Duration.ofMinutes(2))
            .build();
}
```

---

## 模块结构

StreamMQ 采用多模块 Maven 工程，共 9 个核心模块：

| 模块                              | 说明                                       | 选择建议                         |
| --------------------------------- | ------------------------------------------ | -------------------------------- |
| `streammq-bom`                    | BOM，统一版本管理                          | 外部用户 import 即可             |
| `streammq-parent`                 | Parent POM，统一插件与依赖管理             | 内部模块继承                     |
| `streammq-core`                   | 核心库，定义 API 接口与默认实现             | 所有模块的基础依赖               |
| `streammq-redisson-adapter`       | Redisson 适配器，基于 Redis Stream 实现     | 需要存储后端时引入               |
| `streammq-spring-boot-starter`    | Spring Boot 3 Starter，自动装配             | Spring Boot 用户必选             |
| `streammq-native`                 | 原生 API，无 Spring 依赖                  | 非 Spring 项目使用               |
| `streammq-kafka-compat`            | Kafka 兼容层，平滑迁移                     | 从 Kafka 迁移时使用              |
| `streammq-amqp-compat`             | AMQP 兼容层，平滑迁移                     | 从 RabbitMQ 迁移时使用           |
| `streammq-test`                    | 测试工具，提供嵌入式测试支持              | 测试场景使用                     |

### 依赖关系图

```
                       ┌─────────────────────┐
                       │   streammq-bom      │  ← 外部 import 版本管理
                       └─────────────────────┘
                                  │
              ┌───────────────────┼───────────────────┐
              ▼                   ▼                   ▼
   ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
   │  streammq-core   │  │  streammq-parent  │  │  streammq-test   │
   │  (API + 默认实现) │  │  (插件/依赖管理)  │  │  (测试工具)       │
   └──────────────────┘  └──────────────────┘  └──────────────────┘
              │
              ▼
   ┌──────────────────────┐
   │ streammq-redisson-   │  ← Redis Stream 适配实现
   │      adapter         │
   └──────────────────────┘
              │
              ▼
   ┌──────────────────────┐     ┌──────────────────┐
   │ streammq-spring-boot │ ──▶ │  streammq-native │  ← 原生 API
   │       -starter       │     └──────────────────┘
   └──────────────────────┘
              │
   ┌──────────┴──────────┐
   ▼                     ▼
┌─────────────┐  ┌──────────────┐
│ kafka-compat│  │  amqp-compat │  ← 兼容层
└─────────────┘  └──────────────┘
```

### 选择建议

| 使用场景                       | 推荐模块组合                                            |
| ------------------------------ | ------------------------------------------------------- |
| Spring Boot 3 项目（最常见）  | `streammq-spring-boot-starter` + `redisson-spring-boot-starter` |
| 非 Spring 项目                 | `streammq-native` + `streammq-redisson-adapter`        |
| 仅需 API 定义（自定义实现）   | `streammq-core`                                         |
| 从 Kafka 迁移                  | 上述组合 + `streammq-kafka-compat`                       |
| 从 RabbitMQ 迁移               | 上述组合 + `streammq-amqp-compat`                        |
| 测试需要嵌入式 Redis           | 上述组合 + `streammq-test`（test scope）                 |

---

## 可观测性

StreamMQ 提供开箱即用的可观测性能力：

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

### 日志

- 结构化日志，支持 SLF4J 2.0+
- 关键路径（发送/消费/重试/事务）均有日志埋点
- 支持 MDC 透传 traceId

日志示例（logback.xml 片段）：

```xml
<appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    <encoder>
        <pattern>%d{yyyy-MM-dd HH:mm:ss} [%thread] [%X{traceId}] %-5level %logger{36} - %msg%n</pattern>
    </encoder>
</appender>

<logger name="io.github.streammq" level="INFO"/>
```

### Trace

- 集成 Micrometer Tracing（兼容 Zipkin / OpenTelemetry）
- 跨生产者-消费者链路透传 traceId

### Prometheus 集成

在 `application.yml` 暴露指标端点：

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
  metrics:
    tags:
      application: ${spring.application.name}
    export:
      prometheus:
        enabled: true
```

Prometheus 抓取配置：

```yaml
scrape_configs:
  - job_name: 'streammq-app'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['localhost:8080']
```

### 健康检查

- Actuator 健康端点：`/actuator/health`
- 检查项：RedissonClient 连通性、Consumer 存活状态

---

## 文档导航

| 文档                            | 内容                           |
| ------------------------------- | ------------------------------ |
| [01-PRD.md](docs/01-PRD.md)     | 产品需求文档                   |
| [02-architecture.md](docs/02-architecture.md) | 架构设计文档 |
| [03-functional-design.md](docs/03-functional-design.md) | 功能设计文档 |
| [04-detailed-design.md](docs/04-detailed-design.md) | 详细设计文档 |
| [05-review-report.md](docs/05-review-report.md) | 骨架阶段审查修复报告 |

---

## 路线图

### v0.1.0 — MVP（当前阶段）

- 核心发送/消费链路
- 注解驱动声明式消费
- 集群消费 + 广播消费
- `StreamMqTemplate` 同步/异步发送
- Spring Boot 3 自动装配

### v0.2.0 — 高级特性

- 事务消息（半消息 + 回查）
- 延时消息（18 级）
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

### v2.0+ — 未来

- 多后端支持（除 Redis 外支持其他存储）
- Cloud Native 部署优化
- 管理控制台
- 更多序列化器实现

---

## 贡献与许可

### 贡献流程

欢迎为 StreamMQ 贡献代码！请遵循以下流程：

1. **提交 Issue**：在 GitHub 提交 Issue 描述问题或建议
2. **Fork & Branch**：Fork 仓库并创建特性分支（`feature/xxx` 或 `fix/xxx`）
3. **开发规范**：
   - 遵循 [Google Java Style](https://google.github.io/styleguide/javaguide.html)
   - 提交信息遵循 [Conventional Commits](https://www.conventionalcommits.org/) 规范
   - 保证 `mvn -B -DskipTests compile` 零警告（项目启用 `-Werror`）
   - 保证 `mvn enforcer:enforce` 通过（依赖版本收敛）
4. **测试要求**：
   - 单元测试以 `*Test.java` 命名
   - 集成测试以 `*IT.java` 命名（需 Redis 7.2+ 实例）
   - 新增功能需配套测试用例
5. **提交 PR**：发起 Pull Request 至 `main` 分支，描述变更内容与关联 Issue
6. **Code Review**：维护者审查通过后合并

### 代码规范要点

- JDK 21+，可使用 record / sealed / pattern matching 等新特性
- 所有传递依赖必须版本收敛（dependencyConvergence）
- 公共 API 需添加 Javadoc
- 禁止提交敏感信息（密钥、凭据等）

### MIT 许可证

本项目基于 [MIT License](LICENSE) 开源。

```
MIT License

Copyright (c) 2026 StreamMQ Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### 贡献者

<!-- 贡献者列表将随项目演进维护 -->

感谢所有为 StreamMQ 贡献代码与想法的开发者！

---

*StreamMQ · 让 Redis 成为你的消息总线。*
