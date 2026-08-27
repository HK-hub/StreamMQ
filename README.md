<div align="center">

# StreamMQ

### 让 Redis 成为你的消息总线

基于 Redis Stream + Redisson 构建的高性能消息中间件 SDK，提供类 RocketMQ 的编程体验

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.java.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.x-green.svg)](https://spring.io/projects/spring-boot)
[![Redisson](https://img.shields.io/badge/Redisson-3.34.x-red.svg)](https://redisson.org/)
[![Version](https://img.shields.io/badge/version-0.1.0-blue.svg)](https://github.com/HK-hub/StreamMQ)
[![CI](https://github.com/HK-hub/StreamMQ/actions/workflows/ci.yml/badge.svg)](https://github.com/HK-hub/StreamMQ/actions/workflows/ci.yml)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-ff69b4.svg)](https://github.com/HK-hub/StreamMQ/pulls)
[![Stars](https://img.shields.io/github/stars/HK-hub/StreamMQ?style=social)](https://github.com/HK-hub/StreamMQ)

</div>

---

> **StreamMQ** 是一款基于 **Redis Stream** 与 **Redisson** 构建的开源消息中间件 SDK，以 MIT 协议发布。它将 Redis Stream 的原生能力封装为一套类 RocketMQ 的、面向业务开发者友好的消息 API，让你在无需引入重量级 MQ 集群的前提下，获得注解驱动消费、事务消息、延时消息、顺序消息等企业级特性。

---

## 演示视频

| 资源 | 链接 |
|------|------|
| 📺 YouTube 演示视频 | 即将上线 |
| 🎯 Product Hunt 展示 | 即将上线 |
| 📝 GIF 制作指南 | [docs/demo/demo-gif-guide.md](docs/demo/demo-gif-guide.md) |
| 🖼️ 截图素材清单 | [docs/demo/screenshots/README.md](docs/demo/screenshots/README.md) |
| 🚀 一键演示脚本 | [docs/demo/quickstart-demo.sh](docs/demo/quickstart-demo.sh) |

> 💡 一键演示脚本已内置发送演示消息（应用启动即自动发送），并在超时未检测到消费时以非零退出码失败，便于录屏一次通过。

---

## 目录

- [为什么选择 StreamMQ](#为什么选择-streammq)
- [架构总览](#架构总览)
- [与同类产品对比](#与同类产品对比)
- [性能基准测试](#性能基准测试)
- [快速开始](#快速开始)
- [核心特性](#核心特性)
- [模块结构](#模块结构)
- [配置参考](#配置参考)
- [SPI 扩展机制](#spi-扩展机制)
- [可观测性](#可观测性)
- [示例工程](#示例工程)
- [文档导航](#文档导航)
- [路线图](#路线图)
- [贡献指南](#贡献指南)
- [社区](#社区)
- [安全](#安全)
- [许可证](#许可证)

---

## 为什么选择 StreamMQ

### 零额外部署

已有 Redis？你已经拥有了消息中间件。StreamMQ 复用现有 Redis 基础设施，无需引入 NameServer、Broker、Zookeeper 等额外组件，**一个 Redis 即是一个 MQ 集群**。

### 类 RocketMQ API 体验

对齐 RocketMQ `RocketMQTemplate` / `@RocketMQMessageListener` 的编程模型，迁移成本低，学习曲线平缓。如果你熟悉 RocketMQ，你已经会使用 StreamMQ。

### 丰富的高级特性

事务消息、18 级延时消息、顺序消息、批量发送、死信队列、消息压缩、消息过滤——开箱即用的企业级能力，不输独立 MQ 集群。

### Spring Boot 3 深度集成

自动装配、配置绑定、Actuator 端点、Micrometer 指标——与 Spring 生态无缝衔接，`@EnableStreamMQ` 一键开启。

### 12 个 SPI 扩展点

序列化器、转换器、过滤器、拦截器、重试策略、重平衡策略、压缩编解码器、死信失败策略、管理鉴权器、链路追踪采集器——几乎一切可扩展。

### 生产就绪

单元测试 827 个 + 集成测试 197 个（由 surefire/failsafe 报告汇总，`mvn test` / `mvn verify` 可复现；无 Redis 环境 IT 自动跳过并可在 CI 日志核对执行数下限守门），覆盖核心消息能力、事务流程、延时投递、顺序消费、DLQ 处理、PEL 认领、广播消费等场景。

---

## 架构总览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        StreamMQ Architecture                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌───────────────────────────────────────────────────────────────────┐ │
│   │                    Spring Boot Application                       │ │
│   │  ┌─────────────┐  ┌──────────────┐  ┌──────────────────────────┐ │ │
│   │  │@EnableStreamMQ│ │@StreamMQConsumer│ │  StreamMessageTemplate  │ │ │
│   │  │  (自动装配)   │  │  (声明式消费)  │ │   (统一发送入口)         │ │ │
│   │  └──────┬──────┘  └──────┬───────┘  └───────────┬──────────────┘ │ │
│   └─────────┼─────────────────┼─────────────────────┼────────────────┘ │
│             │                 │                     │                  │
│   ┌─────────▼─────────────────▼─────────────────────▼────────────────┐ │
│   │                     streammq-core                                │ │
│   │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │ │
│   │  │ Message  │ │ Template │ │ Consumer │ │Producer  │ │Transaction│ │
│   │  │ Builder  │ │ Service  │ │ Listener │ │ Factory  │ │ Executor │ │
│   │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └────────┘ │ │
│   │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │ │
│   │  │ Filter   │ │Interceptor│ │ Retry    │ │ Rebalance│ │  DLQ   │ │ │
│   │  │ Chain    │ │ Chain    │ │ Policy   │ │ Strategy │ │Handler │ │ │
│   │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └────────┘ │ │
│   │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │ │
│   │  │Serializer│ │Converter │ │Codec     │ │ Trace    │ │Metrics │ │ │
│   │  │  (SPI)   │ │  (SPI)   │ │  (SPI)   │ │Collector │ │  (Mic.)│ │ │
│   │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └────────┘ │ │
│   └───────────────────────────────────────────────────────────────────┘ │
│             │                 │                     │                  │
│   ┌─────────▼─────────────────▼─────────────────────▼────────────────┐ │
│   │                   streammq-redisson                               │ │
│   │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │ │
│   │  │ Redisson │ │ Stream   │ │ Delay    │ │ PEL      │ │ Tx     │ │ │
│   │  │ Producer │ │ Listener │ │ Scheduler│ │ Claimer  │ │Scanner │ │ │
│   │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └────────┘ │ │
│   └───────────────────────────────────────────────────────────────────┘ │
│             │                                                           │
│   ┌─────────▼────────────────────────────────────────────────────────┐ │
│   │                       Redis 7.2+                                 │ │
│   │   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │ │
│   │   │  Stream  │  │   ZSet   │  │   Hash   │  │  Sorted  │       │ │
│   │   │ (消息存储)│  │(延时队列)│  │(事务状态)│  │   Set    │       │ │
│   │   └──────────┘  └──────────┘  └──────────┘  └──────────┘       │ │
│   └──────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 与同类产品对比

| 能力 | StreamMQ | Redisson RStream | Spring Data Redis Stream | RocketMQ | Kafka |
|------|----------|-----------------|-------------------------|----------|-------|
| 底层存储 | Redis Stream | Redis Stream | Redis Stream | NameServer+Broker | Broker+KRaft |
| 部署复杂度 | **低（仅 Redis）** | 低（仅 Redis） | 低（仅 Redis） | 高（独立集群） | 高（独立集群） |
| 注解声明式消费 | **支持** | 不支持 | 部分支持 | 支持 | 不支持 |
| Template 编程模型 | **支持** | 不支持 | 不支持 | 支持 | 支持（KafkaTemplate，spring-kafka 提供，非注解声明式） |
| 事务消息 | **支持** | 不支持 | 不支持 | 支持 | 不支持 |
| 延时消息 | **支持（18 级+任意）** | 不支持 | 不支持 | 支持（18 级） | 不支持 |
| 顺序消息 | **支持** | 不支持 | 不支持 | 支持 | 支持（分区内） |
| 死信队列 | **支持（含二级 DLQ）** | 不支持 | 不支持 | 支持 | 支持（spring-kafka DLT/@RetryableTopic，非注解式） |
| 消息过滤 | **Tag + SQL92** | 不支持 | 不支持 | 支持 | 不支持 |
| 消息压缩 | **支持（GZIP SPI）** | 不支持 | 不支持 | 支持 | 支持 |
| 背压控制 | **支持（InflightQueue）** | 不支持 | 不支持 | 支持 | 支持 |
| Spring Boot 3 集成 | **深度集成** | 一般 | 一般 | 一般（第三方） | 一般（第三方） |
| SPI 扩展点数量 | **12 个** | 0 | 0 | 少量 | 少量 |
| 管理接口 | **REST API + Actuator** | 无 | 无 | Dashboard | 无 |
| 链路追踪 | **支持（TraceCollector SPI）** | 不支持 | 不支持 | 支持 | 不支持 |
| 学习成本 | **低** | 中 | 中 | 中 | 中 |
| 适用规模 | 中小规模（< 1 亿/天） | 中小规模 | 中小规模 | 大规模 | 超大规模 |

---

## 性能基准测试

### 测试环境

| 项目 | 配置 |
|------|------|
| JDK | OpenJDK 21.0.11 (Eclipse Adoptium) |
| Spring Boot | 3.3.5 |
| Redisson | 3.34.1 |
| Redis | 7.x (本地单机, 无密码) |
| JMH | 1.37 |
| 操作系统 | Windows 11 |
| 连接池 | 16 连接, 4 最小空闲 |

### 序列化性能 (Throughput, ops/s)

测试 1KB 消息体的序列化/反序列化吞吐量（messageCount=1000，批量 1000 次）。

> ⚠️ **方法学修正（v0.1.0 发布前）**：批量序列化基准现已加入 JMH `Blackhole` 消费，防止 JIT 死码消除导致
> 吞吐虚高。下表为旧实现（无 Blackhole）测得的历史数字，仅供参考；新基线将由 CI 手动基准任务
> （`benchmark.yml`）重新生成后回填，届时请以新数据为准。

| 序列化器 | Serialize (ops/s) | Deserialize (ops/s) | RoundTrip (ops/s) | 单次序列化 (ops/s) | 单次反序列化 (ops/s) |
|----------|-------------------|---------------------|-------------------|--------------------|----------------------|
| **Fury** | **7,749,744** | **4,377,141** | **3,977,079** | **7,879,107** | **4,496,204** |
| Jackson  | 1,055,039 | 1,978,002 | 680,324 | 1,003,220 | 1,943,087 |
| JDK      | 457,713 | 148,372 | 103,880 | 454,467 | 148,306 |

> **结论**: Fury 序列化吞吐量是 Jackson 的 **7.3x**，是 JDK 的 **16.9x**。推荐对性能有要求的场景使用 Fury。

### 消息发送性能 (Throughput, ops/s)

使用 `StreamMessageTemplate` 发送消息，批量大小 100 条，负载大小见参数。

| 发送模式 | 100B 负载 (ops/s) | 1KB 负载 (ops/s) | 10KB 负载 (ops/s) |
|----------|-------------------|------------------|-------------------|
| **异步批量发送** | **11,948** | **10,062** | **7,863** |
| 同步批量发送 | 2,587 | 2,703 | 2,344 |
| 同步单条发送 | 2,309 | 2,188 | 1,877 |

> **结论**: 异步发送性能约为同步的 **4~5 倍**，推荐高吞吐场景使用 `asyncSend`。

### 消息消费性能

> ⚠️ **方法学修正（v0.1.0 发布前）**：此前本节引用的 "Stream 消费吞吐 ~269,760 ops/s" 来自一个
> 不 ACK、不做字段转换、且预热数据耗尽后实际测量空 XREADGROUP 网络往返的基准实现，该数字
> 不能代表真实消费能力，已从本文档移除。`StreamConsumerBenchmark#consumeThroughput` 已重写为
> 「XREADGROUP 拉取 → 反序列化转换 → 业务回调 → XACK」的完整端到端路径并配合持续灌数，
> 新基线数据将在 CI 基准任务中重新生成后回填。请以修正后的基准代码自行测量为准。

| 消费模式 | 说明 |
|----------|------|
| `consumeThroughput` | 完整消费路径：网络拉取 + 字段解码 + 回调 + ACK（含持续灌数） |
| `serializationRoundTrip` | Jackson 序列化/反序列化回环（纯内存） |
| `messageCreateAndConsume` | 纯内存消息构建 + 回调（无网络，仅衡量对象模型开销） |

### 性能优化建议

1. **序列化选择**: 默认 Jackson；对吞吐有要求的场景可配置为 Fury（`streammq.producer.serializer` 指定 `FurySerializer`），其吞吐量是 Jackson 的 7 倍以上
2. **发送策略**: 高吞吐场景使用 `asyncSend`，可提升 4~5 倍性能
3. **负载大小**: 10KB 大消息建议启用 GZIP 压缩（`MessageCompressor` SPI）
4. **连接池**: 默认 16 连接可满足多数场景，高并发可调至 32~64
5. **批量消费**: 使用 `pullBatchSize`（注解）或 `streammq.consumer.batch-size`（全局配置）批量拉取，减少网络往返

---

## 快速开始

### 环境要求

| 组件 | 最低版本 | 推荐版本 |
|------|----------|----------|
| JDK | 21 | 21+ |
| Maven | 3.9 | 3.9+ |
| Redis | 7.2 | 7.2+ |
| Spring Boot | 3.3 | 3.3.5 |

### 1. 引入依赖

```xml
<dependencyManagement>
    <dependencies>
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

> ⚠️ 必须同时引入 `redisson-spring-boot-starter` 以提供 `RedissonClient` Bean；缺失时启动将报 `NoSuchBeanDefinitionException`。

### 2. 配置

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

### 3. 启用

```java
@SpringBootApplication
@EnableStreamMQ
public class DemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
```

### 4. 发送消息

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
                .withUserProperty("traceId", "t-001")
                .build();
        return template.syncSend(message);
    }
}
```

### 5. 消费消息

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

就这样！启动应用，发送一条消息，消费者会自动接收并处理。

---

## 核心特性

### 注解驱动消费

一行注解，声明式定义消费者，支持并发消费、顺序消费、广播消费、DLQ 消费四种模型。

```java
// 并发消费（默认）
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group")

// 顺序消费
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group",
                  messageModel = MessageModel.ORDERLY, shardCount = 8)

// 广播消费
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group",
                  consumeMode = ConsumeMode.BROADCASTING)

// DLQ 消费
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group", dlqMode = true)
```

### StreamMessageTemplate 编程模型

统一的发送入口。0.1.0 起 API 已收敛：每个发送模式仅保留一个 `SendOptions` 规范形，
此前的 timeout / retry / callback 伸缩重载全部移除；零参便捷形式以 default 方法提供。

```java
public interface StreamMessageTemplate {
    // 规范形（唯一参数空间）
    <T> SendResult syncSend(Message<T> message, SendOptions options);
    <T> CompletableFuture<SendResult> asyncSend(Message<T> message, SendOptions options);
    <T> List<SendResult> syncSendBatch(BatchMessage<T> batch, SendOptions options);
    <T> void sendOneway(Message<T> message);          // fire-and-forget
    <T> SendResult executeInTransaction(Message<T> message, TransactionCallback<T> callback);

    // 便捷 default 方法
    <T> SendResult syncSend(Message<T> message);
    <T> CompletableFuture<SendResult> asyncSend(Message<T> message);
    <T> void asyncSend(Message<T> message, SendCallback callback);
    <T> void asyncSend(Message<T> message, SendOptions options, SendCallback callback);
    <T> List<SendResult> syncSendBatch(BatchMessage<T> batch);

    // SPI 访问器：拦截器/过滤器/转换器管理
}
```

`StreamMessageService` 门面同步收敛为三种正交维度：发送模式 × 载体形态（完整 Message 或
topic+body+`MessageMetadataBuilder`）× 参数（`SendOptions` / 元数据内联超时重试）。

### 事务消息

半消息 + 本地事务 + 回查机制，保证最终一致性。

```java
// 发送事务消息
TransactionCallback<String> callback = (message, ctx) -> {
    try {
        executeLocalTransaction(message.getBody());
        return LocalTransactionState.COMMIT_MESSAGE;
    } catch (Exception e) {
        return LocalTransactionState.ROLLBACK_MESSAGE;
    }
};
SendResult result = template.executeInTransaction(message, callback);

// 事务回查器
@Component
@StreamMQTransactionConsumer(transactionGroup = "default-tx-group")
public class TransactionCheckerImpl implements TransactionChecker<String> {
    @Override
    public LocalTransactionState check(Message<String> message, TransactionContext context) {
        return checkLocalTransactionStatus(context.getTransactionId());
    }
}
```

### 延时消息

内置 18 级固定延时，亦可自定义任意毫秒延时。

```java
// 固定延时（18 级）
Message<String> msg1 = MessageBuilder.<String>withTopic("delay-topic")
        .body("content")
        .delayLevel(DelayLevel.MINUTE_5)   // 延时 5 分钟
        .build();

// 任意延时毫秒
Message<String> msg2 = MessageBuilder.<String>withTopic("delay-topic")
        .body("content")
        .delayTimeMillis(15 * 60 * 1000L)  // 延时 15 分钟
        .build();
```

**延时级别对照表：**

| 级别 | 延时 | 级别 | 延时 | 级别 | 延时 |
|------|------|------|------|------|------|
| `SECOND_1` | 1s | `MINUTE_3` | 3m | `MINUTE_20` | 20m |
| `SECOND_5` | 5s | `MINUTE_4` | 4m | `MINUTE_30` | 30m |
| `SECOND_10` | 10s | `MINUTE_5` | 5m | `HOUR_1` | 1h |
| `SECOND_30` | 30s | `MINUTE_6` | 6m | `HOUR_2` | 2h |
| `MINUTE_1` | 1m | `MINUTE_7` | 7m | | |
| `MINUTE_2` | 2m | `MINUTE_8` | 8m | | |
| `MINUTE_9` | 9m | `MINUTE_10` | 10m | | |

### 顺序消息

基于 ShardingKey 的分片顺序消费，保证同一分片内严格有序。

> 实现：单 Stream + 分片分布式锁。消费失败时在当前线程内按 `maxReconsumeTimes` 重试，每次失败按
> `suspendCurrentQueueTimeMillis`（默认 1000ms）挂起，不越过失败消息继续消费（严格有序）；重试耗尽后直接进入 DLQ。
> 消费者实例崩溃后的消息由 PEL 认领调度器恢复重投（空闲阈值默认 60s，且会检查分片锁活性——
> 正在处理中的消息不会被认领）。
> 分片并发控制由分布式锁保证（获取等待上限默认 5s，超时转 RECONSUME_LATER 防止持有者挂死导致分片停摆）；
> `RebalanceStrategy` 提供分片分配元数据（assignment Hash + REBALANCE 通知，供管理端点观测），
> 手动重平衡见 `/actuator/streammq/rebalance/{group}`。

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
    public ConsumeAction onMessage(Message<String> message, ConsumeOrderlyContext context) {
        processOrder(message.getBody());
        return ConsumeAction.SUCCESS;
    }
}

// 发送时指定 shardingKey
Message<String> message = MessageBuilder.<String>withTopic("order-topic")
        .shardingKey("user-123")
        .body("content")
        .build();
```

### 批量发送

`BatchMessage` 批量投递，充分利用 Redis Pipeline 提升吞吐。

```java
BatchMessage<String> batch = BatchMessage.<String>withTopic("order-topic")
        .add(msg1)
        .add(msg2)
        .add(msg3)
        .build();

List<SendResult> results = template.syncSendBatch(batch);
```

### 死信队列

消费重试耗尽后的消息自动进入 DLQ，支持二级 DLQ 与自定义失败策略。

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

### 消息过滤

支持 Tag 表达式与 SQL92 表达式两种过滤模式。

```java
// Tag 过滤
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    selectorExpression = "tag1 || tag2"
)

// SQL92 过滤
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    selectorType = SelectorType.SQL92,
    selectorExpression = "a = 1 AND b > 2"
)
```

### 消息压缩

通过 `CompressionCodec` SPI 支持 GZIP 压缩，可配置压缩阈值。

```yaml
streammq:
  producer:
    compress-threshold: 1024    # 消息体超过 1KB 时自动压缩
```

---

## 模块结构

| 模块 | 说明 |
|------|------|
| **streammq-bom** | BOM（Bill of Materials），统一版本管理 |
| **streammq-core** | 核心抽象层，定义消息模型、API、SPI 接口（无 Spring 依赖） |
| **streammq-redisson** | Redisson 适配层，基于 Redis Stream 实现核心能力 |
| **streammq-spring-boot-starter** | Spring Boot 3 自动装配、配置绑定、Actuator 集成 |
| **streammq-tracing-opentelemetry** | OpenTelemetry 链路追踪集成 |
| **streammq-diagnostics** | 消息画像、慢消费、积压、DLQ 诊断 |
| **streammq-kubernetes** | K8s 健康检查、HPA、优雅停机、CRD Operator |
| **streammq-spring-cloud-stream-binder** | Spring Cloud Stream Binder 实现 |
| **streammq-benchmark** | JMH 基准测试 |
| **streammq-test** | 测试工具包，提供嵌入式 Redis、断言工具、Mock 工具 |
| **streammq-samples** | 示例工程集合，覆盖快速开始、事务、延时、顺序、DLQ、拦截器、诊断、链路追踪 |

---

## 配置参考

### 完整配置示例

```yaml
streammq:
  enabled: true
  namespace: streammq

  # 生产者配置
  producer:
    group: default-producer
    send-message-timeout: 3000        # 发送超时（毫秒）
    retry-times: 2                    # 同步发送重试次数
    compress-threshold: 0             # 0=不压缩，>0 时超过阈值自动压缩

  # 消费者配置（并发度由虚拟线程按需调度，无需线程池配置）
  consumer:
    batch-size: 32                    # 单次拉取批量大小
    pull-interval: 0                  # 拉取间隔（毫秒）
    inflight-capacity: 0              # 背压队列容量（0=禁用；>0 时拉取与处理解耦，队列满则拉取阻塞）

  # 重试配置
  retry:
    max-reconsume-times: 16           # 消费失败最大重试次数

  # 事务配置
  transaction:
    check-interval: 60s               # 回查间隔
    max-check-times: 15               # 最大回查次数

  # 死信队列配置
  dlq:
    max-dlq-retry-attempts: 3         # DLQ 消费失败最大重试次数
    # failure-strategy: io.github.streammq.adapter.redisson.dlq.LogAndDropDlqFailureStrategy  # 可选：DLQ 失败策略

  # 管理端点开关（与 health.enabled 解耦；false 时仅关闭管理 REST 端点，健康检查不受影响）
  admin:
    enabled: true

  # 可观测性配置（指标开关由 streammq.enabled 控制）
  tracing:
    enabled: false
```

### @StreamMQConsumer 属性速查

| 属性 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `topic` | String | - | 主题（必填） |
| `consumerGroup` | String | - | 消费组（必填） |
| `messageModel` | MessageModel | CONCURRENT | 消费模型：CONCURRENT / ORDERLY |
| `consumeMode` | ConsumeMode | CLUSTERING | 消费模式：CLUSTERING / BROADCASTING |
| `consumeThreadMin` | int | 1 | **并发消费循环数**（仅 CONCURRENT 集群消费生效；每循环独立 XREADGROUP 拉取，共享 consumer name 原子分配互不相交） |
| `consumeThreadMax` | int | 64 | 并发消费循环数上限（夹取上界） |
| `maxReconsumeTimes` | int | 16 | 最大重试次数 |
| `consumeTimeout` | long | 30000 | 消费超时（毫秒） |
| `pullBatchSize` | int | 32 | 单次拉取批量 |
| `selectorExpression` | String | "*" | Tag/SQL92 过滤表达式 |
| `selectorType` | SelectorType | TAG | 过滤类型：TAG / SQL92 |
| `shardCount` | int | 4 | 顺序消费分片数 |
| `dlqMode` | boolean | false | 是否 DLQ 消费者 |
| `pullInterval` | long | 0 | 拉取间隔（毫秒） |
| `streamMaxLen` | int | 0 | Stream 最大长度（0=不限制） |
| `retryStreamMaxLen` | int | 0 | 重试 Stream 最大长度 |
| `enableMsgTrace` | boolean | false | 是否启用消息追踪 |
| `serializer` | Class | MessageSerializer.class | 序列化器（默认全局） |
| `messageConverter` | Class | MessageConverter.class | 消息转换器（默认全局） |
| `retryPolicy` | Class | RetryPolicy.class | 重试策略（默认全局） |
| `rebalanceStrategy` | Class | RebalanceStrategy.class | 重平衡策略（默认全局） |
| `consumerFilter` | Class[] | {} | 消费者专属过滤器 |

> 完整配置参考请查看本文件「[配置参考](#配置参考)」章节与 [架构设计文档](docs/02-architecture.md)。

---

## SPI 扩展机制

StreamMQ 通过 SPI 提供丰富的扩展点，几乎一切可替换：

| SPI 接口 | 作用 | 默认实现 |
|----------|------|----------|
| `MessageSerializer` | 消息序列化/反序列化 | `JacksonJsonSerializer` / `JdkSerializer` |
| `MessageConverter` | 消息体与业务对象转换 | `DefaultMessageConverter` / `CompactMessageConverter` / `PassThroughMessageConverter` |
| `ProducerFilter` | 生产者过滤器（过滤链） | 无默认 |
| `ConsumerFilter` | 消费者过滤器（全局+per-consumer） | `TagSelectorFilter` / `SqlSelectorFilter` |
| `ProducerInterceptor` | 生产者拦截器（拦截链） | 无默认 |
| `ConsumerInterceptor` | 消费者拦截器（拦截链） | 无默认 |
| `RetryPolicy` | 重试策略 | `FixedArrayRetryPolicy` |
| `RebalanceStrategy` | 消费者重平衡策略 | `AverageRebalanceStrategy` / `ConsistentHashRebalanceStrategy` |
| `CompressionCodec` | 消息压缩编解码 | `GzipCompressionCodec` |
| `TraceCollector` | 链路追踪上下文采集 | `NoopTraceCollector` |
| `ManagementAuthenticator` | 管理接口鉴权 | `AllowAllAuthenticator` / `BasicAuthAuthenticator` / `TokenAuthenticator` / `DenyAllAuthenticator` |
| `DlqFailureStrategy` | 死信消费失败策略 | `LogAndDropDlqFailureStrategy` |

### 自定义 SPI 示例

```java
@Component
public class CustomMessageSerializer implements MessageSerializer {

    @Override
    public byte[] serialize(Object obj) throws SerializationException {
        // 自定义序列化逻辑
        return customSerialize(obj);
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> type) throws SerializationException {
        // 自定义反序列化逻辑
        return customDeserialize(bytes, type);
    }

    @Override
    public String name() {
        return "custom";
    }
}
```

```java
// 在注解中指定使用自定义 SPI
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    serializer = CustomMessageSerializer.class,
    consumerFilter = { CustomFilter.class }
)
```

---

## 可观测性

### Micrometer 指标

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `streammq.send.total` | Counter | 发送总数（tag：`success`） |
| `streammq.send.duration` | Timer | 发送耗时 |
| `streammq.consume.total` | Counter | 消费总数 |
| `streammq.consume.duration` | Timer | 消费耗时 |
| `streammq.retry.total` | Counter | 重试数 |
| `streammq.dlq.total` | Counter | 进入 DLQ 数 |
| `streammq.delay.total` | Counter | 延时投递数 |
| `streammq.transaction.commit.total` | Counter | 事务提交数 |
| `streammq.transaction.rollback.total` | Counter | 事务回滚数 |
| `streammq.transaction.check.total` | Counter | 事务回查数 |

### Actuator 端点

| 端点 | 说明 |
|------|------|
| `/actuator/health` | 健康检查（含 StreamMQ 组件状态） |
| `/actuator/metrics` | Micrometer 指标 |
| `/actuator/prometheus` | Prometheus 格式指标 |

### 管理 REST API

所有操作均注册在 `/actuator/streammq` 之下，按 HTTP 方法 + 路径段分发：

| 端点 | 方法 | 说明 |
|------|------|------|
| `/actuator/streammq` | GET | 总览（状态、消费组、Topic） |
| `/actuator/streammq/groups` | GET | 消费组列表 |
| `/actuator/streammq/topics` | GET | Topic 列表 |
| `/actuator/streammq/pending/{group}/{topic}` | GET | Pending 消息 |
| `/actuator/streammq/dlq/{group}` | GET | DLQ 消息 |
| `/actuator/streammq/dlq/{group}?messageId&targetTopic` | POST | DLQ 重新入队 |
| `/actuator/streammq/dlq/{group}/{messageId}` | DELETE | 删除 DLQ 消息 |
| `/actuator/streammq/stats/{group}/{topic}` | GET | 运行时统计 |
| `/actuator/streammq/ack/{group}/{topic}?messageId` | POST | 手动 ACK |
| `/actuator/streammq/rebalance/{group}` | POST | 触发重平衡 |
| `/actuator/streammq/topics?topic=` | POST | 创建 Topic |
| `/actuator/streammq/topics/{topic}` | DELETE | 删除 Topic |
| `/actuator/streammq/config/{group}` | POST | 更新消费组配置 |

> 所有操作均需通过 `ManagementAuthenticator` 鉴权；默认 `DenyAllAuthenticator` 拒绝所有访问（返回 401），需注册 `AllowAllAuthenticator` / `BasicAuthAuthenticator` / `TokenAuthenticator` Bean 后开放。管理端点可通过 `streammq.admin.enabled=false` 单独关闭。

> ⚠️ **暴露面注意事项**：
>
> - diagnostics 端点挂载在应用**主端口**（MVC 端点实现），不受 `management.endpoints.web.exposure.*` 治理——即使 Actuator 仅暴露 health，`/actuator/streammq/**` 仍随主端口可达，请通过网络层（安全组/Ingress）限制其访问来源；
> - 若启用了 JMX 暴露，建议将 StreamMQ 端点从 JMX 排除，避免管理能力被二次暴露：
>
>   ```yaml
>   management:
>     endpoints:
>       jmx:
>         exposure:
>           exclude: "streammq"
>   ```

### 链路追踪

StreamMQ 提供两条互补的追踪路径，按需选择：

| 路径 | 机制 | 适用场景 |
|------|------|----------|
| `TraceCollector` SPI | 生产/消费上下文采集（Redis 存储 / Slf4j 日志 / Noop），支持 traceId 透传 | 轻量审计、消息级流转画像（配合诊断模块拓扑图） |
| `streammq-tracing-opentelemetry` | 标准 OTel `ProducerInterceptor` / `ConsumerInterceptor`，导出标准 Span | 已有 OpenTelemetry 栈（Collector/Jaeger/Tempo）的链路观测 |

两条路径独立生效、互不依赖；同一应用可同时启用（OTel Span 用于分布式追踪，TraceCollector 用于消息画像）。

**三条追踪开关对照表：**

| 开关 | 作用 | 产物 | 典型组合 |
|------|------|------|----------|
| `streammq.tracing.enabled` | TraceCollector SPI 总开关（消息级追踪采集，traceId 透传） | Slf4j 追踪日志 / 自定义 Collector 输出 | 轻量审计；配合诊断模块消息画像 |
| `streammq.trace.enabled`（+ `streammq.trace.storage=redis`） | 消息轨迹的持久化存储与查询 | Redis Stream 存储的轨迹数据（可经管理端点查询） | 需要事后排查消息流转路径时开启 |
| `streammq.tracing.otel.enabled` | OpenTelemetry 集成开关（拦截器注入 Span） | 标准 OTLP Span（Jaeger / Tempo / Collector 可视） | 已有 OTel 栈的分布式链路观测 |

```java
MDC.put("traceId", "t-001");
template.syncSend(message);  // traceId 自动透传到消费者
```

---

## 示例工程

| 示例 | 说明 |
|------|------|
| [streammq-sample-quickstart](streammq-samples/streammq-sample-quickstart) | 快速开始示例 |
| [streammq-sample-transaction](streammq-samples/streammq-sample-transaction) | 事务消息示例 |
| [streammq-sample-delay](streammq-samples/streammq-sample-delay) | 延时消息示例 |
| [streammq-sample-orderly](streammq-samples/streammq-sample-orderly) | 顺序消息示例 |
| [streammq-sample-dlq](streammq-samples/streammq-sample-dlq) | 死信队列示例 |
| [streammq-sample-interceptor](streammq-samples/streammq-sample-interceptor) | 拦截器示例 |
| [streammq-sample-diagnostics](streammq-samples/streammq-sample-diagnostics) | 诊断画像与慢消费示例 |
| [streammq-sample-tracing](streammq-samples/streammq-sample-tracing) | OpenTelemetry 链路追踪示例 |

---

## 文档导航

| 文档 | 说明 |
|------|------|
| [架构设计](docs/02-architecture.md) | 总体架构与模块划分（历史设计稿，以代码为准） |
| [功能设计](docs/03-functional-design.md) | 核心特性与功能说明（历史设计稿，以代码为准） |
| [详细设计](docs/04-detailed-design.md) | 关键实现细节（历史设计稿，以代码为准） |
| [贡献指南](CONTRIBUTING.md) | 参与贡献 |
| Javadoc | 随 Maven Central 发布的构件附带 sources/javadoc jar |

> ⚠️ docs/02 ~ 04 为实现前的历史设计稿，其中的类名、配置键与部分机制描述已随实现演进过时；
> 当前权威参考是本 README 与代码 Javadoc。

### 设计文档

| 文档 | 说明 |
|------|------|
| [产品需求文档](docs/01-PRD.md) | V1.0 PRD |
| [架构设计](docs/02-architecture.md) | V1.0 架构设计 |
| [功能设计](docs/03-functional-design.md) | V1.0 功能设计 |
| [详细设计](docs/04-detailed-design.md) | V1.0 详细设计 |

---

## 路线图

### V1.0（已完成）

- [x] 注解驱动消费（`@StreamMQConsumer`）
- [x] `StreamMessageTemplate` 编程模型（同步/异步/单向/批量/事务）
- [x] 集群消费 + 广播消费
- [x] 顺序消费（ShardingKey 分片）
- [x] 事务消息（半消息 + 回查）
- [x] 延时消息（18 级 + 任意毫秒）
- [x] 死信队列（含二级 DLQ）
- [x] 消息过滤（Tag + SQL92）
- [x] 消息压缩（GZIP）
- [x] 背压控制（InflightQueue）
- [x] 消费超时自动取消
- [x] Micrometer 指标 + MDC 日志
- [x] 链路追踪（TraceCollector SPI）
- [x] 管理 REST API
- [x] 12 个 SPI 扩展点
- [x] Spring Boot 3 自动装配 + Actuator 集成
- [x] Spring Cloud Stream Binder（实现 Spring Cloud Stream Binder SPI）
- [x] Kubernetes 集成（实验性预览：CRD 控制器 / HPA / 配置热更新，默认关闭；需显式开启 `streammq.cloud.k8s.enabled=true`）
- [x] 消息画像与拓扑图（可视化消息流转拓扑）
- [x] 分布式追踪增强（OpenTelemetry 集成）

### V2.0（规划中）

- [ ] **多后端抽象层**（BackendProvider SPI，支持 Redis / Kafka / RabbitMQ / Pulsar）
- [ ] **Kafka 后端实现**（基于 Kafka Client 的 BackendProvider）
- [ ] **跨机房复制**（异步复制，RPO ≤ 1s）
- [ ] **Kafka 线网协议兼容**（原生 Kafka Client 零代码接入）

---

## 贡献指南

欢迎参与 StreamMQ 开源建设！请阅读 [贡献指南](CONTRIBUTING.md) 了解详细信息。

### 快速贡献

```bash
# 1. Fork & Clone
git clone https://github.com/<your-username>/streammq.git
cd streammq

# 2. 创建分支
git checkout -b feature/your-feature

# 3. 编写代码 & 测试
mvn clean test

# 4. 提交（遵循 Conventional Commits）
git commit -m "feat: add your feature"

# 5. 发起 PR
```

### 贡献方式

- **Bug 报告**：提交 [Issue](https://github.com/HK-hub/StreamMQ/issues)，描述问题与复现步骤
- **功能请求**：提交 [Issue](https://github.com/HK-hub/StreamMQ/issues)，描述期望功能与使用场景
- **代码贡献**：提交 [Pull Request](https://github.com/HK-hub/StreamMQ/pulls)，关联相关 Issue
- **文档改进**：完善文档、修正错误、补充示例
- **问题解答**：在 [Discussions](https://github.com/HK-hub/StreamMQ/discussions) 中帮助其他用户

---

## 社区

- **GitHub Issues**：[https://github.com/HK-hub/StreamMQ/issues](https://github.com/HK-hub/StreamMQ/issues)
- **GitHub Discussions**：[https://github.com/HK-hub/StreamMQ/discussions](https://github.com/HK-hub/StreamMQ/discussions)
- **Pull Requests**：[https://github.com/HK-hub/StreamMQ/pulls](https://github.com/HK-hub/StreamMQ/pulls)

---

## 适用场景

### 推荐使用

- 已有 Redis 基础设施，希望复用为消息总线
- 中小规模业务（单集群日消息量 < 1 亿）
- 需要事务消息 / 延时消息 / 顺序消息能力但不想引入独立 MQ 集群
- 微服务架构下基于 Spring Boot 3 的轻量级异步通信
- 电商订单状态流转、支付回调、库存扣减、通知推送

### 不建议使用

- 超大规模流式数据处理（单集群日消息量 > 1 亿）—— 建议使用 Kafka
- 对消息吞吐要求极高且可容忍少量丢失 —— 建议使用 Kafka
- 需要复杂路由规则（topic 通配符、多级路由）—— 建议使用 RabbitMQ
- 已有成熟 MQ 集群且无 Redis 资源 —— 直接复用现有 MQ

---

## 技术栈

| 技术 | 版本 | 用途 |
|------|------|------|
| Java | 21+ | 运行时 |
| Spring Boot | 3.3.5 | 框架基础 |
| Redisson | 3.34.1 | Redis 客户端 |
| Jackson | 2.18.1 | JSON 序列化 |
| Fury | 0.9.0 | 高性能序列化（可选） |
| Protostuff | 1.8.0 | Protobuf 序列化（可选） |
| Lombok | - | 代码简化 |
| Micrometer | - | 指标收集 |
| SLF4J | - | 日志门面 |

---

## 安全

StreamMQ 重视您的安全。遵循以下最佳实践以确保安全部署：

### 密钥管理

- **凭据不落日志**：StreamMQ 从不将 Redis 密码等鉴权凭据输出到日志，并建议通过环境变量注入。
- **配置安全存储**：切勿将 Redis 密码硬编码在代码或公开的配置文件中。生产环境建议使用环境变量、配置中心（如 Nacos、Apollo）或密钥管理服务（如 Vault、AWS Secrets Manager）进行管理。
- **最小权限原则**：Redis 实例应使用具有最小必要权限的账号，避免直接复用管理员密码。

### 安全配置

```yaml
redisson:
  singleServerConfig:
    # 启用 TLS/SSL
    address: "rediss://127.0.0.1:6379"
    # 使用认证（推荐通过环境变量注入）
    password: ${REDIS_PASSWORD:}
```

### 反序列化安全

`FurySerializer` 与 `JdkSerializer` 默认均为**安全优先（secure-by-default）**：

- `FurySerializer` 默认强制类注册白名单（`requireClassRegistration=true`），首次使用前需注册业务消息体类型；若 Redis 实例完全可信，可显式关闭白名单换取任意 POJO 开箱即用：
  ```java
  MessageSerializer<?> serializer = new FurySerializer(false); // 仅限完全可信的 Redis
  ```
- `JdkSerializer` 内置 JEP 290 类名白名单过滤器（目标类型 + JDK 基础类型），反序列化前拦截未知类；
  第三方业务类型通过 `addAllowedClasses(...)` 显式放行。切勿使用 `JdkSerializer.unrestricted()`。

若 Redis 实例可能被不可信方写入（共享实例、多租户场景），请保持默认白名单模式以收窄反序列化攻击面。完整安全策略见 [SECURITY.md](SECURITY.md)。

### 安全策略

- **版本更新**：关注 [GitHub Security Advisories](https://github.com/HK-hub/StreamMQ/security/advisories) 及时获取安全公告。
- **依赖扫描**：StreamMQ 提供 OWASP Dependency-Check 配置（`mvn verify -Dowasp.skip=false` 触发），CI 中执行依赖漏洞扫描。
- **负责任披露**：如发现安全漏洞，请通过 [GitHub Security](https://github.com/HK-hub/StreamMQ/security) 页面私下报告，我们将在 48 小时内响应。

### 日志脱敏

StreamMQ 从不将 Redis 密码等鉴权凭据输出到日志，并建议通过环境变量注入。

---

## 许可证

本项目基于 [MIT License](LICENSE) 开源。

<div align="center">

---

**StreamMQ** · 让 Redis 成为你的消息总线

如果这个项目对你有帮助，欢迎给一个 ⭐ Star！

[![Star History](https://api.star-history.com/svg?repos=HK-hub/StreamMQ&type=Date)](https://github.com/HK-hub/StreamMQ)

</div>
