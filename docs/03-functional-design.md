# StreamMQ 功能设计文档

> 配套 PRD：[01-PRD.md](./01-PRD.md)　架构设计：[02-architecture.md](./02-architecture.md)
> 本文档定义 StreamMQ 对外暴露的全部功能 API：注解、Template、Listener、Builder、SPI 扩展点、配置属性、异常体系，并给出完整使用示例。所有接口签名以本文档为准。

| 字段 | 内容 |
|---|---|
| 文档版本 | v0.1-draft |
| 状态 | 起草中 |
| 创建日期 | 2026-06-30 |
| 配套 PRD | v0.1-draft |
| 配套架构 | v0.1-draft |
| 技术栈 | JDK 21 / Spring Boot 3.3.x / Redisson 3.34.x / Redis 7.2+ |
| 文档语言 | 中文（Javadoc/注释中文，标识符英文） |

---

## 目录

1. [文档信息](#1-文档信息)
2. [设计总览](#2-设计总览)
3. [注解清单与详细签名](#3-注解清单与详细签名)
4. [编程模型 API 详解](#4-编程模型-api-详解)
5. [Listener 接口设计](#5-listener-接口设计)
6. [事务消息 API](#6-事务消息-api)
7. [延时与批量](#7-延时与批量)
8. [SPI 扩展点接口](#8-spi-扩展点接口)
9. [配置属性类](#9-配置属性类)
10. [异常体系](#10-异常体系)
11. [完整使用示例](#11-完整使用示例)
12. [附录](#12-附录)

---

## 1. 文档信息

| 项 | 内容 |
|---|---|
| 文档版本 | v0.1-draft |
| 当前状态 | 起草中 |
| 维护者 | StreamMQ 团队 |
| 变更记录 | 2026-06-30 v0.1 初稿建立，含注解、Template、Listener、SPI、配置、异常、示例 |
| 相关文档 | 01-PRD.md / 02-architecture.md / 04-detailed-design.md |
| 适用版本 | StreamMQ v1.0.x |
| 编程模型 | 注解（Annotation）+ Template + Listener 三套并存 |

---

## 2. 设计总览

### 2.1 设计目标回顾

StreamMQ 的功能 API 围绕以下目标设计（派生自 PRD 第 3 章）：

- **开箱即用**：`@EnableStreamMq` 一键启动，注解 + Template 即可收发
- **类 RocketMQ 体感**：注解名、方法名、枚举值对齐 RocketMQ Spring Starter，降低学习成本
- **企业级能力**：事务消息、延时消息、顺序消息、批量、死信、重试策略全覆盖
- **全链路泛型**：`Message<T>` 从生产到消费类型不丢失，序列化器按目标类型还原
- **可插拔 SPI**：序列化、重试、Rebalance、Trace、拦截器全部接口化
- **Spring 一等公民**：`@ConfigurationProperties` 绑定、`SmartLifecycle` 容器管理、虚拟线程

### 2.2 API 风格总览

StreamMQ 提供三套并存的编程入口，覆盖不同偏好：

| 风格 | 入口 | 适用场景 |
|---|---|---|
| 注解驱动 | `@StreamMqListener` / `@StreamMqOrderlyListener` | 消费侧声明式接入，最常用 |
| Template 编程 | `StreamMqTemplate<T>` | 生产侧灵活发送（同步/异步/批量/事务） |
| Builder 构造 | `MessageBuilder` | 构造 `Message<T>`，配合 Template 使用 |

消费侧统一通过实现 Listener 接口 + 注解声明完成接入；生产侧统一通过 `StreamMqTemplate` 发送。三套风格共享同一套 `Message<T>` 模型与枚举体系。

### 2.3 关键决策清单

以下 12 项决策已确认，本文档所有 API 设计均严格遵守：

| 编号 | 决策项 | 选定方案 | 影响 |
|---|---|---|---|
| D1 | Template 风格 | RocketMQ 风格 | syncSend / asyncSend / sendOneway / syncSendBatch / executeInTransaction |
| D2 | Listener 参数 | Message + Context 双参数 | `Action onMessage(Message<T>, ConsumerContext)` |
| D3 | 异常处理 | 返回 Action 枚举优先 + 抛异常兜底 | Action 控制重试；RuntimeException 视为失败 |
| D4 | 配置绑定 | 传统 POJO 风格 | `@ConfigurationProperties` + getter/setter + 嵌套 POJO |
| D5 | Message 泛型 | 全链路泛型 | `Message<T>` + `StreamMqTemplate<T>` + `StreamMqListener<T>` |
| D6 | 序列化 SPI | byte[] 进出 | `serialize(T, Class<T>)` / `deserialize(byte[], Class<T>)` |
| D7 | RetryPolicy SPI | 二者均提供 | FixedArrayRetryPolicy + ExponentialBackoffRetryPolicy |
| D8 | TransactionChecker 注册 | `@StreamMqTransactionListener` | 类级注解，类需实现 TransactionChecker |
| D9 | 拦截器 | RocketMQ 风格 | beforeSend + afterSend / beforeConsume + afterConsume |
| D10 | MessageBuilder | 流式调用 | 静态工厂 + 链式方法 |
| D11 | ListenerContainer | Spring SmartLifecycle | phase = MIN_VALUE + 100，虚拟线程 |
| D12 | Acknowledgment | ack/nack/defer 三方法 | 手动 ack 模式注入 ConsumerContext |

---

## 3. 注解清单与详细签名

本章列出 StreamMQ 全部 5 个核心注解。所有注解均位于 `io.streammq.annotation` 包。

### 3.1 @EnableStreamMq（启动注解）

标注在 `@Configuration` 类或启动类上，触发 `StreamMqAutoConfiguration` 自动装配，扫描 `@StreamMqListener` / `@StreamMqTransactionListener` 并注册 ListenerContainer。

```java
package io.streammq.annotation;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
@Import(StreamMqAutoConfiguration.class)
public @interface EnableStreamMq {

    /** 启动模式，默认 STANDARD（标准模式）；预留 LITE（精简模式，仅核心收发） */
    Mode mode() default Mode.STANDARD;

    /** Listener 扫描包路径，默认取标注类所在包 */
    String[] scanBasePackages() default {};
}
```

属性说明：

| 属性 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| mode | `Mode` | 否 | `STANDARD` | STANDARD 全功能；LITE 精简模式（v1.1+） |
| scanBasePackages | `String[]` | 否 | 标注类所在包 | Listener 扫描根包，支持多包 |

用法示例：

```java
@SpringBootApplication
@EnableStreamMq(scanBasePackages = {"com.example.mq"})
public class OrderApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }
}
```

### 3.2 @StreamMqProducer（生产者注入）

标注在 `StreamMqTemplate<T>` 字段上，按 `group` 注入对应生产者实例。一个应用可注入多个不同 group 的 Template。

```java
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface StreamMqProducer {

    /** 生产者组名，必填，对应 consumer-groups/producer 配置 */
    String group();

    /** 命名空间，用于多租户/多环境隔离，默认空 */
    String namespace() default "";

    /** 序列化器实现类，覆盖全局配置；默认不指定走全局配置 */
    Class<? extends MessageSerializer<?>> serializer() default MessageSerializer.class;
}
```

属性说明：

| 属性 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| group | `String` | 是 | — | 生产者组，决定使用哪组 producer 配置 |
| namespace | `String` | 否 | `""` | 命名空间，隔离 topic 前缀 |
| serializer | `Class<? extends MessageSerializer<?>>` | 否 | `MessageSerializer.class`（占位，表示走全局） | 覆盖全局序列化器 |

用法示例：

```java
@Component
public class OrderProducer {
    @StreamMqProducer(group = "order-producer-group")
    private StreamMqTemplate<OrderEvent> template;
}
```

### 3.3 @StreamMqListener（消费监听）

标注在实现 `StreamMqListener<T>` 或 `StreamMqAckListener<T>` 的 Bean 方法/类上，声明一个并发消费监听器。这是最常用的消费注解。

```java
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@Documented
public @interface StreamMqListener {

    /** 主题，必填；支持 "topic" 或 "topic:tag" 形式 */
    String topic();

    /** 消费组名，必填，同一组内负载均衡 */
    String consumerGroup();

    /** 消费模式：CLUSTERING 集群（默认）/ BROADCASTING 广播 */
    ConsumeMode consumeMode() default ConsumeMode.CLUSTERING;

    /** 消息模型：CONCURRENT 并发（默认）/ ORDERLY 顺序（顺序消费请用 @StreamMqOrderlyListener） */
    MessageModel messageModel() default MessageModel.CONCURRENT;

    /** ACK 模式：AUTO 自动（默认，方法返回即 ack）/ MANUAL 手动 */
    AcknowledgeMode acknowledgeMode() default AcknowledgeMode.AUTO;

    /** 最小消费线程，默认 1 */
    int consumeThreadMin() default 1;

    /** 最大消费线程，默认 64 */
    int consumeThreadMax() default 64;

    /** 最大重投次数，超过进死信；默认 16 */
    int maxReconsumeTimes() default 16;

    /** 单条消息消费超时，单位毫秒；默认 30000（30s） */
    long consumeTimeout() default 30000L;

    /** tag 过滤表达式，支持 "tag1||tag2" 或 "*"；默认 "*" 全接收 */
    String selectorExpression() default "*";

    /** 序列化器实现类，覆盖全局；默认走全局 */
    Class<? extends MessageSerializer<?>> serializer() default MessageSerializer.class;
}
```

属性说明：

| 属性 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| topic | `String` | 是 | — | 主题，支持 `topic:tag` |
| consumerGroup | `String` | 是 | — | 消费组 |
| consumeMode | `ConsumeMode` | 否 | `CLUSTERING` | 集群/广播 |
| messageModel | `MessageModel` | 否 | `CONCURRENT` | 并发（顺序请用专用注解） |
| acknowledgeMode | `AcknowledgeMode` | 否 | `AUTO` | 自动/手动 ack |
| consumeThreadMin | `int` | 否 | `1` | 最小线程 |
| consumeThreadMax | `int` | 否 | `64` | 最大线程 |
| maxReconsumeTimes | `int` | 否 | `16` | 最大重投次数 |
| consumeTimeout | `long` | 否 | `30000` | 单条超时（ms） |
| selectorExpression | `String` | 否 | `"*"` | tag 过滤 |
| serializer | `Class<? extends MessageSerializer<?>>` | 否 | 全局 | 覆盖序列化器 |

用法示例：

```java
@Component
@StreamMqListener(topic = "order-topic", consumerGroup = "order-consumer-group")
public class OrderConsumer implements StreamMqListener<OrderEvent> {
    @Override
    public Action onMessage(Message<OrderEvent> message, ConsumerContext context) {
        process(message.getBody());
        return Action.SUCCESS;
    }
}
```

### 3.4 @StreamMqOrderlyListener（顺序消费专用）

声明顺序消费监听器。`messageModel` 固定为 `ORDERLY`，类必须实现 `StreamMqOrderlyListener<T>`。同一 shardingKey 的消息会被路由到同一消费线程串行执行。

```java
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@Documented
public @interface StreamMqOrderlyListener {

    String topic();
    String consumerGroup();
    ConsumeMode consumeMode() default ConsumeMode.CLUSTERING;
    AcknowledgeMode acknowledgeMode() default AcknowledgeMode.AUTO;
    int consumeThreadMin() default 1;
    int consumeThreadMax() default 64;
    int maxReconsumeTimes() default Integer.MAX_VALUE;
    long consumeTimeout() default 30000L;
    String selectorExpression() default "*";
    Class<? extends MessageSerializer<?>> serializer() default MessageSerializer.class;
}
```

> 与 `@StreamMqListener` 的区别：不暴露 `messageModel`（固定 `ORDERLY`）；`maxReconsumeTimes` 默认 `Integer.MAX_VALUE`（顺序消费失败会挂起当前队列而非立即重投）。

用法示例：

```java
@Component
@StreamMqOrderlyListener(topic = "order-topic", consumerGroup = "order-orderly-group")
public class OrderOrderlyConsumer implements StreamMqOrderlyListener<OrderEvent> {
    @Override
    public Action onMessage(Message<OrderEvent> message, OrderlyContext context) {
        applyOrder(message.getBody(), context.shardingKey());
        return Action.SUCCESS;
    }
}
```

### 3.5 @StreamMqTransactionListener（事务回查）

类级注解，声明事务消息回查监听器。被标注的类需实现 `TransactionChecker` 接口。框架在启动时扫描该注解，将实例注册到对应 `transactionGroup` 的事务回查调度器。

```java
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface StreamMqTransactionListener {

    /** 事务消息组，必填，需与生产端 executeInTransaction 使用的 transactionGroup 一致 */
    String transactionGroup();

    /** 回查超时时间，单位毫秒，默认 60000（60s） */
    long checkTimeout() default 60000L;
}
```

属性说明：

| 属性 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| transactionGroup | `String` | 是 | — | 事务组，与生产端一致 |
| checkTimeout | `long` | 否 | `60000` | 单次回查超时（ms） |

用法示例：

```java
@Component
@StreamMqTransactionListener(transactionGroup = "order-tx-group")
public class OrderTransactionChecker implements TransactionChecker {
    @Override
    public LocalTransactionState check(Message<?> message, TransactionContext context) {
        String txId = context.transactionId();
        return orderService.isCommitted(txId)
            ? LocalTransactionState.COMMIT_MESSAGE
            : LocalTransactionState.ROLLBACK_MESSAGE;
    }
}
```

---

## 4. 编程模型 API 详解

本章定义生产侧与消息模型的全部公开 API。所有类位于 `io.streammq.client` 或 `io.streammq.message` 包。

### 4.1 StreamMqTemplate<T>

生产者核心模板，RocketMQ 风格。一个 `group` 对应一个 Template 实例。支持同步、异步、oneway、批量、事务发送。

```java
package io.streammq.client;

/**
 * StreamMQ 生产者模板。
 * 所有发送方法线程安全，可在多线程间共享。
 * @param <T> 消息体泛型
 */
public interface StreamMqTemplate<T> {

    /**
     * 同步发送消息，阻塞至收到 Redis ACK。
     * @param message 待发送消息
     * @return 发送结果
     * @throws StreamMqClientException 客户端错误
     * @throws ProducerTimeoutException 发送超时
     * @throws StreamMqBrokerException Redis 服务端错误
     */
    SendResult syncSend(Message<T> message)
        throws StreamMqClientException, ProducerTimeoutException, StreamMqBrokerException;

    /** 同步发送，指定超时时间（毫秒） */
    SendResult syncSend(Message<T> message, long timeout)
        throws StreamMqClientException, ProducerTimeoutException, StreamMqBrokerException;

    /** 同步发送，指定超时与重试次数 */
    SendResult syncSend(Message<T> message, long timeout, int retryTimes)
        throws StreamMqClientException, ProducerTimeoutException, StreamMqBrokerException;

    /**
     * 异步发送，返回 CompletableFuture。
     * 注意：超时由全局 send-message-timeout 控制。
     */
    CompletableFuture<SendResult> asyncSend(Message<T> message)
        throws StreamMqClientException;

    /** 异步发送，回调通知 */
    void asyncSend(Message<T> message, SendCallback callback)
        throws StreamMqClientException;

    /** 异步发送，回调通知 + 指定超时 */
    void asyncSend(Message<T> message, SendCallback callback, long timeout)
        throws StreamMqClientException;

    /**
     * 单向发送，不等待 ACK，不返回结果。
     * 用于日志/埋点等容忍丢失的场景。
     */
    void sendOneway(Message<T> message) throws StreamMqClientException;

    /**
     * 批量同步发送。
     * @param batchMessage 批量消息
     * @return 每条消息的发送结果列表，顺序与输入一致
     */
    List<SendResult> syncSendBatch(BatchMessage<T> batchMessage)
        throws StreamMqClientException, ProducerTimeoutException, StreamMqBrokerException;

    /**
     * 事务消息发送。
     * 半消息写入后执行 callback，根据返回的 LocalTransactionState 决定 commit/rollback。
     * @param message 消息
     * @param callback 本地事务回调
     * @return 发送结果（SEND_OK 表示半消息已写入）
     */
    SendResult executeInTransaction(Message<T> message, TransactionCallback<T> callback)
        throws StreamMqClientException, StreamMqBrokerException, TransactionException;

    /** 设置消息转换器（连接 Message 与 byte[]） */
    void setMessageConverter(MessageConverter converter);

    /** 获取消息转换器 */
    MessageConverter getMessageConverter();

    /** 设置生产者拦截器链 */
    void setProducerInterceptors(List<ProducerInterceptor> interceptors);

    /** 获取生产者拦截器链 */
    List<ProducerInterceptor> getProducerInterceptors();
}
```

### 4.2 MessageBuilder<T>

流式构造 `Message<T>`，静态工厂 + 链式方法。

```java
package io.streammq.message;

/**
 * 消息构造器，链式 API。
 * 示例：
 *   Message<OrderEvent> msg = MessageBuilder
 *       .withTopic("order-topic")
 *       .withTag("created")
 *       .withKeys("order-123")
 *       .withShardingKey("1001")
 *       .withBody(event)
 *       .withDelayLevel(DelayLevel.MINUTE_5)
 *       .build();
 */
public final class MessageBuilder<T> {

    /** 以 topic 起始构造 */
    public static <T> MessageBuilder<T> withTopic(String topic);
    /** 以 payload 起始构造 */
    public static <T> MessageBuilder<T> withPayload(T payload);
    /** 创建空构造器 */
    public static <T> MessageBuilder<T> create();

    public MessageBuilder<T> withTopic(String topic);
    public MessageBuilder<T> withTag(String tag);
    public MessageBuilder<T> withKeys(String keys);
    public MessageBuilder<T> withShardingKey(String shardingKey);
    public MessageBuilder<T> withBody(T body);
    public MessageBuilder<T> withProperty(String key, String value);
    public MessageBuilder<T> withProperties(java.util.Map<String, String> properties);
    public MessageBuilder<T> withUserProperty(String key, String value);
    public MessageBuilder<T> withDelayLevel(DelayLevel delayLevel);
    public MessageBuilder<T> withDelayTimeMillis(long delayTimeMillis);

    /** 构造不可变 Message */
    public Message<T> build();
}
```

### 4.3 Message<T>

消息统一模型，贯穿生产、消费、序列化全链路。

```java
package io.streammq.message;

/**
 * StreamMQ 消息模型。
 * @param <T> 消息体泛型
 */
public class Message<T> {

    /** 主题 */
    private String topic;
    /** 标签 */
    private String tag;
    /** 业务唯一键，多个用半角逗号分隔 */
    private String keys;
    /** 分片键，顺序消费按此路由 */
    private String shardingKey;
    /** 系统属性（框架内部使用，如 __retryCount） */
    private java.util.Map<String, String> properties;
    /** 用户自定义属性 */
    private java.util.Map<String, String> userProperties;
    /** 消息体 */
    private T body;
    /** 延时级别（与 delayTimeMillis 二选一） */
    private DelayLevel delayLevel;
    /** 精确延时毫秒（优先级高于 delayLevel） */
    private long delayTimeMillis;
    /** 框架生成的全局唯一 ID */
    private String messageId;
    /** 消息产生时间戳（生产端写入） */
    private long bornTimestamp;
    /** 产生消息的主机 */
    private String bornHost;
    /** 已重投次数（消费端读取） */
    private int reconsumeTimes;
    /** 事务 ID（事务消息专用） */
    private String transactionId;

    // 标准 getter / setter 省略
}
```

字段说明：

| 字段 | 类型 | 说明 |
|---|---|---|
| topic | `String` | 主题 |
| tag | `String` | 标签，用于过滤 |
| keys | `String` | 业务键，便于查询/幂等 |
| shardingKey | `String` | 分片键，顺序消费路由依据 |
| properties | `Map<String,String>` | 系统属性 |
| userProperties | `Map<String,String>` | 用户属性 |
| body | `T` | 消息体泛型 |
| delayLevel | `DelayLevel` | 延时级别 |
| delayTimeMillis | `long` | 精确延时（ms） |
| messageId | `String` | 框架生成全局唯一 ID |
| bornTimestamp | `long` | 产生时间戳 |
| bornHost | `String` | 产生主机 |
| reconsumeTimes | `int` | 已重投次数 |
| transactionId | `String` | 事务 ID |

### 4.4 SendResult / SendStatus

```java
package io.streammq.client;

/** 发送状态 */
public enum SendStatus {
    SEND_OK,            // 成功
    SEND_FAILED,        // 失败
    SLAVE_NOT_READY,    // 从节点未就绪（主从复制场景）
    FLUSH_DISK_TIMEOUT  // 刷盘超时（预留，Redis 场景恒为 SEND_OK）
}

/** 发送结果 */
public class SendResult {
    private String messageId;
    private String topic;
    private String tag;
    private SendStatus sendStatus;
    private long bornTimestamp;
    private String regionId;
    // getter / setter
}
```

### 4.5 Action 枚举（消费返回）

消费侧返回值，控制后续动作。

```java
package io.streammq.client;

public enum Action {
    /** 成功，从 PEL 移除，不再投递 */
    SUCCESS,
    /** 失败，稍后按 RetryPolicy 重投 */
    RECONSUME_LATER,
    /** 顺序消费专用：挂起当前队列片刻后重试 */
    SUSPEND_CURRENT_QUEUE_A_MOMENT,
    /** 事务消息 commit（事务回查专用） */
    COMMIT,
    /** 事务消息 rollback（事务回查专用） */
    ROLLBACK
}
```

### 4.6 MessageModel 枚举

```java
package io.streammq.client;

public enum MessageModel {
    /** 并发消费，消息间无顺序保证 */
    CONCURRENT,
    /** 顺序消费，同 shardingKey 串行执行 */
    ORDERLY
}
```

### 4.7 ConsumeMode 枚举

```java
package io.streammq.client;

public enum ConsumeMode {
    /** 集群消费，同组内负载均衡，每条消息被一个消费者处理 */
    CLUSTERING,
    /** 广播消费，同组每个消费者都处理全量消息 */
    BROADCASTING
}
```

### 4.8 AcknowledgeMode 枚举

```java
package io.streammq.client;

public enum AcknowledgeMode {
    /** 自动 ack：Listener 方法返回后框架自动 ack/nack */
    AUTO,
    /** 手动 ack：用户需调用 ConsumerContext.acknowledge() 显式确认 */
    MANUAL
}
```

---

## 5. Listener 接口设计

本章定义消费侧 4 个核心接口与上下文。所有接口位于 `io.streammq.client` 包。统一采用 `Message + Context` 双参数（决策 D2）。

### 5.1 StreamMqAckListener<T>（自动 ack 并发消费）

```java
package io.streammq.client;

/**
 * 自动 ACK 并发消费监听器（默认消费入口）。
 * 返回 Action.SUCCESS 视为成功并自动 ack；
 * 返回 Action.RECONSUME_LATER 或抛出 RuntimeException 视为失败，进入重试。
 */
public interface StreamMqListener<T> {

    /**
     * 处理消息。
     * @param message 消息
     * @param context 消费上下文
     * @return 动作枚举
     */
    Action onMessage(Message<T> message, ConsumerContext context);
}
```

### 5.2 StreamMqAckListener<T>（手动 ack 并发消费）

```java
package io.streammq.client;

/**
 * 手动 ACK 并发消费监听器。
 * 方法无返回值，用户需通过 context.acknowledge() 显式确认；
 * 未显式 ack 将在消费超时后自动 nack 重投。
 */
public interface StreamMqAckListener<T> {

    void onMessage(Message<T> message, ConsumerContext context);
}
```

### 5.3 StreamMqOrderlyListener<T>（顺序消费）

```java
package io.streammq.client;

/**
 * 顺序消费监听器。
 * 同 shardingKey 的消息串行执行。返回 SUSPEND_CURRENT_QUEUE_A_MOMENT
 * 会挂起当前队列片刻后重试，避免乱序。
 */
public interface StreamMqOrderlyListener<T> {

    Action onMessage(Message<T> message, OrderlyContext context);
}
```

### 5.4 ConsumerContext 接口

消费上下文，统一注入到所有 Listener。手动 ack 模式通过 `acknowledge()` 获取 `Acknowledgment`（决策 D12）。

```java
package io.streammq.client;

import java.time.Duration;

/**
 * 消费上下文，提供消息元信息与控制能力。
 */
public interface ConsumerContext {

    /** 主题 */
    String topic();
    /** 消费组 */
    String consumerGroup();
    /** 消费者实例名 */
    String consumerName();
    /** 已重投次数（首次消费为 0） */
    int reconsumeTimes();
    /** 消息产生时间戳 */
    long bornTimestamp();
    /** 消息产生主机 */
    String bornHost();
    /** 消息追踪信息（traceId / spanId 等） */
    String messageTrack();
    /** 扩展属性 */
    String ext(String key);
    /** ACK 模式 */
    AcknowledgeMode ackMode();

    /**
     * 获取 ACK 控制器（仅 MANUAL 模式有效）。
     * AUTO 模式调用将抛出 IllegalStateException。
     */
    Acknowledgment acknowledge();

    /**
     * 挂起当前消费片刻（顺序消费专用，并发消费返回 RECONSUME_LATER 即可）。
     * @param delay 挂起时长
     */
    void suspend(Duration delay);
}
```

### 5.5 Acknowledgment 接口

手动确认控制器，三方法设计（决策 D12）。

```java
package io.streammq.client;

import java.time.Duration;

/**
 * 手动 ACK 控制器，仅 MANUAL 模式可用。
 * 三种动作互斥，多次调用仅首次生效。
 */
public interface Acknowledgment {

    /** 确认成功，从 PEL 移除 */
    void acknowledge();

    /** 立即重新投递（nack，进入下次消费） */
    void nack();

    /** 延迟一段时间后重投 */
    void defer(Duration delay);
}
```

`OrderlyContext` 继承 `ConsumerContext`，扩展顺序消费元信息：

```java
package io.streammq.client;

public interface OrderlyContext extends ConsumerContext {
    /** 当前分片键 */
    String shardingKey();
    /** 当前分片 ID */
    int shardId();
    /** 当前队列位移 */
    long queueOffset();
}
```

---

## 6. 事务消息 API

StreamMQ 提供完整的事务消息能力：半消息 → 本地事务 → commit/rollback → 回查。所有类位于 `io.streammq.transaction` 包。

### 6.1 TransactionCallback<T>

```java
package io.streammq.transaction;

/**
 * 本地事务回调，在半消息写入成功后执行。
 */
public interface TransactionCallback<T> {

    /**
     * 执行本地事务。
     * @param message 半消息
     * @param context 事务上下文
     * @return 本地事务状态
     */
    LocalTransactionState execute(Message<T> message, TransactionContext context);
}
```

`TransactionContext`：

```java
package io.streammq.transaction;

public class TransactionContext {
    /** 事务 ID */
    private String transactionId;
    /** 事务组 */
    private String transactionGroup;
    /** 生产者组 */
    private String producerGroup;
    /** 扩展属性 */
    private java.util.Map<String, String> extAttributes;
    // getter / setter
}
```

### 6.2 TransactionChecker

```java
package io.streammq.transaction;

/**
 * 事务回查接口，通过 @StreamMqTransactionListener 注册。
 */
public interface TransactionChecker {

    /**
     * 回查本地事务状态。
     * 框架对未决事务消息定期调用本方法，根据返回值决定 commit/rollback。
     * @param message 待回查消息
     * @param context 事务上下文
     * @return 本地事务状态（UNKNOW 表示仍未知，稍后再查）
     */
    LocalTransactionState check(Message<?> message, TransactionContext context);
}
```

### 6.3 LocalTransactionState 枚举

```java
package io.streammq.transaction;

public enum LocalTransactionState {
    /** 提交，消息对消费者可见 */
    COMMIT_MESSAGE,
    /** 回滚，删除半消息 */
    ROLLBACK_MESSAGE,
    /** 未知，等待回查 */
    UNKNOW
}
```

### 6.4 完整事务消息流程

事务消息遵循 RocketMQ 半消息模型，5 步流程：

1. **发送半消息**：生产者调用 `executeInTransaction`，框架将消息写入半消息 topic（对消费者不可见），返回 `SendResult`。
2. **执行本地事务**：半消息写入成功后，框架同步调用 `TransactionCallback.execute()` 执行本地事务。
3. **提交/回滚**：根据 `execute()` 返回的 `LocalTransactionState`：
   - `COMMIT_MESSAGE`：将消息从半消息 topic 转投到目标 topic，消费者可见。
   - `ROLLBACK_MESSAGE`：删除半消息。
   - `UNKNOW`：保留半消息，等待回查。
4. **事务回查**：框架对 `UNKNOW` 状态的半消息按 `check-interval` 定期调用 `TransactionChecker.check()`，最长回查 `check-max-times` 次。
5. **超时处理**：超过 `transaction-timeout` 仍未决的半消息，按回查失败处理（默认 rollback）。

---

## 7. 延时与批量

### 7.1 DelayLevel 枚举（18 级）

延时级别对齐 RocketMQ 默认 18 级，每个枚举值带 `toMillis()` 方法。延时消息底层基于 Redis ZSet 实现。

```java
package io.streammq.message;

/**
 * 延时级别（18 级，对齐 RocketMQ 默认配置）。
 */
public enum DelayLevel {

    SECOND_1(1_000L),
    SECOND_5(5_000L),
    SECOND_10(10_000L),
    SECOND_30(30_000L),
    MINUTE_1(60_000L),
    MINUTE_2(120_000L),
    MINUTE_3(180_000L),
    MINUTE_4(240_000L),
    MINUTE_5(300_000L),
    MINUTE_6(360_000L),
    MINUTE_7(420_000L),
    MINUTE_8(480_000L),
    MINUTE_9(540_000L),
    MINUTE_10(600_000L),
    MINUTE_20(1_200_000L),
    MINUTE_30(1_800_000L),
    HOUR_1(3_600_000L),
    HOUR_2(7_200_000L);

    private final long millis;

    DelayLevel(long millis) {
        this.millis = millis;
    }

    /** 转毫秒 */
    public long toMillis() {
        return millis;
    }
}
```

延时级别对照表：

| 枚举值 | 延时 |
|---|---|
| SECOND_1 | 1s |
| SECOND_5 | 5s |
| SECOND_10 | 10s |
| SECOND_30 | 30s |
| MINUTE_1 ~ MINUTE_10 | 1m ~ 10m |
| MINUTE_20 / MINUTE_30 | 20m / 30m |
| HOUR_1 / HOUR_2 | 1h / 2h |

> 若需任意延时，使用 `MessageBuilder.withDelayTimeMillis(long)` 精确指定（精度依赖 Redis ZSet 轮询间隔，默认 1s）。

### 7.2 BatchMessage<T>

批量消息容器，链式构造。底层通过 Redisson RBatch 一次性 XADD 多条。

```java
package io.streammq.message;

public final class BatchMessage<T> {

    /** 以 topic 起始构造 */
    public static <T> BatchMessage.Builder<T> withTopic(String topic);

    public static final class Builder<T> {
        public Builder<T> add(Message<T> message);
        public BatchMessage<T> build();
    }

    /** 配置：单批最大条数，默认 100 */
    private int maxSize = 100;
    /** 配置：单批最大字节数，默认 1MB */
    private long maxBytes = 1024 * 1024L;
    /** 配置：失败策略 */
    private FailStrategy failStrategy = FailStrategy.ALL_OR_NOTHING;
}
```

`FailStrategy` 枚举：

```java
package io.streammq.message;

public enum FailStrategy {
    /** 部分成功：返回每条结果，失败条状态为 SEND_FAILED */
    PARTIAL_SUCCESS,
    /** 全有或全无：任一条失败整批回滚 */
    ALL_OR_NOTHING
}
```

配置说明：

| 属性 | 默认值 | 说明 |
|---|---|---|
| max-size | `100` | 单批最大条数 |
| max-bytes | `1MB` | 单批最大字节 |
| fail-strategy | `ALL_OR_NOTHING` | 失败策略 |

---

## 8. SPI 扩展点接口

StreamMQ 全部扩展能力以 SPI 形式暴露，用户实现接口并注册为 Spring Bean 即可生效（决策 D6/D7/D9）。本章列出 8 个 SPI 接口及默认实现。

### 8.1 MessageSerializer<T>

序列化 SPI，byte[] 进出（决策 D6）。框架按 Listener 注册时解析的目标类型调用。

```java
package io.streammq.serialization;

/**
 * 消息序列化器 SPI。
 * 实现类需注册为 Spring Bean；可通过 @StreamMqListener(serializer=...) 覆盖。
 */
public interface MessageSerializer<T> {

    /**
     * 序列化为 byte[]。
     * @param object 待序列化对象
     * @param type 目标类型
     */
    byte[] serialize(T object, Class<T> type) throws SerializationException;

    /**
     * 反序列化。
     * @param bytes 字节
     * @param type 目标类型
     */
    <R> R deserialize(byte[] bytes, Class<R> type) throws SerializationException;
}
```

默认实现：

| 实现类 | 说明 |
|---|---|
| `JacksonJsonSerializer` | 默认实现，基于 Jackson，JSON 格式 |
| `JdkSerializer` | 备选，基于 JDK 序列化，需实现 Serializable |

### 8.2 ProducerInterceptor

生产者拦截器，RocketMQ 风格（决策 D9）。`beforeSend` 返回 `false` 中止本次发送。

```java
package io.streammq.interceptor;

public interface ProducerInterceptor {

    /** 发送前拦截；返回 false 取消本次发送 */
    boolean beforeSend(Message<?> message);

    /** 发送后回调 */
    void afterSend(Message<?> message, SendResult result);

    /** 拦截器名，默认类名 */
    default String name() {
        return getClass().getSimpleName();
    }

    /** 执行顺序，默认 0，升序执行 */
    default int order() {
        return 0;
    }
}
```

### 8.3 ConsumerInterceptor

消费者拦截器，RocketMQ 风格。`beforeConsume` 返回 `false` 跳过本次消费（视为消费失败，进入重试）。

```java
package io.streammq.interceptor;

public interface ConsumerInterceptor {

    /** 消费前拦截；返回 false 跳过 */
    boolean beforeConsume(Message<?> message);

    /** 消费后回调 */
    void afterConsume(Message<?> message, Action action);

    default String name() {
        return getClass().getSimpleName();
    }

    default int order() {
        return 0;
    }
}
```

### 8.4 RetryPolicy

重试策略 SPI（决策 D7），提供两种默认实现。

```java
package io.streammq.retry;

import java.time.Duration;

/**
 * 重试策略 SPI。
 * 决定失败消息下次重投的延迟时长与是否停止重试。
 */
public interface RetryPolicy {

    /**
     * 计算下次重投延迟。
     * @param reconsumeTimes 已重投次数（首次失败为 1）
     * @param message 失败消息
     */
    Duration nextRetryDelay(int reconsumeTimes, Message<?> message);

    /**
     * 是否停止重试（超过 maxReconsumeTimes 时框架默认停止，本方法可叠加自定义条件）。
     */
    boolean shouldStopRetry(int reconsumeTimes, Message<?> message);
}
```

默认实现：

| 实现类 | 说明 |
|---|---|
| `FixedArrayRetryPolicy` | 对齐 RocketMQ 16 级数组 `[10s,30s,1m,2m,3m,4m,5m,6m,7m,8m,9m,10m,20m,30m,1h,2h]`，超 16 次停止 |
| `ExponentialBackoffRetryPolicy` | 指数退避，`initial=1s, multiplier=2.0, max=2h` |

### 8.5 RebalanceStrategy

消费组重平衡策略，决定 Stream 分片如何在消费者间分配。

```java
package io.streammq.rebalance;

public interface RebalanceStrategy {

    /**
     * 计算分片分配。
     * @param shards 全部分片
     * @param consumers 全部消费者
     * @param consumerGroup 消费组
     * @return 分片 → 消费者映射
     */
    java.util.Map<StreamShard, Consumer> assign(
        java.util.List<StreamShard> shards,
        java.util.List<Consumer> consumers,
        String consumerGroup);
}
```

默认实现：

| 实现类 | 说明 |
|---|---|
| `ConsistentHashRebalanceStrategy` | 默认实现，一致性哈希，减少 rebalance 时分片迁移 |
| `AverageRebalanceStrategy` | 平均分配，按消费者数量均分分片 |

### 8.6 TraceCollector

链路追踪采集 SPI。

```java
package io.streammq.trace;

public interface TraceCollector {

    /** 记录发送 */
    void recordSend(SendTraceContext context);

    /** 记录消费 */
    void recordConsume(ConsumeTraceContext context);

    default String name() {
        return getClass().getSimpleName();
    }

    /** 是否启用 */
    default boolean isEnabled() {
        return true;
    }
}
```

### 8.7 ManagementAuthenticator（v1.0）

管理接口鉴权 SPI，用于 Actuator/HTTP 管理端点认证。

```java
package io.streammq.management;

public interface ManagementAuthenticator {

    /**
     * 鉴权。
     * @param username 用户名
     * @param password 密码
     * @param resource 资源标识
     * @return 是否通过
     */
    boolean authenticate(String username, String password, String resource);
}
```

### 8.8 MessageConverter

连接 `Message<T>` 与 `byte[]` 的转换器，整合序列化器与系统属性编解码。

```java
package io.streammq.message;

public interface MessageConverter {

    /**
     * Message 转字节流（含 header + body）。
     */
    byte[] toBytes(Message<?> message) throws SerializationException;

    /**
     * 字节流转 Message。
     * @param targetType body 目标类型，用于反序列化消息体
     */
    Message<?> fromBytes(byte[] bytes, Class<?> targetType) throws SerializationException;
}
```

---

## 9. 配置属性类

本章定义全部配置属性，采用传统 POJO 风格（决策 D4）：`@ConfigurationProperties` + 标准 getter/setter + 嵌套 POJO。前缀 `streammq`。

### 9.1 StreamMqProperties（顶层）

```java
package io.streammq.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "streammq")
public class StreamMqProperties {

    /** 是否启用，默认 true */
    private boolean enabled = true;
    /** 命名空间，多租户隔离前缀 */
    private String namespace = "";
    /** Redisson 配置引用 */
    private RedissonConfig redisson = new RedissonConfig();
    /** 默认消费配置 */
    private ConsumerDefaults defaultConsumer = new ConsumerDefaults();
    /** 默认生产配置 */
    private ProducerDefaults defaultProducer = new ProducerDefaults();
    /** 消费组列表 */
    private java.util.Map<String, ConsumerGroup> consumerGroups = new java.util.HashMap<>();
    /** 生产组列表 */
    private java.util.Map<String, ProducerGroup> producerGroups = new java.util.HashMap<>();
    /** 事务配置 */
    private TransactionConfig transaction = new TransactionConfig();
    /** trace 配置 */
    private TraceConfig trace = new TraceConfig();
    // getter / setter
}
```

### 9.2 StreamMqConsumerProperties

消费侧配置，对应 `streammq.consumer-groups.<name>`。

```java
package io.streammq.config;

public class StreamMqConsumerProperties {
    /** 消费模式 CLUSTERING / BROADCASTING，默认 CLUSTERING */
    private ConsumeMode consumeMode = ConsumeMode.CLUSTERING;
    /** 消息模型 CONCURRENT / ORDERLY，默认 CONCURRENT */
    private MessageModel messageModel = MessageModel.CONCURRENT;
    /** ACK 模式 AUTO / MANUAL，默认 AUTO */
    private AcknowledgeMode acknowledgeMode = AcknowledgeMode.AUTO;
    /** 最小消费线程，默认 1 */
    private int consumeThreadMin = 1;
    /** 最大消费线程，默认 64 */
    private int consumeThreadMax = 64;
    /** 最大重投次数，默认 16 */
    private int maxReconsumeTimes = 16;
    /** 单条消费超时（ms），默认 30000 */
    private long consumeTimeout = 30000L;
    /** 重试策略类名，默认 FixedArrayRetryPolicy */
    private Class<? extends RetryPolicy> retryPolicy = FixedArrayRetryPolicy.class;
    /** 序列化器类名，默认 JacksonJsonSerializer */
    private Class<? extends MessageSerializer<?>> serializer = JacksonJsonSerializer.class;
    /** tag 过滤表达式，默认 "*" */
    private String selectorExpression = "*";
    // getter / setter
}
```

消费组默认值表：

| 属性 | 默认值 |
|---|---|
| consume-mode | `CLUSTERING` |
| message-model | `CONCURRENT` |
| acknowledge-mode | `AUTO` |
| consume-thread-min | `1` |
| consume-thread-max | `64` |
| max-reconsume-times | `16` |
| consume-timeout | `30000`（ms） |
| retry-policy | `FixedArrayRetryPolicy` |
| serializer | `JacksonJsonSerializer` |
| selector-expression | `"*"` |

### 9.3 StreamMqProducerProperties

生产侧配置，对应 `streammq.producer-groups.<name>`。

```java
package io.streammq.config;

public class StreamMqProducerProperties {
    /** 生产组名 */
    private String group;
    /** 同步发送超时（ms），默认 3000 */
    private long sendMessageTimeout = 3000L;
    /** 同步重试次数，默认 2 */
    private int retryTimes = 2;
    /** 异步重试次数，默认 0（不重试） */
    private int asyncRetryTimes = 0;
    /** 压缩阈值字节，默认 0（不压缩） */
    private long compressThreshold = 0L;
    /** 序列化器类名，默认 JacksonJsonSerializer */
    private Class<? extends MessageSerializer<?>> serializer = JacksonJsonSerializer.class;
    /** 批量配置 */
    private BatchConfig batch = new BatchConfig();
    // getter / setter
}
```

生产组默认值表：

| 属性 | 默认值 |
|---|---|
| send-message-timeout | `3000`（ms） |
| retry-times | `2` |
| async-retry-times | `0` |
| compress-threshold | `0`（不压缩） |
| serializer | `JacksonJsonSerializer` |
| batch.max-size | `100` |
| batch.max-bytes | `1MB` |
| batch.fail-strategy | `ALL_OR_NOTHING` |

### 9.4 StreamMqTransactionProperties

事务消息配置。

```java
package io.streammq.config;

public class StreamMqTransactionProperties {
    /** 半消息 topic 前缀，默认 "streammq:half:" */
    private String halfTopicPrefix = "streammq:half:";
    /** 回查间隔（ms），默认 10000（10s） */
    private long checkInterval = 10000L;
    /** 最大回查次数，默认 15 */
    private int checkMaxTimes = 15;
    /** 事务超时（ms），默认 60000（60s） */
    private long transactionTimeout = 60000L;
    // getter / setter
}
```

事务默认值表：

| 属性 | 默认值 |
|---|---|
| half-topic-prefix | `streammq:half:` |
| check-interval | `10000`（ms） |
| check-max-times | `15` |
| transaction-timeout | `60000`（ms） |

### 9.5 完整 application.yml 示例

```yaml
streammq:
  enabled: true
  namespace: ""
  redisson:
    config: classpath:redisson.yaml     # 或引用现有 RedissonClient Bean
  default-consumer:
    consume-mode: CLUSTERING
    acknowledge-mode: AUTO
    max-reconsume-times: 16
    consume-thread-max: 64
    retry-policy: io.streammq.retry.FixedArrayRetryPolicy
    serializer: io.streammq.serialization.JacksonJsonSerializer
  default-producer:
    send-message-timeout: 3000
    retry-times: 2
    serializer: io.streammq.serialization.JacksonJsonSerializer
    batch:
      max-size: 100
      max-bytes: 1048576
      fail-strategy: ALL_OR_NOTHING
  consumer-groups:
    order-consumer-group:
      topic: order-topic
      consume-mode: CLUSTERING
      max-reconsume-times: 16
    broadcast-group:
      topic: order-topic
      consume-mode: BROADCASTING
  producer-groups:
    order-producer-group:
      send-message-timeout: 5000
      retry-times: 3
  transaction:
    half-topic-prefix: "streammq:half:"
    check-interval: 10000
    check-max-times: 15
    transaction-timeout: 60000
  trace:
    enabled: true
    collector: io.streammq.trace.Slf4jTraceCollector
```

---

## 10. 异常体系

所有异常位于 `io.streammq.exception` 包，继承关系如下：

```
RuntimeException
  └── StreamMqException                  (基类)
        ├── StreamMqClientException     (客户端配置/调用错误)
        ├── StreamMqBrokerException     (Redis 服务端错误)
        ├── SerializationException       (序列化失败)
        ├── ProducerTimeoutException     (发送超时)
        ├── ConsumerInterruptedException(消费线程中断)
        └── TransactionException         (事务消息错误)
```

```java
package io.streammq.exception;

/** StreamMQ 异常基类，所有框架异常均继承此类 */
public class StreamMqException extends RuntimeException {
    public StreamMqException(String message) { super(message); }
    public StreamMqException(String message, Throwable cause) { super(message, cause); }
}

/** 客户端错误：配置非法、参数缺失、Template 未初始化等 */
public class StreamMqClientException extends StreamMqException {
    public StreamMqClientException(String message) { super(message); }
    public StreamMqClientException(String message, Throwable cause) { super(message, cause); }
}

/** Redis 服务端错误：XADD 失败、连接断开、集群不可用等 */
public class StreamMqBrokerException extends StreamMqException {
    private final int errorCode;
    public StreamMqBrokerException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }
    public StreamMqBrokerException(int errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }
    public int getErrorCode() { return errorCode; }
}

/** 序列化/反序列化失败 */
public class SerializationException extends StreamMqException {
    public SerializationException(String message) { super(message); }
    public SerializationException(String message, Throwable cause) { super(message, cause); }
}

/** 发送超时 */
public class ProducerTimeoutException extends StreamMqException {
    public ProducerTimeoutException(String message) { super(message); }
}

/** 消费线程被中断 */
public class ConsumerInterruptedException extends StreamMqException {
    public ConsumerInterruptedException(String message) { super(message); }
}

/** 事务消息错误：半消息写入失败、回查失败等 */
public class TransactionException extends StreamMqException {
    public TransactionException(String message) { super(message); }
    public TransactionException(String message, Throwable cause) { super(message, cause); }
}
```

用法示例（消费侧）：

```java
@Override
public Action onMessage(Message<OrderEvent> message, ConsumerContext context) {
    try {
        orderService.process(message.getBody());
        return Action.SUCCESS;
    } catch (BusinessException e) {
        // 业务异常：返回 RECONSUME_LATER 触发重试
        return Action.RECONSUME_LATER;
    } catch (Exception e) {
        // 兜底：抛出异常同样进入重试流程
        throw new StreamMqClientException("处理失败", e);
    }
}
```

---

## 11. 完整使用示例

本章给出 10 个完整可运行示例，覆盖全部核心能力。

### 11.1 普通消息发送 + 消费

```java
// 生产者
@Component
public class OrderProducer {
    @StreamMqProducer(group = "order-producer-group")
    private StreamMqTemplate<OrderEvent> template;

    public void send(OrderEvent event) {
        Message<OrderEvent> msg = MessageBuilder
            .withTopic("order-topic")
            .withTag("created")
            .withKeys(event.getOrderId())
            .withShardingKey(event.getUserId())
            .withBody(event)
            .build();
        SendResult result = template.syncSend(msg);
        if (result.getSendStatus() != SendStatus.SEND_OK) {
            throw new RuntimeException("发送失败: " + result);
        }
    }
}

// 消费者
@Component
@StreamMqListener(topic = "order-topic", consumerGroup = "order-consumer-group")
public class OrderConsumer implements StreamMqListener<OrderEvent> {
    @Override
    public Action onMessage(Message<OrderEvent> message, ConsumerContext context) {
        System.out.printf("收到订单 %s，第 %d 次消费%n",
            message.getBody().getOrderId(), context.reconsumeTimes());
        return Action.SUCCESS;
    }
}
```

### 11.2 异步发送 + SendCallback

```java
@Component
public class AsyncOrderProducer {
    @StreamMqProducer(group = "order-producer-group")
    private StreamMqTemplate<OrderEvent> template;

    public void sendAsync(OrderEvent event) {
        Message<OrderEvent> msg = MessageBuilder.withPayload(event)
            .withTopic("order-topic").withTag("async").build();
        template.asyncSend(msg, new SendCallback() {
            @Override
            public void onSuccess(SendResult result) {
                log.info("发送成功 msgId={}", result.getMessageId());
            }
            @Override
            public void onException(Throwable e) {
                log.error("发送失败", e);
            }
        }, 5000L);
    }
}
```

### 11.3 手动 ack 消费

```java
@Component
@StreamMqListener(
    topic = "order-topic",
    consumerGroup = "order-manual-group",
    acknowledgeMode = AcknowledgeMode.MANUAL
)
public class ManualAckConsumer implements StreamMqAckListener<OrderEvent> {
    @Override
    public void onMessage(Message<OrderEvent> message, ConsumerContext context) {
        try {
            if (orderService.tryProcess(message.getBody())) {
                context.acknowledge().acknowledge();   // 成功确认
            } else {
                context.acknowledge().defer(Duration.ofSeconds(30)); // 延后重试
            }
        } catch (Exception e) {
            context.acknowledge().nack();               // 立即重投
        }
    }
}
```

### 11.4 顺序消费

```java
@Component
@StreamMqOrderlyListener(topic = "order-topic", consumerGroup = "order-orderly-group")
public class OrderOrderlyConsumer implements StreamMqOrderlyListener<OrderEvent> {
    @Override
    public Action onMessage(Message<OrderEvent> message, OrderlyContext context) {
        // 同 userId 的订单事件串行执行
        applyOrder(message.getBody(), context.shardingKey());
        if (downstreamBusy()) {
            return Action.SUSPEND_CURRENT_QUEUE_A_MOMENT; // 挂起当前队列
        }
        return Action.SUCCESS;
    }
}
```

### 11.5 延时消息

```java
// 5 分钟后投递
Message<OrderEvent> msg = MessageBuilder
    .withTopic("order-delay-topic")
    .withTag("cancel")
    .withKeys(orderId)
    .withBody(event)
    .withDelayLevel(DelayLevel.MINUTE_5)
    .build();
template.syncSend(msg);

// 任意延时：90 秒后
Message<OrderEvent> msg2 = MessageBuilder
    .withTopic("order-delay-topic")
    .withBody(event)
    .withDelayTimeMillis(90_000L)
    .build();
template.syncSend(msg2);
```

### 11.6 批量发送

```java
BatchMessage<OrderEvent> batch = BatchMessage
    .<OrderEvent>withTopic("order-topic")
    .add(MessageBuilder.withPayload(e1).withTag("created").build())
    .add(MessageBuilder.withPayload(e2).withTag("created").build())
    .add(MessageBuilder.withPayload(e3).withTag("created").build())
    .build();

List<SendResult> results = template.syncSendBatch(batch);
results.stream()
    .filter(r -> r.getSendStatus() != SendStatus.SEND_OK)
    .findFirst()
    .ifPresent(r -> { throw new RuntimeException("部分发送失败"); });
```

### 11.7 事务消息完整流程

```java
// 生产端
@Component
public class OrderTxProducer {
    @StreamMqProducer(group = "order-producer-group")
    private StreamMqTemplate<OrderEvent> template;

    public void sendTx(OrderEvent event) {
        Message<OrderEvent> msg = MessageBuilder
            .withTopic("order-topic").withTag("tx").withBody(event).build();
        template.executeInTransaction(msg, (message, ctx) -> {
            // 半消息已写入，执行本地事务
            try {
                orderService.createLocalOrder(message.getBody(), ctx.transactionId());
                return LocalTransactionState.COMMIT_MESSAGE;
            } catch (Exception e) {
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }
        });
    }
}

// 回查端
@Component
@StreamMqTransactionListener(transactionGroup = "order-tx-group")
public class OrderTransactionChecker implements TransactionChecker {
    @Override
    public LocalTransactionState check(Message<?> message, TransactionContext context) {
        boolean committed = orderService.isCommitted(context.transactionId());
        return committed ? LocalTransactionState.COMMIT_MESSAGE
                         : LocalTransactionState.ROLLBACK_MESSAGE;
    }
}
```

### 11.8 广播消费

```java
@Component
@StreamMqListener(
    topic = "order-topic",
    consumerGroup = "order-broadcast-group",
    consumeMode = ConsumeMode.BROADCASTING   // 每个实例都消费全量
)
public class CacheRefreshConsumer implements StreamMqListener<OrderEvent> {
    @Override
    public Action onMessage(Message<OrderEvent> message, ConsumerContext context) {
        cache.refresh(message.getBody());
        return Action.SUCCESS;
    }
}
```

### 11.9 拦截器 SPI 实现

```java
@Component
public class AuditInterceptor implements ProducerInterceptor {

    @Override
    public boolean beforeSend(Message<?> message) {
        // 记录审计日志；返回 false 可拦截非法消息
        audit.log("send", message.getTopic(), message.getKeys());
        return true;
    }

    @Override
    public void afterSend(Message<?> message, SendResult result) {
        metrics.recordSendLatency(System.currentTimeMillis() - message.getBornTimestamp());
    }

    @Override
    public int order() {
        return 10;  // 在业务拦截器之后执行
    }
}

// 注册到 Template
@Configuration
public class InterceptorConfig {
    @Bean
    public List<ProducerInterceptor> producerInterceptors(AuditInterceptor audit) {
        return List.of(audit);
    }
}
```

### 11.10 自定义 RetryPolicy SPI

```java
@Component
public class CustomRetryPolicy implements RetryPolicy {

    @Override
    public Duration nextRetryDelay(int reconsumeTimes, Message<?> message) {
        // 前 3 次快速重试，之后指数退避
        if (reconsumeTimes <= 3) {
            return Duration.ofSeconds(1);
        }
        long delay = (long) Math.pow(2, reconsumeTimes - 3);
        return Duration.ofSeconds(Math.min(delay, 600));
    }

    @Override
    public boolean shouldStopRetry(int reconsumeTimes, Message<?> message) {
        // 订单类消息最多重试 10 次
        if ("order-topic".equals(message.getTopic())) {
            return reconsumeTimes > 10;
        }
        return reconsumeTimes > 16;
    }
}

// 通过注解或配置指定
@StreamMqListener(
    topic = "order-topic",
    consumerGroup = "order-consumer-group",
    // 注解不直接支持 retryPolicy，请在配置中指定：
    // streammq.consumer-groups.order-consumer-group.retry-policy: com.example.CustomRetryPolicy
)
```

---

## 12. 附录

### 12.1 方法签名索引表（按字母排序）

| 类/接口 | 方法签名 |
|---|---|
| Acknowledgment | `void acknowledge()` |
| Acknowledgment | `void nack()` |
| Acknowledgment | `void defer(Duration delay)` |
| ConsumerContext | `String topic()` |
| ConsumerContext | `String consumerGroup()` |
| ConsumerContext | `String consumerName()` |
| ConsumerContext | `int reconsumeTimes()` |
| ConsumerContext | `long bornTimestamp()` |
| ConsumerContext | `String bornHost()` |
| ConsumerContext | `String messageTrack()` |
| ConsumerContext | `String ext(String key)` |
| ConsumerContext | `AcknowledgeMode ackMode()` |
| ConsumerContext | `Acknowledgment acknowledge()` |
| ConsumerContext | `void suspend(Duration delay)` |
| ConsumerInterceptor | `boolean beforeConsume(Message<?>)` |
| ConsumerInterceptor | `void afterConsume(Message<?>, Action)` |
| ManagementAuthenticator | `boolean authenticate(String, String, String)` |
| MessageConverter | `byte[] toBytes(Message<?>)` |
| MessageConverter | `Message<?> fromBytes(byte[], Class<?>)` |
| MessageSerializer | `byte[] serialize(T, Class<T>)` |
| MessageSerializer | `<R> R deserialize(byte[], Class<R>)` |
| OrderlyContext | `String shardingKey()` |
| OrderlyContext | `int shardId()` |
| OrderlyContext | `long queueOffset()` |
| ProducerInterceptor | `boolean beforeSend(Message<?>)` |
| ProducerInterceptor | `void afterSend(Message<?>, SendResult)` |
| RebalanceStrategy | `Map<StreamShard,Consumer> assign(List, List, String)` |
| RetryPolicy | `Duration nextRetryDelay(int, Message<?>)` |
| RetryPolicy | `boolean shouldStopRetry(int, Message<?>)` |
| StreamMqAckListener | `void onMessage(Message<T>, ConsumerContext)` |
| StreamMqListener | `Action onMessage(Message<T>, ConsumerContext)` |
| StreamMqOrderlyListener | `Action onMessage(Message<T>, OrderlyContext)` |
| StreamMqTemplate | `SendResult syncSend(Message<T>)` |
| StreamMqTemplate | `SendResult syncSend(Message<T>, long)` |
| StreamMqTemplate | `SendResult syncSend(Message<T>, long, int)` |
| StreamMqTemplate | `CompletableFuture<SendResult> asyncSend(Message<T>)` |
| StreamMqTemplate | `void asyncSend(Message<T>, SendCallback)` |
| StreamMqTemplate | `void asyncSend(Message<T>, SendCallback, long)` |
| StreamMqTemplate | `void sendOneway(Message<T>)` |
| StreamMqTemplate | `List<SendResult> syncSendBatch(BatchMessage<T>)` |
| StreamMqTemplate | `SendResult executeInTransaction(Message<T>, TransactionCallback<T>)` |
| TraceCollector | `void recordSend(SendTraceContext)` |
| TraceCollector | `void recordConsume(ConsumeTraceContext)` |
| TransactionCallback | `LocalTransactionState execute(Message<T>, TransactionContext)` |
| TransactionChecker | `LocalTransactionState check(Message<?>, TransactionContext)` |

### 12.2 枚举值汇总

| 枚举 | 值 |
|---|---|
| Action | SUCCESS / RECONSUME_LATER / SUSPEND_CURRENT_QUEUE_A_MOMENT / COMMIT / ROLLBACK |
| MessageModel | CONCURRENT / ORDERLY |
| ConsumeMode | CLUSTERING / BROADCASTING |
| AcknowledgeMode | AUTO / MANUAL |
| SendStatus | SEND_OK / SEND_FAILED / SLAVE_NOT_READY / FLUSH_DISK_TIMEOUT |
| LocalTransactionState | COMMIT_MESSAGE / ROLLBACK_MESSAGE / UNKNOW |
| DelayLevel | SECOND_1 / SECOND_5 / SECOND_10 / SECOND_30 / MINUTE_1 ~ MINUTE_10 / MINUTE_20 / MINUTE_30 / HOUR_1 / HOUR_2（共 18 级） |
| FailStrategy | PARTIAL_SUCCESS / ALL_OR_NOTHING |

### 12.3 注解属性汇总

| 注解 | 属性 | 必填 | 默认值 |
|---|---|---|---|
| @EnableStreamMq | mode | 否 | STANDARD |
| @EnableStreamMq | scanBasePackages | 否 | 标注类所在包 |
| @StreamMqProducer | group | 是 | — |
| @StreamMqProducer | namespace | 否 | `""` |
| @StreamMqProducer | serializer | 否 | 全局 |
| @StreamMqListener | topic | 是 | — |
| @StreamMqListener | consumerGroup | 是 | — |
| @StreamMqListener | consumeMode | 否 | CLUSTERING |
| @StreamMqListener | messageModel | 否 | CONCURRENT |
| @StreamMqListener | acknowledgeMode | 否 | AUTO |
| @StreamMqListener | consumeThreadMin | 否 | 1 |
| @StreamMqListener | consumeThreadMax | 否 | 64 |
| @StreamMqListener | maxReconsumeTimes | 否 | 16 |
| @StreamMqListener | consumeTimeout | 否 | 30000 |
| @StreamMqListener | selectorExpression | 否 | `"*"` |
| @StreamMqListener | serializer | 否 | 全局 |
| @StreamMqOrderlyListener | topic | 是 | — |
| @StreamMqOrderlyListener | consumerGroup | 是 | — |
| @StreamMqOrderlyListener | maxReconsumeTimes | 否 | Integer.MAX_VALUE |
| @StreamMqTransactionListener | transactionGroup | 是 | — |
| @StreamMqTransactionListener | checkTimeout | 否 | 60000 |

### 12.4 默认值常量表

| 配置项 | 默认值 |
|---|---|
| streammq.enabled | `true` |
| streammq.namespace | `""` |
| 消费 consume-thread-min | `1` |
| 消费 consume-thread-max | `64` |
| 消费 max-reconsume-times | `16` |
| 消费 consume-timeout | `30000` ms |
| 消费 retry-policy | `FixedArrayRetryPolicy` |
| 消费 serializer | `JacksonJsonSerializer` |
| 顺序消费 max-reconsume-times | `Integer.MAX_VALUE` |
| 生产 send-message-timeout | `3000` ms |
| 生产 retry-times | `2` |
| 生产 async-retry-times | `0` |
| 生产 compress-threshold | `0`（不压缩） |
| 批量 max-size | `100` |
| 批量 max-bytes | `1MB` |
| 批量 fail-strategy | `ALL_OR_NOTHING` |
| 事务 half-topic-prefix | `streammq:half:` |
| 事务 check-interval | `10000` ms |
| 事务 check-max-times | `15` |
| 事务 transaction-timeout | `60000` ms |
| ListenerContainer phase | `Integer.MIN_VALUE + 100` |
| FixedArrayRetryPolicy 数组 | `[10s,30s,1m,2m,3m,4m,5m,6m,7m,8m,9m,10m,20m,30m,1h,2h]`（16 级） |
| ExponentialBackoffRetryPolicy initial | `1s` |
| ExponentialBackoffRetryPolicy multiplier | `2.0` |
| ExponentialBackoffRetryPolicy max | `2h` |

### 12.5 变更记录

| 日期 | 版本 | 变更说明 |
|---|---|---|
| 2026-06-30 | v0.1-draft | 初稿建立，含 5 注解、Template、Listener、事务、延时、批量、8 SPI、配置、异常、10 示例、附录 |

---

> 本文档所有接口签名以本文为准；如与 PRD/架构文档冲突，功能 API 以本文档为准。后续详细设计（04-detailed-design.md）将基于本文档的 API 定义展开内部实现。
