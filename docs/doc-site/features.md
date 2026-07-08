# 核心特性

---

## 注解驱动消费

通过 `@StreamMQConsumer` 注解声明式定义消费者，无需手动配置。

```java
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {
    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        return ConsumeAction.SUCCESS;
    }
}
```

---

## Template 编程模型

`StreamMessageTemplate` 提供统一的发送入口，支持同步、异步、单向、批量和事务发送。

```java
public interface StreamMessageTemplate {
    <T> SendResult syncSend(Message<T> message);
    <T> CompletableFuture<SendResult> asyncSend(Message<T> message);
    <T> void sendOneway(Message<T> message);
    <T> List<SendResult> syncSendBatch(BatchMessage<T> batch);
    <T> SendResult executeInTransaction(Message<T> message, TransactionCallback<T> callback);
}
```

---

## 事务消息

半消息 + 本地事务 + 回查机制，保证最终一致性。

```java
TransactionCallback<String> callback = (message, ctx) -> {
    try {
        executeLocalTransaction(message.getBody());
        return LocalTransactionState.COMMIT_MESSAGE;
    } catch (Exception e) {
        return LocalTransactionState.ROLLBACK_MESSAGE;
    }
};

SendResult result = template.executeInTransaction(message, callback);
```

---

## 延时消息

支持 18 级固定延时和任意毫秒延时。

```java
// 固定延时（延时 5 分钟）
MessageBuilder.<String>withTopic("delay-topic")
        .body("content")
        .delayLevel(DelayLevel.LEVEL_9)
        .build();

// 任意延时（延时 15 分钟）
MessageBuilder.<String>withTopic("delay-topic")
        .body("content")
        .delayTimeMillis(15 * 60 * 1000L)
        .build();
```

**延时级别对照表：**

| 级别 | 延时 | 级别 | 延时 |
|------|------|------|------|
| LEVEL_1 | 1s | LEVEL_10 | 6m |
| LEVEL_2 | 5s | LEVEL_11 | 7m |
| LEVEL_3 | 10s | LEVEL_12 | 8m |
| LEVEL_4 | 30s | LEVEL_13 | 9m |
| LEVEL_5 | 1m | LEVEL_14 | 10m |
| LEVEL_6 | 2m | LEVEL_15 | 20m |
| LEVEL_7 | 3m | LEVEL_16 | 30m |
| LEVEL_8 | 4m | LEVEL_17 | 1h |
| LEVEL_9 | 5m | LEVEL_18 | 2h |

---

## 顺序消息

基于 ShardingKey 的分片顺序消费，保证同一分区内严格有序。

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
        return OrderlyAction.SUCCESS;
    }
}
```

---

## 批量发送

`BatchMessage` 批量投递，充分利用 Redis Pipeline 提升吞吐。

```java
BatchMessage<String> batch = BatchMessage.builder()
        .topic("order-topic")
        .addMessage(msg1)
        .addMessage(msg2)
        .addMessage(msg3)
        .build();

List<SendResult> results = template.syncSendBatch(batch);
```

---

## 消费模式

### 集群消费（默认）

同一消费组内每条消息只被一个实例消费。

```java
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group")
```

### 广播消费

同一消费组内每条消息被所有实例消费。

```java
@StreamMQConsumer(
    topic = "order-topic", 
    consumerGroup = "order-group",
    consumeMode = ConsumeMode.BROADCASTING
)
```

---

## 背压控制

InflightQueue 实现拉取-处理解耦，防止内存溢出。

```yaml
streammq:
  consumer:
    inflight-capacity: 1000  # 背压队列容量
```

---

## 消费超时

支持消费超时自动取消并进入重试队列。

```java
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    consumeTimeout = 30000  # 30秒超时
)
```

---

## 死信队列

消费重试耗尽后的消息自动进入 DLQ，用于人工干预。

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

---

## 可观测性

### Micrometer 指标

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `streammq.producer.send.total` | Counter | 发送消息总数 |
| `streammq.producer.send.success` | Counter | 发送成功数 |
| `streammq.producer.send.failed` | Counter | 发送失败数 |
| `streammq.producer.send.duration` | Timer | 发送耗时分布 |
| `streammq.consumer.consume.total` | Counter | 消费消息总数 |
| `streammq.consumer.consume.duration` | Timer | 消费耗时分布 |
| `streammq.consumer.retry.total` | Counter | 重试消息数 |
| `streammq.consumer.dlq.total` | Counter | 进入死信队列数 |

### MDC 结构化日志

支持 traceId 透传，方便链路追踪。

```java
MDC.put("traceId", "t-001");
template.syncSend(message);
```

---

## SPI 扩展机制

StreamMQ 通过 SPI 提供多个扩展点：

| SPI 接口 | 作用 | 默认实现 |
|----------|------|----------|
| `MessageSerializer` | 消息序列化/反序列化 | `JacksonJsonSerializer` |
| `MessageConverter` | 消息体与业务对象转换 | `DefaultMessageConverter` |
| `ProducerInterceptor` | 生产者拦截器 | 无 |
| `ConsumerInterceptor` | 消费者拦截器 | 无 |
| `RetryPolicy` | 重试策略 | `FixedArrayRetryPolicy` |
| `RebalanceStrategy` | 消费者重平衡策略 | `AverageRebalanceStrategy` |
| `TraceCollector` | Trace 上下文采集 | `Slf4jTraceCollector` |
| `ManagementAuthenticator` | 管理接口鉴权 | `AllowAllAuthenticator` |

---

## 兼容性层

### Kafka 兼容 API

从 Kafka 迁移时可使用兼容层，API 风格保持一致。

```java
KafkaProducer<String, String> producer = new KafkaProducer<>(template);
producer.send(new ProducerRecord<>("topic", "key", "value"));
```

### AMQP 兼容 API

从 RabbitMQ 迁移时可使用 AMQP 兼容层。

```java
AmqpClient client = new AmqpClient(template);
AmqpChannel channel = client.createChannel();
channel.basicPublish("exchange", "routingKey", message);
```