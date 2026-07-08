# API 文档

---

## StreamMessageTemplate

统一的消息发送入口。

### syncSend

同步发送消息。

```java
<T> SendResult syncSend(Message<T> message);
<T> SendResult syncSend(Message<T> message, long timeoutMillis);
<T> SendResult syncSend(Message<T> message, long timeoutMillis, int retryTimes);
```

**参数：**
- `message` - 消息对象
- `timeoutMillis` - 超时毫秒数，默认 3000
- `retryTimes` - 重试次数，默认 2

**示例：**
```java
Message<String> message = MessageBuilder.<String>withTopic("order-topic").body("content").build();
SendResult result = template.syncSend(message);
```

### asyncSend

异步发送消息。

```java
<T> CompletableFuture<SendResult> asyncSend(Message<T> message);
<T> void asyncSend(Message<T> message, SendCallback callback);
<T> void asyncSend(Message<T> message, SendCallback callback, long timeoutMillis);
```

**示例：**
```java
template.asyncSend(message).whenComplete((result, ex) -> {
    if (ex == null) {
        System.out.println("发送成功: " + result.getMessageId());
    }
});
```

### sendOneway

单向发送，不等待响应。

```java
<T> void sendOneway(Message<T> message);
```

### syncSendBatch

批量发送。

```java
<T> List<SendResult> syncSendBatch(BatchMessage<T> batch);
```

### executeInTransaction

事务消息发送。

```java
<T> SendResult executeInTransaction(Message<T> message, TransactionCallback<T> callback);
```

---

## MessageBuilder

消息构建器。

```java
Message<String> message = MessageBuilder.<String>withTopic("order-topic")
        .tag("created")
        .keys("order-123")
        .shardingKey("user-456")
        .body("content")
        .delayLevel(DelayLevel.LEVEL_6)
        .userProperty("traceId", "t-001")
        .build();
```

### 方法列表

| 方法 | 说明 |
|------|------|
| `withTopic(String)` | 设置主题 |
| `tag(String)` | 设置标签 |
| `keys(String)` | 设置业务键 |
| `shardingKey(String)` | 设置分片键 |
| `body(T)` | 设置消息体 |
| `delayLevel(DelayLevel)` | 设置延时级别 |
| `delayTimeMillis(long)` | 设置任意延时毫秒 |
| `userProperty(String, String)` | 添加用户属性 |
| `build()` | 构建消息 |

---

## @StreamMQConsumer

消费者注解。

```java
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    messageModel = MessageModel.CONCURRENT,
    consumeMode = ConsumeMode.CLUSTERING,
    consumeThreadMin = 1,
    consumeThreadMax = 64,
    maxReconsumeTimes = 16,
    consumeTimeout = 30000,
    pullBatchSize = 32,
    selectorExpression = "*",
    shardCount = 4,
    dlqMode = false
)
```

### 属性列表

| 属性 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `topic` | String | - | 主题（必填） |
| `consumerGroup` | String | - | 消费组（必填） |
| `messageModel` | MessageModel | CONCURRENT | 消费模型 |
| `consumeMode` | ConsumeMode | CLUSTERING | 消费模式 |
| `consumeThreadMin` | int | 1 | 最小线程数 |
| `consumeThreadMax` | int | 64 | 最大线程数 |
| `maxReconsumeTimes` | int | 16 | 最大重试次数 |
| `consumeTimeout` | long | 30000 | 消费超时毫秒 |
| `pullBatchSize` | int | 32 | 拉取批量 |
| `selectorExpression` | String | "*" | Tag 过滤表达式 |
| `shardCount` | int | 4 | 顺序消费分片数 |
| `dlqMode` | boolean | false | 是否 DLQ 消费者 |

---

## StreamMessageConcurrentlyConsumer

并发消费接口。

```java
public interface StreamMessageConcurrentlyConsumer<T> {
    ConsumeAction onMessage(Message<T> message, ConsumeContext context) throws Exception;
}
```

**示例：**
```java
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {
    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        process(message.getBody());
        return ConsumeAction.SUCCESS;
    }
}
```

---

## StreamMessageOrderlyConsumer

顺序消费接口。

```java
public interface StreamMessageOrderlyConsumer<T> {
    OrderlyAction onMessage(Message<T> message, ConsumeOrderlyContext context) throws Exception;
}
```

**示例：**
```java
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group", 
                  messageModel = MessageModel.ORDERLY)
public class OrderlyConsumer implements StreamMessageOrderlyConsumer<String> {
    @Override
    public OrderlyAction onMessage(Message<String> message, ConsumeOrderlyContext context) {
        process(message.getBody());
        return OrderlyAction.SUCCESS;
    }
}
```

---

## ConsumeAction

消费结果枚举。

```java
// 成功
ConsumeAction.SUCCESS

// 稍后重试
ConsumeAction.RECONSUME_LATER

// 延迟重试
ConsumeAction.defer(Duration.ofSeconds(30))
```

---

## TransactionCallback

事务回调接口。

```java
public interface TransactionCallback<T> {
    LocalTransactionState execute(Message<T> message, TransactionContext context) throws Exception;
}
```

**示例：**
```java
TransactionCallback<String> callback = (message, ctx) -> {
    try {
        executeLocalTransaction(message.getBody());
        return LocalTransactionState.COMMIT_MESSAGE;
    } catch (Exception e) {
        return LocalTransactionState.ROLLBACK_MESSAGE;
    }
};
```

---

## TransactionChecker

事务回查接口。

```java
public interface TransactionChecker<T> {
    LocalTransactionState check(Message<T> message, TransactionContext context) throws Exception;
}
```

**示例：**
```java
@Component
@StreamMQTransactionConsumer(transactionGroup = "default-tx-group")
public class OrderTransactionChecker implements TransactionChecker<String> {
    @Override
    public LocalTransactionState check(Message<String> message, TransactionContext context) {
        return checkLocalTransaction(context.getTransactionId());
    }
}
```

---

## LocalTransactionState

事务状态枚举。

| 状态 | 说明 |
|------|------|
| `COMMIT_MESSAGE` | 提交半消息 |
| `ROLLBACK_MESSAGE` | 回滚半消息 |
| `UNKNOW` | 状态未知，等待回查 |

---

## @EnableStreamMQ

启用 StreamMQ 注解。

```java
@SpringBootApplication
@EnableStreamMQ(
    mode = "standard",
    tracingEnabled = false,
    scanBasePackages = {"com.example"}
)
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
```

### 属性列表

| 属性 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `mode` | String | standard | 启用模式 |
| `tracingEnabled` | boolean | false | 是否启用追踪 |
| `scanBasePackages` | String[] | - | 扫描包路径 |