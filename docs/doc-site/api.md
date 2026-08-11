# API 参考

StreamMQ 提供类 RocketMQ 的 API 体验。本文档覆盖所有公共接口与注解，包含方法签名、参数说明、返回类型与代码示例。

> **包根**：`io.github.streammq.core`
> **版本**：`0.1.0` ｜ Java 21 ｜ Spring Boot 3.3.5

---

## 目录

- [注解](#注解)
  - [@EnableStreamMQ](#enablestreammq)
  - [@StreamMQConsumer](#streammqconsumer)
  - [@StreamMQTransactionConsumer](#streammqtransactionconsumer)
  - [@StreamMQDlqConsumer](#streammqdlqconsumer)
- [消息 API](#消息-api)
  - [StreamMessageTemplate](#streammessagetemplate)
  - [MessageBuilder](#messagebuilder)
  - [Message](#message)
  - [BatchMessage](#batchmessage)
  - [MessageId](#messageid)
  - [SendResult / SendStatus](#sendresult--sendstatus)
- [消费 API](#消费-api)
  - [StreamMessageConcurrentlyConsumer](#streammessageconcurrentlyconsumer)
  - [StreamMessageOrderlyConsumer](#streammessageorderlyconsumer)
  - [DlqMessageConsumer](#dlqmessageconsumer)
  - [ConsumeAction / OrderlyAction](#consumeaction--orderlyaction)
  - [ConsumeContext / ConsumeOrderlyContext](#consumecontext--consumeorderlycontext)
- [事务 API](#事务-api)
  - [TransactionCallback](#transactioncallback)
  - [TransactionChecker](#transactionchecker)
  - [TransactionContext](#transactioncontext)
  - [LocalTransactionState](#localtransactionstate)
- [枚举](#枚举)
  - [DelayLevel](#delaylevel)
  - [MessageModel / ConsumeMode](#messagemodel--consumemode)
  - [SelectorType](#selectortype)
- [SPI 扩展接口](#spi-扩展接口)

---

## 注解

### @EnableStreamMQ

标注在 Spring Boot 启动类上，显式声明启用 StreamMQ。当 `streammq-spring-boot-starter` 在 classpath 时，Spring Boot 通过 `META-INF/spring/AutoConfiguration.imports` 自动装配 `StreamMQAutoConfiguration`，本注解作为显式标记与配置属性载体。

```java
@SpringBootApplication
@EnableStreamMQ
public class OrderApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }
}
```

#### 属性

| 属性 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `mode` | String | `"STANDARD"` | 启用模式，`STANDARD`（标准）/ `LITE`（轻量，不启用 Actuator 指标） |
| `tracingEnabled` | boolean | `false` | 全局追踪开关，true 时启用 Slf4jTraceCollector 与追踪拦截器 |
| `scanBasePackages` | String[] | `{}` | 自定义扫描包路径，默认使用启动类所在包 |

---

### @StreamMQConsumer

消费者注解（类级），标注在 `StreamMessageConcurrentlyConsumer` / `StreamMessageOrderlyConsumer` 实现类上。本注解为统一入口，通过 `messageModel()` 区分并发 / 顺序消费，通过 `dlqMode()` 标识 DLQ 消费者。

```java
// 并发消费
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-cg")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<Order> {
    @Override
    public ConsumeAction onMessage(Message<Order> message, ConsumeContext context) {
        processOrder(message.getBody());
        return ConsumeAction.SUCCESS;
    }
}

// 顺序消费
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-cg",
                  messageModel = MessageModel.ORDERLY, shardCount = 8)
public class OrderOrderlyConsumer implements StreamMessageOrderlyConsumer<Order> {
    @Override
    public OrderlyAction onMessage(Message<Order> message, ConsumeOrderlyContext context) {
        processOrder(message.getBody());
        return OrderlyAction.SUCCESS;
    }
}
```

#### 属性

| 属性 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `topic` | String | — | **主题（必填）** |
| `consumerGroup` | String | — | **消费者组名（必填）**；DLQ 模式下表示原始消费者组，用于构造 DLQ Stream Key |
| `consumeMode` | ConsumeMode | `CLUSTERING` | 消费模式：`CLUSTERING` / `BROADCASTING` |
| `messageModel` | MessageModel | `CONCURRENT` | 消息模型：`CONCURRENT` / `ORDERLY` |
| `consumeThreadMin` | int | `1` | 最小消费线程数 |
| `consumeThreadMax` | int | `64` | 最大消费线程数 |
| `maxReconsumeTimes` | int | `16` | 最大重试次数 |
| `consumeTimeout` | long | `30000` | 单条消息消费超时（毫秒） |
| `selectorExpression` | String | `"*"` | Tag 过滤表达式（SQL92 风格子集），如 `"tag1 \|\| tag2"` |
| `selectorType` | SelectorType | `TAG` | 过滤类型：`TAG` / `SQL92` |
| `pullBatchSize` | int | `32` | 单次拉取批量大小 |
| `pullInterval` | long | `0` | 拉取间隔（毫秒，0 = 不间隔） |
| `shardCount` | int | `4` | 顺序消费分片数，仅 `ORDERLY` 生效 |
| `suspendCurrentQueueTimeMillis` | long | `1000` | 顺序消费挂起时长（毫秒） |
| `dlqMode` | boolean | `false` | 是否为 DLQ 消费者 |
| `consumerFilter` | Class<? extends ConsumerFilter>[] | `{}` | Per-consumer 过滤器类（从 Spring 容器获取实例） |
| `retryPolicy` | Class<? extends RetryPolicy> | `RetryPolicy.class` | Per-consumer 重试策略（默认使用全局） |
| `rebalanceStrategy` | Class<? extends RebalanceStrategy> | `RebalanceStrategy.class` | Per-consumer 重平衡策略 |
| `messageConverter` | Class<? extends MessageConverter> | `MessageConverter.class` | Per-consumer 消息转换器 |
| `serializer` | Class<? extends MessageSerializer> | `MessageSerializer.class` | Per-consumer 序列化器 |
| `dlqFailureStrategy` | Class<? extends DlqFailureStrategy> | `DlqFailureStrategy.class` | DLQ 失败策略（仅 `dlqMode=true` 生效） |
| `streamMaxLen` | int | `0` | Stream 最大长度（0 = 不限制） |
| `retryStreamMaxLen` | int | `0` | retry Stream 最大长度（0 = 不限制，仅并发消费生效） |
| `enableMsgTrace` | boolean | `false` | 是否启用消息追踪（覆盖全局） |
| `enable` | boolean | `true` | 是否启用消费，false 仅注册但不启动 |
| `namespace` | String | `""` | 命名空间（默认使用全局） |
| `consumerName` | String | `""` | 消费者实例名（默认自动生成） |

> **过滤器执行顺序**：先执行 `selectorExpression` 内置过滤器（order = -1），再按 `ConsumerFilter.order()` 升序执行自定义过滤器。

---

### @StreamMQTransactionConsumer

事务回查消费者注解（类级），标注在 `TransactionChecker` 实现类上。框架将按注解参数注册事务回查任务，定时扫描超时未确认的半消息并调用回查。

```java
@Component
@StreamMQTransactionConsumer(transactionGroup = "order-tx-group")
public class OrderTransactionChecker implements TransactionChecker<Order> {
    @Override
    public LocalTransactionState check(Message<Order> message, TransactionContext context) {
        return orderService.isTransactionCommitted(context.getTransactionId())
            ? LocalTransactionState.COMMIT_MESSAGE
            : LocalTransactionState.ROLLBACK_MESSAGE;
    }
}
```

#### 属性

| 属性 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `transactionGroup` | String | — | **事务组名（必填）**，与发送端 `executeInTransaction` 对应 |
| `checkTimeout` | long | `60000` | 单次回查超时（毫秒） |
| `checkIntervalMillis` | long | `60000` | 回查间隔（毫秒） |
| `maxCheckTimes` | int | `15` | 最大回查次数 |
| `batchSize` | int | `100` | 单次扫描批量 |
| `namespace` | String | `""` | 命名空间（默认使用全局） |

---

### @StreamMQDlqConsumer

死信队列消费者注解（类级），标注在 `DlqMessageConsumer`（通常继承 `AbstractDlqMessageConsumer`）实现类上。与 `@StreamMQConsumer(dlqMode=true)` 的区别：本注解专用于死信消费，提供丰富的 DLQ 失败处理配置，`onDlqMessage` 返回 void，失败时进入 `DlqFailureStrategy` 决策。

```java
// 默认策略（LogAndDrop）：DLQ 失败仅记录 ERROR 日志后丢弃
@Component
@StreamMQDlqConsumer(consumerGroup = "order-cg")
public class OrderDlqConsumer extends AbstractDlqMessageConsumer<Order> {
    @Override
    public void onDlqMessage(Message<Order> msg, ConsumeContext ctx) {
        notifyOps("DLQ message from topic=" + ctx.topic() + ": " + msg.getBody());
    }
}

// 有限重试策略：DLQ 失败最多重试 3 次，每次间隔 10s
@Component
@StreamMQDlqConsumer(consumerGroup = "order-cg",
    failureStrategy = "io.github.streammq.adapter.redisson.dlq.LimitedRetryDlqFailureStrategy",
    maxDlqRetryAttempts = 3, dlqRetryDelayMs = 10_000)
public class RetryDlqConsumer extends AbstractDlqMessageConsumer<Order> { ... }
```

#### 属性

| 属性 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `consumerGroup` | String | — | **原始消费者组名（必填）**，用于构造 DLQ Stream Key `streammq:{ns}:dlq:{consumerGroup}` |
| `failureStrategy` | String | `LogAndDropDlqFailureStrategy` | DLQ 失败策略实现类全限定名 |
| `maxDlqRetryAttempts` | int | `3` | DLQ 消费失败后最大重试次数 |
| `dlqRetryDelayMs` | long | `10000` | DLQ 消费重试延迟（毫秒） |
| `dlqRetryBackoffMultiplier` | double | `1.0` | 重试退避倍数（1.0 = 固定延迟） |
| `dlqRetryMaxDelayMs` | long | `300000` | 重试延迟上限（毫秒，5 分钟） |
| `secondaryDlqEnabled` | boolean | `false` | 是否启用二级死信队列 |
| `secondaryDlqKeyPrefix` | String | `"dlq2"` | 二级死信 Stream Key 前缀段 |
| `dlqAlertThreshold` | int | `1` | 告警阈值（DLQ 失败超过此次数后触发告警） |
| `enable` | boolean | `true` | 是否启用消费 |
| `namespace` | String | `""` | 命名空间 |

---

## 消息 API

### StreamMessageTemplate

StreamMQ 消息模板（核心生产者 API），对齐 RocketMQ `RocketMQTemplate`。实现类位于 `streammq-redisson` 模块（`DefaultStreamMessageTemplate`）。

**泛型设计**：泛型参数 `<T>` 声明在方法级别而非类级别，一个 Template 单例可发送不同 body 类型的消息。

所有 `syncSend` / `asyncSend` 调用前后均经过 `ProducerInterceptor` 链。

#### syncSend（同步发送）

```java
<T> SendResult syncSend(Message<T> message);
<T> SendResult syncSend(Message<T> message, long timeoutMillis);
<T> SendResult syncSend(Message<T> message, long timeoutMillis, int retryTimes);
```

| 参数 | 说明 |
|------|------|
| `message` | 消息对象 |
| `timeoutMillis` | 超时毫秒数，默认 `3000` |
| `retryTimes` | 重试次数，默认 `2`（0 = 不重试） |

```java
Message<String> message = MessageBuilder.<String>withTopic("order-topic")
        .tag("created").body("content").build();

// 默认超时与重试
SendResult result = template.syncSend(message);

// 指定超时
SendResult r2 = template.syncSend(message, 5000L);

// 指定超时与重试
SendResult r3 = template.syncSend(message, 5000L, 3);
```

#### asyncSend（异步发送）

```java
<T> CompletableFuture<SendResult> asyncSend(Message<T> message);
<T> void asyncSend(Message<T> message, SendCallback callback);
<T> void asyncSend(Message<T> message, SendCallback callback, long timeoutMillis);
```

```java
// CompletableFuture 方式
template.asyncSend(message).whenComplete((result, ex) -> {
    if (ex == null) {
        System.out.println("发送成功: " + result.getMessageId());
    } else {
        ex.printStackTrace();
    }
});

// SendCallback 方式
template.asyncSend(message, new SendCallback() {
    @Override
    public void onSuccess(SendResult result) {
        log.info("Send success: {}", result.getMessageId());
    }
    @Override
    public void onException(Throwable ex) {
        log.error("Send failed", ex);
    }
}, 5000L);
```

#### sendOneway（单向发送）

不等待响应，不抛异常，性能最高。

```java
<T> void sendOneway(Message<T> message);
```

```java
template.sendOneway(message);
```

#### syncSendBatch（批量发送）

基于 Redisson RBatch（Pipeline）一次性 XADD 多条消息，减少 RTT。所有消息必须同 Topic。

```java
<T> List<SendResult> syncSendBatch(BatchMessage<T> batch);
```

```java
BatchMessage<String> batch = BatchMessage.<String>withTopic("order-topic")
        .add(msg1).add(msg2).add(msg3).build();
List<SendResult> results = template.syncSendBatch(batch);
```

#### executeInTransaction（事务消息）

继承自 `TransactionExecutor` 接口。流程：发送半消息 → 执行本地事务 → 根据返回值 COMMIT/ROLLBACK/UNKNOW。

```java
<T> SendResult executeInTransaction(Message<T> message, TransactionCallback<T> callback);
```

```java
SendResult result = template.executeInTransaction(message, (msg, ctx) -> {
    try {
        orderService.createOrder(msg.getBody());
        return LocalTransactionState.COMMIT_MESSAGE;
    } catch (Exception ex) {
        return LocalTransactionState.ROLLBACK_MESSAGE;
    }
});
// 仅 COMMIT_MESSAGE 时返回 SEND_OK，其他状态为 SEND_FAILED
```

#### 拦截器 / 过滤器管理

| 方法 | 说明 |
|------|------|
| `getMessageConverter()` | 返回消息转换器 |
| `getProducerInterceptors()` | 返回生产者拦截器链（不可修改） |
| `setProducerInterceptors(List)` | 设置拦截器链（覆盖） |
| `addProducerInterceptor(ProducerInterceptor)` | 添加单个拦截器 |
| `addProducerFilter(ProducerFilter)` | 添加单个生产者过滤器 |

#### 默认常量

| 常量 | 值 | 说明 |
|------|----|------|
| `DEFAULT_SEND_TIMEOUT_MILLIS` | `3000L` | 默认发送超时 |
| `DEFAULT_SYNC_RETRY_TIMES` | `2` | 默认同步重试次数 |
| `DEFAULT_ASYNC_RETRY_TIMES` | `0` | 默认异步重试次数（不重试） |

---

### MessageBuilder

消息流式构造器，对齐 RocketMQ `MessageBuilder` 风格。

```java
Message<String> message = MessageBuilder.<String>withTopic("order-topic")
        .tag("created")
        .keys("order-123")
        .shardingKey("user-456")
        .body("content")
        .delayLevel(DelayLevel.MINUTE_5)
        .userProperty("traceId", "t-001")
        .build();
```

#### 静态工厂方法

| 方法 | 说明 |
|------|------|
| `create()` | 创建新的 Builder |
| `withTopic(String)` | 创建指定 Topic 的 Builder |
| `withPayload(T)` | 创建指定 body 的 Builder |

#### 实例方法

| 方法 | 说明 | 必填 |
|------|------|------|
| `topic(String)` | 设置 Topic | 是 |
| `tag(String)` | 设置 Tag | 否 |
| `withTag(String)` | `tag()` 别名 | 否 |
| `keys(String)` | 设置业务键 | 否 |
| `withKeys(String)` | `keys()` 别名 | 否 |
| `shardingKey(String)` | 设置分片键 | 否 |
| `withShardingKey(String)` | `shardingKey()` 别名 | 否 |
| `body(T)` | 设置消息体 | 是 |
| `withBody(T)` | `body()` 别名 | 是 |
| `property(String, String)` | 添加系统属性 | 否 |
| `properties(Map)` | 批量设置系统属性 | 否 |
| `userProperty(String, String)` | 添加用户属性 | 否 |
| `delayLevel(DelayLevel)` | 设置延时级别 | 否 |
| `delayTimeMillis(long)` | 设置任意延时毫秒（必须 > 0，优先级高于 delayLevel） | 否 |
| `bornTimestamp(long)` | 设置出生时间戳（一般框架自动填入） | 否 |
| `bornHost(String)` | 设置出生主机 | 否 |
| `transactionId(String)` | 设置事务 ID（仅事务消息） | 否 |
| `build()` | 构造 Message（topic / body 不可为 null） | — |

> 每个 `with*` 方法均有对应无前缀实例方法，二者等价。

---

### Message

消息载体，对应一条 Redis Stream Entry。通过 `MessageBuilder` 构造，构造后字段不可变。

#### 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `topic` | String | 主题（必填） |
| `tag` | String | 标签，用于过滤 |
| `keys` | String | 业务键，用于幂等 / 查询 |
| `shardingKey` | String | 分片键，用于顺序消息 |
| `body` | T | 消息体（必填） |
| `delayLevel` | DelayLevel | 延时级别 |
| `delayTimeMillis` | Long | 任意延时毫秒 |
| `properties` | Map<String,String> | 系统属性（不可修改视图） |
| `userProperties` | Map<String,String> | 用户属性（不可修改视图） |
| `messageId` | MessageId | 消息 ID（框架回填） |
| `bornTimestamp` | long | 出生时间戳（毫秒） |
| `bornHost` | String | 出生主机 |
| `reconsumeTimes` | int | 已重试消费次数 |
| `transactionId` | String | 事务 ID |

#### 常用方法

| 方法 | 说明 |
|------|------|
| `getProperties()` | 返回系统属性（不可修改视图） |
| `putProperty(key, value)` | 添加系统属性 |
| `getUserProperties()` | 返回用户属性（不可修改视图） |
| `putUserProperty(key, value)` | 添加用户属性 |
| `isDelayMessage()` | 是否为延时消息 |
| `isTransactionMessage()` | 是否为事务消息 |

---

### BatchMessage

批量消息包装类，要求所有消息同 Topic。底层通过 Redisson RBatch Pipeline 一次性 XADD 多条消息。

```java
BatchMessage<String> batch = BatchMessage.<String>withTopic("order-topic")
        .add(msg1)
        .add(msg2)
        .addAll(msgList)
        .build();
List<SendResult> results = template.syncSendBatch(batch);
```

#### Builder 方法

| 方法 | 说明 |
|------|------|
| `withTopic(String)` | 创建指定 Topic 的 Builder |
| `add(Message<T>)` | 添加一条消息（Topic 必须一致） |
| `addAll(List<Message<T>>)` | 批量添加消息 |
| `build()` | 构造批量消息（列表不能为空） |

#### 实例方法

| 方法 | 说明 |
|------|------|
| `getTopic()` | 返回共享 Topic |
| `getMessages()` | 返回不可修改的消息列表 |
| `size()` | 返回消息数量 |
| `isEmpty()` | 是否为空 |

---

### MessageId

消息 ID 包装类，对应 Redis Stream Entry ID，格式 `{timestamp}-{sequence}`。不可变，线程安全，实现 `Comparable<MessageId>`。

| 方法 | 返回类型 | 说明 |
|------|----------|------|
| `getStreamEntryId()` | String | 原始 Stream Entry ID |
| `getTimestamp()` | long | 时间戳部分（毫秒） |
| `getSequence()` | long | 序列号部分 |
| `compareTo(MessageId)` | int | 按时间戳 + 序列号比较 |

---

### SendResult / SendStatus

#### SendResult

由 `syncSend` / `syncSendBatch` 返回，封装发送结果。

| 字段 | 类型 | 说明 |
|------|------|------|
| `messageId` | MessageId | 消息 ID |
| `topic` | String | Topic |
| `tag` | String | Tag（可能为 null） |
| `sendStatus` | SendStatus | 发送状态 |
| `bornTimestamp` | long | 出生时间戳 |
| `regionId` | String | Region ID（多机房场景） |
| `errorMessage` | String | 错误信息（仅 SEND_FAILED 时非空） |

| 方法 | 返回类型 | 说明 |
|------|----------|------|
| `isSuccess()` | boolean | 状态是否为 `SEND_OK` |

#### SendStatus

| 状态 | 说明 |
|------|------|
| `SEND_OK` | 发送成功 |
| `SEND_FAILED` | 发送失败（异常、超时等） |
| `SLAVE_NOT_READY` | 从节点未就绪（多副本场景） |
| `FLUSH_DISK_TIMEOUT` | 刷盘超时（预留） |

---

## 消费 API

### StreamMessageConcurrentlyConsumer

并发消费接口，多线程并发消费，不保证顺序。

```java
public interface StreamMessageConcurrentlyConsumer<T> {
    ConsumeAction onMessage(Message<T> message, ConsumeContext context) throws Exception;
}
```

```java
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {
    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        System.out.println("重试次数: " + context.reconsumeTimes());
        process(message.getBody());
        return ConsumeAction.SUCCESS;
    }
}
```

---

### StreamMessageOrderlyConsumer

顺序消费接口，按 shardingKey 路由到固定 shard，shard 内单线程串行消费。

```java
public interface StreamMessageOrderlyConsumer<T> {
    OrderlyAction onMessage(Message<T> message, ConsumeOrderlyContext context) throws Exception;
}
```

```java
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group",
                  messageModel = MessageModel.ORDERLY, shardCount = 8)
public class OrderlyConsumer implements StreamMessageOrderlyConsumer<String> {
    @Override
    public OrderlyAction onMessage(Message<String> message, ConsumeOrderlyContext context) {
        System.out.println("shardId: " + context.shardId() + ", shardingKey: " + context.shardingKey());
        process(message.getBody());
        return OrderlyAction.SUCCESS;
    }
}
```

---

### DlqMessageConsumer

死信队列消费回调接口，专用于处理 DLQ Stream 中的死信消息。返回 void（非 ConsumeAction），失败时由 `DlqFailureStrategy` 决策。

```java
@FunctionalInterface
public interface DlqMessageConsumer<T> {
    void onDlqMessage(Message<T> message, ConsumeContext context) throws Exception;
}
```

```java
@Component
@StreamMQDlqConsumer(consumerGroup = "order-cg", maxDlqRetryAttempts = 3)
public class OrderDlqConsumer extends AbstractDlqMessageConsumer<Order> {
    @Override
    public void onDlqMessage(Message<Order> msg, ConsumeContext ctx) {
        notifyOps("DLQ from topic=" + ctx.topic() + ": " + msg.getBody());
    }
}
```

> `AbstractDlqMessageConsumer` 提供 DLQ 消费的基础骨架实现，业务通常继承它。

---

### ConsumeAction / OrderlyAction

#### ConsumeAction

并发消费返回值（类，非枚举）。框架以返回值为唯一标准。

| 成员 | 类型 | 说明 |
|------|------|------|
| `ConsumeAction.SUCCESS` | 静态常量 | 消费成功，自动 ACK |
| `ConsumeAction.RECONSUME_LATER` | 静态常量 | 消费失败，按 RetryPolicy 重试 |
| `ConsumeAction.defer(Duration)` | 静态工厂 | 消费失败，按指定延迟重试（覆盖 RetryPolicy） |

| 方法 | 返回类型 | 说明 |
|------|----------|------|
| `type()` | `ConsumeAction.Type` | 动作类型（SUCCESS / RECONSUME_LATER / DEFER） |
| `name()` | String | 类型名称 |
| `isSuccess()` | boolean | 是否成功 |
| `isReconsumeLater()` | boolean | 是否按策略重试 |
| `isDefer()` | boolean | 是否延迟重试 |
| `getDeferDelay()` | Duration | DEFER 延迟时长（非 DEFER 返回 null） |

```java
return ConsumeAction.SUCCESS;
return ConsumeAction.RECONSUME_LATER;
return ConsumeAction.defer(Duration.ofSeconds(30));
```

#### OrderlyAction

顺序消费返回值枚举。

| 枚举值 | 说明 |
|--------|------|
| `SUCCESS` | 消费成功，自动 ACK，下一条继续 |
| `SUSPEND_CURRENT_QUEUE_A_MOMENT` | 暂停当前 shard 一小段时间后重新消费该消息 |

---

### ConsumeContext / ConsumeOrderlyContext

#### ConsumeContext

消费上下文，封装消费过程中的运行时元信息。仅提供元数据读取，不提供手动 ACK/nack/defer 调用。

| 方法 | 返回类型 | 说明 |
|------|----------|------|
| `topic()` | String | 当前消息所属 Topic |
| `consumerGroup()` | String | 当前消费者组名 |
| `consumerName()` | String | 当前 Consumer 实例名 |
| `reconsumeTimes()` | int | 已重试消费次数（首次为 0） |
| `bornTimestamp()` | long | 消息出生时间戳（毫秒） |
| `bornHost()` | String | 消息出生主机（host:port） |
| `messageTrack()` | Map<String,String> | 消息追踪信息（traceId、spanId 等） |
| `ext(String key)` | String | 扩展属性 |

#### ConsumeOrderlyContext

继承 `ConsumeContext`，增加分片信息（仅顺序消费场景）。

| 方法 | 返回类型 | 说明 |
|------|----------|------|
| `shardingKey()` | String | 当前消息的分片键 |
| `shardId()` | int | 当前消息所属 shard ID（0-based） |
| `queueOffset()` | MessageId | 当前 shard 在本消费者的消费位点 |
| `backlog()` | long | 当前 shard 的最大堆积量 |

---

## 事务 API

### TransactionCallback

事务消息本地事务执行回调（`@FunctionalInterface`），对齐 Spring `TransactionTemplate` 风格。

```java
@FunctionalInterface
public interface TransactionCallback<T> {
    LocalTransactionState execute(Message<T> message, TransactionContext context) throws Exception;
}
```

| 返回值 | 说明 |
|--------|------|
| `COMMIT_MESSAGE` | 提交半消息 |
| `ROLLBACK_MESSAGE` | 回滚半消息 |
| `UNKNOW` | 状态未知，等待回查 |

> 抛出异常时，框架将其视为 `UNKNOW`。

```java
TransactionCallback<Order> callback = (message, ctx) -> {
    try {
        orderService.createOrder(message.getBody());
        return LocalTransactionState.COMMIT_MESSAGE;
    } catch (Exception ex) {
        return LocalTransactionState.ROLLBACK_MESSAGE;
    }
};
SendResult result = template.executeInTransaction(msg, callback);
```

---

### TransactionChecker

事务回查接口（`@FunctionalInterface`）。实现类标注 `@StreamMQTransactionConsumer` 即注册为事务回查器。

```java
@FunctionalInterface
public interface TransactionChecker<T> {
    LocalTransactionState check(Message<T> message, TransactionContext context) throws Exception;
}
```

```java
@Component
@StreamMQTransactionConsumer(transactionGroup = "order-tx-group")
public class OrderTransactionChecker implements TransactionChecker<Order> {
    @Override
    public LocalTransactionState check(Message<Order> message, TransactionContext context) {
        return checkLocalTransaction(context.getTransactionId());
    }
}
```

---

### TransactionContext

事务上下文，封装事务消息执行过程中的运行时信息。

| 字段 / 方法 | 类型 | 说明 |
|------|------|------|
| `getTransactionId()` | String | 事务 ID（与半消息关联） |
| `getTransactionGroup()` | String | 事务组名 |
| `getProducerGroup()` | String | 生产者组名 |
| `getBornTimestamp()` | long | 半消息发送时间戳（毫秒） |
| `getExtAttributes()` | Map<String,String> | 扩展属性（不可修改） |
| `ext(String key)` | String | 获取扩展属性 |

---

### LocalTransactionState

本地事务状态枚举。

| 状态 | 说明 |
|------|------|
| `COMMIT_MESSAGE` | 本地事务执行成功，半消息提交，对消费者可见 |
| `ROLLBACK_MESSAGE` | 本地事务执行失败，半消息回滚并删除 |
| `UNKNOW` | 状态未知，等待回查；连续 `max-check-times` 次仍 UNKNOW，框架强制 ROLLBACK |

---

## 枚举

### DelayLevel

18 级固定延时（对齐 RocketMQ）。

| 级别 | 延时 | 级别 | 延时 | 级别 | 延时 |
|------|------|------|------|------|------|
| `SECOND_1` | 1s | `MINUTE_3` | 3m | `MINUTE_20` | 20m |
| `SECOND_5` | 5s | `MINUTE_4` | 4m | `MINUTE_30` | 30m |
| `SECOND_10` | 10s | `MINUTE_5` | 5m | `HOUR_1` | 1h |
| `SECOND_30` | 30s | `MINUTE_6` | 6m | `HOUR_2` | 2h |
| `MINUTE_1` | 1m | `MINUTE_7` | 7m | | |
| `MINUTE_2` | 2m | `MINUTE_8` | 8m | | |
| | | `MINUTE_9` | 9m | | |
| | | `MINUTE_10` | 10m | | |

| 方法 | 返回类型 | 说明 |
|------|----------|------|
| `getDuration()` | Duration | 该级别对应的 Duration |
| `toMillis()` | long | 毫秒数 |
| `toSeconds()` | long | 秒数 |
| `ofIndex(int)` | DelayLevel | 按下标获取（0-based） |
| `closestAbove(long millis)` | DelayLevel | 查找最接近的级别（向上取整，超上限返回 HOUR_2） |

---

### MessageModel / ConsumeMode

#### MessageModel（消息模型）

| 枚举值 | 说明 |
|--------|------|
| `CONCURRENT` | 并发消息（默认），多线程并发消费，不保证顺序 |
| `ORDERLY` | 顺序消息，按 shardingKey 路由到固定 shard，shard 内单线程串行 |

#### ConsumeMode（消费模式）

| 枚举值 | 说明 |
|--------|------|
| `CLUSTERING` | 集群消费（默认），同一 ConsumerGroup 内每条消息仅被一个 Consumer 消费 |
| `BROADCASTING` | 广播消费，每条消息被所有订阅的 Consumer 各处理一次 |

---

### SelectorType

| 枚举值 | 说明 |
|--------|------|
| `TAG` | 基于 Tag 过滤（默认） |
| `SQL92` | 基于 SQL92 表达式过滤 |

---

## SPI 扩展接口

StreamMQ 提供 12 个 SPI 扩展点，业务方可通过实现接口 + Spring Bean 注入或注解指定实现类来扩展行为。

| # | SPI 接口 | 默认 / 内置实现 | 用途 |
|---|----------|-----------------|------|
| 1 | `MessageSerializer` | `JacksonJsonSerializer`（默认）、`JdkSerializer` | body 与 byte[] 双向转换 |
| 2 | `MessageConverter` | `DefaultMessageConverter`、`CompactMessageConverter`、`PassThroughMessageConverter` | Message 与 Stream Entry 字段双向转换 |
| 3 | `ProducerFilter` | 过滤器链 | 发送前过滤，返回 false 阻止发送 |
| 4 | `ConsumerFilter` | 过滤器链（全局 + per-consumer） | 消费前过滤，返回 false 跳过（自动 ACK） |
| 5 | `ProducerInterceptor` | 拦截器链 | 发送前后切面（traceId、审计、限流） |
| 6 | `ConsumerInterceptor` | 拦截器链 | 消费前后切面（解密、解压、限流） |
| 7 | `RetryPolicy` | `FixedArrayRetryPolicy`、`ExponentialBackoffRetryPolicy` | 消费失败重试间隔与停止策略 |
| 8 | `RebalanceStrategy` | `ConsistentHashRebalanceStrategy`（默认）、`AverageRebalanceStrategy` | 分片到 Consumer 分配算法 |
| 9 | `TraceCollector` | `Slf4jTraceCollector` | 追踪埋点上报（对接 OTel/Zipkin/SkyWalking） |
| 10 | `CompressionCodec` | `GzipCompressionCodec`（默认）、`Lz4CompressionCodec` | body 压缩编解码 |
| 11 | `ManagementAuthenticator` | `BasicAuth`、`Token`、`AllowAll`、`DenyAll` | 运维管理端点鉴权 |
| 12 | `DlqFailureStrategy` | `LogAndDropDlqFailureStrategy`（默认）、`LimitedRetryDlqFailureStrategy`、`SecondaryDlqFailureStrategy` | DLQ 消费失败处理 |

### 1. MessageSerializer

负责 Message body 与 `byte[]` 的双向转换。元信息始终为 String，不参与序列化。

```java
public interface MessageSerializer<T> {
    byte[] serialize(T object, Class<T> type) throws SerializationException;
    <R> R deserialize(byte[] bytes, Class<R> type) throws SerializationException;
    default String name() { return getClass().getSimpleName(); }
}
```

### 2. MessageConverter

连接 `Message` 与 Redis Stream Entry 字段 Map 的双向转换，仅做字段映射，不负责 body 序列化。

```java
public interface MessageConverter {
    Map<String, String> toStreamFields(Message<?> message);
    <T> Message<T> fromStreamFields(Map<String, String> fields, Class<T> targetType);
    default String name() { return getClass().getSimpleName(); }
}
```

### 3. ProducerFilter

发送前过滤，多过滤器按 `order()` 升序执行，任一返回 false 则阻止发送。

```java
public interface ProducerFilter {
    boolean accept(Message<?> message);
    default String name() { return getClass().getSimpleName(); }
    default int order() { return 0; }
}
```

### 4. ConsumerFilter

消费前过滤，返回 false 则跳过（自动 ACK）。支持全局 + per-consumer 两个维度。

```java
public interface ConsumerFilter {
    boolean accept(Message<?> message);
    default String name() { return getClass().getSimpleName(); }
    default int order() { return 0; }   // selectorExpression 内置过滤器默认 order = -1
}
```

### 5. ProducerInterceptor

发送前后拦截，对齐 RocketMQ `ProducerInterceptor`。

```java
public interface ProducerInterceptor {
    boolean beforeSend(Message<?> message);                          // false 中止发送
    void afterSend(Message<?> message, SendResult result);
    default void onException(Message<?> message, Exception ex, InvokeTiming timing) {}
    default String name() { return getClass().getSimpleName(); }
    default int order() { return 0; }
}
```

### 6. ConsumerInterceptor

消费前后拦截。`beforeConsume` 返回 false 中止本次消费（视为 RECONSUME_LATER）。

```java
public interface ConsumerInterceptor {
    boolean beforeConsume(Message<?> message, ConsumeContext context);
    void afterConsume(Message<?> message, ConsumeAction action, ConsumeContext context);
    default void onException(Message<?> message, Exception ex, InvokeTiming timing, ConsumeContext context) {}
    default String name() { return getClass().getSimpleName(); }
    default int order() { return 0; }
}
```

### 7. RetryPolicy

控制消费失败后的重试间隔与是否停止。

```java
public interface RetryPolicy {
    Duration nextRetryDelay(int reconsumeTimes, Message<?> message);  // null = 不再重试
    boolean shouldStopRetry(int reconsumeTimes, Message<?> message);  // true = 进入 DLQ
    default String name() { return getClass().getSimpleName(); }
}
```

- `FixedArrayRetryPolicy`：对齐 RocketMQ 16 级固定数组 `[10s,30s,1m,...,2h]`
- `ExponentialBackoffRetryPolicy`：指数退避（initial=1s, multiplier=2.0, max=2h）

### 8. RebalanceStrategy

控制 ConsumerGroup 内分片到 Consumer 的分配算法。

```java
public interface RebalanceStrategy {
    Map<Integer, String> assign(List<Integer> shards, List<String> consumers, String consumerGroup);
    default String name() { return getClass().getSimpleName(); }
}
```

- `ConsistentHashRebalanceStrategy`（默认）：一致性哈希，减少 Rebalance 时分片迁移
- `AverageRebalanceStrategy`：平均分配，精确均衡

### 9. TraceCollector

追踪收集器，对接 OpenTelemetry / Zipkin / SkyWalking 等 APM 系统。

```java
public interface TraceCollector {
    void recordSend(SendTraceContext context);
    void recordConsume(ConsumeTraceContext context);
    default boolean isEnabled() { return true; }
    default String name() { return getClass().getSimpleName(); }
}
```

`SendTraceContext` / `ConsumeTraceContext` 为 record，封装 topic、tag、messageId、producerGroup/consumerGroup、success、durationMillis、traceId、attributes 等字段。

### 10. CompressionCodec

消息体压缩编解码。当 `ProducerConfig.compressThreshold > 0` 且 body 字节数超过阈值时触发。

```java
public interface CompressionCodec {
    byte[] compress(byte[] data);
    byte[] decompress(byte[] data);
    String name();   // 如 "gzip"、"lz4"
}
```

- `GzipCompressionCodec`（默认，mandatory）
- `Lz4CompressionCodec`（optional，需引入 LZ4 依赖）

### 11. ManagementAuthenticator

运维管理端点鉴权，业务方可接入企业鉴权系统（OAuth2 / SSO / LDAP）。

```java
public interface ManagementAuthenticator {
    boolean authenticate(String username, String password, String resource);
    default String name() { return getClass().getSimpleName(); }
}
```

内置实现：`BasicAuth`、`Token`、`AllowAll`、`DenyAll`。

### 12. DlqFailureStrategy

DLQ 消费失败处理策略。返回 `DlqFailureDecision`：`drop()` / `retry(Duration)` / `secondaryDlq()`。

```java
public interface DlqFailureStrategy {
    DlqFailureDecision decide(Message<?> message, DlqFailureContext context);
    default String name() { return getClass().getSimpleName(); }
}
```

内置策略：
- `LogAndDropDlqFailureStrategy`（默认）：始终丢弃，仅记录日志
- `LimitedRetryDlqFailureStrategy`：有限次重试后丢弃
- `SecondaryDlqFailureStrategy`：有限次重试后转投二级死信

---

## 补充说明

### 异常体系

| 异常 | 说明 |
|------|------|
| `StreamMQException` | 框架基础异常 |
| `StreamMQClientException` | 客户端异常 |
| `StreamMQBrokerException` | Broker（Redis）异常，如 XACK 失败 |
| `ProducerTimeoutException` | 发送超时 |
| `SerializationException` | 序列化 / 反序列化失败 |
| `TransactionException` | 事务消息异常（如半消息发送失败） |
| `ConsumerInterruptedException` | 消费中断 |

### InvokeTiming

拦截器异常触发时机枚举：`BEFORE`（前）、`EXECUTING`（执行中）、`AFTER`（后）。
