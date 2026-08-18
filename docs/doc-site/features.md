# 核心特性

本文档详细说明 StreamMQ 的全部特性，包含代码示例与配置说明。

---

## 目录

- [注解驱动消费](#注解驱动消费)
- [StreamMessageTemplate 编程模型](#streammessagetemplate-编程模型)
- [事务消息](#事务消息)
- [延时消息](#延时消息)
- [顺序消息](#顺序消息)
- [批量发送](#批量发送)
- [消费模式](#消费模式)
- [背压控制](#背压控制)
- [消费超时](#消费超时)
- [死信队列（DLQ）](#死信队列dlq)
- [消息过滤](#消息过滤)
- [消息压缩](#消息压缩)
- [可观测性](#可观测性)
- [SPI 扩展机制](#spi-扩展机制)
- [管理 REST API](#管理-rest-api)

---

## 注解驱动消费

通过 `@StreamMQConsumer` 注解声明式定义消费者，无需手动配置 ConsumerGroup 或编写拉取循环。Spring 容器启动时自动扫描带注解的 Bean 并注册到监听容器。

### 并发消费

```java
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {

    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) throws Exception {
        System.out.println("收到消息: " + message.getBody());
        System.out.println("重试次数: " + context.reconsumeTimes());

        // 业务处理
        processOrder(message.getBody());

        return ConsumeAction.SUCCESS;  // 消费成功，自动 ACK
    }
}
```

### 顺序消费

将 `messageModel` 设置为 `ORDERLY`，并实现 `StreamMessageOrderlyConsumer` 接口：

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
    public ConsumeAction onMessage(Message<String> message, ConsumeOrderlyContext context) throws Exception {
        processOrder(message.getBody());
        return ConsumeAction.SUCCESS;
        // 消费失败时返回 SUSPEND_CURRENT_QUEUE_A_MOMENT，消息留在 PEL 等待重新消费
    }
}
```

### DLQ 死信消费

将 `dlqMode` 设置为 `true`，从死信 Stream 消费重试耗尽的消息：

```java
@Component
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    dlqMode = true
)
public class OrderDlqConsumer implements StreamMessageConcurrentlyConsumer<String> {

    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) throws Exception {
        // 死信处理：告警、人工补偿、落库等
        handleDeadLetter(message);
        return ConsumeAction.SUCCESS;
    }
}
```

### 返回值语义

**并发消费（`ConsumeAction`）**：

| 返回值 | 说明 |
|--------|------|
| `ConsumeAction.SUCCESS` | 消费成功，自动 ACK，从 PEL 移除 |
| `ConsumeAction.RECONSUME_LATER` | 消费失败，按 `RetryPolicy` 计算延迟后重投 |
| `ConsumeAction.defer(Duration)` | 消费失败，按指定延迟重投（覆盖 RetryPolicy） |

**顺序消费（`ConsumeAction`）**：

| 返回值 | 说明 |
|--------|------|
| `ConsumeAction.SUCCESS` | 消费成功，自动 ACK，下一条继续 |
| `ConsumeAction.SUSPEND_CURRENT_QUEUE_A_MOMENT` | 暂停当前 shard 一小段时间（默认 1000ms），重新消费同一消息 |

> ⚠️ 抛出 `RuntimeException` 时：并发消费等价于 `RECONSUME_LATER`，顺序消费等价于 `SUSPEND_CURRENT_QUEUE_A_MOMENT`。框架不提供手动 ACK/nack 调用，避免双模式冲突。

---

## StreamMessageTemplate 编程模型

`StreamMessageTemplate` 是 StreamMQ 的核心生产者 API，对齐 RocketMQ `RocketMQTemplate` 体验。所有发送操作均经过 `ProducerInterceptor` 链与 `ProducerFilter` 链。

### 接口定义

```java
public interface StreamMessageTemplate extends TransactionExecutor {

    /** 默认发送超时（毫秒） */
    long DEFAULT_SEND_TIMEOUT_MILLIS = 3000L;
    /** 默认同步发送重试次数 */
    int DEFAULT_SYNC_RETRY_TIMES = 2;
    /** 默认异步发送重试次数（不重试） */
    int DEFAULT_ASYNC_RETRY_TIMES = 0;

    // 同步发送
    <T> SendResult syncSend(Message<T> message);
    <T> SendResult syncSend(Message<T> message, long timeoutMillis);
    <T> SendResult syncSend(Message<T> message, long timeoutMillis, int retryTimes);

    // 异步发送
    <T> CompletableFuture<SendResult> asyncSend(Message<T> message);
    <T> void asyncSend(Message<T> message, SendCallback callback);
    <T> void asyncSend(Message<T> message, SendCallback callback, long timeoutMillis);

    // 单向发送
    <T> void sendOneway(Message<T> message);

    // 批量发送
    <T> List<SendResult> syncSendBatch(BatchMessage<T> batch);

    // 事务消息（继承自 TransactionExecutor）
    <T> SendResult executeInTransaction(Message<T> message, TransactionCallback<T> callback);

    // 拦截器与过滤器管理
    MessageConverter getMessageConverter();
    List<ProducerInterceptor> getProducerInterceptors();
    void setProducerInterceptors(List<ProducerInterceptor> interceptors);
    void addProducerInterceptor(ProducerInterceptor interceptor);
    void addProducerFilter(ProducerFilter filter);
}
```

### 使用示例

```java
@Service
public class OrderService {

    @Autowired
    private StreamMessageTemplate template;

    // 1. 同步发送
    public SendResult syncSend(Order order) {
        Message<Order> msg = MessageBuilder.<Order>withTopic("order-topic")
                .tag("created")
                .keys(order.getId())
                .body(order)
                .build();
        return template.syncSend(msg);
    }

    // 2. 同步发送（自定义超时与重试）
    public SendResult syncSendWithRetry(Order order) {
        Message<Order> msg = MessageBuilder.<Order>withTopic("order-topic")
                .body(order).build();
        return template.syncSend(msg, 5000L, 3);  // 5s 超时，重试 3 次
    }

    // 3. 异步发送（CompletableFuture）
    public CompletableFuture<SendResult> asyncSend(Order order) {
        Message<Order> msg = MessageBuilder.<Order>withTopic("order-topic")
                .body(order).build();
        return template.asyncSend(msg);
    }

    // 4. 异步发送（回调）
    public void asyncSendWithCallback(Order order) {
        Message<Order> msg = MessageBuilder.<Order>withTopic("order-topic")
                .body(order).build();
        template.asyncSend(msg, new SendCallback<>() {
            @Override
            public void onSuccess(SendResult result) {
                log.info("发送成功: {}", result.getMessageId());
            }
            @Override
            public void onException(Throwable e) {
                log.error("发送失败", e);
            }
        });
    }

    // 5. 单向发送（不等待响应，性能最高）
    public void sendOneway(Order order) {
        Message<Order> msg = MessageBuilder.<Order>withTopic("order-topic")
                .body(order).build();
        template.sendOneway(msg);
    }
}
```

### 发送方式对比

| 方式 | 可靠性 | 性能 | 返回值 | 适用场景 |
|------|--------|------|--------|----------|
| `syncSend` | 高 | 中 | `SendResult` | 重要业务消息（默认） |
| `asyncSend` | 高 | 高 | `CompletableFuture` / 回调 | 高吞吐场景 |
| `sendOneway` | 低 | 最高 | 无 | 日志、监控等允许丢失 |
| `syncSendBatch` | 高 | 高 | `List<SendResult>` | 批量投递 |
| `executeInTransaction` | 高 | 低 | `SendResult` | 最终一致性场景 |

---

## 事务消息

事务消息采用半消息 + 本地事务 + 事务回查机制，保证分布式场景下的最终一致性，对齐 RocketMQ 事务消息语义。

### 半消息流程

```
Producer                  StreamMQ                  Consumer
   │                         │                         │
   │  1. 发送半消息            │                         │
   │ ────────────────────────>│                         │
   │                         │ (半消息暂存于 half Stream) │
   │  2. 执行本地事务          │                         │
   │ (callback.execute)       │                         │
   │                         │                         │
   │  3. 返回事务状态          │                         │
   │   COMMIT / ROLLBACK / UNKNOW                       │
   │ ────────────────────────>│                         │
   │                         │                         │
   │           ┌─────────────┴─────────────┐           │
   │           │                           │           │
   │      COMMIT_MESSAGE              ROLLBACK_MESSAGE │
   │           │                           │           │
   │  4. 半消息转投到                  4. 半消息删除     │
   │     目标 Topic Stream                               │
   │                         │ ────────────────────────>│
   │                         │     5. 消费者收到消息      │
   │                         │                         │
   │      UNKNOW（状态未知）                              │
   │           │                                         │
   │  6. 事务回查任务定期调用 TransactionChecker          │
   │ <────────────────────────│                         │
   │  7. 返回回查状态                                      │
   │ ────────────────────────>│                         │
```

### 发送事务消息

```java
@Service
public class OrderTransactionService {

    @Autowired
    private StreamMessageTemplate template;

    public SendResult sendTransactionOrder(Order order) {
        Message<Order> message = MessageBuilder.<Order>withTopic("order-topic")
                .tag("transaction")
                .keys(order.getId())
                .body(order)
                .build();

        // 事务回调：执行本地事务并返回状态
        TransactionCallback<Order> callback = (msg, ctx) -> {
            try {
                // 执行本地事务（如扣减库存、写入订单表）
                orderService.createOrder(msg.getBody());
                log.info("本地事务执行成功, transactionId={}", ctx.getTransactionId());
                return LocalTransactionState.COMMIT_MESSAGE;   // 提交半消息
            } catch (Exception e) {
                log.error("本地事务执行失败", e);
                return LocalTransactionState.ROLLBACK_MESSAGE;  // 回滚半消息
            }
        };

        // 发送事务消息
        return template.executeInTransaction(message, callback);
    }
}
```

### 实现事务回查

当本地事务返回 `UNKNOW` 或回调执行异常时，框架会定期调用 `TransactionChecker` 回查本地事务状态。

```java
@Component
@StreamMQTransactionConsumer
public class OrderTransactionChecker implements TransactionChecker<Order> {

    @Autowired
    private OrderRepository orderRepository;

    @Override
    public LocalTransactionState check(Message<Order> message, TransactionContext context) throws Exception {
        String transactionId = context.getTransactionId();
        String orderId = message.getKeys();

        // 查询本地事务表，判断事务是否已执行
        boolean exists = orderRepository.existsById(orderId);

        if (exists) {
            // 本地事务已成功执行，提交半消息
            return LocalTransactionState.COMMIT_MESSAGE;
        } else {
            // 本地事务未执行或状态未知
            // 返回 UNKNOW 等待下次回查（连续多次 UNKNOW 后框架强制 ROLLBACK）
            return LocalTransactionState.UNKNOW;
        }
    }
}
```

### LocalTransactionState 状态说明

| 状态 | 说明 |
|------|------|
| `COMMIT_MESSAGE` | 本地事务执行成功，提交半消息使其对消费者可见 |
| `ROLLBACK_MESSAGE` | 本地事务执行失败，回滚半消息并删除 |
| `UNKNOW` | 本地事务状态未知，等待事务回查任务稍后再次检查 |

### 事务回查配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `streammq.transaction.check-interval-ms` | `60000` | 回查间隔（毫秒） |
| `streammq.transaction.max-check-times` | `15` | 最大回查次数，超过后强制 ROLLBACK |
| `streammq.transaction.batch-size` | `32` | 单次回查批量大小 |

> 💡 完整示例参考 [`streammq-samples/streammq-sample-transaction`](https://github.com/streammq/streammq/tree/main/streammq-samples/streammq-sample-transaction)

---

## 延时消息

StreamMQ 支持 18 级固定延时与任意毫秒延时，底层基于 Redis ZSet + 定时轮询投递实现。

### 18 级固定延时

| 级别 | 延时 | 级别 | 延时 |
|------|------|------|------|
| `DelayLevel.SECOND_1` | 1s | `DelayLevel.MINUTE_6` | 6m |
| `DelayLevel.SECOND_5` | 5s | `DelayLevel.MINUTE_7` | 7m |
| `DelayLevel.SECOND_10` | 10s | `DelayLevel.MINUTE_8` | 8m |
| `DelayLevel.SECOND_30` | 30s | `DelayLevel.MINUTE_9` | 9m |
| `DelayLevel.MINUTE_1` | 1m | `DelayLevel.MINUTE_10` | 10m |
| `DelayLevel.MINUTE_2` | 2m | `DelayLevel.MINUTE_20` | 20m |
| `DelayLevel.MINUTE_3` | 3m | `DelayLevel.MINUTE_30` | 30m |
| `DelayLevel.MINUTE_4` | 4m | `DelayLevel.HOUR_1` | 1h |
| `DelayLevel.MINUTE_5` | 5m | `DelayLevel.HOUR_2` | 2h |

### 代码示例

```java
// 1. 固定延时（延时 5 分钟）
Message<String> msg1 = MessageBuilder.<String>withTopic("delay-topic")
        .body("订单超时取消")
        .delayLevel(DelayLevel.MINUTE_5)   // 5 分钟后投递
        .build();
template.syncSend(msg1);

// 2. 任意毫秒延时（延时 15 分钟）
Message<String> msg2 = MessageBuilder.<String>withTopic("delay-topic")
        .body("自定义延时")
        .delayTimeMillis(15 * 60 * 1000L)  // 15 分钟后投递
        .build();
template.syncSend(msg2);
```

### DelayLevel 工具方法

```java
// 根据毫秒数查找最接近的延时级别（向上取整）
DelayLevel level = DelayLevel.closestAbove(45_000L);  // 返回 MINUTE_1（1 分钟）

// 根据下标获取延时级别（0-based）
DelayLevel level = DelayLevel.ofIndex(0);  // 返回 SECOND_1

// 获取级别对应的毫秒数
long millis = DelayLevel.MINUTE_5.toMillis();  // 返回 300000
```

### 典型场景

- **订单超时取消**：下单后 30 分钟未支付自动取消
- **延迟重试**：消费失败后按策略延迟重投
- **定时任务**：固定时间后触发业务逻辑
- **消息降级**：高峰期消息延迟到低峰期处理

> 💡 完整示例参考 [`streammq-samples/streammq-sample-delay`](https://github.com/streammq/streammq/tree/main/streammq-samples/streammq-sample-delay)

---

## 顺序消息

基于 ShardingKey 的分片顺序消费，保证同一分区内消息严格有序。适用于订单状态机、流程引擎等需要严格顺序的场景。

### 工作原理

```
Producer                     StreamMQ                     Consumer
   │                            │                            │
   │  发送消息（携带 shardingKey）│                            │
   │ ──────────────────────────>│                            │
   │                            │ 按 shardingKey 哈希路由到    │
   │                            │ 固定 shard（如 shard-3）     │
   │                            │                            │
   │                            │  shard-0: [msg-a1, msg-a2] │
   │                            │  shard-1: [msg-b1, msg-b2] │
   │                            │  shard-2: [msg-c1, msg-c2] │
   │                            │  shard-3: [msg-d1, msg-d2] │
   │                            │                            │
   │                            │  每个 shard 单线程串行消费    │
   │                            │ ──────────────────────────>│
```

> 实现说明：顺序消息使用「单 Stream + 分片分布式锁」而非分片独立 Stream —— 相同 `shardingKey` 哈希到固定分片，
> 消费时获取对应分片锁后串行处理。消费失败时在当前线程内按 `maxReconsumeTimes` 重试同一消息，每次失败按
> `suspendCurrentQueueTimeMillis` 挂起（默认 1000ms），保证同分片严格有序；重试耗尽后直接进入 DLQ。

### 发送顺序消息

```java
// 发送时设置 shardingKey，相同 key 的消息路由到同一 shard
Message<Order> msg = MessageBuilder.<Order>withTopic("order-topic")
        .tag("status-change")
        .keys(order.getId())
        .shardingKey(order.getId())   // 分片键：相同订单 ID 路由到同一分区
        .body(order)
        .build();
template.syncSend(msg);
```

### 消费顺序消息

```java
@Component
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-orderly-group",
    messageModel = MessageModel.ORDERLY,
    shardCount = 8   // 8 个分片，并发度 = 8
)
public class OrderOrderlyConsumer implements StreamMessageOrderlyConsumer<Order> {

    @Override
    public ConsumeAction onMessage(Message<Order> message, ConsumeOrderlyContext context) throws Exception {
        try {
            // 同一订单的状态变更消息将严格按发送顺序消费
            processOrderStatusChange(message.getBody());
            return ConsumeAction.SUCCESS;
        } catch (Exception e) {
            // 消费失败：暂停当前 shard 一小段时间后重新消费同一消息
            return ConsumeAction.SUSPEND_CURRENT_QUEUE_A_MOMENT;
        }
    }
}
```

### 配置说明

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `shardCount` | `4` | 分片数，决定并发度与顺序性粒度 |
| `suspendCurrentQueueTimeMillis` | `1000` | 顺序消费挂起时长（毫秒） |

> ⚠️ 顺序消费以牺牲并发度换取顺序性，`shardCount` 越大并发度越高但顺序性粒度越细。选择 `shardingKey` 时应保证需要顺序的消息落入同一 shard。

> 💡 完整示例参考 [`streammq-samples/streammq-sample-orderly`](https://github.com/streammq/streammq/tree/main/streammq-samples/streammq-sample-orderly)

---

## 批量发送

`BatchMessage` 批量投递，充分利用 Redisson `RBatch`（Pipeline）一次性 XADD 多条消息，减少 RTT，提升吞吐。

### 代码示例

```java
@Service
public class BatchOrderService {

    @Autowired
    private StreamMessageTemplate template;

    public List<SendResult> sendBatch(List<Order> orders) {
        // 构造批量消息（所有消息必须同 Topic）
        BatchMessage<Order> batch = BatchMessage.<Order>withTopic("order-topic")
                .add(MessageBuilder.<Order>withPayload(orders.get(0)).keys("k1").build())
                .add(MessageBuilder.<Order>withPayload(orders.get(1)).keys("k2").build())
                .add(MessageBuilder.<Order>withPayload(orders.get(2)).keys("k3").build())
                .build();

        // 批量发送，返回每条消息的发送结果
        List<SendResult> results = template.syncSendBatch(batch);
        results.forEach(r -> log.info("消息发送成功: {}", r.getMessageId()));
        return results;
    }
}
```

### 使用约束

| 约束 | 说明 |
|------|------|
| 同 Topic | 一个 `BatchMessage` 中所有消息必须属于同一 Topic |
| 不可变 | `BatchMessage` 构造后不可修改 |
| 批量大小 | 单批建议不超过 1000 条（`MAX_BATCH_SIZE_LIMIT`） |

### 适用场景

- **数据同步**：批量同步数据库变更到下游
- **日志采集**：批量上报业务日志
- **通知推送**：批量发送通知消息
- **数据导入**：批量导入初始数据

---

## 消费模式

StreamMQ 支持两种消费模式，通过 `consumeMode` 属性配置。

### 集群消费（默认）

同一 ConsumerGroup 内每条消息仅被其中一个 Consumer 实例消费，利用 Redis Stream 原生 ConsumerGroup 实现自动负载均衡。

```java
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    consumeMode = ConsumeMode.CLUSTERING  // 默认值，可省略
)
```

**特点**：
- 每条消息被组内一个 Consumer 消费一次
- 自动负载均衡，Consumer 扩缩容时自动重平衡
- 适用于大部分业务场景

### 广播消费

同一 Topic 的每条消息会被所有订阅的 Consumer 实例各消费一次。实现机制：为每个 Consumer 实例创建独立 ConsumerGroup（基于 instanceId 拼接）。

```java
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    consumeMode = ConsumeMode.BROADCASTING
)
```

**特点**：
- 每条消息被所有 Consumer 实例消费
- 适用于配置广播、缓存刷新、日志收集等场景
- 消费进度独立，实例重启后从最新消息开始消费

### 消费模式对比

| 维度 | 集群消费（CLUSTERING） | 广播消费（BROADCASTING） |
|------|------------------------|--------------------------|
| 消费分发 | 组内一个 Consumer 消费 | 所有 Consumer 各消费一次 |
| 负载均衡 | 支持 | 不适用 |
| 消费进度 | 组内共享 | 实例独立 |
| 适用场景 | 业务处理 | 广播通知、缓存刷新 |

---

## 背压控制

InflightQueue 实现拉取-处理解耦，防止 Consumer 拉取速度超过处理速度导致内存溢出。

### 工作原理

```
Redis Stream ──> 拉取线程 ──> InflightQueue（有界） ──> 消费线程池
                    │                   │
                    │     容量满时阻塞拉取
                    │
               背压保护：防止 OOM
```

### 配置示例

```yaml
streammq:
  consumer:
    inflight-capacity: 1000   # 背压队列容量，0 = 不启用背压（默认）
    pull-batch-size: 32       # 单次拉取批量大小
    pull-interval: 0          # 拉取间隔（毫秒，0 = 不间隔）
```

### 配置说明

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `inflight-capacity` | `0`（不启用） | InflightQueue 容量，超过时阻塞拉取 |
| `pull-batch-size` | `32` | 单次从 Redis Stream 拉取的消息数 |
| `pull-interval` | `0` | 拉取间隔毫秒数，0 表示连续拉取 |

> 💡 建议在生产环境设置 `inflight-capacity` 为一个合理值（如 1000），防止消费速度跟不上拉取速度导致内存溢出。

---

## 消费超时

支持单条消息消费超时自动取消并进入重试队列，防止慢消费阻塞整个消费线程池。

### 配置方式

**注解级别**：

```java
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    consumeTimeout = 30000   // 30 秒超时
)
```

**全局级别**：

```yaml
streammq:
  consumer:
    consume-timeout: 30000   # 全局默认消费超时（毫秒）
```

### 超时行为

| 阶段 | 行为 |
|------|------|
| 消费超时 | 中断消费线程，消息按 `RECONSUME_LATER` 处理 |
| 重试 | 按 `RetryPolicy` 计算延迟后重投 |
| 达到重试上限 | 进入 DLQ 死信队列 |

> ⚠️ 消费超时依赖 `Future.cancel(true)` 中断线程，业务代码应响应中断（如检查 `Thread.interrupted()` 或捕获 `InterruptedException`）。

---

## 死信队列（DLQ）

消费重试耗尽后的消息自动进入 DLQ（Dead Letter Queue），用于人工干预、告警或补偿处理。

### DLQ 流程

```
Consumer 消费失败
       │
       ▼
返回 RECONSUME_LATER 或抛出异常
       │
       ▼
按 RetryPolicy 重试（默认 16 次）
       │
       ├─ 重试成功 ──> 正常 ACK
       │
       └─ 重试耗尽 ──> 进入 DLQ Stream
                          │
                          │  DLQ Stream Key:
                          │  streammq:{ns}:dlq:{consumerGroup}
                          │
                          ▼
                    DLQ Consumer 消费
                    （dlqMode = true）
                          │
                          ├─ 处理成功 ──> 正常 ACK
                          │
                          └─ 处理失败 ──> DlqFailureStrategy 决策
                                          ├─ drop（丢弃）
                                          ├─ retry（重试）
                                          └─ secondaryDlq（二级死信）
```

### 定义 DLQ 消费者

```java
@Component
@StreamMQConsumer(
    topic = "order-topic",        // 原始 Topic
    consumerGroup = "order-group", // 原始消费者组
    dlqMode = true                 // 标识为 DLQ 消费者
)
public class OrderDlqConsumer implements StreamMessageConcurrentlyConsumer<String> {

    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) throws Exception {
        // 死信处理：告警、人工补偿、落库等
        log.warn("收到死信消息: topic={}, keys={}, body={}, retryCount={}",
                message.getTopic(),
                message.getKeys(),
                message.getBody(),
                context.reconsumeTimes());

        // 发送告警
        alertService.sendAlert("死信告警", message);

        // 落库记录
        deadLetterRepository.save(message);

        return ConsumeAction.SUCCESS;
    }
}
```

### DLQ 失败策略

当 DLQ 消费者也消费失败时，由 `DlqFailureStrategy` SPI 决策后续动作：

| 决策 | 说明 |
|------|------|
| `DlqFailureDecision.drop()` | 丢弃消息（框架 ACK 后由策略记录日志/告警） |
| `DlqFailureDecision.retry(Duration)` | 按指定延迟重试本 DLQ 消息 |
| `DlqFailureDecision.secondaryDlq()` | 转投到二级死信队列 |

### DLQ 配置

```yaml
streammq:
  dlq:
    enabled: true                # 启用 DLQ
    max-retry-times: 3           # DLQ 消费失败最大重试次数
    retry-delay-ms: 10000        # DLQ 重试延迟（毫秒）
    secondary-dlq-enabled: false # 是否启用二级死信队列
```

> 💡 完整示例参考 [`streammq-samples/streammq-sample-dlq`](https://github.com/streammq/streammq/tree/main/streammq-samples/streammq-sample-dlq)

---

## 消息过滤

StreamMQ 支持两种消息过滤方式：Tag 表达式过滤与 SQL92 表达式过滤。

### Tag 表达式过滤

基于 Tag 的简单表达式过滤，支持 `||`（或）与 `&&`（与）运算符。

```java
// 消费者：只消费 tag 为 created 或 paid 的消息
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    selectorType = SelectorType.TAG,
    selectorExpression = "created || paid"
)
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {
    // ...
}
```

**发送方设置 Tag**：

```java
Message<String> msg = MessageBuilder.<String>withTopic("order-topic")
        .tag("created")   // 设置 Tag
        .body("order content")
        .build();
```

**Tag 表达式示例**：

| 表达式 | 说明 |
|--------|------|
| `*` | 接收全部（默认） |
| `created` | 只接收 tag=created |
| `created \|\| paid` | 接收 tag=created 或 tag=paid |
| `created && urgent` | 接收同时含 created 和 urgent（少见） |

### SQL92 表达式过滤

基于 SQL92 子集的表达式过滤，支持对 `userProperties` 进行复杂条件判断。

```java
// 消费者：只消费 region=hangzhou 且 amount>100 的消息
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    selectorType = SelectorType.SQL92,
    selectorExpression = "region = 'hangzhou' AND amount > 100"
)
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {
    // ...
}
```

**发送方设置 userProperties**：

```java
Message<Order> msg = MessageBuilder.<Order>withTopic("order-topic")
        .body(order)
        .withUserProperty("region", "hangzhou")
        .withUserProperty("amount", "150")
        .build();
```

**SQL92 表达式示例**：

| 表达式 | 说明 |
|--------|------|
| `a = 1 AND b > 2` | a 等于 1 且 b 大于 2 |
| `region = 'hangzhou'` | region 等于 hangzhou |
| `amount > 100 AND amount < 1000` | amount 在 (100, 1000) 区间 |
| `tag = 'created' OR tag = 'paid'` | tag 为 created 或 paid |

### 过滤执行顺序

1. 内置 `selectorExpression` 过滤器（order = -1，最先执行）
2. 自定义 `ConsumerFilter`（按 `order()` 升序执行）
3. 任一过滤器返回 `false` 则消息被跳过（自动 ACK）

---

## 消息压缩

通过 `CompressionCodec` SPI 实现消息体压缩，减少网络传输与 Redis 内存占用。当 `ProducerConfig.compressThreshold` 大于 0 且 body 字节数超过阈值时触发压缩。

### 配置示例

```yaml
streammq:
  producer:
    compress-threshold: 1024   # 压缩阈值（字节），body 超过 1KB 时压缩，0 = 禁用（默认）
```

### 工作机制

```
发送方：
  body 字节数 > compressThreshold?
     ├─ 是 ──> CompressionCodec.compress(body) ──> 压缩后字节写入 Stream
     └─ 否 ──> 原始字节写入 Stream

消费方：
  读取 Stream Entry
     ├─ 含压缩标识 ──> CompressionCodec.decompress(body) ──> 原始 body
     └─ 无压缩标识 ──> 直接使用原始 body
```

### 内置实现

| 实现类 | 算法 | 说明 |
|--------|------|------|
| `GzipCompressionCodec` | GZIP | 内置必选实现，基于 JDK GZIP |

### 自定义压缩算法

实现 `CompressionCodec` 接口并注册为 Spring Bean：

```java
@Component
public class Lz4CompressionCodec implements CompressionCodec {

    @Override
    public byte[] compress(byte[] data) {
        // LZ4 压缩实现
        return lz4Compress(data);
    }

    @Override
    public byte[] decompress(byte[] data) {
        // LZ4 解压实现
        return lz4Decompress(data);
    }

    @Override
    public String name() {
        return "lz4";
    }
}
```

---

## 可观测性

StreamMQ 提供完善的可观测性能力，包括 Micrometer 指标、MDC 结构化日志与 Trace 查询 API。

### Micrometer 指标

StreamMQ 自动注册以下 Micrometer 指标，可通过 Actuator `/actuator/metrics` 端点访问：

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `streammq.send.total` | Counter | 发送消息总数（tag `success`） |
| `streammq.send.duration` | Timer | 发送耗时分布 |
| `streammq.consume.total` | Counter | 消费消息总数 |
| `streammq.consume.duration` | Timer | 消费耗时分布 |
| `streammq.retry.total` | Counter | 重试消息数 |
| `streammq.dlq.total` | Counter | 进入死信队列数 |
| `streammq.delay.total` | Counter | 延时消息投递数 |
| `streammq.transaction.commit.total` | Counter | 事务提交数 |
| `streammq.transaction.rollback.total` | Counter | 事务回滚数 |
| `streammq.transaction.check.total` | Counter | 事务回查数 |

### 启用指标

```yaml
streammq:
  metrics:
    enabled: true   # 默认启用
```

### MDC 结构化日志

StreamMQ 自动在 MDC 中注入 traceId 等上下文信息，方便链路追踪：

```java
// 业务代码设置 traceId，发送消息时自动透传到消费者
MDC.put("traceId", "t-001");
template.syncSend(message);   // 消息携带 traceId
MDC.remove("traceId");
```

消费者侧自动从消息属性恢复 MDC：

```java
@Override
public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
    // MDC 中已自动注入 traceId（从消息 userProperties 恢复）
    String traceId = MDC.get("traceId");
    log.info("处理消息, traceId={}", traceId);  // 日志自动携带 traceId
    return ConsumeAction.SUCCESS;
}
```

### Trace 查询 API

`StreamMQTraceService` 提供按消息 ID、Topic、消费组维度的追踪记录查询能力：

> 限制（基于 Redis Stream 存储的默认实现）：
> - `queryByMessageId` 仅查询**今天与昨天**两个 trace Stream，更早数据需直接查询对应日期 Stream
> - 单日单次查询最多读取 10,000 条记录，超出部分静默截断
> - 时间范围查询遍历范围内的每一天并全量内存过滤，适用于中小规模追踪数据；大规模场景建议对接专业 APM（OTel / Zipkin / SkyWalking）

```java
@Autowired
private StreamMQTraceService traceService;

// 1. 按消息 ID 查询完整链路
List<TraceRecord> records = traceService.queryByMessageId("123-0");
records.forEach(r -> System.out.println(r.traceType() + ": " + r.timestamp()));

// 2. 按 Topic 和时间范围查询
List<TraceRecord> topicRecords = traceService.queryByTopic(
        "order-topic",
        System.currentTimeMillis() - 3600_000L,  // 1 小时前
        System.currentTimeMillis()
);

// 3. 按消费组和时间范围查询
List<TraceRecord> groupRecords = traceService.queryByGroup(
        "order-group",
        startTimeMs,
        endTimeMs
);
```

### TraceCollector SPI

实现 `TraceCollector` 接口可将追踪数据上报至 APM 系统（OpenTelemetry、Zipkin、SkyWalking 等）：

```java
@Component
public class OpenTelemetryTraceCollector implements TraceCollector {

    @Override
    public void recordSend(SendTraceContext ctx) {
        // 上报发送事件到 OpenTelemetry
        Span span = tracer.spanBuilder("streammq.send")
                .setAttribute("topic", ctx.topic())
                .setAttribute("messageId", ctx.messageId().toString())
                .setAttribute("success", ctx.success())
                .setAttribute("duration", ctx.durationMillis())
                .startSpan();
        span.end();
    }

    @Override
    public void recordConsume(ConsumeTraceContext ctx) {
        // 上报消费事件到 OpenTelemetry
    }

    @Override
    public boolean isEnabled() {
        return true;
    }
}
```

---

## SPI 扩展机制

StreamMQ 通过 SPI（Service Provider Interface）提供 12 个扩展点，业务方可在不修改框架代码的前提下定制任何环节。

### SPI 扩展点总览

| 序号 | SPI 接口 | 作用 | 内置实现 |
|------|----------|------|----------|
| 1 | `MessageSerializer` | 消息 body 与 byte[] 双向转换 | `JacksonJsonSerializer`（默认）、`JdkSerializer` |
| 2 | `MessageConverter` | Message 与 Redis Stream Entry 字段映射 | `DefaultMessageConverter`（默认）、`CompactMessageConverter`、`PassThroughMessageConverter` |
| 3 | `CompressionCodec` | 消息体压缩/解压 | `GzipCompressionCodec` |
| 4 | `ProducerFilter` | 生产者消息过滤（发送前） | 无（按需实现） |
| 5 | `ConsumerFilter` | 消费者消息过滤（消费前） | `TagSelectorFilter`、`SqlSelectorFilter`、`ExpressionSelectorFilter` |
| 6 | `ProducerInterceptor` | 生产者拦截器（发送前后） | 无（按需实现） |
| 7 | `ConsumerInterceptor` | 消费者拦截器（消费前后） | 无（按需实现） |
| 8 | `RetryPolicy` | 消费失败重试策略 | `FixedArrayRetryPolicy`（默认，对齐 RocketMQ 16 级） |
| 9 | `RebalanceStrategy` | 消费者重平衡策略 | `ConsistentHashRebalanceStrategy`（默认）、`AverageRebalanceStrategy` |
| 10 | `TraceCollector` | Trace 上下文采集与上报 | 无（按需对接 APM） |
| 11 | `ManagementAuthenticator` | 管理 REST 端点鉴权 | `AllowAllAuthenticator`、`DenyAllAuthenticator`、Basic、Token |
| 12 | `DlqFailureStrategy` | DLQ 消费失败处理策略 | `LogAndDropDlqFailureStrategy`、`LimitedRetryDlqFailureStrategy`、`SecondaryDlqFailureStrategy` |

### 1. MessageSerializer（消息序列化器）

负责 Message body 与 byte[] 的双向转换。元信息（topic/tag/keys 等）始终为 String，不参与序列化。

```java
public interface MessageSerializer<T> {
    byte[] serialize(T object, Class<T> type) throws SerializationException;
    <R> R deserialize(byte[] bytes, Class<R> type) throws SerializationException;
    default String name() { return getClass().getSimpleName(); }
}
```

**内置实现**：
- `JacksonJsonSerializer`：基于 Jackson 的 JSON 序列化（默认）
- `JdkSerializer`：基于 JDK 原生序列化（备选）

### 2. MessageConverter（消息转换器）

连接 `Message` 与 Redis Stream Entry 字段的双向转换，仅做字段映射，不负责 body 序列化。

```java
public interface MessageConverter {
    Map<String, String> toStreamFields(Message<?> message);
    <T> Message<T> fromStreamFields(Map<String, String> fields, Class<T> targetType);
    default String name() { return getClass().getSimpleName(); }
}
```

**内置实现**：
- `DefaultMessageConverter`（默认）：标准字段映射
- `CompactMessageConverter`：紧凑字段映射，减少 Stream Entry 字段数
- `PassThroughMessageConverter`：直通转换，body 已是 byte[] 时使用

### 3. CompressionCodec（压缩编解码器）

```java
public interface CompressionCodec {
    byte[] compress(byte[] data);
    byte[] decompress(byte[] data);
    String name();
}
```

详见 [消息压缩](#消息压缩) 章节。

### 4. ProducerFilter（生产者过滤器）

在消息发送前过滤，返回 `false` 阻止发送。

```java
public interface ProducerFilter {
    boolean accept(Message<?> message);
    default String name() { return getClass().getSimpleName(); }
    default int order() { return 0; }   // 升序执行
}
```

**使用示例**：

```java
@Component
public class SensitiveDataFilter implements ProducerFilter {

    @Override
    public boolean accept(Message<?> message) {
        // 阻止包含敏感词的消息发送
        String body = String.valueOf(message.getBody());
        return !body.contains("敏感词");
    }

    @Override
    public int order() { return 10; }
}
```

### 5. ConsumerFilter（消费者过滤器）

在消息消费前过滤，返回 `false` 跳过该消息（自动 ACK）。

```java
public interface ConsumerFilter {
    boolean accept(Message<?> message);
    default String name() { return getClass().getSimpleName(); }
    default int order() { return 0; }
}
```

### 6. ProducerInterceptor（生产者拦截器）

对齐 RocketMQ `ProducerInterceptor`，在发送前后被调用。

```java
public interface ProducerInterceptor {
    boolean beforeSend(Message<?> message);              // 返回 false 中止发送
    void afterSend(Message<?> message, SendResult result);
    default void onException(Message<?> message, Exception e, InvokeTiming timing) {}
    default String name() { return getClass().getSimpleName(); }
    default int order() { return 0; }
}
```

**使用示例**：

```java
@Component
public class TraceProducerInterceptor implements ProducerInterceptor {

    @Override
    public boolean beforeSend(Message<?> message) {
        // 注入 traceId
        if (message.getUserProperty("traceId") == null) {
            message.getUserProperties().put("traceId", generateTraceId());
        }
        return true;
    }

    @Override
    public void afterSend(Message<?> message, SendResult result) {
        log.info("消息已发送: topic={}, messageId={}",
                message.getTopic(), result.getMessageId());
    }
}
```

### 7. ConsumerInterceptor（消费者拦截器）

在消费前后被调用，用于审计、限流、链路追踪等。

### 8. RetryPolicy（重试策略）

控制消息消费失败后的重试间隔与是否停止。

```java
public interface RetryPolicy {
    Duration nextRetryDelay(int reconsumeTimes, Message<?> message);
    boolean shouldStopRetry(int reconsumeTimes, Message<?> message);
    default String name() { return getClass().getSimpleName(); }
}
```

**内置实现**：
- `FixedArrayRetryPolicy`（默认）：对齐 RocketMQ 16 级固定数组 `[10s, 30s, 1m, 2m, 3m, 4m, 5m, 6m, 7m, 8m, 9m, 10m, 20m, 30m, 1h, 2h]`

### 9. RebalanceStrategy（重平衡策略）

控制 ConsumerGroup 内分片到 Consumer 的分配算法。

```java
public interface RebalanceStrategy {
    Map<Integer, String> assign(List<Integer> shards, List<String> consumers, String consumerGroup);
    default String name() { return getClass().getSimpleName(); }
}
```

**内置实现**：
- `ConsistentHashRebalanceStrategy`（默认）：一致性哈希，减少 Rebalance 时分片迁移
- `AverageRebalanceStrategy`：平均分配，精确均衡但 Rebalance 时变动大

### 10. TraceCollector（追踪收集器）

详见 [可观测性 - TraceCollector SPI](#tracecollector-spi) 章节。

### 11. ManagementAuthenticator（管理鉴权器）

用于运维 REST 端点的鉴权。

```java
public interface ManagementAuthenticator {
    boolean authenticate(String username, String password, String resource);
    default String name() { return getClass().getSimpleName(); }
}
```

**内置实现**：
- `AllowAllAuthenticator`：允许全部（开发环境）
- `DenyAllAuthenticator`：拒绝全部
- Basic Auth：HTTP Basic 认证
- Token：Token 认证

### 12. DlqFailureStrategy（DLQ 失败策略）

详见 [死信队列 - DLQ 失败策略](#dlq-失败策略) 章节。

---

## 管理 REST API

StreamMQ 内置 Admin REST 端点，用于运维管理。默认挂载在 `/actuator/streammq` 路径下。

### 启用管理端点

管理端点为 Actuator Web 端点（`@WebEndpoint(id = "streammq")`），当 `streammq.enabled=true` 且 Actuator 在 classpath 时自动注册。需通过 Actuator 暴露配置开放访问：

```yaml
management:
  endpoints:
    web:
      exposure:
        include: streammq   # 或 "*"
```

### 端点列表

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/actuator/streammq` | 总览（状态、消费组、Topic） |
| `GET` | `/actuator/streammq/groups` | 消费组列表 |
| `GET` | `/actuator/streammq/pending/{group}/{topic}` | Pending 消息 |
| `GET` | `/actuator/streammq/dlq/{group}` | 查询指定消费组的死信消息 |
| `POST` | `/actuator/streammq/dlq/{group}/requeue?messageId&targetTopic` | DLQ 消息重新入队 |
| `DELETE` | `/actuator/streammq/dlq/{group}/{messageId}` | 删除指定死信消息 |
| `GET` | `/actuator/streammq/topics` | Topic 列表 |
| `GET` | `/actuator/streammq/stats/{group}/{topic}` | 运行时统计 |
| `POST` | `/actuator/streammq/ack/{group}/{topic}?messageId` | 手动 ACK |
| `POST` | `/actuator/streammq/rebalance/{group}` | 触发重平衡 |
| `POST` | `/actuator/streammq/topics?topic` | 创建 Topic |
| `DELETE` | `/actuator/streammq/topics/{topic}` | 删除 Topic |
| `POST` | `/actuator/streammq/config/{group}` | 更新消费组配置 |

### 鉴权配置

所有操作（含只读）均通过 `ManagementAuthenticator` SPI 鉴权，鉴权失败返回 HTTP 401。默认实现 `DenyAllAuthenticator` 拒绝一切访问；业务方注册自定义 `ManagementAuthenticator` Bean（如 `BasicAuthAuthenticator` / `TokenAuthenticator` / `AllowAllAuthenticator`）即可开放。Basic 凭据取自请求的 `Authorization: Basic` 头。

### 使用示例

```bash
# 查询所有 Topic
curl -u admin:admin123 http://localhost:8080/actuator/streammq/topics

# 暂停消费组
curl -u admin:admin123 -X POST http://localhost:8080/actuator/streammq/consumers/order-group/pause

# 查询死信
curl -u admin:admin123 http://localhost:8080/actuator/streammq/dlq/order-group

# 重发死信
curl -u admin:admin123 -X POST http://localhost:8080/actuator/streammq/dlq/order-group/resend

# 查询消息追踪
curl -u admin:admin123 http://localhost:8080/actuator/streammq/trace/123-0
```

---

## 完整示例导航

| 特性 | 示例路径 |
|------|----------|
| 快速开始 | [`streammq-samples/streammq-sample-quickstart`](https://github.com/streammq/streammq/tree/main/streammq-samples/streammq-sample-quickstart) |
| 事务消息 | [`streammq-samples/streammq-sample-transaction`](https://github.com/streammq/streammq/tree/main/streammq-samples/streammq-sample-transaction) |
| 延时消息 | [`streammq-samples/streammq-sample-delay`](https://github.com/streammq/streammq/tree/main/streammq-samples/streammq-sample-delay) |
| 顺序消息 | [`streammq-samples/streammq-sample-orderly`](https://github.com/streammq/streammq/tree/main/streammq-samples/streammq-sample-orderly) |
| 死信队列 | [`streammq-samples/streammq-sample-dlq`](https://github.com/streammq/streammq/tree/main/streammq-samples/streammq-sample-dlq) |
| 拦截器 | [`streammq-samples/streammq-sample-interceptor`](https://github.com/streammq/streammq/tree/main/streammq-samples/streammq-sample-interceptor) |

---

*相关文档 → [快速开始](quickstart.md) · [核心概念](concepts.md) · [配置参考](configuration.md) · [API 文档](api.md)*
