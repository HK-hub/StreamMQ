# 核心概念

StreamMQ 是一款基于 Redis Stream + Redisson 构建的轻量级消息中间件 SDK，提供类 RocketMQ 的 API 体验。本文档系统介绍 StreamMQ 的核心概念，帮助开发者建立完整的心智模型。

> **项目信息**：版本 `0.1.0` ｜ Java 21 ｜ Spring Boot 3.3.5 ｜ Redisson 3.34.1 ｜ License MIT
> **源码仓库**：<https://github.com/streammq/streammq>

---

## Topic（主题）

Topic 是消息的逻辑分类，对应 Redis 中的一个 Stream Key。它是生产者与消费者之间的消息通道。

- 生产者将消息发送到指定 Topic
- 消费者从指定 Topic 订阅消息
- 每个 Topic 在 Redis 中对应一个独立的 Stream

在启用命名空间的情况下，Topic 对应的 Redis Key 格式为：

```
streammq:{namespace}:{topic}
```

```java
// 发送消息到 order-topic
Message<String> message = MessageBuilder.<String>withTopic("order-topic")
        .body("content")
        .build();
template.syncSend(message);
```

---

## ConsumerGroup（消费组）

ConsumerGroup 是一组消费者的逻辑标识，决定消息在消费者实例间的分发方式。StreamMQ 底层复用 Redis Stream 原生 ConsumerGroup 能力。

| 消费模式 | 行为 | 适用场景 |
|----------|------|----------|
| **CLUSTERING**（集群消费，默认） | 同一 ConsumerGroup 内每条消息仅被其中一个 Consumer 实例消费 | 绝大多数业务场景，自动负载均衡 |
| **BROADCASTING**（广播消费） | 同一 Topic 的每条消息会被所有订阅的 Consumer 实例各处理一次 | 配置广播、缓存刷新、事件通知 |

> 实现细节：广播消费为每个 Consumer 实例创建独立 ConsumerGroup（基于 instanceId 拼接），从而让每个实例都能收到全量消息。

```java
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    consumeMode = ConsumeMode.CLUSTERING   // 默认值，可省略
)
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {
    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        process(message.getBody());
        return ConsumeAction.SUCCESS;
    }
}
```

---

## Message（消息）

Message 是消息载体，封装 Topic / Tag / Keys / ShardingKey / Properties / Body 等字段，对应一条 Redis Stream Entry。

**核心设计**：
- 元信息（topic/tag/keys/shardingKey/properties）始终为 String
- 仅 `body` 字段通过 `MessageSerializer` 序列化为 `byte[]`
- 构造后字段不可变（properties 返回不可修改视图）
- 通过 `MessageBuilder` 构造

### 字段总览

| 字段 | 类型 | 是否必填 | 说明 |
|------|------|----------|------|
| `topic` | String | 是 | 主题，对应一个 Redis Stream |
| `tag` | String | 否 | 标签，同一 Topic 下的二级分类，用于消费端过滤 |
| `keys` | String | 否 | 业务键，用于幂等 / 查询 |
| `shardingKey` | String | 否 | 分片键，用于顺序消息路由 |
| `body` | T | 是 | 消息体，由序列化器决定如何转 `byte[]` |
| `delayLevel` | DelayLevel | 否 | 延时级别，非空时表示固定延时消息 |
| `delayTimeMillis` | Long | 否 | 任意延时毫秒数，优先级高于 `delayLevel` |
| `properties` | Map<String,String> | 否 | 系统属性（框架使用，如 traceId） |
| `userProperties` | Map<String,String> | 否 | 用户属性，业务自定义透传 |
| `messageId` | MessageId | 否 | 消息 ID，发送成功后由框架回填 |
| `bornTimestamp` | long | 否 | 出生时间戳（毫秒），发送端写入 |
| `bornHost` | String | 否 | 出生主机（host:port） |
| `reconsumeTimes` | int | 否 | 已重试消费次数，消费端递增 |
| `transactionId` | String | 否 | 事务 ID，仅事务消息 |

### 判断方法

| 方法 | 说明 |
|------|------|
| `isDelayMessage()` | `delayLevel` 或 `delayTimeMillis` 非空时返回 true |
| `isTransactionMessage()` | `transactionId` 非空时返回 true |

---

## MessageId（消息 ID）

MessageId 是消息的唯一标识，对应 Redis Stream Entry ID，格式为 `{timestamp}-{sequence}`。

- `streamEntryId`：原始 Stream Entry ID 字符串
- `timestamp`：时间戳部分（毫秒，Unix 时间）
- `sequence`：序列号（同时间戳内递增）

MessageId 实现 `Comparable<MessageId>`，可按时间戳 + 序列号排序；不可变且线程安全。

```java
SendResult result = template.syncSend(message);
MessageId msgId = result.getMessageId();
System.out.println("Entry ID: " + msgId.getStreamEntryId());
System.out.println("Timestamp: " + msgId.getTimestamp());
System.out.println("Sequence: " + msgId.getSequence());
```

---

## ConsumeAction（消费结果）

ConsumeAction 是并发消费回调的返回值，框架以返回值为唯一标准控制后续流程，**不再支持手动 `acknowledge()/nack()` 调用**，避免双模式冲突。

| 动作 | 类型 | 说明 |
|------|------|------|
| `ConsumeAction.SUCCESS` | 静态常量 | 消费成功，自动 ACK，从 PEL 移除 |
| `ConsumeAction.RECONSUME_LATER` | 静态常量 | 消费失败，按 `RetryPolicy` 计算延迟后写入 retry ZSet 重投 |
| `ConsumeAction.defer(Duration)` | 工厂方法 | 消费失败，使用业务指定的延迟重投（覆盖 RetryPolicy） |

> 当 Listener 抛出 `RuntimeException` 时，框架将其视为 `RECONSUME_LATER`。

```java
public ConsumeAction onMessage(Message<Order> msg, ConsumeContext ctx) {
    try {
        process(msg.getBody());
        return ConsumeAction.SUCCESS;
    } catch (RetryableException ex) {
        return ConsumeAction.RECONSUME_LATER;
    } catch (BusyException ex) {
        return ConsumeAction.defer(Duration.ofSeconds(30));
    }
}
```

---

## ConsumeAction（顺序消费结果）

ConsumeAction 是顺序消费回调的返回值枚举。

| 动作 | 说明 |
|------|------|
| `SUCCESS` | 消费成功，自动 ACK，从 PEL 移除，下一条继续 |
| `SUSPEND_CURRENT_QUEUE_A_MOMENT` | 暂停当前 shard 一小段时间后重新消费该消息，避免顺序消息丢失 |

> 当 Listener 抛出 `RuntimeException` 时，框架将其视为 `SUSPEND_CURRENT_QUEUE_A_MOMENT`，避免顺序消息丢失。
> 顺序消费以返回值为唯一标准，框架不提供手动 ACK 调用。

---

## ShardingKey（分片键）

ShardingKey 是顺序消息的分片依据。相同 shardingKey 的消息会被路由到同一 shard，shard 内单线程串行消费，从而保证顺序。

仅当 `messageModel = MessageModel.ORDERLY` 时生效。

```java
Message<String> message = MessageBuilder.<String>withTopic("order-topic")
        .shardingKey("user-123")   // 同一用户的订单消息顺序处理
        .body("content")
        .build();
```

---

## DelayLevel（延时级别）

DelayLevel 是 18 级固定延时（对齐 RocketMQ），底层基于 Redis ZSet + 定时轮询投递实现。

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

### 任意延时

除固定级别外，StreamMQ 还支持任意延时（`delayTimeMillis`），优先级高于 `delayLevel`。

```java
// 固定级别延时
MessageBuilder.<String>withTopic("delay-topic")
        .delayLevel(DelayLevel.MINUTE_5)
        .body("content")
        .build();

// 任意延时（15 分钟）
MessageBuilder.<String>withTopic("delay-topic")
        .delayTimeMillis(15 * 60 * 1000L)
        .body("content")
        .build();
```

### 辅助方法

| 方法 | 说明 |
|------|------|
| `getDuration()` | 返回该级别对应的 `Duration` |
| `toMillis()` | 返回毫秒数 |
| `toSeconds()` | 返回秒数 |
| `ofIndex(int)` | 按声明顺序下标获取级别（0-based） |
| `closestAbove(long millis)` | 查找最接近的延时级别（向上取整） |

---

## Half Message（半消息）

Half Message 是事务消息的中间态，本地事务提交前对消费者不可见。

### 事务消息流程

```
┌─────────────┐     1. 发送半消息      ┌──────────────────────┐
│  Producer   │ ─────────────────────▶ │ streammq:half:{txGroup} │
└─────────────┘                        └──────────────────────┘
       │                                          │
       │ 2. 执行本地事务                            │
       ▼                                          │
┌─────────────┐                                   │
│ Local Tx    │                                   │
└─────────────┘                                   │
       │                                          │
       ├─ COMMIT_MESSAGE ──▶ 3. 转投到业务 Topic Stream
       ├─ ROLLBACK_MESSAGE ─▶ 4. 删除半消息
       └─ UNKNOW ──────────▶ 5. 保留半消息，等待事务回查
```

1. 发送半消息到 `streammq:half:{transactionGroup}` Stream（对消费者不可见）
2. 调用 `TransactionCallback.execute()` 执行本地事务
3. 根据返回值：
   - `COMMIT_MESSAGE` → 提交半消息（转投到目标 Topic Stream）
   - `ROLLBACK_MESSAGE` → 回滚半消息（标记删除）
   - `UNKNOW` → 保留半消息，等待事务回查

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

## Transaction Check（事务回查）

Transaction Check 是事务状态不确定时，框架对生产者本地事务状态的回查机制。

**触发场景**：
- 本地事务返回 `UNKNOW`
- 网络抖动导致 COMMIT/ROLLBACK 通知丢失
- 服务宕机恢复后，回查未确认的事务

**回查机制**：
- 框架定时扫描超时未确认的半消息
- 调用 `TransactionChecker.check()` 回查本地事务状态
- 连续 `max-check-times` 次仍为 `UNKNOW`，框架强制 `ROLLBACK_MESSAGE`

```java
@Component
@StreamMQTransactionConsumer(transactionGroup = "order-tx-group")
public class OrderTransactionChecker implements TransactionChecker<String> {
    @Override
    public LocalTransactionState check(Message<String> message, TransactionContext context) {
        String txId = context.getTransactionId();
        return isTransactionCommitted(txId)
            ? LocalTransactionState.COMMIT_MESSAGE
            : LocalTransactionState.ROLLBACK_MESSAGE;
    }
}
```

---

## DLQ（死信队列）

DLQ（Dead Letter Queue）是消费重试耗尽后的消息队列，用于人工干预或后续补偿。

### DLQ 流程

1. 消息消费失败，进入重试队列（retry ZSet）
2. 重试次数达到 `maxReconsumeTimes`，停止重试
3. 消息被转发到 DLQ Stream：`streammq:{ns}:dlq:{consumerGroup}`
4. DLQ 消费者接收并处理

### 两种 DLQ 接入方式

| 方式 | 注解 | 消费接口 | 失败处理 |
|------|------|----------|----------|
| 轻量 DLQ 消费 | `@StreamMQConsumer(dlqMode = true)` | `StreamMessageConcurrentlyConsumer` | 返回值驱动重试 |
| 专用 DLQ 消费 | `@StreamMQDlqConsumer` | `DlqMessageConsumer`（返回 void） | 由 `DlqFailureStrategy` 决策 |

### DlqFailureStrategy 决策

DLQ 消费失败时，由策略决定后续动作：

| 决策 | 说明 |
|------|------|
| `drop()` | 丢弃消息（ACK 后记录日志 / 告警） |
| `retry(Duration)` | 按指定延迟重试本 DLQ 消息 |
| `secondaryDlq()` | 转投到二级死信队列 |

内置策略：`LogAndDropDlqFailureStrategy`（默认）、`LimitedRetryDlqFailureStrategy`、`SecondaryDlqFailureStrategy`。

---

## Namespace（命名空间）

Namespace 是全局前缀，用于多租户 / 多环境隔离。

```yaml
streammq:
  namespace: streammq-dev
```

启用命名空间后，Redis Key 格式统一带前缀：

| 资源 | Key 格式 |
|------|----------|
| Topic Stream | `streammq:{namespace}:{topic}` |
| Retry Stream | `streammq:{namespace}:retry:msg:{topic}:{group}` |
| DLQ Stream | `streammq:{namespace}:dlq:{consumerGroup}` |
| 半消息 Stream | `streammq:half:{transactionGroup}` |

---

## InflightQueue（背压队列）

InflightQueue 是拉取-处理解耦的内部队列，防止消费者拉取速度远超处理速度导致内存溢出。

```yaml
streammq:
  consumer:
    inflight-capacity: 1000   # 背压队列容量，0 = 不启用背压
```

- 消息从 Redis Stream 拉取后先进入 InflightQueue
- 消费线程从 InflightQueue 取消息处理
- 队列满时暂停拉取，实现背压

---

## ConsumerGroupManager（消费组管理器）

ConsumerGroupManager 管理消费者实例注册、心跳、活跃列表维护与分片分配，是集群消费负载均衡的核心组件。

**核心职责**：

| 职责 | 说明 |
|------|------|
| 实例注册与注销 | Consumer 启动 / 关闭时注册 / 注销 |
| 心跳维持 | 定期上报心跳，30s 内有心跳视为活跃 |
| 活跃实例列表管理 | 维护当前 ConsumerGroup 内活跃 Consumer 列表 |
| 分片重平衡 | 实例上下线时触发 Rebalance，重新分配分片 |

重平衡算法由 `RebalanceStrategy` SPI 决定：
- `ConsistentHashRebalanceStrategy`（默认）：一致性哈希，减少 Rebalance 时分片迁移
- `AverageRebalanceStrategy`：平均分配，精确均衡但 Rebalance 时变动大

---

## FilterChain（过滤器链）

StreamMQ 提供生产者与消费者双向过滤器链，用于在消息发送 / 消费前后进行过滤。

### 生产者过滤器链（ProducerFilterChain）

在消息发送前过滤，返回 `false` 则阻止消息发送。

**典型场景**：阻止特定 tag 发送、消息内容校验、敏感词过滤。

### 消费者过滤器链（ConsumerFilterChain）

在消息消费前过滤，返回 `false` 则跳过该消息（自动 ACK）。

**支持两个维度**：
- **全局过滤器**：通过配置注册，对所有消费者生效
- **Per-consumer 过滤器**：通过 `@StreamMQConsumer.consumerFilter` 注解指定，仅对单个消费者生效

### 执行顺序

多过滤器按 `order()` 升序执行，任一过滤器返回 `false` 则终止：

```
selectorExpression 内置过滤器 (order = -1)
        ↓
自定义 ConsumerFilter (按 order 升序)
        ↓
业务 onMessage 回调
```

> `selectorExpression` 对应的内置过滤器（Tag / SQL92）默认 `order = -1`，优先于自定义过滤器执行。

```java
// 自定义消费者过滤器
public class HighPriorityFilter implements ConsumerFilter {
    @Override
    public boolean accept(Message<?> message) {
        return "high".equals(message.getUserProperties().get("priority"));
    }

    @Override
    public int order() {
        return 10;
    }
}

// 绑定到消费者
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    consumerFilter = HighPriorityFilter.class
)
```

---

## InterceptorChain（拦截器链）

StreamMQ 提供生产者与消费者双向拦截器链，用于在发送 / 消费前后进行切面处理，对齐 RocketMQ Interceptor 体验。

### 生产者拦截器（ProducerInterceptor）

StreamMQ 在 `StreamMessageTemplate` 中按 `order()` 升序应用生产者拦截器，方法如下：

| 方法 | 时机 | 用途 |
|------|------|------|
| `beforeSend(Message)` | 发送前 | 添加 traceId、审计日志、限流（返回 false 中止发送） |
| `afterSend(Message, SendResult)` | 发送后 | 记录发送结果、指标埋点 |
| `onException(Message, Exception, InvokeTiming)` | 异常时 | 异常处理 |

### 消费者拦截器链（ConsumerInterceptorChain）

| 方法 | 时机 | 用途 |
|------|------|------|
| `beforeConsume(Message, ConsumeContext)` | 消费前 | 解密、解压、限流（返回 false 中止，视为 RECONSUME_LATER） |
| `afterConsume(Message, ConsumeAction, ConsumeContext)` | 消费后 | 指标埋点、审计 |
| `onException(...)` | 异常时 | 异常处理 |

> 顺序消费返回非成功动作时，消息保留在 PEL（Pending Entry List）中，由 `PelClaimScheduler` 周期重投。
> 多拦截器按 `order()` 升序执行。

---

## BackendProvider（规划中）

> **状态**：V2.0 规划概念，当前版本（0.1.0）尚未实现。

BackendProvider 是 V2.0 规划的后端存储抽象层，目标是解耦消息存储后端，使 StreamMQ 除了 Redis Stream 之外还能对接其他存储后端（如 Kafka、Pulsar 等），实现一套 API、多后端可切换。

当前版本消息后端固定为 Redis Stream + Redisson。V2.0 发布后，开发者可通过实现 BackendProvider SPI 接入自定义后端，而无需改动业务代码。

---

## 概念关系总览

```
┌─────────────────────────────────────────────────────────────────┐
│                         StreamMQ 全景                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Producer                          Consumer                     │
│  ┌──────────────┐                  ┌──────────────────────────┐ │
│  │ MessageBuilder│                 │ @StreamMQConsumer         │ │
│  │       ↓       │                 │       ↓                  │ │
│  │ StreamMessage │   Topic Stream  │ StreamMQListener (PULL)   │ │
│  │ Template      │ ───────────────▶│       ↓                  │ │
│  │  ├ syncSend   │                 │ ConsumerFilterChain      │ │
│  │  ├ asyncSend  │                 │       ↓                  │ │
│  │  ├ oneway     │                 │ ConsumerInterceptorChain │ │
│  │  ├ batch      │                 │       ↓                  │ │
│  │  └ transaction│                 │ StreamMessageConsumer    │ │
│  └──────────────┘                  │  ├ ConcurrentlyConsumer  │ │
│         │                          │  └ OrderlyConsumer       │ │
│  ProducerFilter                    └──────────────────────────┘ │
│  ProducerInterceptor                   │                        │
│                                        ▼                        │
│                              RetryPolicy / DLQ / InflightQueue   │
└─────────────────────────────────────────────────────────────────┘
                   底层：Redis Stream + Redisson
```
