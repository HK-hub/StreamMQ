# StreamMQ 消息类型生产消费链路分析

> 配套 PRD：[01-PRD.md](./01-PRD.md)　架构：[02-architecture.md](./02-architecture.md)　功能设计：[03-functional-design.md](./03-functional-design.md)　详细设计：[04-detailed-design.md](./04-detailed-design.md)
> 本文档分析 StreamMQ 的 6 类消息（普通 / 顺序 / 一次 / 事务 / 批量 / 延时）的生成、存储、状态流转逻辑，并与 RocketMQ 进行对比。

| 字段 | 内容 |
|---|---|
| 文档版本 | v1.0 |
| 创建日期 | 2026-07-03 |
| 文档语言 | 中文 |
| 技术栈 | JDK 21 / Spring Boot 3.3.x / Redisson 3.34.x / Redis 7.2+ |
| 分析范围 | streammq-core + streammq-redisson-adapter |

---

## 目录

1. [概述](#1-概述)
2. [普通消息（Normal Message）](#2-普通消息normal-message)
3. [顺序消息（Ordered Message）](#3-顺序消息ordered-message)
4. [一次消息（Oneway Message）](#4-一次消息oneway-message)
5. [事务消息（Transaction Message）](#5-事务消息transaction-message)
6. [批量消息（Batch Message）](#6-批量消息batch-message)
7. [延时消息（Delayed Message）](#7-延时消息delayed-message)
8. [StreamMQ 与 RocketMQ 总体对比](#8-streammq-与-rocketmq-总体对比)
9. [附录：Redis Key 与数据结构总览](#9-附录redis-key-与数据结构总览)

---

## 1. 概述

StreamMQ 是基于 Redis Stream 的轻量级消息中间件，对齐 RocketMQ 的 API 体验。其底层存储完全依赖 Redis（Stream / Hash / ZSet / String），通过 Redisson 客户端操作。

### 1.1 核心组件链路

所有消息的生产链路统一遵循以下分层调用：

```
用户代码
   │
   ▼
StreamMessageService（便捷 API 层，封装 MessageBuilder）
   │
   ▼
StreamMessageTemplate（编排层，含拦截器链 + 重试）
   │  ├── ProducerInterceptor.beforeSend()
   │  ├── StreamMessageProducer.syncSend/asyncSend/sendOneway/syncSendBatch
   │  └── ProducerInterceptor.afterSend()
   ▼
RedissonStreamProducer（底层抽象，调用 Redisson API）
   │
   ▼
Redis（XADD / RBatch / ZADD / HSET …）
```

消费链路统一遵循以下分层调用：

```
DefaultStreamMQListenerContainer（编排层，虚拟线程消费循环）
   │  ├── ConsumerInterceptorChain.applyBefore()
   │  ├── StreamMQListener.pullBlock（XREADGROUP）
   │  ├── Consumer.onMessage（业务处理）
   │  ├── RetryAndDlqHandler.handleAction（ACK / 重试 / DLQ 路由）
   │  └── ConsumerInterceptorChain.applyAfter()
   ▼
Redis（XREADGROUP / XACK / ZADD / HSET …）
```

### 1.2 消息类型与发送 API 对照

| 消息类型 | 用户调用入口 | 底层 Redis 命令 | 存储 Key 类型 |
|---|---|---|---|
| 普通消息 | `template.syncSend / asyncSend` | `XADD` | Stream |
| 顺序消息 | `template.syncSend` + `shardingKey` + ORDERLY 消费者 | `XADD` + `RLock` | Stream + String 锁 |
| 一次消息 | `template.sendOneway` | `XADD`（异步线程） | Stream |
| 事务消息 | `template.executeInTransaction` | `XADD` + `HSET` + `ZADD` | Stream + Hash + ZSet |
| 批量消息 | `template.syncSendBatch` | `RBatch`（Pipeline 多条 XADD） | Stream |
| 延时消息 | `template.syncSend` + `delayLevel/delayTimeMillis` | `ZADD` + `HSET` | ZSet + Hash |

---

## 2. 普通消息（Normal Message）

普通消息是最基础的消息类型，支持同步与异步两种发送语义。消息直接写入 Redis Stream，消费端通过消费者组（Consumer Group）拉取。

### A. 生产端逻辑

#### A.1 用户调用 API

StreamMQ 提供三层 API，由上至下逐步封装：

**便捷层（StreamMessageService）**：用户无需手动构造 Message 对象。

```java
// 最简调用：topic + body
SendResult result = service.send("order-topic", orderBody);

// 带标签与业务键
SendResult result = service.send("order-topic", orderBody, "created", "order-123");

// 带超时与重试
SendResult result = service.send("order-topic", orderBody, 3000L, 2);
```

**模板层（StreamMessageTemplate）**：用户通过 MessageBuilder 构造完整 Message。

```java
Message<String> msg = MessageBuilder.<String>withTopic("order-topic")
    .tag("created")
    .keys("order-123")
    .body("{\"orderId\":123}")
    .userProperty("traceId", "t-001")
    .build();

// 同步发送（默认超时 3000ms，默认重试 2 次）
SendResult result = template.syncSend(msg);

// 同步发送（指定超时）
SendResult result = template.syncSend(msg, 5000L);

// 异步发送（返回 CompletableFuture）
CompletableFuture<SendResult> future = template.asyncSend(msg);

// 异步发送（回调通知）
template.asyncSend(msg, new SendCallback() {
    @Override public void onSuccess(SendResult result) { ... }
    @Override public void onException(Exception ex) { ... }
});
```

#### A.2 消息构造（MessageBuilder）

`MessageBuilder` 采用流式 API（对齐 RocketMQ `MessageBuilder` 风格），最终构造不可变的 `Message<T>` 对象：

- 静态工厂方法以 `with` 前缀：`withTopic(topic)` / `withPayload(body)` / `create()`
- 实例方法无前缀：`topic(...)` / `tag(...)` / `body(...)` / `keys(...)` / `shardingKey(...)`
- `build()` 时校验 `topic` 与 `body` 非空，`bornTimestamp` 默认取 `System.currentTimeMillis()`

#### A.3 发送经过的组件

```
StreamMessageService.send(topic, body)
    │  MessageBuilder.withTopic(topic).body(body).build()
    ▼
DefaultStreamMessageTemplate.syncSend(message, timeoutMillis, retryTimes)
    │  1. injectProducerMdc(message)   // 注入 MDC 结构化日志
    │  2. applyInterceptorsBefore(message)  // 拦截器链 beforeSend
    │  3. resolveProducer(topic)  // 选择 Producer（当前统一 defaultGroup）
    │  4. for attempt in 0..retryTimes:
    │       producer.syncSend(message, timeoutMillis)
    │  5. applyInterceptorsAfter(message, result)  // 拦截器链 afterSend
    │  6. clearProducerMdc()
    ▼
RedissonStreamProducer.syncSend(message, timeoutMillis)
    │  1. converter.toStreamFields(message)  // 转换为 Stream Entry 字段
    │  2. StreamMQKeys.topicStream(namespace, topic)  // 计算 Stream Key
    │  3. appendStream(streamKey, fields, timeoutMillis)
    │       stream.addAsync(args).get(timeoutMillis)  // 底层 XADD
    │  4. message.setMessageId(...)  // 回填 Stream Entry ID
    ▼
Redis
```

关键源码位置：
- `DefaultStreamMessageTemplate.syncSend`（`streammq-redisson-adapter/.../template/DefaultStreamMessageTemplate.java`）
- `RedissonStreamProducer.syncSend`（`streammq-redisson-adapter/.../producer/RedissonStreamProducer.java`）

#### A.4 底层 Redis 命令

普通消息底层调用 Redis `XADD` 命令（通过 Redisson 的 `RStream.addAsync`）：

```redis
XADD streammq:{ns}:msg:{topic} MAXLEN ~ {maxLen} \
    body {base64} \
    bodyType {className} \
    tag {tag} \
    keys {keys} \
    shardingKey {shardingKey} \
    props {jsonString} \
    bornTs {timestamp} \
    bornHost {host}
```

- 当 `maxLen > 0` 时附加 `MAXLEN ~ {maxLen}`（近似切剪，对应 `trimNonStrict().maxLen(maxLen).noLimit()`）。
- 使用异步 API `addAsync` + `get(timeoutMillis)` 实现真正的超时控制，超时抛 `ProducerTimeoutException`。

### B. 存储模型

#### B.1 Redis Key 命名规则

| 项 | 内容 |
|---|---|
| Key 格式 | `streammq:{namespace}:msg:{topic}` |
| 命名空间为空时 | `streammq:msg:{topic}` |
| 数据结构 | Redis Stream |
| Key 生成方法 | `StreamMQKeys.topicStream(namespace, topic)` |

示例：namespace=`prod`，topic=`order-topic`，则 Key 为 `streammq:prod:msg:order-topic`。

#### B.2 Stream Entry 字段映射

`DefaultMessageConverter.toStreamFields` 将 `Message` 对象映射为 Stream Entry 字段：

| Stream Entry 字段 | Message 字段 | 类型 | 说明 |
|---|---|---|---|
| `body` | body | String | body 序列化后的 Base64 字符串（必填） |
| `bodyType` | body.getClass() | String | body 实际类型类名（用于反序列化校验） |
| `tag` | tag | String | 标签（可选） |
| `keys` | keys | String | 业务键（可选） |
| `shardingKey` | shardingKey | String | 分片键（可选，顺序消息使用） |
| `props` | properties + userProperties | String | 系统+用户属性合并的 JSON 字符串（可选） |
| `bornTs` | bornTimestamp | String | 出生时间戳毫秒（必填） |
| `bornHost` | bornHost | String | 出生主机（可选） |
| `retryTimes` | reconsumeTimes | String | 已重试次数（可选，默认 0） |
| `txId` | transactionId | String | 事务 ID（可选，仅事务消息） |
| `originTopic` | - | String | 原 topic（可选，重试/DLQ 转投场景） |

> 注意：`topic` 字段不写入 Stream Entry，因其由 Stream Key 本身表示。反序列化时由消费端根据读取的 Stream Key 回填 topic。

### C. 消费端逻辑

#### C.1 消费者注册

通过 `@StreamMQConsumer` 注解（类级）标注在 `StreamMessageConcurrentlyConsumer` 实现类上：

```java
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-cg")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<Order> {
    @Override
    public ConsumeAction onMessage(Message<Order> message, ConsumeContext context) {
        processOrder(message.getBody());
        return ConsumeAction.SUCCESS;  // 或 RECONSUME_LATER
    }
}
```

注册流程：`DefaultStreamMQListenerContainer.registerConsumer` → 构造 `ListenerRegistration` → 容器 `start()` 时为每个 Listener 启动虚拟线程消费循环。

#### C.2 消费循环拉取

`DefaultStreamMQListenerContainer.consumeLoop` 在虚拟线程中循环调用 `StreamMQListener.pullBlock`：

```java
while (state == RUNNING) {
    if (paused) { sleep(PAUSED_SLEEP_MILLIS); continue; }
    List<Message<?>> messages = listener.pullBlock(pullBatchSize, pullBlockTimeout);
    for (Message<?> message : messages) {
        handleMessage(message, reg, listener);
    }
}
```

`RedissonStreamListener.pullBlock` 底层调用 `XREADGROUP`：

```redis
XREADGROUP GROUP {group} {consumerName} COUNT {pullBatchSize} BLOCK {blockMillis} >
```

- `>` 表示只读取从未投递给该消费者组的消息。
- 首次拉取时 `ensureGroup()` 通过 `XGROUP CREATE` 创建消费者组（`MKSTREAM` 自动创建 Stream，`id 0-0` 从头消费），`BUSYGROUP` 错误视为正常。

#### C.3 消息反序列化

`RedissonStreamListener.toMessage` 将 Stream Entry 转回 `Message`：

1. 读取 `bodyType` 字段，通过 `Class.forName` 加载类型；加载失败回退到 `targetBodyType`，再回退到 `String.class`。
2. 调用 `converter.fromStreamFields(fields, bodyType)` 反序列化 body（SDK 路径 Base64 解码；跨平台路径 body 为原始字符串，目标为 String 时直接返回）。
3. 通过 `applyTopic` / `applyMessageId` 回填 topic（来自 Stream Key）与 messageId（来自 Stream Entry ID）。

#### C.4 ACK / 重试 / DLQ 处理

`DefaultRetryAndDlqHandler.handleAction` 根据 `ConsumeAction` 路由：

- **`SUCCESS`**：调用 `listener.ack(messageId)`，底层执行 `XACK {streamKey} {group} {entryId}`，从 PEL（Pending Entry List）移除。
- **`RECONSUME_LATER`**（非 DLQ 模式）：
  1. `converter.toStreamFields(message)` 转回字段；
  2. `retryPolicy.nextRetryDelay(retryCount, message)` 计算下一次重试延迟；
  3. 若延迟为 `null`（不再重试）→ 路由到 DLQ Stream（`XADD streammq:{ns}:dlq:{topic}:{group} ...`），附加 `dlqReason` / `originalMessageId` 字段；
  4. 否则写入 payload Hash（`HSET streammq:{ns}:delay:payload:{msgId} ...`，复用延时 payload Key）+ retry ZSet（`ZADD streammq:{ns}:retry:{topic}:{group} {nextRetryAt} {msgId}`），然后 ACK 原消息。
- **`RECONSUME_LATER`**（DLQ 模式）：直接 ACK 丢弃，避免死信消息无限循环。

`RetryScheduler` 周期扫描 retry ZSet（`ZRANGEBYSCORE 0 now LIMIT 0 batchSize`），对到期消息：
- `ZREM` 原子获取（返回 true 才处理）；
- 读取 payload Hash，递增 `retryTimes` 字段；
- `retryCount < maxReconsumeTimes` → `XADD` 到目标 Stream；`retryCount >= maxReconsumeTimes` → `XADD` 到 DLQ Stream（附加 `dlqReason=maxRetry`）；
- `DEL` payload Hash。

### D. 状态流转

普通消息从生成到消费完成的状态流转：

1. **构造中（BUILDING）**：用户通过 `MessageBuilder` 构造 `Message` 对象，设置 topic/tag/keys/body 等字段。
2. **发送中（SENDING）**：`template.syncSend` 调用拦截器链 `beforeSend`，委派 `producer.syncSend` 执行 `XADD`。
3. **已发送（SENT / SEND_OK）**：`XADD` 成功返回 Stream Entry ID，回填到 `message.messageId` 与 `SendResult`。若发送异常，状态为 `SEND_FAILED`，按 `retryTimes` 重试。
4. **已入流（IN_STREAM）**：消息持久化在 `streammq:{ns}:msg:{topic}` Stream 中，等待消费者组拉取。
5. **已投递未确认（DELIVERED_UNACK / IN_PEL）**：消费者组 `XREADGROUP >` 拉取后，消息进入该消费者的 PEL，等待 ACK。
6. **消费成功（CONSUMED）**：Listener 返回 `SUCCESS` → `XACK` 从 PEL 移除，消息生命周期结束。
7. **消费失败待重试（RETRY_SCHEDULED）**：Listener 返回 `RECONSUME_LATER` → 写入 retry ZSet + payload Hash，原消息 ACK。
8. **重试中（RETRYING）**：`RetryScheduler` 扫描到期消息，`XADD` 转投回目标 Stream，`retryTimes` 递增。
9. **进入死信（IN_DLQ）**：`retryCount >= maxReconsumeTimes` 或 `RetryPolicy.nextRetryDelay` 返回 null → `XADD` 到 DLQ Stream。
10. **死信消费（DLQ_CONSUMED）**：DLQ 消费者（`dlqConsumerGroup` 非空）从 `streammq:{ns}:dlq:{topic}:{group}` 拉取并处理。

状态图（文字描述）：

```
BUILDING → SENDING → SENT(SEND_OK) → IN_STREAM → DELIVERED_UNACK
                                              │
                              ┌───────────────┤
                              ▼               ▼
                          CONSUMED      RETRY_SCHEDULED
                              │               │
                              │               ▼
                              │          RETRYING(→ IN_STREAM)
                              │               │
                              │               ▼ (retryCount >= max)
                              │          IN_DLQ → DLQ_CONSUMED
                              ▼
                          (生命周期结束)
```

失败路径：`SENDING` 失败 → 重试 `retryTimes` 次 → 仍失败抛 `StreamMQBrokerException`。

### E. 与 RocketMQ 的对比

| 对比维度 | StreamMQ | RocketMQ |
|---|---|---|
| 底层存储 | Redis Stream（XADD） | CommitLog + ConsumeQueue（磁盘顺序写） |
| 消息 ID | Redis Stream Entry ID（`{ts}-{seq}`） | RocketMQ 自定义 msgId（offsetMsgId / uniqId） |
| 消费模型 | 消费者组 + PEL（XREADGROUP / XACK） | 消费者组 + ConsumeQueue 拉取偏移 |
| 重试机制 | retry ZSet + payload Hash，RetryScheduler 扫描转投 | %DLQ%CONSUMER_GROUP 重试主题，Broker 内置定时任务 |
| DLQ 机制 | DLQ Stream（`streammq:{ns}:dlq:{topic}:{group}`） | %DLQ%CONSUMER_GROUP 主题 |
| Broker | Redis 单机/集群（无独立 Broker） | 独立 Broker + NameServer 注册中心 |
| 持久化 | Redis RDB/AOF | CommitLog 刷盘（同步/异步）+ 主从复制 |
| 吞吐 | 受 Redis 单线程限制（10w 级 QPS） | 10w+ QPS（顺序写磁盘 + 零拷贝） |
| 延迟 | 亚毫秒级（内存操作） | 毫秒级（磁盘 + 网络） |
| 可靠性 | 依赖 Redis 持久化与主从 | 同步/异步刷盘 + 主从同步/异步复制 |
| API 风格 | `MessageBuilder` + `template.syncSend` | `MessageBuilder` + `producer.send` |
| 拦截器 | `ProducerInterceptor` 链（beforeSend/afterSend） | `SendMessageHook` / `ConsumerMessageHook` |
| 消息回溯 | Stream 天然支持按 ID 范围读取 | 通过 ConsumeQueue offset 回溯 |
| 消息堆积 | Stream MAXLEN 截断或内存压力 | 磁盘堆积，支持海量积压 |
| 适用场景 | 轻量级、低延迟、已有 Redis 环境 | 高吞吐、海量堆积、金融级可靠 |

---

## 3. 顺序消息（Ordered Message）

顺序消息通过 `shardingKey` 将同一业务键的消息路由到同一分片，配合分布式锁保证单分片内串行消费。

### A. 生产端逻辑

#### A.1 用户调用 API

顺序消息的生产端调用与普通消息完全一致，区别仅在于必须设置 `shardingKey`：

```java
Message<Order> msg = MessageBuilder.<Order>withTopic("order-topic")
    .tag("created")
    .keys("order-123")
    .shardingKey("order-123")   // 关键：同一订单的消息路由到同一 shard
    .body(order)
    .build();

SendResult result = template.syncSend(msg);
```

或通过便捷 API：

```java
SendResult result = service.send("order-topic", order, "created", "order-123", "order-123");
```

#### A.2 消息构造

`MessageBuilder.shardingKey(String)` 设置分片键，最终写入 `Message.shardingKey` 字段。该字段会通过 `DefaultMessageConverter` 写入 Stream Entry 的 `shardingKey` 字段。

#### A.3 发送经过的组件

顺序消息的发送链路与普通消息完全相同（Service → Template → Interceptors → Producer → Redis），`shardingKey` 仅作为元数据写入 Stream Entry，不影响生产端的路由逻辑。

> **重要说明**：`StreamMQKeys` 中定义了 `shardStream(namespace, topic, shardId)` 方法生成 `streammq:{ns}:msg:{topic}:shard{shardId}` 分片 Stream Key，但当前 `RedissonStreamProducer` 并未使用物理分片 Stream，所有消息统一写入单一 Topic Stream（`streammq:{ns}:msg:{topic}`）。顺序性保证完全在消费端通过分片锁实现。

#### A.4 底层 Redis 命令

与普通消息相同，执行 `XADD streammq:{ns}:msg:{topic} ... shardingKey {shardingKey} ...`。

### B. 存储模型

#### B.1 Redis Key 命名规则

顺序消息与普通消息共享同一个 Stream Key：

| 项 | 内容 |
|---|---|
| Key 格式 | `streammq:{namespace}:msg:{topic}` |
| 数据结构 | Redis Stream |
| 与普通消息区别 | 无存储差异，顺序性由消费端分片锁保证 |

> 预留的物理分片 Key `streammq:{ns}:msg:{topic}:shard{shardId}` 在当前实现中未启用。

#### B.2 Stream Entry 字段映射

除包含普通消息所有字段外，顺序消息必须包含 `shardingKey` 字段（消费端据此计算 shard 索引）。

### C. 消费端逻辑

#### C.1 顺序消费者注册

通过 `@StreamMQConsumer` 注解设置 `messageModel = MessageModel.ORDERLY`，并实现 `StreamMessageOrderlyConsumer`：

```java
@Component
@StreamMQConsumer(topic = "order-topic",
                  consumerGroup = "order-cg",
                  messageModel = MessageModel.ORDERLY,
                  shardCount = 4,
                  acknowledgeMode = AcknowledgeMode.AUTO)
public class OrderOrderlyConsumer implements StreamMessageOrderlyConsumer<Order> {
    @Override
    public OrderlyAction onMessage(Message<Order> message, ConsumeOrderlyContext context) {
        processOrder(message.getBody());
        return OrderlyAction.SUCCESS;  // 或 SUSPEND_CURRENT_QUEUE_A_MOMENT
    }
}
```

注册流程：`DefaultStreamMQListenerContainer.registerOrderlyConsumer`：
1. 通过 `BodyTypeResolver.resolve(consumer)` 解析 body 泛型类型；
2. 调用 `shardLockManager.createShardLocks(...)` 创建 `shardCount` 个 `RLock`；
3. 构造 `ListenerRegistration`（`type = ListenerType.ORDERLY`）。

#### C.2 消费循环拉取

顺序消费的拉取与普通消费共用同一个 `consumeLoop`，底层同样是 `XREADGROUP > COUNT n BLOCK ms`。区别在于 `handleMessage` 分支：

```java
if (reg.getType() == ListenerType.ORDERLY) {
    StreamMessageOrderlyConsumer orderly = (StreamMessageOrderlyConsumer) reg.getConsumer();
    OrderlyAction orderlyAction = shardLockManager.consumeWithShardLock(message, reg, ctx, orderly);
    ...
}
```

#### C.3 顺序消费的分片锁机制

`RedissonOrderlyShardLockManager.consumeWithShardLock` 是顺序消费的核心：

1. **分片锁创建**：`createShardLocks` 为每个 shard 创建 `RLock`，Key 为 `streammq:{ns}:shardlock:{topic}:{group}:{shardId}`（0 到 shardCount-1）。
2. **路由计算**：`shardIndex = Math.abs(shardingKey.hashCode()) % shardCount`，同一 shardingKey 永远路由到同一 shard。
3. **加锁消费**：`lock.lock(consumeTimeoutMillis, TimeUnit.MILLISECONDS)` 获取分布式锁（带超时，防止死锁），执行 `orderly.onMessage`，最后 `unlock`。

```java
int shardIndex = Math.abs(shardingKey.hashCode()) % reg.getShardCount();
RLock lock = reg.getShardLocks()[shardIndex];
try {
    lock.lock(reg.getConsumeTimeoutMillis(), TimeUnit.MILLISECONDS);
    return orderly.onMessage(message, ctx);
} finally {
    if (lock.isHeldByCurrentThread()) {
        lock.unlock();
    }
}
```

这保证同一 `shardingKey` 的消息在分布式环境下串行消费，不同 shard 之间可并行。

#### C.4 ACK / 重试 / DLQ 处理

顺序消费的 ACK 逻辑与并发消费不同：

- **`OrderlyAction.SUCCESS`**：调用 `retryDlqHandler.handleAction(SUCCESS, ...)` 执行 `XACK`。
- **`OrderlyAction.SUSPEND_CURRENT_QUEUE_A_MOMENT`**：**不 ACK**，消息留在 PEL 等待重新投递（Redis Stream 的 PEL 机制天然支持）。不进入 retry ZSet，避免乱序。
- **异常**：框架视为 `SUSPEND_CURRENT_QUEUE_A_MOMENT`，避免顺序消息丢失。
- **MANUAL 模式**：忽略 `onMessage` 返回值，由 `context.acknowledge()` 显式控制；未 ACK 时消息留在 PEL。

### D. 状态流转

顺序消息的状态流转（与普通消息的关键差异在于失败处理）：

1. **构造中（BUILDING）**：设置 `shardingKey`。
2. **已发送（SENT）**：`XADD` 写入 Topic Stream。
3. **已入流（IN_STREAM）**。
4. **已投递未确认（IN_PEL）**：`XREADGROUP >` 拉取，进入 PEL。
5. **加锁消费中（LOCKED_CONSUMING）**：`shardLockManager.consumeWithShardLock` 加锁后执行 `onMessage`。
6. **消费成功（CONSUMED）**：`SUCCESS` → `XACK` 从 PEL 移除，释放分片锁。
7. **挂起重试（SUSPENDED_IN_PEL）**：`SUSPEND_CURRENT_QUEUE_A_MOMENT` 或异常 → **不 ACK**，消息留在 PEL，等待 `XREADGROUP >` 重新拉取或 `XAUTOCLAIM` 转移。
8. **超时清理**：消息长期滞留 PEL（消费者宕机场景），需依赖 `XAUTOCLAIM` 机制转移给其他消费者（当前实现预留，由消费超时控制）。

状态图：

```
BUILDING → SENT → IN_STREAM → IN_PEL → LOCKED_CONSUMING
                                        │
                              ┌─────────┤
                              ▼         ▼
                          CONSUMED   SUSPENDED_IN_PEL
                          (XACK)       (留 PEL)
                              │         │
                              ▼         ▼
                          (结束)    重新拉取(→ IN_PEL)
```

> 注意：顺序消费**不进入 retry ZSet / DLQ**，失败时挂起在 PEL 内重试，保证分片内顺序。这是与普通消息最大的状态流转差异。

### E. 与 RocketMQ 的对比

| 对比维度 | StreamMQ | RocketMQ |
|---|---|---|
| 分片路由 | `shardingKey.hashCode() % shardCount`（消费端计算） | `MessageQueueSelector` 在生产端选择 queue |
| 存储分片 | 单一 Topic Stream（无物理分片） | 多个 MessageQueue（物理分片） |
| 顺序保证 | 消费端 `RLock` 分片锁串行消费 | 生产端按 queue 顺序写入 + 消费端按 queue 拉取 |
| 锁实现 | Redisson `RLock`（Redis 分布式锁，带超时） | 消费端 `ProcessQueue` 锁（本地锁） |
| 失败处理 | `SUSPEND_CURRENT_QUEUE_A_MOMENT` 留 PEL 重试 | `SUSPEND_CURRENT_QUEUE_A_MOMENT` 留 queue 重试 |
| 重试机制 | 不进入 retry ZSet，留 PEL 重新拉取 | 顺序消息不进入重试主题，留原 queue |
| 分片扩展 | `shardCount` 注解参数（默认 4） | MessageQueue 数量（Broker 配置） |
| 全局顺序 | 不支持（单 Stream 限制吞吐） | 单 MessageQueue 支持全局顺序（吞吐低） |
| 适用场景 | 中等吞吐的顺序场景 | 高吞吐顺序场景 |

---

## 4. 一次消息（Oneway Message）

一次消息（Oneway）是 fire-and-forget 语义：发送方不等待响应、不抛异常、性能最高，适用于日志收集、监控上报等可靠性要求低的场景。

### A. 生产端逻辑

#### A.1 用户调用 API

```java
// 通过模板层
template.sendOneway(MessageBuilder.<String>withTopic("log-topic")
    .body("log content")
    .build());

// 通过 Service 便捷 API
service.sendOneway("log-topic", logContent);
service.sendOneway("log-topic", logContent, "info");
```

#### A.2 消息构造

与普通消息一致，通过 `MessageBuilder` 构造。无需特殊字段。

#### A.3 发送经过的组件

```
DefaultStreamMessageTemplate.sendOneway(message)
    │  1. injectProducerMdc(message)
    │  2. applyInterceptorsBefore(message)  // 拦截器仍执行
    │  3. resolveProducer(topic)
    │  4. producer.sendOneway(message)
    │  5. clearProducerMdc()
    ▼
RedissonStreamProducer.sendOneway(message)
    │  asyncExecutor.submit(() -> {
    │      try { syncSend(message, defaultTimeoutMillis); }
    │      catch (RuntimeException ex) { LOG.warn(...); }  // 吞掉异常
    │  })
    ▼
（异步线程内执行 XADD）
```

关键实现：`sendOneway` 通过 `asyncExecutor`（虚拟线程池）提交异步任务，内部仍调用 `syncSend`，但：
- **不返回 `SendResult`**（方法返回 `void`）；
- **不抛异常**：异步任务内的异常仅记录 `LOG.warn`，不传播给调用方；
- **不等待完成**：`submit` 立即返回，发送在虚拟线程后台执行。

#### A.4 底层 Redis 命令

异步线程内执行与普通消息相同的 `XADD` 命令。

### B. 存储模型

与普通消息完全相同：写入 `streammq:{ns}:msg:{topic}` Stream，字段映射一致。

### C. 消费端逻辑

消费端无法区分消息是同步发送还是 oneway 发送，消费逻辑与普通消息完全一致（`XREADGROUP` + ACK / 重试 / DLQ）。

### D. 状态流转

1. **构造中（BUILDING）**。
2. **已提交异步（ASYNC_SUBMITTED）**：`asyncExecutor.submit` 返回，调用方继续执行，不等待结果。
3. **异步发送中（ASYNC_SENDING）**：虚拟线程内执行 `syncSend` → `XADD`。
4. **已发送（SENT）**：`XADD` 成功，消息进入 Stream。调用方无感知（无 `SendResult` 返回）。
5. **发送失败（ASYNC_FAILED）**：异常被吞，仅记录 `LOG.warn`，消息可能丢失。
6. **后续状态**：与普通消息一致（IN_STREAM → IN_PEL → CONSUMED / RETRY / DLQ）。

状态图：

```
BUILDING → ASYNC_SUBMITTED → ASYNC_SENDING → SENT → IN_STREAM → ...
                                  │
                                  ▼ (异常)
                              ASYNC_FAILED (仅日志，消息可能丢失)
```

> 关键差异：oneway 消息的发送结果对调用方不可见，失败时无重试（template 层的 `retryTimes` 不生效，因为异常被 producer 层吞掉）。

### E. 与 RocketMQ 的对比

| 对比维度 | StreamMQ | RocketMQ |
|---|---|---|
| 异步执行 | 虚拟线程池（`newVirtualThreadPerTaskExecutor`） | Netty EventLoop |
| 异常处理 | 吞掉异常，仅 `LOG.warn` | 不等待响应，无异常 |
| 返回值 | `void` | `void` |
| 可靠性 | 依赖 `XADD` 成功（但调用方无感知） | fire-and-forget |
| 拦截器 | 仍执行 `beforeSend`，不执行 `afterSend`（异步未等待） | 执行 hook |
| 性能 | 高（虚拟线程轻量） | 高（无等待） |
| 适用场景 | 日志、监控等容忍丢失的场景 | 同上 |

---

## 5. 事务消息（Transaction Message）

事务消息采用半消息（Half Message）机制，保证本地事务与消息发送的最终一致性。当前实现包含完整的事务回查基础设施（`TransactionScanner`）与简化版的模板编排。

### A. 生产端逻辑

#### A.1 用户调用 API

```java
// 通过模板层
SendResult result = template.executeInTransaction(
    MessageBuilder.<Order>withTopic("order-topic")
        .body(order)
        .build(),
    new TransactionCallback<Order>() {
        @Override
        public LocalTransactionState execute(Message<Order> message, TransactionContext context) {
            try {
                orderService.createOrder(message.getBody());
                return LocalTransactionState.COMMIT_MESSAGE;
            } catch (Exception ex) {
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }
            // 或返回 UNKNOW 等待回查
        }
    }
);

// 通过 Service 便捷 API
SendResult result = service.sendTransaction("order-topic", order, callback);
```

#### A.2 消息构造

通过 `MessageBuilder` 构造业务消息，`transactionId` 由 `executeInTransaction` 内部自动生成（`UUID.randomUUID()`）并设置到 `message.transactionId`。

#### A.3 发送经过的组件

`DefaultStreamMessageTemplate.executeInTransaction` 的当前实现（简化版）：

```
executeInTransaction(message, callback)
    │  1. transactionId = UUID.randomUUID()
    │  2. message.setTransactionId(transactionId)
    │  3. sendResult = syncSend(message)  // 直接发送到目标 Stream
    │     // 注意：当前简化版直接发送业务消息，未写入 half Stream
    │  4. ctx = new TransactionContext(transactionId, transactionGroup, ...)
    │  5. state = callback.execute(message, ctx)  // 执行本地事务
    │  6. switch(state):
    │       COMMIT_MESSAGE  → 直接返回 sendResult（消息已发送）
    │       ROLLBACK_MESSAGE → 返回 SEND_FAILED（仅日志，需人工清理）
    │       UNKNOW          → 返回 sendResult（等待 TransactionScanner 回查）
    ▼
```

> **实现状态说明**：当前 `executeInTransaction` 是简化版，注释明确标注「完整的半消息 + 回查调度由 TransactionScanner (p6) 实现」。`TransactionScanner` 已实现完整的 `registerHalfMessage` / `markCommit` / `markRollback` 方法，但 template 当前未调用它们，而是直接发送到目标 Stream。完整版应将消息先写入 half Stream，本地事务后通过 `markCommit`/`markRollback` 决定是否转投到业务 Stream。

#### A.4 事务回查器注册

通过 `@StreamMQTransactionConsumer` 注解标注在 `TransactionChecker` 实现类上：

```java
@Component
@StreamMQTransactionConsumer(transactionGroup = "order-tx-group")
public class OrderTransactionChecker implements TransactionChecker<Order> {
    @Override
    public LocalTransactionState check(Message<Order> message, TransactionContext context) {
        String txId = context.getTransactionId();
        return orderService.isTransactionCommitted(txId)
            ? LocalTransactionState.COMMIT_MESSAGE
            : LocalTransactionState.ROLLBACK_MESSAGE;
    }
}
```

框架通过 `TransactionScanner.registerChecker(txGroup, checker)` 注册回查器。

#### A.5 底层 Redis 命令

完整事务消息流程涉及的 Redis 命令（`TransactionScanner` 实现）：

```redis
# 1. 注册半消息（registerHalfMessage）
XADD streammq:{ns}:half:{txGroup} ... txId {txId}        # 写入 half Stream
HSET streammq:{ns}:txstate:{txGroup} {txId} PREPARE      # 写入状态
HSET streammq:{ns}:txstate:{txGroup} {txId}.target {topic}
HSET streammq:{ns}:txstate:{txGroup} {txId}.halfId {entryId}
ZADD streammq:{ns}:txcheck:{txGroup} {now+checkInterval} {txId}  # 调度回查

# 2. 标记 COMMIT（markCommit）
XADD streammq:{ns}:msg:{targetTopic} ...                 # 半消息转投到业务 Stream
XDEL streammq:{ns}:half:{txGroup} {halfId}               # 删除 half Stream 消息
HSET streammq:{ns}:txstate:{txGroup} {txId} COMMIT
ZREM streammq:{ns}:txcheck:{txGroup} {txId}
HDEL streammq:{ns}:txstate:{txGroup} {txId}.target {txId}.halfId

# 3. 标记 ROLLBACK（markRollback）
XDEL streammq:{ns}:half:{txGroup} {halfId}               # 删除 half Stream 消息
HSET streammq:{ns}:txstate:{txGroup} {txId} ROLLBACK
ZREM streammq:{ns}:txcheck:{txGroup} {txId}

# 4. 回查扫描（scanTimeoutHalf）
ZRANGEBYSCORE streammq:{ns}:txcheck:{txGroup} 0 now LIMIT 0 batchSize
HGET streammq:{ns}:txcheck:{txGroup}:counter {txId}      # 读取回查次数
HINCRBY streammq:{ns}:txcheck:{txGroup}:counter {txId} 1 # 递增回查次数
```

### B. 存储模型

事务消息涉及 4 个 Redis Key：

| Key 格式 | 数据结构 | 字段/成员 | 用途 |
|---|---|---|---|
| `streammq:{ns}:half:{txGroup}` | Stream | 含业务消息全部字段 + `txId` | 半消息暂存（COMMIT 前对消费者不可见） |
| `streammq:{ns}:txstate:{txGroup}` | Hash | field=`{txId}` value=`PREPARE/COMMIT/ROLLBACK/UNKNOWN`；field=`{txId}.target` value=目标 topic；field=`{txId}.halfId` value=half Stream Entry ID | 事务状态记录 |
| `streammq:{ns}:txcheck:{txGroup}` | ZSet | score=`checkTimeMillis`，member=`txId` | 回查调度队列 |
| `streammq:{ns}:txcheck:{txGroup}:counter` | Hash | field=`txId`，value=已回查次数 | 回查计数（超过 `maxCheckTimes` 强制 ROLLBACK） |

Key 生成方法：`StreamMQKeys.halfStream` / `transactionStateHash` / `transactionCheckZSet` / `transactionCheckCounter`。

### C. 消费端逻辑

事务消息的业务消费（COMMIT 后转投到 `streammq:{ns}:msg:{topic}` 的消息）与普通消息完全一致。

事务回查的"消费"由 `TransactionScanner` 完成：

1. **启动**：`scanExecutor.scheduleAtFixedRate(this::scanAllGroups, 0, checkIntervalMs, ...)`，默认间隔 60s（`DEFAULT_CHECK_INTERVAL_MS`）。
2. **扫描**：对每个注册的 txGroup，`ZRANGEBYSCORE streammq:{ns}:txcheck:{txGroup} 0 now LIMIT 0 batchSize` 获取超时 txId。
3. **回查**：对每个 txId 调用 `triggerCheck`：
   - 读取 `txstate` Hash 的状态；已终态（COMMIT/ROLLBACK）→ 清理调度；
   - 读取 half Stream 中的半消息；
   - 调用 `TransactionChecker.check(halfMessage, ctx)` 返回 `LocalTransactionState`；
   - `COMMIT_MESSAGE` → `markCommit`（转投到业务 Stream + 删除半消息）；
   - `ROLLBACK_MESSAGE` → `markRollback`（删除半消息）；
   - `UNKNOW` → 递增回查计数，若 `checkCount >= maxCheckTimes`（默认 15）强制 `markRollback`，否则重新调度（`ZADD` 下一次 checkTime）。
4. **异常处理**：checker 抛异常视为 `UNKNOW`；half 消息不存在强制 `markRollback`。

### D. 状态流转

事务消息半消息的状态流转（`TransactionScanner` 实现，对齐 RocketMQ 四态机）：

1. **PREPARE（半消息已发送）**：`registerHalfMessage` 写入 half Stream + txstate=`PREPARE` + txcheck ZSet 调度。
2. **本地事务执行**：`callback.execute` 返回三种状态：
   - `COMMIT_MESSAGE` → 转移到 **COMMIT**；
   - `ROLLBACK_MESSAGE` → 转移到 **ROLLBACK**；
   - `UNKNOW` → 保持 **PREPARE**（或标记为 **UNKNOWN**），等待回查。
3. **COMMIT**：`markCommit` 将半消息 `XADD` 到业务 Stream，`XDEL` 半消息，状态置为 `COMMIT`，清理调度。消息对消费者可见。
4. **ROLLBACK**：`markRollback` 删除半消息（`XDEL`），状态置为 `ROLLBACK`，清理调度。消息丢弃。
5. **UNKNOWN（回查中）**：`triggerCheck` 返回 `UNKNOW`，递增回查计数，重新调度下一次回查。
6. **强制 ROLLBACK**：`checkCount >= maxCheckTimes`（默认 15 次）仍为 UNKNOWN → 强制 `markRollback`。

状态图：

```
PREPARE ──本地事务──┬→ COMMIT   (半消息 → 业务 Stream，对消费者可见)
   │                ├→ ROLLBACK (半消息删除)
   │                └→ UNKNOWN  (等待回查)
   │                                  │
   │                                  ▼
   │                          triggerCheck
   │                                  │
   │                    ┌─────────────┤
   │                    ▼             ▼
   │                COMMIT       UNKNOWN (checkCount++)
   │                ROLLBACK         │
   │                                 ▼ (checkCount >= maxCheckTimes)
   └─────────────────────────→ 强制 ROLLBACK
```

> 注意：状态变更通过 `txstate` Hash 的 `HSET` 记录。终态（COMMIT/ROLLBACK）后清理 `.target` / `.halfId` 辅助字段，但保留主状态字段以便查询。

### E. 与 RocketMQ 的对比

| 对比维度 | StreamMQ | RocketMQ |
|---|---|---|
| 半消息存储 | half Stream（`streammq:{ns}:half:{txGroup}`） | 半消息主题 `RMQ_SYS_TRANS_HALF_TOPIC` |
| 状态记录 | txstate Hash（PREPARE/COMMIT/ROLLBACK/UNKNOWN） | op half topic（Operation Log） |
| 回查调度 | txcheck ZSet（score=checkTime）+ counter Hash | Broker 内置定时任务扫描半消息 |
| 回查机制 | `TransactionChecker.check()` | `TransactionListener.checkLocalTransaction()` |
| 回查限制 | 默认 15 次（`maxCheckTimes`），超过强制 ROLLBACK | 默认 15 次 |
| 回查间隔 | 默认 60s（`checkIntervalMs`） | 默认 60s |
| COMMIT 动作 | `XADD` 到业务 Stream + `XDEL` 半消息 | 标记 op half 为 COMMIT，消费者可见 |
| ROLLBACK 动作 | `XDEL` 半消息 | 标记 op half 为 ROLLBACK，丢弃 |
| 当前实现 | template 简化版（直接发送业务 Stream），Scanner 完整 | 生产级完整实现 |
| 可见性控制 | half Stream 与业务 Stream 物理隔离 | 半消息对消费者不可见（特殊主题） |
| 一致性 | 半消息 + 状态 Hash + ZSet 三方协调 | 半消息 + op half topic 二阶段 |

---

## 6. 批量消息（Batch Message）

批量消息通过 Redisson `RBatch`（Pipeline）一次性发送多条消息，减少网络 RTT，提升吞吐。

### A. 生产端逻辑

#### A.1 用户调用 API

```java
// 方式1：构造 BatchMessage 对象
BatchMessage<String> batch = BatchMessage.<String>withTopic("order-topic")
    .add(msg1)
    .add(msg2)
    .add(msg3)
    .build();
List<SendResult> results = template.syncSendBatch(batch);

// 方式2：通过 Service 便捷 API（body 列表）
List<SendResult> results = service.sendBatch("order-topic", Arrays.asList(body1, body2, body3));

// 方式3：通过 Service（带 tag）
List<SendResult> results = service.sendBatch("order-topic", "created", bodyList);

// 方式4：通过 Service（varargs）
List<SendResult> results = service.sendBatch(msg1, msg2, msg3);
```

#### A.2 消息构造（BatchMessage）

`BatchMessage` 是批量消息包装类，约束：
- 所有消息必须**共享同一 Topic**（`Builder.add` 校验，不一致抛 `IllegalArgumentException`）；
- 消息列表不可修改（`List.copyOf`）；
- 列表不能为空（`build()` 校验，抛 `IllegalStateException`）。

```java
BatchMessage<T> batch = BatchMessage.<T>withTopic(topic)
    .add(message1)  // 必须 topic 一致
    .addAll(messageList)
    .build();
```

#### A.3 发送经过的组件

```
DefaultStreamMessageTemplate.syncSendBatch(batch)
    │  1. 对每条 message 执行 applyInterceptorsBefore（任一被中止则抛异常）
    │  2. resolveProducer(batch.getTopic())
    │  3. producer.syncSendBatch(batch.getMessages())
    │  4. 对每条结果执行 applyInterceptorsAfter
    ▼
RedissonStreamProducer.syncSendBatch(messages)
    │  1. 校验同 Topic（firstTopic 一致性检查）
    │  2. 若含延时消息 → 降级为逐条 syncSend（延时消息不支持批量）
    │  3. 否则：
    │     batch = redisson.createBatch()  // 创建 RBatch
    │     for message in messages:
    │       fields = converter.toStreamFields(message)
    │       batch.getStream(streamKey).addAsync(StreamAddArgs.entries(fields))  // Pipeline XADD
    │     batch.execute()  // 一次性提交
    │  4. 为每条消息生成占位 MessageId（UUID 哈希，因 RBatch 不返回每条 ID）
    ▼
Redis
```

#### A.4 底层 Redis 命令

批量发送底层通过 Redisson `RBatch` 执行 Pipeline，等价于一次性发送多条 `XADD`：

```redis
# Redisson RBatch 内部等价于（实际为 Pipeline 一次性发送）
XADD streammq:{ns}:msg:{topic} ...  # 消息1
XADD streammq:{ns}:msg:{topic} ...  # 消息2
XADD streammq:{ns}:msg:{topic} ...  # 消息3
```

- 单次 `batch.execute()` 提交所有命令，减少 N 次 RTT 为 1 次 RTT。
- 由于 `RBatch` 不返回每条 Stream Entry ID，框架为每条消息生成占位 ID：`{currentTimeMillis}-{UUID.hashCode()}`。真实 Stream Entry ID 由消费端从 Stream Entry 获取。

### B. 存储模型

| 项 | 内容 |
|---|---|
| Key 格式 | `streammq:{namespace}:msg:{topic}`（与普通消息共享） |
| 数据结构 | Redis Stream |
| 区别 | 多条 Entry 一次性写入 |

每条消息的 Stream Entry 字段映射与普通消息一致。

### C. 消费端逻辑

消费端无需感知消息是否批量发送，消费逻辑与普通消息完全一致。`XREADGROUP COUNT n` 一次可拉取多条消息（包括批量发送的和单条发送的混合）。

### D. 状态流转

1. **构造中（BUILDING）**：`BatchMessage.Builder` 收集消息，校验同 Topic。
2. **批量发送中（BATCH_SENDING）**：`RBatch` Pipeline 提交多条 `XADD`。
3. **已发送（SENT）**：`batch.execute()` 成功，每条消息生成占位 ID（非真实 Stream Entry ID）。
4. **后续状态**：每条消息独立流转（IN_STREAM → IN_PEL → CONSUMED / RETRY / DLQ），与普通消息一致。

状态图：

```
BUILDING → BATCH_SENDING → SENT(占位ID) → IN_STREAM → ...
                              │
                              ▼ (RBatch.execute 失败)
                          BATCH_FAILED (抛 StreamMQBrokerException)
```

> 注意：批量发送是原子提交（`RBatch.execute`），但 Redis Pipeline 不保证事务原子性——中途某条 `XADD` 失败不影响其他命令。失败时抛 `StreamMQBrokerException`，整体回滚（无重试）。

### E. 与 RocketMQ 的对比

| 对比维度 | StreamMQ | RocketMQ |
|---|---|---|
| 底层机制 | Redisson `RBatch`（Pipeline 多条 XADD） | 批量 `SendMessageRequest`（多 MessageBody） |
| Topic 约束 | 必须同 Topic | 必须同 Topic + 同 queue |
| 返回值 | 占位 ID（非真实 Entry ID） | 每条消息的 msgId |
| 延时消息支持 | 不支持（降级为逐条发送） | 不支持批量延时 |
| 原子性 | Pipeline 非事务原子 | 批量请求整体处理 |
| 大小限制 | `MAX_BATCH_SIZE_LIMIT = 1000` | 默认 4MB（消息总大小） |
| 失败处理 | 整体抛异常，无部分成功 | 部分成功返回 |
| 网络优化 | N 次 RTT → 1 次 RTT | 同 |

---

## 7. 延时消息（Delayed Message）

延时消息在发送时不直接写入业务 Stream，而是先存入延时 ZSet + payload Hash，由 `DelayMessageScheduler` 周期扫描到期后转投到目标 Stream。

### A. 生产端逻辑

#### A.1 用户调用 API

```java
// 方式1：固定延时级别（18 级，对齐 RocketMQ）
SendResult result = service.sendDelay("order-topic", order, DelayLevel.MINUTE_5);

// 方式2：任意延时毫秒数（v1.0+，优先级高于 delayLevel）
SendResult result = service.sendDelay("order-topic", order, 30_000L);  // 30秒后投递

// 方式3：带 tag 的延时
SendResult result = service.sendDelay("order-topic", order, "created", DelayLevel.MINUTE_5);

// 方式4：通过 MessageBuilder
Message<Order> msg = MessageBuilder.<Order>withTopic("order-topic")
    .body(order)
    .delayLevel(DelayLevel.MINUTE_5)   // 或 .delayTimeMillis(30_000L)
    .build();
SendResult result = template.syncSend(msg);
```

#### A.2 延时级别

`DelayLevel` 枚举定义 18 级固定延时（对齐 RocketMQ）：

| 级别 | 延时 | 级别 | 延时 | 级别 | 延时 |
|---|---|---|---|---|---|
| SECOND_1 | 1s | SECOND_5 | 5s | SECOND_10 | 10s |
| SECOND_30 | 30s | MINUTE_1 | 1m | MINUTE_2 | 2m |
| MINUTE_3 | 3m | MINUTE_4 | 4m | MINUTE_5 | 5m |
| MINUTE_6 | 6m | MINUTE_7 | 7m | MINUTE_8 | 8m |
| MINUTE_9 | 9m | MINUTE_10 | 10m | MINUTE_20 | 20m |
| MINUTE_30 | 30m | HOUR_1 | 1h | HOUR_2 | 2h |

- `DelayLevel.closestAbove(millis)`：根据任意毫秒数向上取整到最接近的级别（用于 `delayTimeMillis` 场景）。
- 当同时设置 `delayLevel` 与 `delayTimeMillis` 时，`delayTimeMillis` 优先（内部转换为 `closestAbove` 级别）。

#### A.3 消息构造

通过 `MessageBuilder.delayLevel(DelayLevel)` 或 `delayTimeMillis(long)` 设置延时参数。`Message.isDelayMessage()` 判断是否为延时消息（`delayLevel != null || delayTimeMillis != null`）。

#### A.4 发送经过的组件

```
DefaultStreamMessageTemplate.syncSend(message)
    │  （拦截器链 → producer.syncSend）
    ▼
RedissonStreamProducer.syncSend(message, timeoutMillis)
    │  if (message.isDelayMessage()):
    │     return sendDelayMessage(message)  // 走延时发送分支
    ▼
sendDelayMessage(message)
    │  1. msgId = UUID.randomUUID()
    │  2. 计算级别：delayLevel 优先；否则 delayTimeMillis → closestAbove
    │  3. deliverAt = now + level.toMillis()
    │  4. zsetKey = StreamMQKeys.delayZSet(namespace, level.name())
    │     payloadHashKey = StreamMQKeys.delayPayloadHash(namespace, msgId)
    │  5. fields = converter.toStreamFields(message)
    │     fields.put("targetTopic", topic)
    │     fields.put("deliverAt", deliverAt)
    │  6. zset.add(deliverAt, msgId)          // ZADD
    │     payloadMap.putAll(fields)            // HSET
    │  7. 返回合成的 SendResult（msgId = now-hashCode）
    ▼
Redis
```

#### A.5 底层 Redis 命令

```redis
ZADD streammq:{ns}:delay:{level} {deliverAt} {msgId}
HSET streammq:{ns}:delay:payload:{msgId} \
    body {base64} bodyType {className} tag {tag} ... \
    targetTopic {topic} deliverAt {deliverAt}
```

### B. 存储模型

延时消息涉及 2 个 Redis Key（投递前不写入业务 Stream）：

| Key 格式 | 数据结构 | 字段/成员 | 用途 |
|---|---|---|---|
| `streammq:{ns}:delay:{level}` | ZSet | score=`deliverAt`（毫秒），member=`msgId` | 延时调度队列（按级别分队列） |
| `streammq:{ns}:delay:payload:{msgId}` | Hash | 含业务消息全部字段 + `targetTopic` + `deliverAt` | 消息完整 payload 暂存 |

辅助 Key：

| Key 格式 | 数据结构 | 用途 |
|---|---|---|
| `streammq:{ns}:delay:meta:delivered` | Hash | 延时已投递计数（统计用，Key 已定义） |

Key 生成方法：`StreamMQKeys.delayZSet(namespace, level)` / `delayPayloadHash(namespace, msgId)` / `delayDeliveredCounter(namespace)`。

### C. 消费端逻辑

#### C.1 延时调度器（DelayMessageScheduler）

延时消息不直接被消费者拉取，而是先由 `DelayMessageScheduler` 转投到目标 Stream 后，再由消费者正常消费。

调度器启动：`scanExecutor.scheduleAtFixedRate(this::scanAllLevels, 0, scanIntervalMs, ...)`，默认扫描间隔 1s（`DEFAULT_SCAN_INTERVAL_MS`）。

#### C.2 转投流程

`scanExpired(level)` 对每个延时级别执行：

1. `ZRANGEBYSCORE streammq:{ns}:delay:{level} 0 now LIMIT 0 batchSize` 获取到期 msgId；
2. 对每个 msgId：`ZREM streammq:{ns}:delay:{level} {msgId}`（原子移除，返回 true 才处理，避免多实例重复）；
3. 从 payload Hash 读取字段（`HGETALL streammq:{ns}:delay:payload:{msgId}`）；
4. 移除调度元数据字段（`targetTopic` / `deliverAt`）；
5. `XADD streammq:{ns}:msg:{targetTopic} ...` 转投到目标 Stream（通过 `RBatch` 批量）；
6. `DEL streammq:{ns}:delay:payload:{msgId}` 删除 payload Hash。

#### C.3 失败回退

转投失败时（异常），`ZADD streammq:{ns}:delay:{level} {now} {msgId}` 将 msgId 重新写回 ZSet（score=当前时间，立即重试），避免消息丢失。若回写也失败，记录 `CRITICAL` 日志（消息可能丢失）。

#### C.4 业务消费

转投到 `streammq:{ns}:msg:{topic}` 后，消息成为普通消息，消费逻辑与普通消息完全一致。

### D. 状态流转

1. **构造中（BUILDING）**：设置 `delayLevel` 或 `delayTimeMillis`。
2. **已调度（SCHEDULED）**：`ZADD` 写入延时 ZSet + `HSET` 写入 payload Hash，等待到期。
3. **到期待转投（EXPIRED）**：`ZRANGEBYSCORE` 识别为到期消息。
4. **转投中（TRANSFERRING）**：`ZREM` 原子获取 msgId。
5. **已转投（TRANSFERRED）**：`XADD` 到目标业务 Stream，`DEL` payload Hash。
6. **后续状态**：与普通消息一致（IN_STREAM → IN_PEL → CONSUMED / RETRY / DLQ）。
7. **转投失败（TRANSFER_FAILED）**：`ZADD` 回写 ZSet，重新调度（回到 **SCHEDULED**）。

状态图：

```
BUILDING → SCHEDULED → EXPIRED → TRANSFERRING → TRANSFERRED → IN_STREAM → ...
              ▲                       │
              │                       ▼ (异常)
              └───────────────── TRANSFER_FAILED (回写 ZSet 重试)
```

### E. 与 RocketMQ 的对比

| 对比维度 | StreamMQ | RocketMQ |
|---|---|---|
| 调度机制 | ZSet 轮询（`ZRANGEBYSCORE` + `ZREM`） | 延时队列主题 `SCHEDULE_TOPIC_XXXX` + Broker 定时任务 |
| 存储结构 | ZSet（score=deliverAt）+ payload Hash | CommitLog（特殊延时主题） |
| 延时级别 | 18 级（对齐 RocketMQ） | 18 级 |
| 任意延时 | `delayTimeMillis`（向上取整到最近级别） | 5.x 支持任意延时（TimerWheel） |
| 精度 | 受扫描间隔影响（默认 1s） | 秒级（Broker 定时任务） |
| 转投原子性 | `ZREM` + `XADD` 非原子（Java 端协调） | Broker 内部原子处理 |
| 多实例去重 | `ZREM` 返回值判断（true 才处理） | Broker 单点处理（无去重问题） |
| 失败恢复 | 回写 ZSet 重试 | Broker 持久化，重启恢复 |
| 投递可见性 | 转投到业务 Stream 后对消费者可见 | 从延时主题恢复到原主题 |
| 适用场景 | 中等规模延时 | 大规模延时（5.x 支持海量任意延时） |

---

## 8. StreamMQ 与 RocketMQ 总体对比

### 8.1 架构层面对比

| 维度 | StreamMQ | RocketMQ |
|---|---|---|
| Broker | Redis（无独立 Broker 进程） | 独立 Broker 进程 |
| 注册中心 | 无（Redis 即中心） | NameServer 集群 |
| 存储引擎 | Redis Stream / Hash / ZSet | CommitLog + ConsumeQueue + IndexFile |
| 复制 | Redis 主从 / 集群 | Broker 主从同步/异步复制 |
| 客户端 | Redisson（Java） | 自研 Remoting 协议（多语言 SDK） |
| 部署 | Redis 即可用，零额外组件 | Broker + NameServer 独立部署 |
| 运维 | 复用 Redis 运维体系 | 独立运维体系 |

### 8.2 功能特性对比

| 特性 | StreamMQ | RocketMQ |
|---|---|---|
| 普通消息 | ✅ 同步/异步/oneway | ✅ 同步/异步/oneway |
| 顺序消息 | ✅ 消费端分片锁 | ✅ 生产端 queue 路由 |
| 事务消息 | ✅ 半消息+回查（template 简化版） | ✅ 半消息+回查（生产级） |
| 批量消息 | ✅ RBatch Pipeline | ✅ 批量请求 |
| 延时消息 | ✅ 18 级 + 任意延时（向上取整） | ✅ 18 级 + 5.x 任意延时 |
| 死信队列 | ✅ DLQ Stream | ✅ %DLQ%CONSUMER_GROUP |
| 消息回溯 | ✅ Stream 按 ID 范围读取 | ✅ 按 offset 回溯 |
| 消息过滤 | ✅ Tag（SQL92 子集） | ✅ Tag + SQL92 |
| 消息轨迹 | ✅ TraceCollector | ✅ 消息轨迹 |
| 事务回查 | ✅ TransactionChecker | ✅ TransactionListener |
| 多租户 | ✅ namespace 隔离 | ✅ 租户隔离 |
| 跨语言 | ✅ 跨平台 body（bodyType 回退） | ✅ 多语言 SDK |
| 消息堆积 | ⚠️ 受 Redis 内存限制 | ✅ 磁盘海量堆积 |
| 高可用 | ⚠️ 依赖 Redis 集群 | ✅ Broker 主从切换 |

### 8.3 性能与可靠性对比

| 维度 | StreamMQ | RocketMQ |
|---|---|---|
| 单机吞吐 | ~10w QPS（Redis 单线程瓶颈） | ~10w+ QPS（顺序写磁盘 + 零拷贝） |
| 延迟 | 亚毫秒级（内存操作） | 毫秒级（磁盘 + 网络） |
| 持久化 | RDB/AOF（异步刷盘为主） | 同步/异步刷盘可选 |
| 数据可靠性 | 依赖 Redis 持久化策略 | 同步刷盘 + 同步复制（金融级） |
| 消息丢失风险 | Redis 故障切换可能丢失（异步复制） | 同步复制下零丢失 |
| 消费堆积上限 | Redis 内存上限 | 磁盘容量上限 |

### 8.4 API 风格对比

| API 维度 | StreamMQ | RocketMQ |
|---|---|---|
| 消息构造 | `MessageBuilder.withTopic().body().build()` | `MessageBuilder.withTopic().body().build()` |
| 同步发送 | `template.syncSend(msg)` | `producer.send(msg)` |
| 异步发送 | `template.asyncSend(msg)` / `asyncSend(msg, callback)` | `producer.send(msg, sendCallback)` |
| 单向发送 | `template.sendOneway(msg)` | `producer.sendOneway(msg)` |
| 批量发送 | `template.syncSendBatch(batch)` | `producer.send(batch)` |
| 事务消息 | `template.executeInTransaction(msg, callback)` | `TransactionMQProducer.sendMessageInTransaction(msg, executor, arg)` |
| 延时消息 | `MessageBuilder.delayLevel(level)` | `message.setDelayTimeLevel(level)` |
| 消费者注解 | `@StreamMQConsumer` | `@RocketMQMessageListener` |
| 事务回查注解 | `@StreamMQTransactionConsumer` | 无注解（实现 `TransactionListener`） |
| 消费返回值 | `ConsumeAction` / `OrderlyAction` | `ConsumeConcurrentlyStatus` / `ConsumeOrderlyStatus` |

---

## 9. 附录：Redis Key 与数据结构总览

### 9.1 全部 Redis Key 清单

| Key 格式 | 数据结构 | 用途 | 涉及消息类型 |
|---|---|---|---|
| `streammq:{ns}:msg:{topic}` | Stream | 业务消息主存储 | 普通/顺序/一次/批量/延时投递后/事务COMMIT后 |
| `streammq:{ns}:msg:{topic}:shard{shardId}` | Stream | 顺序消息物理分片（预留，未启用） | 顺序（预留） |
| `streammq:{ns}:cg:{group}:instances` | Hash | 消费组实例列表 | 所有消费 |
| `streammq:{ns}:cg:{group}:semaphore` | String | 消费组信号量 | 所有消费 |
| `streammq:{ns}:cg:{group}:assignment` | Hash | 消费组分片分配 | 所有消费 |
| `streammq:{ns}:cg:{group}:notify` | PubSub | 消费组通知频道 | 所有消费 |
| `streammq:{ns}:retry:{topic}:{group}` | ZSet | 重试队列（score=nextRetryAt） | 普通/一次/批量（消费失败重试） |
| `streammq:{ns}:dlq:{topic}:{group}` | Stream | 死信队列 | 普通/一次/批量（重试超限） |
| `streammq:{ns}:retry:{topic}:{group}:transfer:lock` | String | 重试转移降级锁 | 重试转移 |
| `streammq:{ns}:delay:{level}` | ZSet | 延时调度队列（score=deliverAt） | 延时 |
| `streammq:{ns}:delay:payload:{msgId}` | Hash | 延时消息 payload 暂存（也复用为重试 payload） | 延时/重试 |
| `streammq:{ns}:delay:meta:delivered` | Hash | 延时已投递计数 | 延时（统计） |
| `streammq:{ns}:half:{txGroup}` | Stream | 半消息暂存 | 事务 |
| `streammq:{ns}:txstate:{txGroup}` | Hash | 事务状态（PREPARE/COMMIT/ROLLBACK/UNKNOWN） | 事务 |
| `streammq:{ns}:txcheck:{txGroup}` | ZSet | 事务回查调度（score=checkTime） | 事务 |
| `streammq:{ns}:txcheck:{txGroup}:counter` | Hash | 事务回查计数 | 事务 |
| `streammq:{ns}:shardlock:{topic}:{group}:{shardId}` | String（RLock） | 顺序消费分片锁 | 顺序 |
| `streammq:{ns}:meta:offset:{group}:{topic}` | String | 消费位点 | 所有消费 |
| `streammq:{ns}:meta:counter:{group}:{topic}` | Hash | 消费计数 | 所有消费 |
| `streammq:{ns}:meta:stats:{group}:{topic}` | Hash | 运行时统计 | 所有消费 |

### 9.2 数据结构使用统计

| 数据结构 | 使用场景 |
|---|---|
| **Stream** | 业务消息主存储、DLQ、半消息暂存 |
| **Hash** | 消费组元数据、事务状态、回查计数、延时/重试 payload、消费统计 |
| **ZSet** | 延时调度、重试调度、事务回查调度 |
| **String** | 消费位点、分片锁、信号量、降级锁 |
| **PubSub** | 消费组通知频道 |

### 9.3 关键源码文件索引

| 文件路径 | 职责 |
|---|---|
| `streammq-core/.../message/Message.java` | 消息实体（topic/tag/keys/shardingKey/body/delayLevel/transactionId 等） |
| `streammq-core/.../message/BatchMessage.java` | 批量消息包装（同 Topic 约束） |
| `streammq-core/.../message/MessageBuilder.java` | 消息流式构造器 |
| `streammq-core/.../enums/DelayLevel.java` | 18 级延时枚举 |
| `streammq-core/.../StreamMQConstants.java` | 全局常量（超时、重试、回查等默认值） |
| `streammq-core/.../service/DefaultStreamMessageService.java` | 便捷 API 层 |
| `streammq-core/.../template/StreamMessageTemplate.java` | 模板接口（syncSend/asyncSend/sendOneway/syncSendBatch/executeInTransaction） |
| `streammq-core/.../producer/StreamMessageProducer.java` | 生产者接口 |
| `streammq-core/.../annotation/StreamMQConsumer.java` | 消费者注解 |
| `streammq-core/.../annotation/StreamMQTransactionConsumer.java` | 事务回查消费者注解 |
| `streammq-core/.../consumer/StreamMessageConcurrentlyConsumer.java` | 并发消费接口 |
| `streammq-core/.../consumer/StreamMessageOrderlyConsumer.java` | 顺序消费接口 |
| `streammq-core/.../transaction/TransactionCallback.java` | 事务本地执行回调 |
| `streammq-core/.../transaction/TransactionChecker.java` | 事务回查接口 |
| `streammq-redisson-adapter/.../producer/RedissonStreamProducer.java` | 生产者实现（XADD/RBatch/ZADD+HSET） |
| `streammq-redisson-adapter/.../template/DefaultStreamMessageTemplate.java` | 模板实现（拦截器链+重试+事务编排） |
| `streammq-redisson-adapter/.../converter/DefaultMessageConverter.java` | Message ↔ Stream Entry 字段转换 |
| `streammq-redisson-adapter/.../listener/RedissonStreamListener.java` | 消费者实现（XREADGROUP/XACK） |
| `streammq-redisson-adapter/.../container/DefaultStreamMQListenerContainer.java` | 消费容器（虚拟线程消费循环编排） |
| `streammq-redisson-adapter/.../container/DefaultRetryAndDlqHandler.java` | ACK/重试/DLQ 路由处理 |
| `streammq-redisson-adapter/.../container/RedissonOrderlyShardLockManager.java` | 顺序消费分片锁管理 |
| `streammq-redisson-adapter/.../scheduler/DelayMessageScheduler.java` | 延时消息调度器 |
| `streammq-redisson-adapter/.../scheduler/RetryScheduler.java` | 重试消息调度器 |
| `streammq-redisson-adapter/.../scheduler/TransactionScanner.java` | 事务回查调度器 |
| `streammq-redisson-adapter/.../support/StreamMQKeys.java` | Redis Key 命名工具类 |

---

> 本文档基于 StreamMQ 源码分析生成，覆盖 6 类消息的生产端逻辑、存储模型、消费端逻辑、状态流转及与 RocketMQ 的对比。所有 Redis Key 与数据结构均来自 `StreamMQKeys` 与各组件源码。
