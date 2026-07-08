# 核心概念

---

## Topic（主题）

消息的逻辑分类，对应 Redis Stream 的一个 key。

- 生产者将消息发送到指定 Topic
- 消费者从指定 Topic 订阅消息
- 每个 Topic 对应一个 Redis Stream

```java
// 发送到 order-topic
template.syncSend(MessageBuilder.<String>withTopic("order-topic").body("content").build());
```

---

## ConsumerGroup（消费组）

一组消费者的逻辑标识。

**集群消费**：同一消费组内每条消息只被一个实例消费
**广播消费**：同一消费组内每条消息被所有实例消费

```java
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group")
```

---

## Message（消息）

消息载体，包含以下字段：

| 字段 | 说明 | 是否必填 |
|------|------|----------|
| topic | 主题 | 是 |
| tag | 标签，用于过滤 | 否 |
| keys | 业务键，用于幂等 | 否 |
| shardingKey | 分片键，用于顺序消息 | 否 |
| body | 消息体 | 是 |
| delayLevel | 延时级别 | 否 |
| delayTimeMillis | 任意延时毫秒 | 否 |
| properties | 系统属性 | 否 |
| userProperties | 用户属性 | 否 |

---

## MessageId（消息 ID）

消息的唯一标识，对应 Redis Stream Entry ID。

格式：`{timestamp}-{sequence}`

```java
SendResult result = template.syncSend(message);
MessageId msgId = result.getMessageId();
```

---

## ConsumeAction（消费结果）

消费返回值，控制后续流程：

| 动作 | 说明 |
|------|------|
| `SUCCESS` | 消费成功，自动 ACK |
| `RECONSUME_LATER` | 消费失败，按 RetryPolicy 重试 |
| `defer(Duration)` | 消费失败，按指定延迟重试 |

---

## OrderlyAction（顺序消费结果）

顺序消费返回值：

| 动作 | 说明 |
|------|------|
| `SUCCESS` | 消费成功，自动 ACK |
| `SUSPEND_CURRENT_QUEUE_A_MOMENT` | 暂停当前分片，消息留在 PEL |

---

## ShardingKey（分片键）

顺序消息的分片依据，相同 shardingKey 的消息路由到同一分片。

```java
MessageBuilder.<String>withTopic("order-topic")
        .shardingKey("user-123")
        .body("content")
        .build();
```

---

## DelayLevel（延时级别）

18 级固定延时：

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

## Half Message（半消息）

事务消息的中间态，本地事务提交前对消费者不可见。

**事务流程：**
1. 发送半消息到 half Stream
2. 执行本地事务
3. 本地事务成功 → COMMIT（转投到业务 Stream）
4. 本地事务失败 → ROLLBACK（删除半消息）

---

## Transaction Check（事务回查）

事务状态不确定时，框架回查生产者的本地事务状态。

**触发场景：**
- 本地事务返回 UNKNOW
- 网络抖动导致 COMMIT/ROLLBACK 通知丢失
- 服务宕机恢复后，回查未确认的事务

---

## DLQ（死信队列）

消费重试耗尽后的消息队列，用于人工干预。

**DLQ 流程：**
1. 消息消费失败，进入重试队列
2. 重试次数达到 maxReconsumeTimes
3. 消息被转发到 DLQ Stream
4. DLQ 消费者接收并处理

---

## Namespace（命名空间）

全局前缀，用于多租户/多环境隔离。

```yaml
streammq:
  namespace: streammq-dev
```

Redis Key 格式：`streammq:{namespace}:{topic}`

---

## InflightQueue（背压队列）

拉取-处理解耦的内部队列，防止内存溢出。

```yaml
streammq:
  consumer:
    inflight-capacity: 1000
```

---

## ConsumerGroupManager（消费组管理器）

管理消费者实例注册、心跳、活跃列表维护与分片分配。

**核心职责：**
- 实例注册与注销
- 心跳维持
- 活跃实例列表管理
- 分片重平衡