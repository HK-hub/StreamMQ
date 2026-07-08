# 配置参考

---

## 完整配置示例

```yaml
streammq:
  enabled: true
  namespace: streammq
  producer:
    group: default-producer-group
    send-timeout: 3000
    retry-times: 2
  consumer:
    consume-thread-min: 1
    consume-thread-max: 64
    pull-batch-size: 32
    max-reconsume-times: 16
    consume-timeout: 30000
    inflight-capacity: 1000
    pull-interval: 0
  transaction:
    check-interval-ms: 60000
    max-check-times: 15
    batch-size: 32
  dlq:
    enabled: true
    max-retry-times: 3
  metrics:
    enabled: true
  tracing:
    enabled: false
  management:
    enabled: true
```

---

## 全局配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `streammq.enabled` | boolean | true | 是否启用 StreamMQ |
| `streammq.namespace` | string | "" | 全局命名空间 |

---

## 生产者配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `streammq.producer.group` | string | default-producer-group | 默认生产者组 |
| `streammq.producer.send-timeout` | int | 3000 | 发送超时（毫秒） |
| `streammq.producer.retry-times` | int | 2 | 同步发送重试次数 |

---

## 消费者配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `streammq.consumer.consume-thread-min` | int | 1 | 最小消费线程数 |
| `streammq.consumer.consume-thread-max` | int | 64 | 最大消费线程数 |
| `streammq.consumer.pull-batch-size` | int | 32 | 单次拉取批量 |
| `streammq.consumer.max-reconsume-times` | int | 16 | 最大重试次数 |
| `streammq.consumer.consume-timeout` | long | 30000 | 消费超时（毫秒） |
| `streammq.consumer.inflight-capacity` | int | 1000 | 背压队列容量 |
| `streammq.consumer.pull-interval` | long | 0 | 拉取间隔（毫秒） |

---

## 事务配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `streammq.transaction.check-interval-ms` | long | 60000 | 回查间隔（毫秒） |
| `streammq.transaction.max-check-times` | int | 15 | 最大回查次数 |
| `streammq.transaction.batch-size` | int | 32 | 单次扫描批量 |

---

## DLQ 配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `streammq.dlq.enabled` | boolean | true | 是否启用 DLQ |
| `streammq.dlq.max-retry-times` | int | 3 | DLQ 消费最大重试次数 |

---

## 可观测性配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `streammq.metrics.enabled` | boolean | true | 是否启用指标收集 |
| `streammq.tracing.enabled` | boolean | false | 是否启用追踪 |

---

## 管理配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `streammq.management.enabled` | boolean | true | 是否启用管理端点 |

---

## Redisson 配置

Redisson 通过独立配置文件或 Spring Boot 配置进行配置。

### 独立配置文件

创建 `redisson.yml`：

```yaml
singleServerConfig:
  address: "redis://127.0.0.1:6379"
  database: 0
  connectionPoolSize: 64
  connectionMinimumIdleSize: 24
```

### Spring Boot 配置

```yaml
redisson:
  singleServerConfig:
    address: "redis://127.0.0.1:6379"
    database: 0
```

### 集群配置

```yaml
redisson:
  clusterServersConfig:
    nodeAddresses:
      - "redis://127.0.0.1:7001"
      - "redis://127.0.0.1:7002"
      - "redis://127.0.0.1:7003"
```

---

## 顺序消费配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `shardCount` | int | 4 | 分片数 |
| `suspendCurrentQueueTimeMillis` | long | 10 | 挂起时间（毫秒） |

---

## 延时消息配置

延时消息通过 `MessageBuilder` 配置，无需额外的全局配置。

```java
MessageBuilder.<String>withTopic("delay-topic")
        .delayLevel(DelayLevel.LEVEL_9)  // 固定延时
        .build();

MessageBuilder.<String>withTopic("delay-topic")
        .delayTimeMillis(15 * 60 * 1000L)  // 任意延时
        .build();
```

---

## 消费模式配置

### 集群消费（默认）

```java
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    consumeMode = ConsumeMode.CLUSTERING
)
```

### 广播消费

```java
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    consumeMode = ConsumeMode.BROADCASTING
)
```

---

## Tag 过滤配置

```java
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    selectorExpression = "tag1 || tag2"
)
```

支持的表达式：
- `*` - 匹配所有
- `tag1` - 匹配 tag1
- `tag1 || tag2` - 匹配 tag1 或 tag2
- `tag1 && tag2` - 匹配 tag1 且 tag2