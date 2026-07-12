# StreamMQ V2.0 功能设计文档

> 配套 PRD：[01-PRD-v2.0.md](./01-PRD-v2.0.md)　架构设计：[02-architecture-v2.0.md](./02-architecture-v2.0.md)
> 本文档定义 StreamMQ V2.0 对外暴露的全部功能 API 与内部 SPI 接口签名。

| 字段 | 内容 |
|---|---|
| 文档版本 | v2.0-draft |
| 状态 | 规划中 |
| 创建日期 | 2026-07-10 |
| 基线版本 | StreamMQ v1.0 GA |
| 文档语言 | 中文（Javadoc/注释中文，标识符英文） |

---

## 目录

1. [V2.0 API 变更概览](#1-v20-api-变更概览)
2. [多后端配置 API](#2-多后端配置-api)
3. [BackendProvider SPI 详细设计](#3-backendprovider-spi-详细设计)
4. [跨机房复制 API](#4-跨机房复制-api)
5. [Kafka 线网协议 API](#5-kafka-线网协议-api)
6. [Spring Cloud Stream Binder API](#6-spring-cloud-stream-binder-api)
7. [云原生 API](#7-云原生-api)
8. [持久化抽象 API](#8-持久化抽象-api)
9. [追踪增强 API](#9-追踪增强-api)
10. [消息画像与诊断 API](#10-消息画像与诊断-api)

---

## 1. V2.0 API 变更概览

### 1.1 向后兼容保障

V2.0 严格遵守 SemVer，所有 V1.x API 保持不变：

| API | V1.x | V2.0 | 兼容性 |
|-----|------|------|--------|
| `@EnableStreamMQ` | ✅ | ✅ 不变 | 100% |
| `@StreamMQConsumer` | ✅ | ✅ 不变 | 100% |
| `StreamMessageTemplate` | ✅ | ✅ 新增 default 方法 | 100% |
| `Message<T>` / `MessageBuilder` | ✅ | ✅ 不变 | 100% |
| `StreamMQConsumer` 注解属性 | ✅ | ✅ 不变 | 100% |
| 配置 `streammq.*` | ✅ | ✅ 新增 `backend.*` | 100% |

### 1.2 V2.0 新增 API 一览

| API | 类型 | 描述 |
|-----|------|------|
| `BackendProvider` | SPI | 后端提供者接口 |
| `BackendProducer` / `BackendConsumer` | SPI | 后端生产者/消费者 |
| `BackendCapabilities` | Record | 后端能力描述 |
| `ScheduleStore` | SPI | 调度存储抽象 |
| `DlqStore` | SPI | 死信存储抽象 |
| `ReplicationProvider` | SPI | 跨机房复制 |
| `PersistenceProvider` | SPI | 持久化存储 |
| `StreamMQMessageBinder` | Binder | Spring Cloud Stream Binder |
| `StreamMQCluster` | CRD | K8s 集群资源 |
| `MessageTrace` | API | 消息全链路追踪 |

---

## 2. 多后端配置 API

### 2.1 配置属性

```yaml
streammq:
  enabled: true
  # V2.0 新增：后端类型选择
  backend:
    type: redis  # redis / kafka / rabbitmq / pulsar (默认 redis，V1.x 行为)

    # Redis 后端配置（默认，与 V1.x 完全兼容）
    redis:
      namespace: "streammq"
      redisson:
        config: classpath:redisson.yaml

    # Kafka 后端配置
    kafka:
      bootstrap-servers: localhost:9092
      producer:
        acks: all
        retries: 3
        batch-size: 16384
        linger-ms: 10
        max-request-size: 1048576
      consumer:
        auto-offset-reset: latest
        enable-auto-commit: false
        max-poll-records: 500
        session-timeout-ms: 30000

    # RabbitMQ 后端配置
    rabbitmq:
      host: localhost
      port: 5672
      virtual-host: /
      username: guest
      password: guest
      publisher-confirms: true
      publisher-returns: true
      listener:
        acknowledge-mode: manual
        prefetch: 100
```

### 2.2 多后端共存

V2.0 支持同一应用使用多个后端（不同 Topic 使用不同后端）：

```yaml
streammq:
  backend:
    type: redis  # 默认后端
    redis:
      namespace: "streammq"

    # 多后端路由规则
    routes:
      - topic-pattern: "kafka-*"
        backend: kafka
      - topic-pattern: "amqp-*"
        backend: rabbitmq
      - topic-pattern: "*"
        backend: redis  # 兜底

    kafka:
      bootstrap-servers: kafka-cluster:9092
    rabbitmq:
      host: rabbitmq-cluster
```

### 2.3 @StreamMQConsumer 后端指定

```java
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    backend = "kafka"  // V2.0 新增：指定后端（可选，默认使用全局配置）
)
public class OrderConsumer implements StreamMQListener<Order> {
    @Override
    public ConsumeAction onMessage(Message<Order> message, ConsumeContext context) {
        // ...
        return ConsumeAction.SUCCESS;
    }
}
```

---

## 3. BackendProvider SPI 详细设计

### 3.1 BackendProvider 接口

```java
package io.github.streammq.backend.spi;

/**
 * 后端提供者 SPI，抽象不同 MQ 后端（Redis / Kafka / RabbitMQ / Pulsar）。
 *
 * <p>实现类通过 {@code META-INF/services/io.github.streammq.backend.spi.BackendProvider}
 * 自动注册，由 {@link BackendManager} 根据 {@code streammq.backend.type} 选择激活。
 *
 * @author StreamMQ Contributors
 * @since 2.0.0
 */
public interface BackendProvider extends AutoCloseable {

    /** 后端标识（如 "redis" / "kafka" / "rabbitmq"） */
    String name();

    /** 初始化后端 */
    void initialize(BackendConfig config);

    /** 创建生产者 */
    BackendProducer createProducer(BackendProducerConfig config);

    /** 创建消费者 */
    BackendConsumer createConsumer(BackendConsumerConfig config);

    // === Topic 管理 ===
    void createTopic(String topic, int partitions);
    void deleteTopic(String topic);
    boolean topicExists(String topic);
    List<String> listTopics();

    // === ConsumerGroup 管理 ===
    void createConsumerGroup(String topic, String group);
    void deleteConsumerGroup(String topic, String group);
    long getPendingCount(String topic, String group);
    List<PendingMessage> listPending(String topic, String group, int count);

    // === 消息管理 ===
    void ack(String topic, String group, String messageId);
    void deleteMessage(String topic, String messageId);

    // === 能力 ===
    BackendCapabilities capabilities();

    // === 可选能力 ===
    default Optional<TransactionBackend> createTransactionBackend() {
        return Optional.empty();
    }

    default Optional<DelayBackend> createDelayBackend() {
        return Optional.empty();
    }

    default Optional<ScheduleStore> createScheduleStore() {
        return Optional.empty();
    }

    default Optional<DlqStore> createDlqStore() {
        return Optional.empty();
    }
}
```

### 3.2 BackendProducer 接口

```java
public interface BackendProducer extends AutoCloseable {
    SendResult send(Message<?> message, long timeoutMillis);
    CompletableFuture<SendResult> asyncSend(Message<?> message);
    List<SendResult> batchSend(List<Message<?>> messages);
    void sendOneway(Message<?> message);
}
```

### 3.3 BackendConsumer 接口

```java
public interface BackendConsumer extends AutoCloseable {
    List<BackendMessage> poll(long timeoutMillis, int maxCount);
    void ack(String messageId);
    void nack(String messageId);
    void seek(String messageId);
    void pause();
    void resume();
}
```

### 3.4 BackendCapabilities

```java
public record BackendCapabilities(
    boolean supportsBroadcasting,
    boolean supportsOrderly,
    boolean supportsTransaction,
    boolean supportsDelay,
    boolean supportsBatch,
    boolean supportsPull,
    boolean supportsSharding,
    boolean supportsCompression,
    int maxMessageSize,
    long maxTopicCount,
    int maxPartitionsPerTopic
) {
    public void requireOrderly() {
        if (!supportsOrderly) {
            throw new UnsupportedOperationException("Backend does not support orderly messages");
        }
    }
    // ... 其他 require 方法
}
```

### 3.5 Redis Backend 实现要点

| 接口方法 | Redis 实现方案 |
|---------|---------------|
| `createProducer` | 包装 `RedissonStreamProducer` |
| `createConsumer` | 包装 `RedissonStreamListener` |
| `createTopic` | `redisson.getStream(key).createGroup(group)` |
| `deleteTopic` | `redisson.getStream(key).delete()` |
| `getPendingCount` | `stream.listPending(group, ...).size()` |
| `ack` | `stream.ack(group, streamMessageId)` |
| `createScheduleStore` | ZSet 实现（复用 V1.x 逻辑） |
| `createDlqStore` | DLQ Stream 实现（复用 V1.x 逻辑） |

### 3.6 Kafka Backend 实现要点

| 接口方法 | Kafka 实现方案 |
|---------|---------------|
| `createProducer` | 包装 `KafkaProducer` |
| `createConsumer` | 包装 `KafkaConsumer` |
| `createTopic` | `AdminClient.createTopics()` |
| `deleteTopic` | `AdminClient.deleteTopics()` |
| `getPendingCount` | lag = `endOffset - committedOffset` |
| `ack` | `consumer.commitSync()` |
| `createScheduleStore` | 内部 Topic + 时间轮（降级方案） |
| `createDlqStore` | DLQ Topic 实现 |
| `createTransactionBackend` | `KafkaProducer.initTransactions()` |

---

## 4. 跨机房复制 API

### 4.1 ReplicationProvider SPI

```java
package io.github.streammq.replication;

public interface ReplicationProvider {
    void startReplication(ReplicationConfig config);
    void stopReplication();
    ReplicationStatus getStatus();
    void fullSync(String topic);
    List<ReplicationLag> getLagDetails();
}

public record ReplicationConfig(
    String sourceBackendType,
    String targetBackendType,
    BackendConfig sourceConfig,
    BackendConfig targetConfig,
    ReplicationMode mode,
    List<String> topics,
    long syncIntervalMs,
    int maxRetries
) {}

public enum ReplicationMode {
    ASYNC,       // 异步复制，RPO ≤ 1s
    SYNC,        // 同步复制，RPO = 0
    BIDIRECTIONAL // 双活，双向复制
}
```

### 4.2 配置

```yaml
streammq:
  replication:
    enabled: false
    mode: async  # async / sync / bidirectional
    source:
      backend: redis
      namespace: streammq-dc1
    target:
      backend: redis
      namespace: streammq-dc2
    topics:
      - "order-*"
      - "payment-*"
    sync-interval-ms: 1000
    max-retries: 5
```

### 4.3 复制状态查询

```java
@Autowired
private ReplicationProvider replicationProvider;

ReplicationStatus status = replicationProvider.getStatus();
// status.running() → true
// status.lagMessages() → 42
// status.lastSyncTimestamp() → 1720000000000
// status.totalReplicated() → 1000000
// status.failedCount() → 3
```

---

## 5. Kafka 线网协议 API

### 5.1 启动 Protocol Server

```yaml
streammq:
  kafka-protocol:
    enabled: false
    port: 9092
    bind-address: 0.0.0.0
    max-connections: 1000
    request-timeout-ms: 30000
    # 底层后端
    backend: redis  # 将 Kafka 协议请求转发到 Redis 后端
```

### 5.2 原生 Kafka Client 接入

```java
// 零代码修改，仅修改 bootstrap-servers 指向 StreamMQ Protocol Server
Properties props = new Properties();
props.put("bootstrap.servers", "streammq-protocol:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "key", "value");
producer.send(record);
```

### 5.3 支持的 Kafka API

| API | 方法 | 支持状态 |
|-----|------|---------|
| Produce | v0-v9 | ✅ |
| Fetch | v0-v9 | ✅ |
| ListOffsets | v0-v3 | ✅ |
| Metadata | v0-v7 | ✅ |
| OffsetCommit | v0-v7 | ✅ |
| OffsetFetch | v0-v6 | ✅ |
| FindCoordinator | v0-v3 | ✅ |
| JoinGroup | v0-v7 | ✅ |
| Heartbeat | v0-v4 | ✅ |
| LeaveGroup | v0-v3 | ✅ |
| SyncGroup | v0-v3 | ✅ |
| DescribeGroups | v0-v3 | ✅ |
| ListGroups | v0-v2 | ✅ |
| CreateTopics | v0-v4 | ✅ |
| DeleteTopics | v0-v4 | ✅ |
| DescribeConfigs | v0-v2 | ⚠️ v2.1+ |

---

## 6. Spring Cloud Stream Binder API

### 6.1 配置

```yaml
spring:
  cloud:
    stream:
      bindings:
        orderOutput:
          destination: order-topic
          producer:
            partition-key-expression: headers['partitionKey']
            partition-count: 8
        orderInput:
          destination: order-topic
          group: order-consumer-group
          consumer:
            concurrency: 3
            max-attempts: 3
            back-off-initial-interval: 1000
      streammq:
        binder:
          backend: redis
          namespace: streammq
```

### 6.2 函数式编程模型

```java
@Bean
public Function<Order, Shipment> processOrder() {
    return order -> {
        // 处理订单，返回发货信息
        return new Shipment(order.getId());
    };
}

@Bean
public Consumer<Payment> processPayment() {
    return payment -> {
        // 处理支付
    };
}

@Bean
public Supplier<Inventory> supplyInventory() {
    return () -> new Inventory(UUID.randomUUID().toString());
}
```

### 6.3 Binder 实现类

```java
public class StreamMQMessageBinder
        extends AbstractMessageBinder<
            ConsumerDestination,
            ProducerDestination,
            StreamMQConsumerProperties,
            StreamMQProducerProperties>
        implements EmbeddedHeadersSupport {

    @Override
    protected MessageProducer createConsumer(
            ConsumerDestination destination, String group,
            StreamMQConsumerProperties consumerProperties) {
        // 创建 StreamMQ Consumer 并包装为 Spring Integration MessageProducer
    }

    @Override
    protected MessageHandler createProducer(
            ProducerDestination destination,
            StreamMQProducerProperties producerProperties) {
        // 创建 StreamMQ Template 并包装为 Spring Integration MessageHandler
    }
}
```

---

## 7. 云原生 API

### 7.1 K8s CRD 定义

```yaml
# StreamMQCluster - 集群定义
apiVersion: streammq.io/v1
kind: StreamMQCluster
metadata:
  name: production-streammq
spec:
  backend:
    type: redis
    redis:
      address: redis://redis-cluster:6379
      namespace: production
  config:
    retry:
      max-reconsume-times: 16
    delay:
      enabled: true
    tracing:
      enabled: true

---
# StreamMQTopic - Topic 管理
apiVersion: streammq.io/v1
kind: StreamMQTopic
metadata:
  name: order-topic
spec:
  clusterRef: production-streammq
  partitions: 8
  max-length: 1000000
  retention-hours: 72

---
# StreamMQConsumerGroup - 消费者组管理
apiVersion: streammq.io/v1
kind: StreamMQConsumerGroup
metadata:
  name: order-consumer-group
spec:
  clusterRef: production-streammq
  topic: order-topic
  replicas: 3
  autoScale:
    enabled: true
    minReplicas: 2
    maxReplicas: 10
    targetLag: 100
    scaleUpThreshold: 80
    scaleDownThreshold: 20
```

### 7.2 优雅上下线

```java
// V2.0 新增：K8s 探针支持
@RestController
@RequestMapping("/streammq/health")
public class StreamMQHealthController {

    @GetMapping("/liveness")
    public ResponseEntity<Map<String, Object>> liveness() {
        Map<String, Object> health = new LinkedHashMap<>();
        health.put("status", "UP");
        health.put("backend", backendManager.getActiveProvider().name());
        return ResponseEntity.ok(health);
    }

    @GetMapping("/readiness")
    public ResponseEntity<Map<String, Object>> readiness() {
        Map<String, Object> ready = new LinkedHashMap<>();
        ready.put("ready", container.isRunning());
        ready.put("consumers", container.getConsumerCount());
        return ResponseEntity.ok(ready);
    }
}
```

### 7.3 配置热更新

```java
// V2.0 新增：运行时配置变更
public interface StreamMQConfigRefresher {
    /** 刷新重试配置 */
    void refreshRetryPolicy(int maxReconsumeTimes, long[] retryIntervals);
    /** 刷新消费线程数 */
    void refreshConsumerThreads(int min, int max);
    /** 刷新扫描间隔 */
    void refreshScanInterval(long retryScanMs, long delayScanMs);
}
```

---

## 8. 持久化抽象 API

### 8.1 PersistenceProvider SPI

```java
public interface PersistenceProvider {
    /** 写入消息 */
    void write(String topic, Message<?> message);
    /** 读取消息 */
    Message<?> read(String topic, String messageId);
    /** 范围读取 */
    List<Message<?>> readRange(String topic, long startSeq, int count);
    /** 删除消息 */
    void delete(String topic, String messageId);
    /** 获取 Topic 大小 */
    long size(String topic);
    /** 清空 Topic */
    void clear(String topic);
}
```

### 8.2 实现选项

| 实现 | 存储 | 延迟 | 堆积上限 | 适用场景 |
|------|------|------|---------|---------|
| RedisPersistence | Redis 内存 | < 1ms | < 100w | 低延迟 |
| RocksDBPersistence | 磁盘 | ~5ms | < 1亿 | 大堆积 |
| KafkaPersistence | Kafka | ~10ms | 无限 | 高吞吐 |

### 8.3 配置

```yaml
streammq:
  persistence:
    type: redis  # redis / rocksdb / kafka
    rocksdb:
      data-dir: /data/streammq/rocksdb
      write-buffer-size: 67108864
      max-write-buffer-number: 3
      target-file-size-base: 67108864
```

---

## 9. 追踪增强 API

### 9.1 OpenTelemetry 集成

```java
// V2.0 自动注入 OpenTelemetry Span
@Autowired
private StreamMQTracing tracing;

// 消息发送时自动创建 Span
tracing.injectProducerSpan(message);
template.syncSend(message);

// 消费时自动提取 Span
@StreamMQConsumer(topic = "order-topic")
public class OrderConsumer implements StreamMQListener<Order> {
    @Override
    public ConsumeAction onMessage(Message<Order> message, ConsumeContext ctx) {
        // Span 自动从 message properties 中提取
        // 消费完成后自动记录 Span
        return ConsumeAction.SUCCESS;
    }
}
```

### 9.2 消息拓扑图 API

```java
@Autowired
private StreamMQTopologyService topologyService;

// 获取 Topic 的消费拓扑
TopologyGraph graph = topologyService.getTopicTopology("order-topic");
// graph.getProducers() → 生产者列表
// graph.getConsumers() → 消费者列表
// graph.getRoutes() → 消息路由路径

// 获取消息的完整链路
MessageTrace trace = topologyService.getMessageTrace("1234567890-0");
// trace.getEvents() → [SEND, DELIVER, CONSUME, ACK]
// trace.getDuration() → 15ms
// trace.getPath() → Producer → Topic → ConsumerGroup → Consumer
```

### 9.3 Span 定义

| Span Name | 操作 | 属性 |
|-----------|------|------|
| `streammq.producer.send` | 发送消息 | topic, tag, messageId, success, duration |
| `streammq.consumer.consume` | 消费消息 | topic, group, messageId, success, duration, reconsumeTimes |
| `streammq.scheduler.retry` | 重试调度 | topic, group, messageId, retryCount |
| `streammq.scheduler.delay` | 延时投递 | topic, delayLevel, messageId |
| `streammq.dlq.route` | 死信路由 | topic, group, messageId, reason |

---

## 10. 消息画像与诊断 API

### 10.1 消息画像

```java
@Autowired
private MessageProfileService profileService;

// 获取消息画像
MessageProfile profile = profileService.getProfile("1234567890-0");
// profile.getBornTimestamp() → 消息创建时间
// profile.getSendDuration() → 发送耗时
// profile.getConsumeHistory() → 消费历史列表
// profile.getRetryCount() → 重试次数
// profile.getFinalStatus() → SUCCESS / DLQ / PROCESSING
// profile.getRoutePath() → 完整路由路径
```

### 10.2 异常诊断

```java
@Autowired
private StreamMQDiagnosticsService diagnosticsService;

// 诊断消费慢
SlowConsumeReport report = diagnosticsService.diagnoseSlowConsume("order-topic", "order-group");
// report.getConsumeRate() → 消费速率
// report.getProduceRate() → 生产速率
// report.getAvgConsumeTime() → 平均消费耗时
// report.getThreadPoolStatus() → 线程池状态
// report.getBottleneck() → 瓶颈分析

// 诊断消息堆积
BacklogReport backlog = diagnosticsService.diagnoseBacklog("order-topic");
// backlog.getCurrentBacklog() → 当前堆积量
// backlog.getGrowthRate() → 堆积增长率
// backlog.getEstimatedClearTime() → 预计清空时间
// backlog.getRecommendation() → 建议操作

// 诊断 DLQ
DlqReport dlq = diagnosticsService.diagnoseDlq("order-group");
// dlq.getTotalDlqCount() → DLQ 总量
// dlq.getTopFailureReasons() → 高频失败原因
// dlq.getTopFailedTopics() → 高频失败 Topic
```

---

## 附录

### A. 配置属性完整清单

| 配置项 | 类型 | 默认值 | 版本 | 说明 |
|--------|------|--------|------|------|
| `streammq.backend.type` | String | redis | 2.0 | 后端类型 |
| `streammq.backend.redis.namespace` | String | streammq | 2.0 | Redis 命名空间 |
| `streammq.backend.kafka.bootstrap-servers` | String | - | 2.0 | Kafka 地址 |
| `streammq.backend.routes` | List | - | 2.0 | 多后端路由 |
| `streammq.replication.enabled` | boolean | false | 2.0 | 复制开关 |
| `streammq.replication.mode` | Enum | async | 2.0 | 复制模式 |
| `streammq.kafka-protocol.enabled` | boolean | false | 2.0 | 协议服务开关 |
| `streammq.kafka-protocol.port` | int | 9092 | 2.0 | 协议服务端口 |
| `streammq.persistence.type` | String | redis | 2.0 | 持久化类型 |
| `streammq.tracing.otlp-endpoint` | String | - | 2.0 | OTLP 上报地址 |

### B. 变更记录

| 版本 | 日期 | 变更 |
|------|------|------|
| v2.0-draft | 2026-07-10 | 初稿建立，含多后端、复制、线网协议、云原生、持久化、追踪、诊断 API 设计 |
