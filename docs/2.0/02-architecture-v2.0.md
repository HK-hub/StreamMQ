# StreamMQ V2.0 架构设计文档

> 配套 PRD：[01-PRD-v2.0.md](./01-PRD-v2.0.md)
> 本文档定义 StreamMQ V2.0 的整体架构、模块划分、后端抽象层设计、扩展点。

| 字段 | 内容 |
|---|---|
| 文档版本 | v2.0-draft |
| 状态 | 规划中 |
| 创建日期 | 2026-07-10 |
| 基线版本 | StreamMQ v1.0 GA |
| 技术栈 | JDK 21+ / Spring Boot 3.3.x+ / Redisson 3.34.x+ / Redis 7.2+ / Kafka 3.6+ |

---

## 目录

1. [架构概述](#1-架构概述)
2. [V1.x → V2.0 架构演进](#2-v1x--v20-架构演进)
3. [多后端抽象层设计](#3-多后端抽象层设计)
4. [模块划分](#4-模块划分)
5. [核心组件设计](#5-核心组件设计)
6. [跨机房复制架构](#6-跨机房复制架构)
7. [Kafka 线网协议架构](#7-kafka-线网协议架构)
8. [云原生架构](#8-云原生架构)
9. [SPI 扩展点总览](#9-spi-扩展点总览)
10. [关键技术决策](#10-关键技术决策)

---

## 1. 架构概述

StreamMQ V2.0 采用 **6 层分层 + 横切关注点** 架构，在 V1.x 的 5 层基础上新增 **后端抽象层**：

```
┌─────────────────────────────────────────────────────────────────┐
│  API 层 (streammq-api)                                          │
│  @EnableStreamMQ / @StreamMQConsumer / StreamMessageTemplate    │
├─────────────────────────────────────────────────────────────────┤
│  编排层 (streammq-spring-boot-starter)                           │
│  自动装配 / 配置加载 / Lifecycle 管理 / Actuator                  │
├─────────────────────────────────────────────────────────────────┤
│  Core 层 (streammq-core)                                        │
│  Message 模型 / Producer/Consumer 抽象 / 序列化 SPI / Filter     │
├─────────────────────────────────────────────────────────────────┤
│  ★ 后端抽象层 (streammq-backend-spi) ★ —— V2.0 新增              │
│  BackendProvider / BackendCapabilities / 统一适配接口             │
├─────────────────────────────────────────────────────────────────┤
│  后端实现层 (streammq-redisson / streammq-kafka-backend / ...)   │
│  Redis Backend / Kafka Backend / RabbitMQ Backend               │
├─────────────────────────────────────────────────────────────────┤
│  底层客户端 (Redisson / Kafka Client / RabbitMQ Client)          │
└─────────────────────────────────────────────────────────────────┘

横切关注点：配置 / 可观测性 / 异常处理 / 安全 / 复制 / 云原生
```

---

## 2. V1.x → V2.0 架构演进

### 2.1 V1.x 架构局限

```
API 层 → Core 层 → Redisson Adapter → Redisson → Redis
                                    ↑
                              强绑定 Redis Stream
```

V1.x 的 `streammq-redisson` 模块直接实现了 Core 层的所有抽象（Producer / Consumer / Scheduler / Handler），导致：
- Core 层接口设计受 Redis Stream 特性影响（如 ConsumerGroup / PEL 概念）
- 无法替换底层为 Kafka / RabbitMQ 而不修改 Core
- 延时消息/重试消息的实现绑定 Redis ZSet

### 2.2 V2.0 演进策略

```
API 层 → Core 层 → Backend SPI → Redis Backend / Kafka Backend / RabbitMQ Backend
                ↑                    ↓              ↓              ↓
              不变               Redisson      Kafka Client   RabbitMQ Client
```

**关键原则**：
1. Core 层 API 保持 V1.x 兼容，V1.x 用户零代码升级
2. 新增 Backend SPI 层，所有后端特定逻辑下沉到 Backend 实现
3. V1.x 的 `streammq-redisson` 重构为 `streammq-redis-backend`，实现 Backend SPI
4. 延时/重试/DLQ 逻辑从后端实现上移到 Core 层（通过 Backend SPI 的通用接口）

### 2.3 向后兼容保障

| V1.x 依赖 | V2.0 兼容方案 |
|-----------|--------------|
| `streammq-core` | API 100% 兼容，新增方法以 default 方法提供 |
| `streammq-redisson` | 保留为兼容模块，内部委托到 `streammq-redis-backend` |
| `streammq-spring-boot-starter` | 配置默认 `backend.type=redis`，行为与 V1.x 一致 |
| `@StreamMQConsumer` 注解 | 不变 |
| `StreamMessageTemplate` | 不变 |
| Redis Key 格式 | 不变 |

---

## 3. 多后端抽象层设计

### 3.1 BackendProvider SPI

```java
public interface BackendProvider extends AutoCloseable {
    /** 后端标识 */
    String name();

    /** 初始化 */
    void initialize(BackendConfig config);

    /** 创建生产者 */
    BackendProducer createProducer(BackendProducerConfig config);

    /** 创建消费者 */
    BackendConsumer createConsumer(BackendConsumerConfig config);

    /** Topic 管理 */
    void createTopic(String topic, int partitions);
    void deleteTopic(String topic);
    boolean topicExists(String topic);

    /** ConsumerGroup 管理 */
    void createConsumerGroup(String topic, String group);
    void deleteConsumerGroup(String topic, String group);
    long getPendingCount(String topic, String group);
    List<PendingMessage> listPending(String topic, String group, int count);

    /** 消息管理 */
    void ack(String topic, String group, String messageId);
    void deleteMessage(String topic, String messageId);

    /** 能力描述 */
    BackendCapabilities capabilities();

    /** 事务支持（可选） */
    default Optional<TransactionBackend> createTransactionBackend() {
        return Optional.empty();
    }

    /** 延时支持（可选） */
    default Optional<DelayBackend> createDelayBackend() {
        return Optional.empty();
    }
}
```

### 3.2 BackendProducer / BackendConsumer

```java
public interface BackendProducer extends AutoCloseable {
    /** 同步发送 */
    SendResult send(Message<?> message, long timeoutMillis);
    /** 异步发送 */
    CompletableFuture<SendResult> asyncSend(Message<?> message);
    /** 批量发送 */
    List<SendResult> batchSend(List<Message<?>> messages);
    /** 单向发送 */
    void sendOneway(Message<?> message);
}

public interface BackendConsumer extends AutoCloseable {
    /** 拉取消息（PUSH 模式由内部线程驱动） */
    List<BackendMessage> poll(long timeoutMillis, int maxCount);
    /** 确认消息 */
    void ack(String messageId);
    /** 回退消息（重新投递） */
    void nack(String messageId);
    /** 关闭消费者 */
    void close();
}
```

### 3.3 能力矩阵与降级策略

```java
public record BackendCapabilities(
    boolean supportsBroadcasting,
    boolean supportsOrderly,
    boolean supportsTransaction,
    boolean supportsDelay,
    boolean supportsBatch,
    boolean supportsPull,
    boolean supportsSharding,
    int maxMessageSize,
    long maxTopicCount
) {
    /** 检查能力，不满足时抛异常 */
    public void require(String feature, boolean supported) {
        if (!supported) {
            throw new UnsupportedOperationException(
                "Backend does not support: " + feature);
        }
    }
}
```

| 能力 | Redis | Kafka | RabbitMQ | 降级策略 |
|------|-------|-------|----------|---------|
| 广播消费 | ✅ | ✅ | ✅ | - |
| 分区顺序 | ✅ shard | ✅ Partition | ✅ Queue | - |
| 事务消息 | ✅ 半消息 | ✅ Tx Producer | ❌ | 降级为本地事务 |
| 延时消息 | ✅ ZSet | ❌ | ✅ DLX+TTL | Kafka 降级为外部 ZSet |
| 批量发送 | ✅ RBatch | ✅ Batch | ⚠️ | 降级为循环发送 |
| 自动 Rebalance | ✅ | ✅ | ✅ | - |

### 3.4 配置与自动发现

```yaml
streammq:
  backend:
    type: redis  # redis / kafka / rabbitmq / pulsar
    # 后端特定配置
    redis:
      namespace: "streammq"
      redisson:
        config: classpath:redisson.yaml
    kafka:
      bootstrap-servers: localhost:9092
      producer:
        acks: all
        retries: 3
        batch-size: 16384
      consumer:
        auto-offset-reset: latest
        enable-auto-commit: false
```

后端通过 `META-INF/services/io.github.streammq.backend.spi.BackendProvider` 自动注册，Spring Boot Auto-Configuration 根据 `streammq.backend.type` 选择对应 Provider。

---

## 4. 模块划分

### 4.1 V2.0 Maven 模块树

```
streammq
├── streammq-bom                          # BOM（依赖版本管理）
├── streammq-core                         # Core 层（不变，新增 Backend SPI 依赖）
├── streammq-backend-spi                  # ★ V2.0 新增：后端抽象层
├── streammq-redis-backend               # ★ V2.0 新增：Redis 后端（从 redisson 重构）
├── streammq-redisson                     # V1.x 兼容（委托到 redis-backend）
├── streammq-kafka-backend               # ★ V2.0 新增：Kafka 后端
├── streammq-rabbitmq-backend            # ★ V2.0 新增：RabbitMQ 后端（v2.1+）
├── streammq-kafka-protocol              # ★ V2.0 新增：Kafka 线网协议 Server
├── streammq-spring-boot-starter          # Spring Boot Starter（增强多后端配置）
├── streammq-spring-cloud-stream-binder  # ★ V2.0 新增：Spring Cloud Stream Binder
├── streammq-replication                 # ★ V2.0 新增：跨机房复制
├── streammq-cloud-k8s                   # ★ V2.0 新增：K8s Operator
├── streammq-persistence-spi             # ★ V2.0 新增：持久化抽象
├── streammq-tracing                     # ★ V2.0 新增：追踪增强
├── streammq-samples                      # 示例代码
└── streammq-test                         # 测试工具
```

### 4.2 模块依赖关系

```
                    streammq-bom
                        |
                    streammq-core
                   /      |      \
    streammq-backend-spi  |   streammq-test
         /     |     \    |
redis-backend kafka-backend rabbitmq-backend
      \        |        /
       \       |       /
    streammq-spring-boot-starter
         /              \
  kafka-protocol   cloud-stream-binder
```

---

## 5. 核心组件设计

### 5.1 BackendManager

```java
public class BackendManager {
    private final Map<String, BackendProvider> providers = new ConcurrentHashMap<>();
    private volatile BackendProvider activeProvider;

    /** 注册 BackendProvider */
    public void registerProvider(BackendProvider provider) { ... }

    /** 激活指定后端 */
    public void activate(String backendType, BackendConfig config) {
        BackendProvider provider = providers.get(backendType);
        if (provider == null) {
            throw new IllegalStateException("Backend not found: " + backendType);
        }
        provider.initialize(config);
        this.activeProvider = provider;
    }

    /** 获取当前活跃后端 */
    public BackendProvider getActiveProvider() { return activeProvider; }
}
```

### 5.2 统一调度器抽象

V1.x 的 `RetryScheduler` / `DelayMessageScheduler` / `TransactionScanner` 绑定 Redis ZSet。V2.0 抽象为：

```java
public interface ScheduleStore {
    /** 添加延时任务 */
    void schedule(String key, long executeAt, byte[] payload);
    /** 扫描到期任务 */
    List<ScheduledTask> scanExpired(int batchSize);
    /** 移除任务 */
    void remove(String key);
    /** 任务失败重试 */
    void reschedule(String key, long executeAt, byte[] payload);
}
```

Redis 实现：基于 ZSet；Kafka 实现：基于内部 Topic + 时间轮。

### 5.3 统一 DLQ 抽象

```java
public interface DlqStore {
    /** 写入死信 */
    void writeDlq(String group, Message<?> message, String reason);
    /** 读取死信列表 */
    List<DlqEntry> listDlq(String group, int count);
    /** 重投死信 */
    void requeueDlq(String group, String messageId, String targetTopic);
    /** 删除死信 */
    void deleteDlq(String group, String messageId);
    /** 死信计数 */
    long countDlq(String group);
}
```

---

## 6. 跨机房复制架构

### 6.1 复制拓扑

```
                ┌─────────────────────────────┐
                │      ReplicationManager      │
                │  (ReplicationProvider SPI)   │
                └──────────┬──────────────────┘
                           │
           ┌───────────────┼───────────────┐
           │               │               │
    ┌──────┴──────┐ ┌──────┴──────┐ ┌──────┴──────┐
    │ Redis Replica│ │ Kafka MM2   │ │ Self-Repl   │
    │ (Redisson)  │ │ (MirrorMaker)│ │ (Tail + Apply)│
    └─────────────┘ └─────────────┘ └─────────────┘
```

### 6.2 复制 SPI

```java
public interface ReplicationProvider {
    void startReplication(ReplicationConfig config);
    void stopReplication();
    ReplicationStatus getStatus();
    void fullSync(String topic);

    record ReplicationConfig(
        String sourceBackend,
        String targetBackend,
        ReplicationMode mode,  // ASYNC / SYNC / BIDIRECTIONAL
        List<String> topics,
        long syncIntervalMs
    ) {}

    record ReplicationStatus(
        boolean running,
        long lagMessages,
        long lastSyncTimestamp,
        long totalReplicated,
        long failedCount
    ) {}
}
```

### 6.3 数据一致性

| 模式 | 一致性 | 性能 | 适用场景 |
|------|--------|------|---------|
| 异步复制 | 最终一致（RPO ≤ 1s） | 高 | 容灾备份 |
| 同步复制 | 强一致（RPO = 0） | 中 | 金融场景 |
| 双活复制 | 最终一致 + 双写 | 中 | 两机房同时读写 |

---

## 7. Kafka 线网协议架构

### 7.1 整体架构

```
原生 Kafka Client (Producer/Consumer/Connect/Streams)
         │
         │ Kafka Wire Protocol (TCP)
         │
┌────────┴──────────────────────────────────────┐
│         StreamMQ Kafka Protocol Server         │
│         (Netty 4.1, 基于 Kafka Protocol Spec)  │
│                                                │
│  ┌─────────────┐  ┌─────────────┐            │
│  │ API Router  │→ │ Handler     │            │
│  │ (API Key)   │  │ Registry    │            │
│  └─────────────┘  └─────────────┘            │
│  ┌─────────────┐  ┌─────────────┐            │
│  │ Produce     │  │ Fetch       │            │
│  │ Handler     │  │ Handler     │            │
│  └─────────────┘  └─────────────┘            │
│  ┌─────────────┐  ┌─────────────┐            │
│  │ GroupCoord  │  │ TopicAdmin  │            │
│  │ Handler     │  │ Handler     │            │
│  └─────────────┘  └─────────────┘            │
└──────────────────┬────────────────────────────┘
                   │
           BackendManager
                   │
          BackendProvider (Redis / Kafka / RabbitMQ)
```

### 7.2 协议实现范围

| API Key | API 名称 | 实现优先级 | 说明 |
|---------|---------|-----------|------|
| 0 | Produce | P0 | 支持 compression / batching |
| 1 | Fetch | P0 | 支持 follower fetch |
| 2 | ListOffsets | P0 | |
| 3 | Metadata | P0 | |
| 8 | OffsetCommit | P0 | |
| 9 | OffsetFetch | P0 | |
| 10 | FindCoordinator | P0 | |
| 11 | JoinGroup | P0 | Rebalance 协议 |
| 12 | Heartbeat | P0 | |
| 13 | LeaveGroup | P0 | |
| 14 | SyncGroup | P0 | |
| 15 | DescribeGroups | P0 | |
| 16 | ListGroups | P0 | |
| 19 | CreateTopics | P1 | |
| 20 | DeleteTopics | P1 | |
| 32 | DescribeConfigs | P2 | |

---

## 8. 云原生架构

### 8.1 Kubernetes Operator 架构

```
┌─────────────────────────────────────────┐
│           Kubernetes API Server          │
└──────────────┬──────────────────────────┘
               │
┌──────────────┴──────────────────────────┐
│       StreamMQ Operator (Java)          │
│                                         │
│  Reconcile Loop:                        │
│  1. Watch StreamMQCluster CRD           │
│  2. Watch StreamMQTopic CRD             │
│  3. Watch StreamMQConsumerGroup CRD     │
│  4. Reconcile desired vs actual state   │
│  5. Scale Consumer Pods based on lag    │
│  6. Manage Topic lifecycle              │
└─────────────────────────────────────────┘
```

### 8.2 CRD 定义

```yaml
apiVersion: streammq.io/v1
kind: StreamMQCluster
metadata:
  name: my-streammq
spec:
  backend:
    type: redis
    redis:
      address: redis://redis-cluster:6379
      namespace: production
  consumers:
    - topic: order-topic
      group: order-consumer-group
      replicas: 3
      autoScale:
        minReplicas: 2
        maxReplicas: 10
        targetLag: 100
```

### 8.3 优雅上下线流程

```
K8s 发送 SIGTERM
       ↓
StreamMQ Consumer 收到信号
       ↓
1. 停止从 Backend 拉取新消息
2. 等待处理中消息完成（最长 gracePeriodSeconds）
3. ACK 已处理消息
4. 从 ConsumerGroup 注销
5. 从 K8s Endpoints 移除
6. 关闭连接池
7. 退出
```

---

## 9. SPI 扩展点总览

| SPI 接口 | V1.x | V2.0 | 说明 |
|---------|------|------|------|
| MessageSerializer | ✅ | ✅ | 消息序列化 |
| RetryPolicy | ✅ | ✅ | 重试策略 |
| DlqFailureStrategy | ✅ | ✅ | DLQ 失败策略 |
| ConsumerFilter / ProducerFilter | ✅ | ✅ | 消息过滤器 |
| ConsumerInterceptor / ProducerInterceptor | ✅ | ✅ | 拦截器 |
| TraceCollector | ✅ | ✅ | 追踪收集 |
| ManagementAuthenticator | ✅ | ✅ | 管理端点鉴权 |
| RebalanceStrategy | ✅ | ✅ | Rebalance 策略 |
| ConsumerGroupManager | ✅ | ✅ | 消费者组管理 |
| **BackendProvider** | ❌ | ★ 新增 | 后端提供者 |
| **ScheduleStore** | ❌ | ★ 新增 | 调度存储 |
| **DlqStore** | ❌ | ★ 新增 | 死信存储 |
| **ReplicationProvider** | ❌ | ★ 新增 | 跨机房复制 |
| **PersistenceProvider** | ❌ | ★ 新增 | 持久化存储 |
| **CompressionCodec** | ✅ | ✅ | 压缩编解码 |

---

## 10. 关键技术决策

| 编号 | 决策项 | 选定方案 | 理由 |
|------|--------|---------|------|
| D1 | 后端抽象粒度 | BackendProvider 全量接口 | 保证后端可替换的完整性 |
| D2 | V1.x 兼容策略 | redisson 模块委托到 redis-backend | 零代码升级 |
| D3 | Kafka 协议实现 | 基于 Netty 自研 | 避免 Kafka Broker 代码依赖 |
| D4 | 跨机房复制 | SPI + 多实现 | 不同后端复制方式不同 |
| D5 | K8s Operator | Java + Fabric8 K8s Client | 与 Spring Boot 生态一致 |
| D6 | 延时消息后端 | Redis ZSet 优先 | Kafka 不原生支持延时 |
| D7 | 配置热更新 | Spring Cloud Config + ConfigMap | 标准 Spring 方案 |
| D8 | 追踪集成 | OpenTelemetry SDK | 行业标准 |
| D9 | 持久化抽象 | SPI + RocksDB/LevelDB | 可选磁盘存储 |
| D10 | 消息格式 | 保持 V1.x Message 格式 | 向后兼容 |
