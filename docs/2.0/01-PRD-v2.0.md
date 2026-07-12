# StreamMQ V2.0 产品需求文档 (PRD)

> StreamMQ V2.0 — 从 Redis-only MQ SDK 进化为多后端、可观测、云原生的轻量级消息中间件 SDK

| 字段 | 内容 |
|---|---|
| 文档版本 | v2.0-draft |
| 状态 | 规划中 |
| 创建日期 | 2026-07-10 |
| 基线版本 | StreamMQ v1.0 GA |
| 技术栈 | JDK 21+ / Spring Boot 3.3.x+ / Redisson 3.34.x+ / Redis 7.2+ |
| 文档语言 | 中文（Javadoc/注释中文，标识符英文） |

---

## 目录

1. [V2.0 背景与动机](#1-v20-背景与动机)
2. [产品愿景与目标](#2-产品愿景与目标)
3. [V2.0 核心特性概览](#3-v20-核心特性概览)
4. [功能需求详细设计](#4-功能需求详细设计)
5. [非功能需求](#5-非功能需求)
6. [版本路线与里程碑](#6-版本路线与里程碑)
7. [风险与依赖](#7-风险与依赖)
8. [附录](#8-附录)

---

## 1. V2.0 背景与动机

### 1.1 V1.x 现状总结

StreamMQ V1.x 已实现以下能力：

| 能力域 | V1.x 实现状态 |
|--------|--------------|
| 核心消息能力 | syncSend / asyncSend / sendOneway / syncSendBatch / CLUSTERING + BROADCASTING |
| 高级能力 | 分区顺序 / 事务消息 / 固定级别+任意延时 / 批量发送 / 重试+DLQ（含二级 DLQ） |
| Spring Boot 集成 | @EnableStreamMQ / @StreamMQConsumer / 自动装配 / Actuator 端点 / 健康检查 |
| 可观测性 | Micrometer 指标 / MDC 结构化日志 / Trace 收集器 SPI / Trace 查询 API |
| 运维管理 | Admin REST 端点（ConsumerGroup 管理 / DLQ 管理 / Topic 管理 / 手动 ACK / Rebalance） |
| 消息压缩 | GZIP 压缩（compressThreshold 配置） |
| 安全 | ManagementAuthenticator SPI（BasicAuth / Token / DenyAll / AllowAll） |

### 1.2 V2.0 动机

V1.x 仍然绑定 Redis Stream 作为唯一后端存储。用户反馈和高频需求集中在：

1. **多后端支持**：部分团队已有 Kafka/RabbitMQ 基础设施，希望复用 StreamMQ 的编程模型但不更换底层
2. **跨机房容灾**：单机房 Redis 故障导致 MQ 不可用，需要跨机房复制能力
3. **线网协议兼容**：原生 Kafka/RabbitMQ Client 零代码迁移需求
4. **云原生部署**：Kubernetes 环境下的弹性伸缩、配置热更新、优雅上下线
5. **更高吞吐**：10w+ TPS 场景下 Redis 单线程瓶颈，需要分片/多后端方案
6. **消息持久化**：Redis 内存限制下的大消息堆积场景，需要磁盘存储选项

### 1.3 V2.0 不做的事

- 不做独立 Broker：StreamMQ 仍然是 SDK，不独立部署
- 不做自研存储引擎：复用现有中间件（Redis / Kafka / RabbitMQ / Pulsar）
- 不做严格 ACID 事务：事务消息仍然是最终一致
- 不做实时流处理：不做窗口/聚合/Join 等 Flink 类能力

---

## 2. 产品愿景与目标

### 2.1 一句话定位

StreamMQ V2.0 是一个多后端、可观测、云原生的轻量级消息中间件 SDK，提供统一的编程模型，底层可切换 Redis Stream / Kafka / RabbitMQ / Pulsar，让业务代码与 MQ 中间件解耦。

### 2.2 V2.0 可量化目标

| 维度 | V1.x 基线 | V2.0 目标 |
|------|-----------|-----------|
| 支持后端数 | 1（Redis Stream） | ≥ 3（Redis / Kafka / RabbitMQ） |
| 单节点 TPS | 10k（Redis） | 50k+（Kafka 后端） |
| 跨机房容灾 | 不支持 | 支持（异步复制，RPO ≤ 1s） |
| 线网协议兼容 | API 风格兼容 | Kafka wire protocol 兼容 |
| Kubernetes 部署 | 基础支持 | 完整（HPA / ConfigMap 热更新 / 优雅上下线） |
| GitHub Star | 500（V1.0 目标） | ≥ 3,000 |
| 生产用户 | 1 家（V1.0 目标） | ≥ 10 家 |

### 2.3 设计原则（V2.0 新增）

- **后端无关**：Core API 不绑定任何后端，切换后端零代码修改
- **渐进增强**：V1.x 用户可平滑升级，默认行为不变
- **云原生优先**：12-Factor App 原则，Kubernetes 一等公民
- **可观测深度**：从指标/日志/Trace 升级到分布式追踪 + 拓扑图 + 消息画像

---

## 3. V2.0 核心特性概览

| 特性 | 优先级 | 模块 | 描述 |
|------|--------|------|------|
| 多后端抽象层 | P0 | `streammq-backend-spi` | 统一 BackendProvider SPI，支持 Redis / Kafka / RabbitMQ / Pulsar |
| Kafka 后端实现 | P0 | `streammq-kafka-backend` | 基于 Kafka Client 的 BackendProvider 实现 |
| 跨机房复制 | P1 | `streammq-replication` | 异步/同步复制，支持 Redis Replica / Kafka MirrorMaker / 自研 |
| Kafka 线网协议兼容 | P1 | `streammq-kafka-protocol` | 实现 Kafka wire protocol server，原生 Client 零代码接入 |
| Spring Cloud Stream Binder | P1 | `streammq-spring-cloud-stream-binder` | 实现 Spring Cloud Stream Binder SPI |
| 云原生增强 | P2 | `streammq-cloud-k8s` | K8s Operator / HPA 指标 / 优雅上下线 / ConfigMap 热更新 |
| 消息持久化抽象 | P2 | `streammq-persistence-spi` | 支持磁盘存储选项（RocksDB / LevelDB） |
| 分布式追踪增强 | P2 | `streammq-tracing` | OpenTelemetry 原生集成 + 消息拓扑图 |
| 消息画像与诊断 | P3 | `streammq-diagnostics` | 消息生命周期可视化 + 异常诊断 |

---

## 4. 功能需求详细设计

### 4.1 多后端抽象层（P0）

#### 4.1.1 BackendProvider SPI

```java
public interface BackendProvider extends AutoCloseable {
    // 后端标识
    String name(); // "redis" / "kafka" / "rabbitmq" / "pulsar"

    // 生产者
    StreamMessageProducer createProducer(ProducerConfig config);

    // 消费者
    StreamMessageConsumer createConsumer(ConsumerConfig config);

    // 管理操作
    void createTopic(String topic, int partitions);
    void deleteTopic(String topic);
    boolean topicExists(String topic);
    long getTopicBacklog(String topic, String group);

    // 事务支持
    boolean supportsTransaction();
    TransactionBackend createTransactionBackend();

    // 延时支持
    boolean supportsDelay();
    DelayBackend createDelayBackend();

    // 能力描述
    BackendCapabilities capabilities();
}
```

#### 4.1.2 BackendCapabilities

```java
public record BackendCapabilities(
    boolean supportsBroadcasting,    // 广播消费
    boolean supportsOrderly,         // 顺序消息
    boolean supportsTransaction,     // 事务消息
    boolean supportsDelay,           // 延时消息
    boolean supportsBatch,           // 批量发送
    boolean supportsPull,            // 拉取模式
    boolean supportsSharding,        // 分片
    int maxMessageSize,              // 最大消息体
    long maxTopicCount               // 最大 Topic 数
) {}
```

#### 4.1.3 配置切换

```yaml
streammq:
  backend:
    type: redis  # redis / kafka / rabbitmq / pulsar
    redis:
      namespace: "streammq"
      redisson-config: classpath:redisson.yaml
    kafka:
      bootstrap-servers: localhost:9092
      acks: all
      retries: 3
    rabbitmq:
      host: localhost
      port: 5672
      virtual-host: /
```

### 4.2 Kafka 后端实现（P0）

#### 4.2.1 KafkaBackendProvider

- 基于 `org.apache.kafka.clients.producer.KafkaProducer` 和 `org.apache.kafka.clients.consumer.KafkaConsumer`
- Topic → Kafka Topic 映射
- ConsumerGroup → Kafka ConsumerGroup 映射
- Tag 过滤 → Kafka Header 过滤
- 顺序消息 → Kafka Partition + 同 Key 路由
- 事务消息 → Kafka Transactional Producer
- 延时消息 → Kafka 不原生支持，降级为 ZSet + 定时投递（复用 Redis 方案）

#### 4.2.2 能力矩阵

| 能力 | Redis 后端 | Kafka 后端 |
|------|-----------|------------|
| 集群消费 | ✅ ConsumerGroup | ✅ ConsumerGroup |
| 广播消费 | ✅ 独立 Group | ✅ 每个 Consumer 独立 Group |
| 分区顺序 | ✅ shard Stream | ✅ Partition |
| 事务消息 | ✅ 半消息+回查 | ✅ Transactional Producer |
| 延时消息 | ✅ ZSet | ⚠️ 降级方案 |
| 批量发送 | ✅ RBatch | ✅ ProducerBatch |
| 重试+DLQ | ✅ ZSet+DLQ Stream | ✅ retry topic + DLQ topic |
| 自动 Rebalance | ✅ Redisson | ✅ Kafka Rebalance |
| 最大消息体 | 1MB | 10MB（默认） |

### 4.3 跨机房复制（P1）

#### 4.3.1 复制架构

```
机房 A (主)                          机房 B (从)
┌──────────────┐                   ┌──────────────┐
│  StreamMQ    │                   │  StreamMQ    │
│  Producer     │                   │  Consumer    │
│      ↓        │                   │      ↑        │
│  Redis/Kafka  │ ── 复制通道 ──→  │  Redis/Kafka  │
│  (主集群)     │                   │  (从集群)     │
└──────────────┘                   └──────────────┘
```

#### 4.3.2 复制模式

| 模式 | RPO | RTO | 适用场景 |
|------|-----|-----|----------|
| 异步复制 | ≤ 1s | ≤ 30s | 容灾备份 |
| 同步复制 | 0 | ≤ 30s | 金融级要求 |
| 双活复制 | ≤ 1s | 0 | 两机房同时读写 |

#### 4.3.3 复制 SPI

```java
public interface ReplicationProvider {
    // 启动复制
    void startReplication(ReplicationConfig config);
    // 停止复制
    void stopReplication();
    // 复制状态
    ReplicationStatus getStatus();
    // 手动全量同步
    void fullSync(String topic);
}
```

### 4.4 Kafka 线网协议兼容（P1）

#### 4.4.1 目标

实现 Kafka wire protocol server，让原生 Kafka Client（包括 Kafka Producer / Consumer / Kafka Connect / Kafka Streams）零代码修改接入 StreamMQ。

#### 4.4.2 架构

```
原生 Kafka Client
       ↓ (Kafka wire protocol)
StreamMQ Kafka Protocol Server (Netty)
       ↓ (内部协议)
StreamMQ Core (BackendProvider)
       ↓
Redis / Kafka / RabbitMQ 后端
```

#### 4.4.3 兼容范围

| Kafka API | 兼容级别 | 说明 |
|-----------|---------|------|
| Produce API (v0-v9) | ✅ 完全兼容 | 支持 compression / batching / idempotent |
| Fetch API (v0-v9) | ✅ 完全兼容 | 支持 follower fetch / fetch isolation |
| ListGroups / DescribeGroups | ✅ 完全兼容 | |
| JoinGroup / SyncGroup / Heartbeat | ✅ 完全兼容 | Rebalance 协议 |
| OffsetCommit / OffsetFetch | ✅ 完全兼容 | |
| CreateTopics / DeleteTopics | ✅ 完全兼容 | |
| Kafka Connect | ⚠️ 部分兼容 | Source Connector 支持，Sink Connector 需适配 |
| Kafka Streams | ⚠️ 部分兼容 | 简单拓扑支持，复杂窗口/聚合不支持 |

### 4.5 Spring Cloud Stream Binder（P1）

#### 4.5.1 目标

实现 Spring Cloud Stream Binder SPI，让 Spring Cloud Stream 用户零代码切换到 StreamMQ。

#### 4.5.2 使用方式

```yaml
spring:
  cloud:
    stream:
      bindings:
        orderOutput:
          destination: order-topic
        orderInput:
          destination: order-topic
          group: order-consumer-group
      binder:
        type: streammq
```

#### 4.5.3 实现范围

- `StreamMQMessageBinder` 实现 `Binder<MessageChannel, ConsumerProperties, ProducerProperties>`
- 支持Spring Cloud Stream 的 `@EnableBinding` 注解
- 支持函数式编程模型（`java.util.function.Function` / `Supplier` / `Consumer`）
- 支持批量消费 (`spring.cloud.stream.bindings.input.consumer.batch-mode=true`)

### 4.6 云原生增强（P2）

#### 4.6.1 Kubernetes Operator

- CRD 定义：`StreamMQCluster` / `StreamMQTopic` / `StreamMQConsumerGroup`
- Operator 自动管理 Consumer 实例的伸缩
- 与 K8s HPA 集成，基于消费延迟自动扩缩容

#### 4.6.2 优雅上下线

- Consumer 启动时注册到 K8s Endpoints
- Consumer 收到 SIGTERM 时：停止拉取 → 完成处理中消息 → ACK → 注销 → 退出
- 超时强制退出（`spring-boot-graceful-shutdown-timeout`）

#### 4.6.3 配置热更新

- ConfigMap 变更自动感知
- 支持运行时动态调整：maxReconsumeTimes / consumeThread / scanInterval
- 不支持运行时变更：backend type / namespace / serializer

### 4.7 消息持久化抽象（P2）

#### 4.7.1 目标

提供磁盘存储选项，突破 Redis 内存限制，支持亿级消息堆积。

#### 4.7.2 架构

```
StreamMQ Core
      ↓
PersistenceProvider SPI
      ↓
┌─────────┬─────────┬──────────┐
│ Redis   │ RocksDB │ LevelDB  │
│ (默认)  │ (磁盘)  │ (磁盘)   │
└─────────┴─────────┴──────────┘
```

#### 4.7.3 使用场景

| 场景 | 推荐后端 | 堆积上限 |
|------|---------|---------|
| 低延迟 + 小堆积 | Redis | < 100w |
| 中等延迟 + 大堆积 | RocksDB | < 1亿 |
| 高吞吐 + 海量堆积 | Kafka Backend | 无限 |

### 4.8 分布式追踪增强（P2）

#### 4.8.1 OpenTelemetry 原生集成

- 自动注入 Trace Context（W3C TraceContext 标准）
- Span 信息：producer.send / consumer.consume / retry / dlq
- 与 Micrometer Tracing / Zipkin / Jaeger / SkyWalking 无缝对接

#### 4.8.2 消息拓扑图

- 可视化消息流转路径：Producer → Topic → ConsumerGroup → Consumer
- 消息生命周期时间线：发送 → 入队 → 消费 → ACK/重试/DLQ
- 异常消息诊断：失败原因 / 堆栈 / 处理耗时分布

### 4.9 消息画像与诊断（P3）

#### 4.9.1 消息画像

- 每条消息的完整生命周期记录
- 消息指纹：topic + tag + keys + bodyHash
- 消费轨迹：哪些 Consumer 消费过、耗时多少、结果如何

#### 4.9.2 异常诊断

- 消费慢诊断：Consumer 线程池 / GC / 网络 IO 分析
- 消息堆积诊断：生产速率 vs 消费速率对比、消费者处理时间分布
- DLQ 诊断：失败原因分类、高频失败 Topic/Group 排行

---

## 5. 非功能需求

### 5.1 性能指标

| 指标 | V1.x | V2.0（Kafka 后端） | V2.0（Redis 后端） |
|------|------|-------------------|-------------------|
| 单节点发送 TPS | 10k | 50k+ | 15k+ |
| 单节点消费 TPS | 8k | 50k+ | 12k+ |
| 端到端延迟 P99 | 5ms | 10ms | 5ms |
| 消息体大小 | 1MB | 10MB | 1MB |
| 消息堆积上限 | < 100w | 无限 | < 100w |

### 5.2 可靠性指标

| 指标 | V1.x | V2.0 |
|------|------|------|
| 消息不丢失率 | 99.99% | 99.999% |
| 跨机房 RPO | N/A | ≤ 1s（异步）/ 0（同步） |
| 跨机房 RTO | N/A | ≤ 30s |
| 故障恢复时间 | ≤ 30s | ≤ 10s |

### 5.3 兼容性

| 项 | V1.x | V2.0 |
|----|------|------|
| JDK | 21+ | 21+ |
| Spring Boot | 3.3.x+ | 3.3.x+ |
| Redis | 7.2+ | 7.2+ |
| Kafka | N/A | 3.6+ |
| RabbitMQ | N/A | 3.12+ |
| Kubernetes | 基础 | 1.28+ |
| 向后兼容 | - | 100%（V1.x 代码零修改升级） |

---

## 6. 版本路线与里程碑

| 版本 | 目标 | 核心交付 | 时间 |
|------|------|---------|------|
| **v2.0.0** | 多后端 + Kafka 线网协议 | BackendProvider SPI + Kafka Backend + Kafka Protocol Server | Q4 2026 |
| **v2.1.0** | 跨机房容灾 | ReplicationProvider + Redis Replica + Kafka MirrorMaker2 | Q1 2027 |
| **v2.2.0** | 云原生 | K8s Operator + 优雅上下线 + ConfigMap 热更新 | Q2 2027 |
| **v2.3.0** | Spring Cloud Stream | StreamMQ Binder + 函数式编程模型 | Q2 2027 |
| **v2.4.0** | 持久化 + 追踪增强 | RocksDB Provider + OpenTelemetry 集成 + 消息拓扑图 | Q3 2027 |
| **v2.5.0** | 诊断 + 优化 | 消息画像 + 异常诊断 + 性能优化 | Q4 2027 |

---

## 7. 风险与依赖

### 7.1 技术风险

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|---------|
| 多后端抽象设计不当导致 V1.x API 破坏 | 中 | 高 | V2.0 默认 Redis 后端，V1.x 用户零感知 |
| Kafka 线网协议实现复杂度超预期 | 高 | 高 | 分阶段实现：Produce/Fetch 优先，Connect/Streams 后置 |
| 跨机房复制一致性保障 | 中 | 高 | 提供 at-least-once 语义，文档明确限制 |
| K8s Operator 运维复杂度 | 中 | 中 | 参考 Strimzi Kafka Operator 设计，充分测试 |

### 7.2 外部依赖

| 依赖 | 用途 | 风险 |
|------|------|------|
| Kafka Client 3.6+ | Kafka Backend | API 变更 |
| Netty 4.1+ | Kafka Protocol Server | 版本兼容 |
| Spring Cloud Stream 4.x | Binder | API 变更 |
| Kubernetes Client Java | Operator | K8s 版本兼容 |
| RocksDB JNI | 磁盘持久化 | 平台兼容性 |

---

## 8. 附录

### 8.1 术语表

| 术语 | 定义 |
|------|------|
| BackendProvider | 后端提供者 SPI，抽象不同 MQ 后端 |
| BackendCapabilities | 后端能力描述，声明支持/不支持的特性 |
| ReplicationProvider | 跨机房复制 SPI |
| PersistenceProvider | 持久化存储 SPI |
| StreamMQMessageBinder | Spring Cloud Stream Binder 实现 |

### 8.2 参考文档

- [Kafka Protocol Specification](https://kafka.apache.org/protocol.html)
- [Spring Cloud Stream Reference](https://docs.spring.io/spring-cloud-stream/reference/)
- [Strimzi Kafka Operator](https://strimzi.io/)
- [OpenTelemetry Specification](https://opentelemetry.io/docs/specs/otel/)

### 8.3 变更记录

| 版本 | 日期 | 变更 |
|------|------|------|
| v2.0-draft | 2026-07-10 | 初稿建立，含多后端、跨机房、线网协议、云原生、持久化、追踪增强规划 |
