# StreamMQ 架构设计文档

> **⚠️ 历史设计稿声明**：本文档为实现前的设计稿，其中的类名、模块名、配置键与部分机制
> 描述已随实现演进过时（如 `StreamMqTemplate` → `StreamMessageTemplate`、`@StreamMqListener`
> → `@StreamMQConsumer`、SPI 默认值、Redis Key 布局等）。**当前权威参考是 README 与代码
> Javadoc**，请勿将本文档中的 API/配置细节作为集成依据。

> 配套 PRD：[01-PRD.md](./01-PRD.md)
> 本文档定义 StreamMQ 的整体架构、模块划分、关键组件设计、数据流、部署形态、扩展点。

| 字段 | 内容 |
|---|---|
| 文档版本 | v0.1-draft |
| 状态 | 起草中 |
| 创建日期 | 2026-06-29 |
| 配套 PRD | v0.1-draft |
| 技术栈 | JDK 21 / Spring Boot 3.3.x / Redisson 3.34.x / Redis 7.2+ |

---

## 目录

1. 架构概述
2. 设计目标与原则
3. 分层架构
4. Maven 模块划分
5. 核心组件设计
6. 数据流设计
7. Redis 数据结构设计
8. 部署架构
9. SPI 扩展点
10. 关键技术决策
11. 模块依赖关系
12. 附录

---

## 1. 架构概述

StreamMQ 采用 **5 层分层 + 横切关注点** 的架构：

- **API 层**：暴露给用户的编程接口（注解 / Template / Builder / Listener）
- **编排层**：协调 Core 与外部能力（Spring Boot 装配、Actuator 集成、配置加载）
- **Core 层**：协议中立的 MQ 核心抽象（Producer / Consumer / Message / 序列化 SPI）
- **适配层**：多协议兼容包（Kafka / AMQP / 原生 Stream API）
- **底层**：Redisson RStream 封装

横切关注点：配置、可观测性、异常处理、虚拟线程管理。

整体数据流向：

```
用户代码 (Annotation / Template)
       ↓
   API 层 → 拦截器链 (Interceptor Chain)
       ↓
   编排层 → 路由 / 装配 / 配置查找
       ↓
   Core 层 → 消息模型 / 序列化 / 重试策略
       ↓
   适配层 → (Kafka / AMQP / 原生风格)
       ↓
   底层 → Redisson RStream 操作
       ↓
   Redis Server (Stream / ZSet / Hash)
```

---

## 2. 设计目标与原则

### 2.1 架构目标（从 PRD 派生）

- **开箱即用**：5 分钟接入；零样板代码
- **分层清晰**：API / 编排 / Core / 适配 / 底层严格分离，每层职责单一
- **协议中立**：Core 抽象不绑定任何 MQ 协议；兼容层独立打包
- **可插拔**：序列化器 / 拦截器 / Rebalance / Trace 均为 SPI
- **可观测**：每层暴露指标 / 日志 / Trace 钩子
- **向后兼容**：Core API 通过 SemVer 严格管理

### 2.2 架构原则

- **依赖倒置 (DIP)**：上层不直接依赖下层具体实现，通过接口；Core 不依赖 Spring
- **单一职责 (SRP)**：每个模块 / 类只做一件事
- **开闭原则 (OCP)**：扩展开放（SPI），修改关闭（Core 稳定）
- **显式优于隐式**：配置项明确、行为可观测、不魔法
- **失败可见**：所有失败可被捕获、可观测、可恢复

### 2.3 非目标（明确不做）

- 不做独立 Broker
- 不做跨机房复制
- 不做磁盘持久化
- 不做严格 ACID 事务

---

## 3. 分层架构

### 3.1 五层分层

```
┌─────────────────────────────────────────────────────────┐
│  API 层 (streammq-api)                                  │
│  - @EnableStreamMq, @StreamMqListener, @StreamMqProducer│
│  - StreamMqTemplate, MessageBuilder                    │
│  - StreamMqListener, StreamMqAckListener               │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│  编排层 (streammq-spring-boot-starter)                  │
│  - StreamMqAutoConfiguration                            │
│  - ListenerContainer 注册与生命周期                       │
│  - Properties 加载与校验                                 │
│  - Actuator / Health / Metrics 集成                     │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│  Core 层 (streammq-core)                                │
│  - Producer / Consumer / Listener 抽象接口              │
│  - Message 模型 / SendResult / Action                   │
│  - 序列化 SPI (MessageSerializer)                       │
│  - 拦截器 SPI (ProducerInterceptor / ConsumerInterceptor)│
│  - 重试策略 SPI (RetryPolicy)                           │
│  - Rebalance 策略 SPI (RebalanceStrategy)               │
│  - 事务消息编排 (TransactionExecutor)                   │
│  - 延时消息定时器 (DelayMessageScheduler)               │
│  - DLQ 处理器                                            │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│  适配层 (streammq-native / kafka-compat / amqp-compat)  │
│  - NativeProducer / NativeConsumer                     │
│  - KafkaProducer / KafkaConsumer (类名兼容)             │
│  - AmqpChannel / AmqpClient                             │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│  底层 (streammq-redisson-adapter)                        │
│  - RedissonStreamTemplate (包装 RStream)                │
│  - RedissonBatchTemplate (包装 RBatch)                  │
│  - RedissonZSetTemplate (延时/重试 ZSet)                │
└─────────────────────────────────────────────────────────┘
                          ↓
                  Redis Server
```

### 3.2 横切关注点

| 关注点 | 实现位置 | 说明 |
|---|---|---|
| **配置** | 编排层 `StreamMqProperties` | yml + 注解参数合并 |
| **可观测性** | Core 拦截器 + Actuator 集成 | Micrometer 指标、MDC 日志、Trace Hook |
| **异常处理** | Core 层 `StreamMqException` 体系 | 区分可重试 / 不可重试异常 |
| **虚拟线程** | 编排层 `ListenerContainer` | Java 21 虚拟线程执行器 |
| **安全** | 编排层 + 运维端点 | 鉴权 SPI、运维端点关闭默认 |

### 3.3 依赖方向

严格遵守依赖倒置原则：

- API 层 → 仅依赖 Spring 注解 API（轻量）
- 编排层 → 依赖 API + Core
- Core 层 → **不依赖 Spring**（可被非 Spring 项目使用）
- 适配层 → 依赖 Core
- 底层 → 依赖 Core 抽象 + Redisson

---

## 4. Maven 模块划分

### 4.1 模块清单

```
streammq-parent (pom)
├── streammq-bom                    # BOM 版本管理
├── streammq-core                    # 核心抽象（无 Spring 依赖）
├── streammq-redisson-adapter        # Redisson 底层适配
├── streammq-spring-boot-starter     # Spring Boot Starter
├── streammq-native                  # 原生 Redis Stream API
├── streammq-kafka-compat            # Kafka API 风格兼容
├── streammq-amqp-compat             # AMQP API 风格兼容
├── streammq-test                    # 测试支持
└── streammq-samples                 # 示例
    ├── streammq-sample-quickstart
    ├── streammq-sample-transaction
    ├── streammq-sample-delay
    └── streammq-sample-orderly
```

### 4.2 模块职责

| 模块 | artifactId | 职责 | MVP 必做 |
|---|---|---|---|
| **BOM** | `streammq-bom` | 版本统一管理，供 import | ✅ |
| **Core** | `streammq-core` | 核心抽象、消息模型、SPI 接口 | ✅ |
| **Redisson Adapter** | `streammq-redisson-adapter` | Redisson 底层封装 | ✅ |
| **Spring Boot Starter** | `streammq-spring-boot-starter` | 自动装配、Actuator 集成 | ✅ |
| **Native** | `streammq-native` | 原生 Redis Stream 风格 API | ❌ (v1.0) |
| **Kafka Compat** | `streammq-kafka-compat` | Kafka API 风格兼容 | ❌ (v1.0) |
| **AMQP Compat** | `streammq-amqp-compat` | AMQP API 风格兼容 | ❌ (v1.0) |
| **Test** | `streammq-test` | 测试支持（Embedded Redis、断言工具） | ✅ |

### 4.3 依赖关系图

```
                ┌──────────────────┐
                │  streammq-bom    │
                └──────────────────┘
                         │ (import)
        ┌────────────────┼─────────────────┐
        │                │                 │
        ▼                ▼                 ▼
┌───────────────┐ ┌─────────────┐ ┌──────────────────┐
│  streammq-core │ │ streammq-   │ │ streammq-spring- │
│ (no Spring)   │ │ redisson-   │ │ boot-starter     │
│               │ │ adapter     │ │                  │
└───────────────┘ └─────────────┘ └──────────────────┘
        ▲                ▲                 │
        │                │                 │
        └────────────────┴─── (impl) ─────┘
                ▲
                │ (extend)
   ┌────────────┼────────────┐
   │            │            │
   ▼            ▼            ▼
┌────────┐ ┌─────────┐ ┌──────────┐
│ native │ │ kafka-  │ │ amqp-    │
│        │ │ compat  │ │ compat   │
└────────┘ └─────────┘ └──────────┘
```

### 4.4 包名规范

所有模块统一使用 `io.github.streammq.<module>` 包名前缀：

| 模块 | 顶级包 |
|---|---|
| core | `io.github.streammq.core` |
| redisson-adapter | `io.github.streammq.adapter.redisson` |
| spring-boot-starter | `io.github.streammq.spring.boot` |
| native | `io.github.streammq.nativeapi` |
| kafka-compat | `io.github.streammq.kafka` |
| amqp-compat | `io.github.streammq.amqp` |
| test | `io.github.streammq.test` |

---

## 5. 核心组件设计

### 5.1 核心接口清单

```java
// === Core 层接口（streammq-core） ===

// 生产者
public interface StreamMqProducer {
    SendResult syncSend(Message<?> message);
    CompletableFuture<SendResult> asyncSend(Message<?> message);
    void sendOneway(Message<?> message);
    List<SendResult> syncSendBatch(BatchMessage<?> batch);
}

// 消费者
public interface StreamMqConsumer {
    List<Message<?>> pull(int batchSize);
    List<Message<?>> pullBlock(int batchSize, Duration timeout);
    void ack(MessageId messageId);
}

// 监听器容器
public interface StreamMqListenerContainer {
    void registerListener(Object listenerBean, StreamMqListener annotation);
    void start();
    void stop();
    void pause();
    void resume();
}

// 模板（API 层组合 Producer + 配置）
public class StreamMqTemplate {
    private final StreamMqProducer producer;
    // 提供业务友好的封装方法
}

// 事务执行器
public interface TransactionExecutor {
    <T> SendResult executeInTransaction(Message<T> message, TransactionCallback<T> callback);
}

// 工厂
public interface StreamMqProducerFactory { StreamMqProducer createProducer(Properties props); }
public interface StreamMqConsumerFactory { StreamMqConsumer createConsumer(Properties props); }
```

### 5.2 Listener 接口

```java
// 自动 ack Listener
public interface StreamMqListener<T> {
    Action onMessage(Message<T> message, ConsumeContext context);
}

// 手动 ack Listener
public interface StreamMqAckListener<T> {
    void onMessage(Message<T> message, Acknowledgment ack, ConsumeContext context);
}

// 顺序消费 Listener
public interface StreamMqOrderlyListener<T> {
    Action onMessage(Message<T> message, OrderlyContext context);
}

// 事务回查
public interface TransactionChecker<T> {
    LocalTransactionState check(Message<T> message);
}

// 事务回调
public interface TransactionCallback<T> {
    LocalTransactionState execute(Message<T> message, TransactionContext ctx);
}
```

### 5.3 SPI 接口清单

| SPI | 接口 | 默认实现 | 可选实现 |
|---|---|---|---|
| 序列化 | `MessageSerializer` | `JsonMessageSerializer` | Protobuf / Kryo / Hessian / Avro |
| 生产者拦截器 | `ProducerInterceptor` | 无（用户实现） | Micrometer Tracing / SkyWalking |
| 消费者拦截器 | `ConsumerInterceptor` | 无（用户实现） | 同上 |
| 重试策略 | `RetryPolicy` | `ExponentialBackoffRetryPolicy` | `FixedIntervalRetryPolicy` / 自定义 |
| Rebalance 策略 | `RebalanceStrategy` | `HashSlotRebalanceStrategy` | 自定义 |
| 负载均衡器 | `LoadBalancer` | `RoundRobinLoadBalancer` | `ConsistentHashLoadBalancer` / 自定义 |
| Trace 收集器 | `TraceCollector` | `NoopTraceCollector` / `RedisStreamTraceCollector` | 自定义 |
| 鉴权 | `ManagementAuthenticator` | `BasicAuthAuthenticator` | 自定义 |

### 5.4 关键类图

略（详见详细设计文档 04-detailed-design.md）

---

## 6. 数据流设计

### 6.1 发送流程（同步）

```
1. 用户调用 template.syncSend(msg)
2. API 层校验 Message
3. 调用 ProducerInterceptor.beforeSend() 链
4. 序列化 body
5. Core 调用底层 RedissonStreamTemplate.xadd()
6. Redisson 调用 Redis XADD
7. 返回 MessageId
8. 包装 SendResult
9. 调用 ProducerInterceptor.afterSend() 链
10. 返回给用户
```

### 6.2 消费流程（PUSH 模式）

```
1. ListenerContainer 启动虚拟线程
2. 虚拟线程循环调用 RedissonStreamTemplate.xreadgroup(block=5s)
3. 收到消息 → 反序列化 → 包装 Message
4. 调用 ConsumerInterceptor.beforeConsume() 链
5. 调用 Listener.onMessage(msg, ctx)
6. 根据返回 Action：
   - SUCCESS → XACK
   - RECONSUME_LATER → 写入 retry ZSet
7. 调用 ConsumerInterceptor.afterConsume() 链
8. 暴露 Micrometer 指标
9. 写入 MDC 日志
```

### 6.3 事务消息流程

```
1. Producer 调用 template.executeInTransaction(msg, callback)
2. 发送半消息到 streammq:half:{topic}（专用 Stream，对业务不可见）
3. 调用 callback.execute(msg, ctx) 执行本地事务
4. 根据返回 LocalTransactionState：
   - COMMIT → 将半消息转投到目标 topic
   - ROLLBACK → 删除半消息
   - UNKNOWN → 不处理，等待回查
5. 后台定时任务（每 60s）扫描 streammq:half:* 中超时未确认的半消息
6. 对每条超时半消息调用 TransactionChecker.check(msg)
7. 根据回查结果决定 commit/rollback
8. 连续 15 次回查仍 UNKNOWN → 强制 rollback
```

### 6.4 延时消息流程

```
1. 用户发送 msg.delayLevel(MINUTE_30)
2. Core 判断 delayLevel 非 null → 不写入目标 Stream
3. 改写入 Redis ZSet: streammq:delay:{level}
   - member = msgId (UUID)
   - score = now + delayMillis
4. 同时写入 Hash: streammq:delay:payload:{msgId} 保存消息体
5. 后台定时任务（每秒）扫描 ZSet 中 score <= now 的 entry
6. 取出 payload，XADD 到目标 Stream
7. 从 ZSet 与 Hash 中删除
```

### 6.5 重试 + DLQ 流程

```
消费失败 → 写入 streammq:retry:{topic}:{group} ZSet (score = nextRetryAt)
       → 同时从 PEL 删除（XACK）
后台扫描 retry ZSet → 取出 score <= now 的 entry → 重投到原 Stream
重试次数 +1
若次数 > maxReconsumeTimes：
   → 写入 streammq:dlq:{topic}:{group} Stream（保留原消息内容 + 失败原因）
   → 触发告警 Hook（DLQ Alert Interceptor）
```

---

## 7. Redis 数据结构设计

### 7.1 Redis Key 命名规范

所有 Key 统一前缀：`streammq:{namespace}:{type}:{...}`

| Key 模式 | 类型 | 用途 |
|---|---|---|
| `streammq:{ns}:topic:{topic}` | Stream | 业务消息 Topic |
| `streammq:{ns}:topic:{topic}:shard:{shardId}` | Stream | 分区顺序消息分片 |
| `streammq:{ns}:half:{topic}` | Stream | 事务半消息 |
| `streammq:{ns}:delay:{level}` | ZSet | 延时消息按级别 |
| `streammq:{ns}:delay:payload:{msgId}` | Hash | 延时消息体 |
| `streammq:{ns}:retry:{topic}:{group}` | ZSet | 待重试消息 |
| `streammq:{ns}:dlq:{topic}:{group}` | Stream | 死信队列 |
| `streammq:{ns}:trace:{date}` | Stream | Trace 数据（可选） |
| `streammq:{ns}:heartbeat:{group}` | String + TTL | Consumer 心跳 |
| `streammq:{ns}:group:{group}:members` | Hash | ConsumerGroup 成员列表 |

### 7.2 Stream Entry 字段规范

业务消息 Stream Entry 字段：

| 字段 | 类型 | 必需 | 说明 |
|---|---|---|---|
| `body` | string | ✅ | 消息体（序列化后） |
| `tag` | string | ❌ | 标签 |
| `keys` | string | ❌ | 业务键 |
| `shardingKey` | string | ❌ | 分片键 |
| `props` | string (JSON) | ❌ | 扩展属性 |
| `bornTs` | string (long) | ✅ | 出生时间戳 |
| `retryTimes` | string (int) | ❌ | 已重试次数（首次为 0） |
| `originTopic` | string | ❌ | 重试/DLQ 消息的原 topic |

### 7.3 内存与 TTL 策略

- 业务 Stream：用户配置 `MAXLEN ~ N`（默认 100w，受内存限制）
- 半消息 Stream：`MAXLEN ~ 100w` + 后台扫描清理
- 延时 ZSet：投递后立即删除
- 延时 payload Hash：投递后立即删除
- 重试 ZSet：重投后立即删除
- DLQ Stream：保留 7 天，提供 REST 端点让用户手动清理
- Trace Stream：按天滚动，保留 3 天

---

## 8. 部署架构

### 8.1 单机部署（开发环境）

```
[应用 + StreamMQ] → [Redis Server 7.2+ (单实例)]
```

适用场景：本地开发、单元测试、CI

### 8.2 主从部署（中小规模）

```
[应用集群 (3+ 实例)] → [Redis 主从 + Sentinel]
```

适用场景：日均消息千万级以下，可用性要求 99.9%

### 8.3 Redis Cluster 部署（中大规模）

```
[应用集群 (10+ 实例)] → [Redis Cluster (5+ 节点)]
```

适用场景：日均消息亿级，可用性要求 99.99%

### 8.4 多机房部署（不推荐）

StreamMQ v1.x **不内置跨机房复制**。如需多机房：

- 方案 A：Redis 自身的多机房复制（如 Redis Cluster 跨机房）
- 方案 B：业务层多机房分别部署 StreamMQ，业务自行同步
- 方案 C：等待 StreamMQ v2.x 评估跨机房复制支持

### 8.5 应用部署形态

StreamMQ 是 SDK，无独立 Broker。应用部署形态：

- Spring Boot Fat Jar：内置 streammq-spring-boot-starter
- 容器化：Docker / Kubernetes，无状态
- 多实例：通过 ListenerContainer 自动 rebalance

---

## 9. SPI 扩展点

### 9.1 SPI 加载机制

- **Java SPI**：通过 `META-INF/services/` 加载（Core 层使用）
- **Spring Bean**：通过 Spring 容器加载（编排层使用，优先级高于 Java SPI）
- **配置指定**：yml 中显式指定实现类全限定名

加载顺序：yml 显式配置 > Spring Bean > Java SPI > 默认实现

### 9.2 SPI 接口清单

详见 5.3 章节

### 9.3 自定义示例

```java
// 自定义序列化器
@Component
public class ProtobufSerializer implements MessageSerializer {
    @Override
    public byte[] serialize(Object obj) { /* ... */ }
    @Override
    public <T> T deserialize(byte[] bytes, Class<T> type) { /* ... */ }
}

// 自定义拦截器（接入 SkyWalking）
@Component
public class SkyWalkingProducerInterceptor implements ProducerInterceptor {
    @Override
    public Message<?> beforeSend(Message<?> message) {
        ContextSnapshot snap = ContextManager.capture();
        message.getProperties().put("sw-trace", snap.traceId());
        return message;
    }
}
```

---

## 10. 关键技术决策

### 10.1 Java 21 虚拟线程

**决策**：消费侧使用虚拟线程（`Executors.newVirtualThreadPerTaskExecutor()`）

**理由**：
- Java 21 LTS 原生支持，无外部依赖
- 高并发消费场景下避免平台线程耗尽
- 阻塞式 I/O（XREADGROUP）天然适合虚拟线程

**限制**：
- 虚拟线程内不能使用 synchronized（应改用 ReentrantLock）
- Redisson 阻塞调用需确认兼容性（建议 3.34+）

**配置**：`streammq.consumer.virtual-thread.enabled=true`（默认 true）

### 10.2 Redisson 集成

**决策**：底层统一通过 Redisson 操作 Redis，不直接使用 Jedis / Lettuce

**理由**：
- Redisson 提供 RStream / RBatch / RScoredSortedSet 等完整 API
- 内置连接池、Cluster、Sentinel 支持
- 与 Spring Boot 集成成熟

**版本**：锁定 Redisson 3.34.x

### 10.3 配置加载

**决策**：启动时一次性加载并校验，运行时不重载（除运维端点动态配置）

**配置优先级**（高 → 低）：
1. 注解参数
2. application-{profile}.yml
3. application.yml
4. 默认值

### 10.4 异常处理

**异常体系**：

```
StreamMqException (runtime)
├── StreamMqSendException       // 发送失败
├── StreamMqConsumeException    // 消费失败
├── StreamMqTimeoutException    // 超时
├── StreamMqSerializationException // 序列化失败
├── StreamMqTransactionException // 事务相关
└── StreamMqConfigException     // 配置错误
```

**重试分类**：
- 可重试异常（如网络超时）：自动重试
- 不可重试异常（如消息体非法）：直接进 DLQ

### 10.5 配置默认值策略

所有配置项提供合理默认值，做到"开箱即用"：

- consumeMode: CLUSTERING
- acknowledgeMode: AUTO
- maxReconsumeTimes: 16
- consumeThreadMax: 64
- send-message-timeout: 3000ms
- serializer: json
- virtual-thread.enabled: true

---

## 11. 模块依赖关系

### 11.1 Maven 依赖

```
streammq-spring-boot-starter
    ├── streammq-core
    ├── streammq-redisson-adapter
    ├── spring-boot-starter (provided)
    ├── spring-boot-starter-actuator (optional)
    ├── micrometer-core (optional)
    └── redisson-spring-boot-starter (provided)

streammq-core
    ├── (无 Spring 依赖)
    └── SLF4J (provided)

streammq-redisson-adapter
    ├── streammq-core
    └── redisson (provided)

streammq-kafka-compat
    ├── streammq-core
    └── streammq-redisson-adapter

streammq-amqp-compat
    ├── streammq-core
    └── streammq-redisson-adapter
```

### 11.2 第三方依赖版本

| 依赖 | 版本 | 用途 |
|---|---|---|
| JDK | 21 (LTS) | 基础 |
| Spring Boot | 3.3.x | 框架 |
| Redisson | 3.34.x | Redis 客户端 |
| Redis Server | 7.2+ | 服务端 |
| Jackson | 2.17+ | JSON 序列化 |
| SLF4J | 2.0+ | 日志门面 |
| Micrometer | 1.13+ | 指标 |
| JUnit | 5.10+ | 测试 |
| Testcontainers | 1.19+ | 集成测试 |

---

## 12. 附录

### 12.1 相关文档

- [01-PRD.md](./01-PRD.md) 产品需求文档
- [03-functional-design.md](./03-functional-design.md) 功能设计文档
- [04-detailed-design.md](./04-detailed-design.md) 详细设计文档
- [decisions/](./decisions/) ADR (架构决策记录)

### 12.2 架构决策记录 (ADR) 索引

| ADR | 标题 | 状态 |
|---|---|---|
| ADR-001 | 采用 5 层分层架构 | Accepted |
| ADR-002 | Core 不依赖 Spring | Accepted |
| ADR-003 | 使用 Redisson 而非 Jedis/Lettuce | Accepted |
| ADR-004 | 消费侧使用 Java 21 虚拟线程 | Accepted |
| ADR-005 | 多协议兼容仅做 API 风格（不做线网协议） | Accepted |
| ADR-006 | 事务消息采用半消息 + 事务回查模型 | Accepted |
| ADR-007 | 延时消息基于 Redis ZSet + 定时轮询 | Accepted |
| ADR-008 | DLQ 使用独立 Stream 而非原 Stream 属性 | Accepted |

### 12.3 变更记录

| 版本 | 日期 | 变更 |
|---|---|---|
| v0.1-draft | 2026-06-29 | 初稿建立，含 5 层架构、模块划分、核心组件、数据流、Redis 设计、部署、SPI、技术决策、依赖关系 |
