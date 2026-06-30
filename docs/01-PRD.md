# StreamMQ 产品需求文档 (PRD)

> 基于 Redis Stream + Redisson 封装的开箱即用 Redis MQ SDK，提供类 RocketMQ 的注解 / Template / Service 编程模型，并兼容 Kafka / AMQP 协议结构。

| 字段 | 内容 |
|---|---|
| 文档版本 | v0.1-draft |
| 状态 | 起草中 |
| 创建日期 | 2026-06-29 |
| 许可证 | MIT |
| 技术栈 | JDK 21 / Spring Boot 3.3.x / Redisson 3.34.x / Redis 7.2+ |
| 文档语言 | 中文（Javadoc/注释中文，标识符英文） |

---

## 目录

1. [文档信息](#1-文档信息)
2. [项目背景](#2-项目背景)
3. [产品愿景与目标](#3-产品愿景与目标)
4. [目标用户与场景](#4-目标用户与场景)
5. [名词术语表](#5-名词术语表)
6. [产品功能需求](#6-产品功能需求)
7. [非功能需求](#7-非功能需求)
8. [约束与边界](#8-约束与边界)
9. [里程碑与发布计划](#9-里程碑与发布计划)
10. [成功指标](#10-成功指标)
11. [风险与依赖](#11-风险与依赖)
12. [附录](#12-附录)

---

## 1. 文档信息

| 项 | 内容 |
|---|---|
| 文档版本 | v0.1-draft |
| 当前状态 | 起草中 |
| 维护者 | StreamMQ 团队 |
| 变更记录 | 2026-06-29 v0.1 初稿建立，含愿景、用户、术语、核心能力、高级能力 |
| 相关文档 | 02-architecture.md 架构设计 / 03-functional-design.md 功能设计 / 04-detailed-design.md 详细设计 |

---

## 2. 项目背景

### 2.1 痛点分析

当前 Java 生态使用 Redis 做消息队列时面临三重痛点：

1. Redisson RStream 太底层：仅提供 XADD/XREADGROUP 等裸 API，无消费模型抽象、无重试/DLQ、无注解、无 Spring 集成，业务方需自行封装大量样板代码
2. Spring Data Redis Stream 不够企业级：仅提供基础 Stream 操作，缺乏事务消息、延时消息、顺序消息、死信队列、自动 Rebalance 等企业级特性
3. RocketMQ/Kafka 太重：需独立部署集群、运维复杂、资源占用大；小团队或轻量场景难以承担运维成本

### 2.2 技术选型动机

为什么选择 Redis Stream 作为底层：

- 复用现有 Redis 集群：业务方通常已有 Redis（缓存/分布式锁），无需额外部署 MQ
- Redis Stream 原生支持 ConsumerGroup + PEL + 持久化，基础能力完备
- 性能：单节点 10w+ TPS 可达，足够中小规模业务
- Redis 7.x 后 Stream 稳定性大幅提升

为什么选择 Redisson 作为底层客户端：

- Redisson 是 Java 生态最成熟的 Redis 客户端，社区活跃、文档完善
- 提供 RStream / RBatch / RScoredSortedSet 等完整 API
- 内置连接池、集群、哨兵支持
- 与 Spring Boot 集成成熟

### 2.3 与现有方案对比

详见 3.3 差异化定位章节

---

## 3. 产品愿景与目标

### 3.1 一句话定位

StreamMQ 是一个基于 Redis Stream 与 Redisson 的轻量级、开箱即用的企业级 MQ SDK，提供类 RocketMQ 的注解 / Template / Service 编程模型，并兼容 Kafka / AMQP 协议结构。

"轻量"指部署与运维轻——复用现有 Redis 集群、无需独立 MQ 中间件；功能不轻——提供企业级 MQ 的全部高级能力。

### 3.2 三年长期愿景

Java 生态使用 Redis Stream 作为消息队列的事实标准 SDK，对标参照系：

- 编程体验：对齐 RocketMQ Spring Starter
- 后端能力：覆盖 Kafka / RocketMQ 核心特性集合
- 部署成本：与 Spring Data Redis Stream 相当
- 3 年目标：Maven Central 同类下载量 Top 3，GitHub Star ≥ 5k，至少 3 家中大型企业生产可用

### 3.3 差异化定位

- vs Redisson RStream：Redisson 仅提供裸 Stream API；StreamMQ 提供上层 RocketMQ 风格抽象（注解 / Template / Service / Listener 容器）
- vs Spring Data Redis Stream：Spring Data 仅提供基础 Stream 操作；StreamMQ 提供企业级特性全套（事务/延时/顺序/批量/重试+DLQ/自动 Rebalance）
- vs RocketMQ / Kafka：需独立部署集群、运维复杂；StreamMQ 零额外部署，复用业务现有 Redis 集群即可获得 MQ 能力

### 3.4 可量化目标

MVP（v0.1.0）发布时：
- 单节点发送 TPS ≥ 10,000 / 单节点消费 TPS ≥ 8,000（消息体 1KB）
- 端到端延迟 P99 ≤ 5ms（同机房）
- 提供 ≥ 12 个开箱即用注解 / Builder / Template API
- 单元测试覆盖率 ≥ 80%，集成测试覆盖核心场景 ≥ 30 个

v1.0 GA 发布时：
- 集群（5 节点 Redis Cluster + 10 个 Consumer）下消费 TPS ≥ 50,000
- 端到端延迟 P99 ≤ 10ms（同机房集群）
- Kafka 协议兼容（API 风格）可跑通原生 Kafka Client 代码风格
- 至少 1 家真实企业生产用户

3 年目标：
- GitHub Star ≥ 5,000
- Maven Central 月下载量 ≥ 10,000
- 至少 3 家中大型企业（员工 ≥ 500）生产环境使用
- 至少 10 位外部 Contributor

### 3.5 设计原则

- 开箱即用：引入依赖 + 配置 + 注解即可使用，零样板代码
- 分层清晰：API 层 / 编排层 / 适配层 / 底层 Redisson 严格分离
- 可插拔：序列化器、负载均衡器、Trace、Rebalance 策略均为 SPI 接口
- 协议中立：Core 抽象不绑定任何 MQ 协议，兼容层独立打包
- 可观测：指标 / 日志 / Trace / 健康检查一应俱全
- 向后兼容：严格遵守 SemVer，minor 版本不破坏 API

---

## 4. 目标用户与场景

### 4.1 目标用户画像

画像 A：个人开发者 / 创业团队
- 背景：1-5 人小团队，全栈工程师主导，无专职运维；通常使用云 Redis 实例
- 痛点：想用 MQ 解耦业务但不愿为 RocketMQ/Kafka 付出运维成本；现有 Redis pub/sub 又不可靠
- 需求：引入依赖 → 写注解 → 跑起来，5 分钟内完成 MQ 接入
- 典型场景：创业期电商订单异步、SaaS 多租户事件通知、个人项目削峰

画像 B：中小团队后端架构师
- 背景：50-200 人公司，后端 5-20 人；已有 Redis 集群（主从或 Cluster）
- 痛点：RocketMQ 集群运维要求高；Spring Data Redis Stream 太底层，缺乏企业级特性
- 需求：消费模型、重试、死信、延时、顺序等企业特性齐全；可观测性指标齐全
- 典型场景：微服务异步解耦、秒杀削峰、延时任务、订单顺序处理、分布式事务通知

画像 C：中大型企业架构师 / 中间件团队
- 背景：500+ 人公司，有中间件团队；已有 Redis Cluster
- 痛点：小流量、临时、内部工具类项目用大 MQ 太重；多个业务方诉求多样
- 需求：多协议兼容；高可观测性；SLA 保障；可插拔 SPI
- 典型场景：内部中间件平台、轻量场景替代、多协议适配层

### 4.2 典型使用场景

场景 1：异步解耦
- 业务背景：电商下单后需触发库存扣减、积分发放、物流通知、风控审计等多个下游业务
- StreamMQ 能力：@StreamMqProducer 注解 + StreamMqTemplate 一行发送；@StreamMqListener 注解订阅；CLUSTERING 模式下多实例分担消费；失败自动重试 + DLQ 兜底

场景 2：削峰填谷
- 业务背景：秒杀/抢购瞬时 QPS 数万，后端处理能力 1k QPS
- StreamMQ 能力：Stream 天然堆积能力；Consumer 按速率限制消费；堆积指标暴露给监控告警

场景 3：定时 / 延时任务
- 业务背景：订单超时未支付 30 分钟自动关闭
- StreamMQ 能力：DelayLevel 内置延时级别（类似 RocketMQ 18 级）；基于 ZSet + 定时轮询投递；v1.0 增加任意延时

场景 4：顺序业务处理
- 业务背景：订单状态机需要严格顺序
- StreamMQ 能力：分区顺序（相同 shardingKey 路由到同一分区）；全局顺序（单分区单消费者）

场景 5：分布式事务通知
- 业务背景：跨服务最终一致性
- StreamMQ 能力：半消息 + 本地事务执行 + commit/rollback；本地事务状态回查机制（事务回查 SPI）

场景 6：广播通知
- 业务背景：配置变更广播、缓存失效通知
- StreamMQ 能力：BROADCASTING 模式；每个消费者独立 group + 独立 offset

### 4.3 反场景（不建议使用 StreamMQ）

- 超大规模数据管道（≥ 10 万 TPS 持续吞吐）：Redis 单线程模型无法支撑；推荐 Kafka / Pulsar
- 金融级强一致事务消息：金融场景需要严格 ACID、监管合规；推荐 RocketMQ 事务消息
- 亿级消息堆积：Redis 内存有限；推荐 Kafka（磁盘存储）
- 多机房跨地域部署：跨地域复制延迟问题；推荐 RocketMQ Cluster / Pulsar Geo-Replication
- 嵌入式 IoT 设备：资源受限；推荐 MQTT Broker

### 4.4 业务领域优先级

- P0 首要：互联网业务（电商、O2O、SaaS）
- P1 次要：金融支付
- P1 次要：企业内部集成

---

## 5. 名词术语表

为避免文档与代码术语混用，统一定义如下。本表对齐 RocketMQ / Redis Stream 既有术语，并标注 StreamMQ 中的具体含义。

### 5.1 实体类术语

- 主题 (Topic)：逻辑消息分类；对应一个 Redis Key（Stream）
- 流 (Stream)：Redis 5.0+ 数据结构，Topic 的底层物理实体
- 生产者 (Producer)：发送消息的客户端实例（XADD 调用方）
- 消费者 (Consumer)：接收消息的客户端实例（XREADGROUP 调用方）
- 消费者组 (ConsumerGroup)：一组共同消费同一 Topic 的 Consumer
- 消费者名 (ConsumerName)：ConsumerGroup 内单个 Consumer 的标识
- 消息 (Message)：Producer 发送到 Topic 的单条数据
- 消息 ID (MessageId)：消息在 Stream 中的唯一 ID（Stream Entry ID）
- 标签 (Tag)：同一 Topic 下的二级分类，用于过滤
- 消息键 (Keys)：业务主键，用于幂等/查询
- 分片键 (ShardingKey)：用于分区顺序消息路由的键

### 5.2 行为类术语

- 生产 (Produce/Send)：Producer 将消息写入 Topic
- 消费 (Consume/Subscribe)：Consumer 从 Topic 读取消息
- 确认 (ACK)：Consumer 处理成功后通知 Stream 删除该消息的 pending 记录
- 消费位点 (Offset/Last-delivered-id)：ConsumerGroup 上次读取到的最新消息 ID
- 待处理列表 (PEL - Pending Entries List)：已读但未 ACK 的消息列表
- 重试 (Retry)：消费失败后重新投递给 Consumer
- 死信 (DLQ - Dead Letter Queue)：超过最大重试次数仍失败的消息进入的特殊 Topic
- 重平衡 (Rebalance)：ConsumerGroup 内 Consumer 上下线导致分片重新分配
- 回查 (Transaction Check)：事务消息中，Broker 回查 Producer 本地事务状态的机制

### 5.3 模式 / 类型术语

- 集群消费 (CLUSTERING)：同一 ConsumerGroup 下多个 Consumer 共同消费 Topic
- 广播消费 (BROADCASTING)：同一 Topic 的每条消息会被所有订阅的 Consumer 各处理一次
- 顺序消息 (Orderly Message)：保证消息按发送顺序被消费的消息类型
- 分区顺序 (Partition Orderly)：同一 ShardingKey 的消息有序
- 全局顺序 (Global Orderly)：整个 Topic 全局有序（牺牲吞吐）
- 并发消息 (Concurrent Message)：不保证顺序，最大化吞吐
- 事务消息 (Transactional Message)：两阶段提交消息
- 半消息 (Half Message)：事务消息第一阶段发送的不可见消息
- 延时消息 (Delayed Message)：发送后延迟指定时间才投递的消息
- 定时消息 (Scheduled Message)：在指定时刻投递的消息

### 5.4 组件类术语

- Starter：Spring Boot Starter，提供自动装配的模块
- Template (StreamMqTemplate)：编程式发送 API 的核心类
- Listener (StreamMqListener)：消费消息的回调接口
- Listener Container：管理 Listener 生命周期、消费线程、Rebalance 的容器
- ProducerFactory / ConsumerFactory：创建 Producer/Consumer 实例的工厂
- Interceptor：拦截发送/消费行为的钩子

---

## 6. 产品功能需求

本章按模块拆分功能需求。每个子模块标注 MVP（v0.1.0）必做、v1.0 GA 必做、v2.0+ 规划。包含以下子章节：

- 6.1 核心消息能力（MVP 必做）：API 风格总览、核心组件、消息模型、生产者/消费者 API、配置体系、消费模型、序列化
- 6.2 高级能力（部分 MVP 必做）：顺序消息、事务消息（v0.2+）、延时消息、批量发送、重试+DLQ
- 6.3 Spring Boot Starter 集成（MVP 必做）：自动装配、配置类、Actuator 集成
- 6.4 可观测性（部分 MVP 必做）：Micrometer 指标、结构化日志+MDC、Trace 模块（v0.2+）、健康检查
- 6.5 多协议兼容（v1.0 GA）：原生 API 模块、Kafka API 风格兼容、AMQP API 风格兼容
- 6.6 运维管理（v1.0 GA）：运维 REST 端点、安全策略、鉴权 SPI

### 6.1 核心消息能力

#### 6.1.1 API 风格总览

采用类 RocketMQ Spring Starter 的 API 风格——重注解、轻模板、明确语义：

- 启用开关：@EnableStreamMq 触发自动装配
- 生产者：@StreamMqProducer + StreamMqTemplate
- 消费者：@StreamMqListener 方法级注解
- 事务回查：@StreamMqTransactionListener
- Builder：MessageBuilder / ProducerBuilder
- 配置：application.yml + 注解参数覆盖

#### 6.1.2 核心组件清单

- StreamMqTemplate：编程式生产者，提供 syncSend / asyncSend / sendOneway
- StreamMqProducerFactory / StreamMqConsumerFactory：创建 Producer/Consumer 实例
- StreamMqListenerContainer：管理 Listener 生命周期、消费线程、Rebalance
- StreamMqListener<T>：自动 ack 消费回调，返回 Action 枚举
- StreamMqAckListener<T>：手动 ack 消费回调，参数含 Acknowledgment
- Message<T>：消息载体
- MessageBuilder<T>：流式构造 Message
- SendResult / SendStatus / Action：返回结果与枚举

#### 6.1.3 消息模型

Message<T> 包含字段：topic, tag, keys, shardingKey, properties (Map<String,String>), body (T), delayTimeMillis, messageId, bornTimestamp, reconsumeTimes

Message → Redis Stream Entry 映射规则：
- body（序列化后）→ Stream Entry "body" 字段
- topic → 隐含于 Stream Key
- tag / keys / shardingKey → 同名字段
- properties → "props" 字段（JSON 字符串）
- messageId → Stream Entry ID
- bornTimestamp → "bornTs" 字段
- delayTimeMillis → 延时消息不直接入 Stream

#### 6.1.4 生产者 API

三种发送语义：
- 同步发送：template.syncSend(message) 返回 SendResult
- 异步发送：template.asyncSend(message) 返回 CompletableFuture<SendResult>
- 单向发送：template.sendOneway(message)

Producer 配置（yml）：
streammq.producer.group / send-message-timeout=3000ms / retry-times=2 / async-retry-times=0 / compress-threshold=4096 / serializer=json

Builder API：MessageBuilder.topic().tag().keys().shardingKey().body().build()

#### 6.1.5 消费者 API

两种 Listener 接口并存：

1) 自动 ack：实现 StreamMqListener<T>，返回 Action.SUCCESS 或 Action.RECONSUME_LATER
@StreamMqListener 注解参数：topic, consumerGroup, consumeMode=CLUSTERING, consumeThreadMin, consumeThreadMax, maxReconsumeTimes, consumeTimeout

2) 手动 ack：实现 StreamMqAckListener<T>，通过 Acknowledgment.acknowledge() 显式 ack
@StreamMqListener(acknowledgeMode=MANUAL)

消费线程模型：使用 Java 21 虚拟线程，由 ListenerContainer 内部管理线程池

#### 6.1.6 配置体系

配置优先级（高 → 低）：注解参数 > application-{profile}.yml > application.yml > 默认值

核心 yml 配置：
streammq:
  enabled: true
  redisson.config: classpath:redisson.yaml
  namespace: "streammq"
  default.consumer: consume-mode=CLUSTERING, consume-thread-max=64, max-reconsume-times=16, acknowledge-mode=AUTO
  default.producer: send-message-timeout=3000ms
  consumer-groups: 列表

#### 6.1.7 消费模型

CLUSTERING 集群消费（默认）：利用 Redis Stream 原生 ConsumerGroup；同一 ConsumerGroup 多实例自动 Rebalance

BROADCASTING 广播消费：为每个 Consumer 实例创建独立 ConsumerGroup（基于 instanceId 拼接）

PUSH 模式（伪推送）：默认模式；底层为长轮询（XREADGROUP BLOCK）；由 ListenerContainer 管理轮询线程

PULL 模式：编程式主动拉取；StreamMqConsumer.pull(pullSize) / pullBlock(timeout)；适合批量消费、定时消费

#### 6.1.8 序列化

默认 JSON（Jackson）；SPI 接口 MessageSerializer；元信息（tag/keys/shardingKey/properties）始终为 String 不参与序列化

---

### 6.2 高级能力（顺序/事务/延时/批量/DLQ）

#### 6.2.1 顺序消息

两种 API 并存——参数控制 + 独立接口

方式 1 参数控制：
@StreamMqListener(topic, consumerGroup, messageModel=MessageModel.ORDERLY)
实现 StreamMqListener<T>

方式 2 独立接口：
@StreamMqOrderlyListener(topic, consumerGroup)
实现 StreamMqOrderlyListener<T>，方法签名含 OrderlyContext

实现机制：
- 分区顺序：通过 shardingKey 哈希到固定数量 shard（Stream 分裂为 topic-{shardId}）；同一 shard 由单线程串行消费
- 全局顺序：单 Stream 单 Consumer，ListenerContainer 内单线程消费

MVP 实现范围：分区顺序必做；全局顺序可选（v1.0 GA 必做）

#### 6.2.2 事务消息

采用类 Spring TransactionTemplate 风格——单方法 execute 接收回调

发送事务消息：
1) 实现 TransactionCallback<T> 接口，方法 execute(Message, TransactionContext) 返回 LocalTransactionState（COMMIT/ROLLBACK/UNKNOWN）
2) template.executeInTransaction(msg, callback)

事务回查 SPI：
@StreamMqTransactionListener
实现 TransactionChecker<T> 接口，方法 check(Message) 返回 LocalTransactionState

实现机制（对齐 RocketMQ）：
1. Producer 发送 half message 到 topic-half 专用 Stream
2. Producer 执行本地事务，根据返回值决定 commit/rollback/unknown
3. 后台任务定时扫描超时未确认的 half message，触发 TransactionChecker.check() 回查
4. 连续 N 次回查仍 UNKNOWN 强制 rollback

配置：
streammq.transaction.half-topic-prefix="streammq:half:"
streammq.transaction.check-interval=60s
streammq.transaction.check-max-times=15
streammq.transaction.transaction-timeout=6s

#### 6.2.3 延时消息

MVP 仅支持固定延时级别（对齐 RocketMQ 18 级）：
DelayLevel 枚举：SECOND_1, SECOND_5, SECOND_10, SECOND_30, MINUTE_1...MINUTE_10, MINUTE_20, MINUTE_30, HOUR_1, HOUR_2

发送 API：
MessageBuilder.topic().body().delayLevel(DelayLevel.MINUTE_30).build()
template.syncSend(msg)

实现机制：
1. 延时消息发送时写入 Redis ZSet：streammq:delay:{level}，score = expireAt
2. 后台定时任务（每秒扫描）取出 score <= now 的 entry
3. 转投到目标 topic 的 Stream
4. v1.0 增加任意延时 delayTimeMillis

#### 6.2.4 批量发送

采用 BatchMessage 包装类，底层通过 Redis Pipeline 一次性 XADD 多条

API：
BatchMessage.topic().add(msg1).add(msg2).add(msg3).build()
template.syncSendBatch(batch) 返回 List<SendResult>

实现机制：
- 底层使用 Redisson 的 RBatch（基于 Redis Pipeline）
- 一次 pipeline 内多次调用 XADD，减少 RTT
- 失败处理：failStrategy=PARTIAL_SUCCESS（返回每条独立结果）或 ALL_OR_NOTHING（任一失败全部回滚）

配置：
streammq.producer.batch.max-size=100, max-bytes=1mb, fail-strategy=PARTIAL_SUCCESS

#### 6.2.5 重试 + 死信队列

重试策略：
- 默认 16 次重试（对齐 RocketMQ）
- 重试间隔递增：10s, 30s, 1m, 2m, 3m, 4m, 5m, 6m, 7m, 8m, 9m, 10m, 20m, 30m, 1h, 2h
- 实现：失败消息写入 streammq:retry:{topic}:{group} ZSet，score = nextRetryAt；后台扫描重投

死信队列 DLQ：
- 超过 maxReconsumeTimes 仍失败的消息转入独立 Stream：streammq:dlq:{topic}:{group}
- DLQ 是普通 Stream，用户可通过 @StreamMqListener 消费 DLQ，或通过运维 REST 端点查询/重投
- 配置告警，DLQ 出现消息时告警通知

配置：
streammq.default.consumer.max-reconsume-times=16
streammq.default.consumer.retry.intervals=[10s,30s,1m,2m,3m,4m,5m,6m,7m,8m,9m,10m,20m,30m,1h,2h]
streammq.default.consumer.dlq.enabled=true, topic-suffix=".DLQ", alert-threshold=10

消费失败处理流程：
消息消费 → SUCCESS → ACK → 完成
         ↓ FAIL
         重试次数+1 → 是否超过 maxReconsume?
                       ↓ 否：写入 retry ZSet → 等待下次重投
                       ↓ 是：写入 DLQ Stream → 触发告警 Hook → 完成

---

### 6.3 Spring Boot Starter 集成

#### 6.3.1 自动装配策略

采用 `@EnableStreamMq` 注解手动启动模式（对齐 RocketMQ Spring Starter），用户必须在 Spring Boot 启动类显式标注才触发自动装配。

```java
@SpringBootApplication
@EnableStreamMq
public class OrderApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }
}
```

`@EnableStreamMq` 通过 `@Import(StreamMqAutoConfiguration.class)` 触发自动装配，装配内容：

- `RedissonClient` Bean（若用户未自定义）
- `StreamMqProducerFactory` / `StreamMqConsumerFactory` Bean
- `StreamMqTemplate` Bean
- `StreamMqListenerContainer` Bean 并扫描所有 `@StreamMqListener` 注解类
- `StreamMqHealthIndicator` / `StreamMqMetrics` Bean（若 Actuator 在 classpath）

#### 6.3.2 配置类层级

```
StreamMqProperties (顶层 @ConfigurationProperties("streammq"))
├── RedissonProperties (redisson 配置)
├── ProducerDefaultProperties (默认生产者配置)
├── ConsumerDefaultProperties (默认消费者配置)
├── List<ConsumerGroupProperties> (消费者组列表)
├── TransactionProperties (事务消息配置)
├── DelayProperties (延时消息配置)
├── DlqProperties (DLQ 配置)
├── ManagementProperties (运维端点配置)
└── TraceProperties (Trace 配置)
```

#### 6.3.3 Actuator 集成

提供两类 Actuator 端点：

1. **健康检查**：`/actuator/health` 下新增 `streammq` 子节点，返回 UP/DOWN 状态（Redis 连通性 + ConsumerGroup 状态）
2. **独立指标端点**：`/actuator/streammqmetrics` 暴露完整指标

健康检查响应示例：

```json
{
  "status": "UP",
  "components": {
    "streammq": {
      "status": "UP",
      "details": {
        "redis": "UP",
        "consumerGroups": [
          {"group": "order-consumer-group", "status": "UP", "pending": 0},
          {"group": "payment-consumer-group", "status": "UP", "pending": 3}
        ]
      }
    }
  }
}
```

#### 6.3.4 多 Redisson 数据源

支持多 Redis 实例：通过 `@EnableStreamMq(redissonRef = "secondaryRedissonClient")` 指定使用的 RedissonClient Bean 名称，可用于多机房/多租户场景。

### 6.4 可观测性

#### 6.4.1 可观测性总览

提供四层可观测能力：指标（Metrics） + 日志（Logs） + 轨迹（Trace） + 健康检查（Health）。

#### 6.4.2 Micrometer 指标

内置指标暴露到 Micrometer，可接入 Prometheus / Grafana / Datadog：

| 指标名 | 类型 | 说明 |
|---|---|---|
| `streammq.producer.send.total` | Counter | 发送总次数 |
| `streammq.producer.send.success` | Counter | 发送成功次数 |
| `streammq.producer.send.failure` | Counter | 发送失败次数 |
| `streammq.producer.send.latency` | Timer | 发送延迟分布 |
| `streammq.consumer.consume.total` | Counter | 消费总次数 |
| `streammq.consumer.consume.success` | Counter | 消费成功次数 |
| `streammq.consumer.consume.failure` | Counter | 消费失败次数 |
| `streammq.consumer.consume.latency` | Timer | 消费延迟分布 |
| `streammq.consumer.pending.size` | Gauge | 当前 pending 消息数 |
| `streammq.consumer.dlq.size` | Gauge | DLQ 累积消息数 |
| `streammq.consumer.reconsume.total` | Counter | 重试总次数 |
| `streammq.topic.backlog` | Gauge | Topic 积压消息数 |

#### 6.4.3 结构化日志 + MDC

集成 SLF4J MDC，所有日志自动注入以下上下文：

- `traceId`：消息 traceId（与 OpenTelemetry 兼容）
- `msgId`：消息 ID
- `topic`：消息主题
- `consumerGroup`：消费者组
- `shardingKey`：分片键（如有）

日志示例：

```
2026-06-29 14:23:11.234 INFO [traceId=abc123,msgId=1234567890-0,topic=order-topic,group=order-group] - 消费成功，耗时 12ms
```

#### 6.4.4 内置 Trace 模块 + SPI 双模

**内置 Trace（默认关闭，可开启）**：
- 配置 `streammq.trace.enabled=true` 开启
- Trace 数据写入独立 Redis Stream：`streammq:trace:{date}`
- Trace 记录：msgId, topic, group, producer/consumer, timestamp, latency, status, exception
- 提供查询 API：`StreamMqTraceService.query(msgId)` / `queryByTopic(topic, timeRange)`

**SPI Hook（默认开启）**：
- `ProducerInterceptor`：发送前后回调，可注入到 Micrometer Tracing / SkyWalking / Zipkin
- `ConsumerInterceptor`：消费前后回调
- 通过 `META-INF/services/` 或 Spring Bean 注册

#### 6.4.5 健康检查

见 6.3.3 章节，通过 `StreamMqHealthIndicator` 实现 Spring Boot Actuator 健康检查。

### 6.5 多协议兼容（Kafka / AMQP）

#### 6.5.1 多协议兼容总览

StreamMQ 提供三个协议兼容包，全部以独立 Maven 模块形式发布，用户按需引入：

| 模块 | artifactId | 兼容目标 | 兼容级别 |
|---|---|---|---|
| 原生 | `streammq-native` | Redis Stream 裸 API | 包装 Redisson RStream |
| Kafka 兼容 | `streammq-kafka-compat` | Kafka Client API 风格 | 类名 + 方法名对齐 |
| AMQP 兼容 | `streammq-amqp-compat` | RabbitMQ / AMQP API 风格 | 概念映射 + Channel 风格 API |

**重要边界声明**：StreamMQ v1.x 仅提供 API 风格兼容，不提供线网协议兼容（即不实现 Kafka wire protocol / AMQP 0.9.1 wire protocol server）。线网协议兼容评估延后至 v2.x。

#### 6.5.2 原生 API 模块 (streammq-native)

直接包装 Redisson RStream，提供更友好的 Producer/Consumer API，但保留 Redis Stream 原生概念（不引入 RocketMQ 抽象）：

```java
// 适合高级用户：保留 Stream 原生概念
NativeProducer producer = NativeProducer.builder()
    .stream("mystream")
    .redissonClient(redissonClient)
    .build();

StreamEntryId id = producer.xadd(map);
```

适用场景：需要直接操作 Stream 原生 API、需要最大灵活性、不希望引入 RocketMQ 概念的用户。

#### 6.5.3 Kafka API 风格兼容 (streammq-kafka-compat)

提供与 Kafka Client 同名的类与方法签名，底层调用 StreamMQ Core：

| Kafka Client 类 | StreamMQ Kafka 兼容包对应类 |
|---|---|
| `org.apache.kafka.clients.producer.KafkaProducer` | `io.github.streammq.kafka.KafkaProducer` |
| `org.apache.kafka.clients.consumer.KafkaConsumer` | `io.github.streammq.kafka.KafkaConsumer` |
| `org.springframework.kafka.core.KafkaTemplate` | `io.github.streammq.kafka.KafkaCompatTemplate` |
| `@org.springframework.kafka.annotation.KafkaListener` | `@io.github.streammq.kafka.KafkaListener` |
| `ProducerRecord<K,V>` | `ProducerRecord<K,V>`（同包名不同 namespace） |

使用方式：

```java
// 与 Kafka Client 几乎相同的代码，仅 import 不同
Properties props = new Properties();
props.put("bootstrap.servers", "redis://localhost:6379");  // 改为 Redis 地址
props.put("key.serializer", "io.github.streammq.kafka.serialization.StringSerializer");
props.put("value.serializer", "io.github.streammq.kafka.serialization.StringSerializer");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "key", "value");
producer.send(record);
```

**概念映射**：

| Kafka 概念 | StreamMQ 对应 |
|---|---|
| Topic | Topic (Redis Stream Key) |
| Partition | Topic 内部 shard（分区顺序消息使用） |
| Consumer Group | ConsumerGroup |
| Offset | Stream Last-delivered-id |
| Producer / Consumer | 同名类，底层调用 StreamMQ Core |

#### 6.5.4 AMQP API 风格兼容 (streammq-amqp-compat)

提供 RabbitMQ / AMQP 风格的 Channel API，概念映射到 StreamMQ：

| AMQP 概念 | StreamMQ 对应 |
|---|---|
| Exchange | Topic |
| Queue | ConsumerGroup |
| Binding (Exchange → Queue) | ConsumerGroup 订阅 Topic |
| Routing Key | Tag |
| Channel | AmqpChannel（包装 StreamMQ Producer/Consumer） |

使用示例：

```java
AmqpChannel channel = AmqpClient.create(config).newChannel();

// 声明 Exchange（实际创建 Topic）
channel.exchangeDeclare("order-exchange", "direct", true);

// 声明 Queue（实际创建 ConsumerGroup）
channel.queueDeclare("order-queue", true, false, false, null);

// 绑定（实际订阅）
channel.queueBind("order-queue", "order-exchange", "order.created");

// 发布
channel.basicPublish("order-exchange", "order.created", null, bodyBytes);
```

#### 6.5.5 兼容包实现策略

所有兼容包共享 StreamMQ Core 抽象，不重复实现 Redis 操作：

```
streammq-core  ← 共享核心
   ↓
streammq-native       ← 原生 Redis Stream API
streammq-kafka-compat ← Kafka API 风格
streammq-amqp-compat  ← AMQP API 风格
```

### 6.6 运维管理

#### 6.6.1 运维管理总览

提供 REST 端点支持运维操作：查询状态、重投消息、管理 ConsumerGroup、管理 DLQ。

#### 6.6.2 端点列表

| 端点 | 方法 | 路径 | 功能 |
|---|---|---|---|
| 查询 ConsumerGroup | GET | `/streammq/admin/groups` | 列出所有 ConsumerGroup 及状态 |
| 查询 pending | GET | `/streammq/admin/groups/{group}/pending` | 列出 pending 消息 |
| 查询 DLQ | GET | `/streammq/admin/dlq/{topic}/{group}` | 列出 DLQ 消息 |
| 重投 DLQ | POST | `/streammq/admin/dlq/{topic}/{group}/requeue` | 将 DLQ 消息重投到原 Topic |
| 删除 DLQ | DELETE | `/streammq/admin/dlq/{topic}/{group}/{msgId}` | 删除指定 DLQ 消息 |
| 手动 ack | POST | `/streammq/admin/groups/{group}/ack/{msgId}` | 手动 ack pending 消息 |
| 触发 rebalance | POST | `/streammq/admin/groups/{group}/rebalance` | 手动触发 rebalance |
| 动态创建 Topic | POST | `/streammq/admin/topics` | 动态创建 Topic |
| 删除 Topic | DELETE | `/streammq/admin/topics/{topic}` | 删除 Topic |
| 修改 ConsumerGroup 配置 | PUT | `/streammq/admin/groups/{group}/config` | 修改 maxReconsume 等配置 |

#### 6.6.3 安全策略

- **默认关闭**：`streammq.management.endpoints.enabled=false`，需显式开启
- **必须鉴权**：开启后强制要求 Basic Auth 或自定义 `ManagementAuthenticator` SPI
- **端口隔离**：建议通过 `streammq.management.server.port` 配置独立端口（与业务端口隔离）
- **生产环境建议**：通过 Spring Security 集成更严格的鉴权

#### 6.6.4 鉴权 SPI

```java
public interface ManagementAuthenticator {
    boolean authenticate(String username, String password, HttpServletRequest request);
}

// 通过 Bean 注册自定义鉴权
@Bean
public ManagementAuthenticator authenticator() {
    return (user, pass, req) -> ldapService.authenticate(user, pass);
}
```

---

## 7. 非功能需求

### 7.1 性能指标

| 指标 | MVP 目标 | v1.0 GA 目标 |
|---|---|---|
| 单节点发送 TPS | ≥ 10,000 | ≥ 15,000 |
| 单节点消费 TPS | ≥ 8,000 | ≥ 12,000 |
| 集群消费 TPS | - | ≥ 50,000（5 节点 + 10 消费者） |
| 端到端延迟 P99 | ≤ 5ms（同机房） | ≤ 10ms（同机房集群） |
| 端到端延迟 P50 | ≤ 2ms | ≤ 5ms |
| 消息体大小 | ≤ 1MB | ≤ 1MB（支持分片大消息） |

### 7.2 可靠性指标

| 指标 | 目标 |
|---|---|
| 消息不丢失率 | ≥ 99.99% |
| 消息不重复率 | ≥ 99.9%（依赖业务幂等） |
| 可用性（单 Redis 主从） | ≥ 99.9% |
| 可用性（Redis Cluster + Sentinel） | ≥ 99.99% |
| 故障恢复时间 | ≤ 30s（自动 rebalance） |

### 7.3 兼容性

| 项 | 兼容范围 |
|---|---|
| JDK | 21+（使用虚拟线程、Pattern Matching 等特性） |
| Spring Boot | 3.3.x+ |
| Redis Server | 7.2+（需要 Stream XADD MAXLEN ~ 限制、XAUTOCLAIM） |
| Redisson | 3.34.x+ |
| 序列化 | JSON / Protobuf / Kryo / Hessian / Avro（SPI 扩展） |
| Spring Cloud | 2024.x+（可选，Micrometer Tracing 集成） |

### 7.4 可扩展性

- 单 Stream 支持的 Consumer 数量：≥ 1000
- 单实例支持的 Listener 数量：≥ 100
- 单 Redis 集群支持的 Topic 数量：≥ 10000（受 Redis maxmemory 限制）
- 横向扩展：Consumer 实例数无上限，自动 rebalance

### 7.5 资源占用

| 资源 | 占用 |
|---|---|
| JVM 堆内存 | ≤ 256MB（默认配置，单实例） |
| JVM 非堆内存 | ≤ 128MB |
| 启动时间 | ≤ 3s（Spring Boot 集成） |
| CPU | ≤ 1 核（10k TPS 场景下） |

---

## 8. 约束与边界

### 8.1 明确不做的事

- **不做线网协议兼容**：v1.x 不实现 Kafka wire protocol server、AMQP 0.9.1/1.0 wire protocol server。原生 Kafka/RabbitMQ Client 无法零代码迁移
- **不做跨机房复制**：不内置跨机房数据同步机制
- **不做磁盘持久化**：所有数据存于 Redis 内存，不提供磁盘存储选项
- **不做消息堆积的无限支持**：单 Topic 堆积上限受 Redis 内存限制
- **不做严格 ACID 事务**：事务消息是最终一致，不是严格 ACID
- **不提供自研 Broker**：StreamMQ 是 SDK，不是独立部署的 Broker；复用业务 Redis

### 8.2 显式支持的边界

| 边界 | 上限 |
|---|---|
| 单消息体大小 | 1MB（默认）/ 10MB（配置上限） |
| 单 Topic 分区数 | 64（推荐 8-16） |
| 单 ConsumerGroup Consumer 数 | 1000 |
| 单实例 Listener 数量 | 100 |
| 堆积消息数 | 受 Redis 内存限制（建议 < 100w） |
| 延时消息最长时间 | 7 天 |

### 8.3 不推荐的使用方式

- 替代 RocketMQ/Kafka 处理超大规模数据管道（≥ 10w TPS 持续）
- 金融级强一致事务场景（应使用 RocketMQ 事务消息）
- 亿级消息堆积场景（应使用 Kafka）
- 多机房跨地域部署（应使用 RocketMQ Cluster / Pulsar Geo-Replication）
- 嵌入式 IoT 设备场景（应使用 MQTT Broker）

---

## 9. 里程碑与发布计划

### 9.1 版本路线

| 版本 | 目标 | 核心交付 |
|---|---|---|
| **v0.1.0 (MVP)** | 基础可用 | 核心消息能力 + Spring Boot Starter + 基础可观测性 + 分区顺序 + 延时消息（固定级别）+ 批量发送 + 重试+DLQ |
| **v0.2.0** | 高级能力补齐 | 事务消息 + 全局顺序 + 广播消费 + PULL 模式 + 完整 Micrometer 指标 + Actuator 集成 |
| **v1.0 GA** | 生产可用 | 原生 API 模块 + Kafka API 兼容 + AMQP API 兼容 + 运维 REST 端点 + 性能压测达标 + 至少 1 家生产用户 |
| **v1.1+** | 持续演进 | 任意延时消息 + Trace 内置模块 + Spring Cloud Stream Binder + 文档完善 + 生态扩展 |
| **v2.x** | 探索 | 线网协议兼容（评估）+ 跨机房复制（评估）+ 多后端抽象（评估） |

### 9.2 MVP 详细范围

**MVP 必做（v0.1.0）**：
- streammq-core：Message 模型、Producer/Consumer 抽象、CLUSTERING 消费、PUSH 模式
- streammq-spring-boot-starter：@EnableStreamMq / @StreamMqListener / @StreamMqProducer / StreamMqTemplate / 自动装配
- 高级能力子集：分区顺序消息、固定级别延时消息、批量发送、重试 + DLQ
- 可观测性：基础 Micrometer 指标 + 结构化日志 + MDC
- 配置：application.yml 全局配置 + 注解参数覆盖

**MVP 不做（推迟到 v0.2.0+）**：
- 事务消息、全局顺序、广播消费、PULL 模式、完整可观测性、原生 API 模块、Kafka/AMQP 兼容、运维 REST 端点

### 9.3 发布节奏

- v0.1.0：MVP 发布，SNAPSHOT 持续集成
- v0.2.0：高级能力补齐，Beta 测试
- v1.0.0 GA：生产可用，至少 1 家企业生产用户验证
- 后续：每 3 个月一个 minor 版本，每 6 个月一个 major 版本

---

## 10. 成功指标

### 10.1 量化成功指标

| 维度 | MVP 时 | v1.0 GA 时 | 1 年内 | 3 年内 |
|---|---|---|---|---|
| GitHub Star | ≥ 100 | ≥ 500 | ≥ 1,500 | ≥ 5,000 |
| Maven Central 月下载 | ≥ 100 | ≥ 500 | ≥ 2,000 | ≥ 10,000 |
| 生产用户 | 0 | ≥ 1 家 | ≥ 5 家 | ≥ 30 家（其中 ≥ 3 家中大型企业） |
| Contributor | 1-3 人 | ≥ 5 人 | ≥ 10 人 | ≥ 30 人 |
| Issue 处理时长 | ≤ 7 天 | ≤ 3 天 | ≤ 2 天 | ≤ 1 天 |

### 10.2 性能基准

定期发布性能基准报告，对标：
- 单节点 vs Redisson RStream 原生
- 单节点 vs Spring Data Redis Stream
- 集群 vs RocketMQ Spring Starter（同硬件）

### 10.3 社区健康度

- Issue 响应时长
- PR 合并时长
- 文档完整度（README + Wiki + 示例项目）
- 中文 + 英文双语 README

---

## 11. 风险与依赖

### 11.1 技术风险

| 风险 | 概率 | 影响 | 缓解措施 |
|---|---|---|---|
| Redis Stream 在极高并发下性能瓶颈 | 中 | 高 | 性能压测 + 提供分片策略 + 文档明确边界 |
| 事务消息实现复杂度超预期 | 高 | 中 | MVP 不做，v0.2 详细评估 |
| 自动 rebalance 实现稳定性 | 中 | 高 | 充分测试 + 灰度发布 + 兜底手动 rebalance |
| 虚拟线程兼容性（Redisson 等阻塞调用） | 中 | 中 | 评估 Redisson 虚拟线程兼容性 + 必要时回退线程池 |
| 多协议兼容工作量超预期 | 高 | 中 | MVP 不做，分阶段实现 |

### 11.2 外部依赖

| 依赖 | 风险 | 缓解 |
|---|---|---|
| Redisson | 版本兼容性、Bug 影响 | 锁定版本 + 跟进社区 + 必要时打补丁 |
| Redis Server | 版本特性依赖（XAUTOCLAIM 等） | 明确最低版本要求 7.2+ |
| Spring Boot | API 变化 | 锁定 3.3.x，跟进 Spring 6.x 变化 |
| Micrometer | 指标 API 变化 | 跟进 Micrometer 1.13+ |
| Java 21 | 虚拟线程 GA 稳定性 | 关注 OpenJDK 后续 LTS |

### 11.3 项目风险

| 风险 | 缓解 |
|---|---|
| 文档与代码不同步 | 文档驱动开发（本 PRD → 架构 → 实现） |
| 开源传播不及预期 | 提供 Getting Started 5 分钟 + 性能基准 + 与 Redisson/Spring Data 对比 |
| 与 Redisson 重复 | 明确差异化：StreamMQ 提供上层抽象 + 企业级特性 |

---

## 12. 附录

### 12.1 参考资料

- RocketMQ 官方文档：https://rocketmq.apache.org/docs/
- RocketMQ Spring Starter：https://github.com/apache/rocketmq-spring
- Redis Stream 官方文档：https://redis.io/docs/data-types/streams/
- Redisson 官方文档：https://github.com/redisson/redisson
- Spring Data Redis Reference：https://docs.spring.io/spring-data/redis/reference/
- Spring Kafka Reference：https://docs.spring.io/spring-kafka/reference/
- AMQP 0-9-1 Protocol：https://www.rabbitmq.com/amqp-0-9-1-quickref.html

### 12.2 竞品对比矩阵

| 特性 | StreamMQ | Redisson RStream | Spring Data Redis Stream | RocketMQ | Kafka | RabbitMQ |
|---|---|---|---|---|---|---|
| 底层 | Redis Stream | Redis Stream | Redis Stream | 自研 | 自研 | 自研 |
| 部署 | 复用 Redis | 复用 Redis | 复用 Redis | 独立集群 | 独立集群 | 独立集群 |
| 注解 | ✅ | ❌ | 部分 | ✅ | ✅ | ❌ |
| Template | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ |
| 消费模型 | 集群+广播 | 仅集群 | 仅集群 | 集群+广播 | 集群 | 集群+广播 |
| 顺序消息 | ✅ | ❌ | ❌ | ✅ | ✅ | ❌ |
| 事务消息 | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ |
| 延时消息 | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ |
| 重试+DLQ | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ |
| 自动 Rebalance | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ |
| 多协议兼容 | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| 可观测性 | ✅ | ❌ | 部分 | ✅ | ✅ | 部分 |

### 12.3 变更记录

| 版本 | 日期 | 变更 |
|---|---|---|
| v0.1-draft | 2026-06-29 | 初稿建立，包含愿景、用户、术语、核心能力、高级能力、非功能、约束、路线、指标、风险 |
