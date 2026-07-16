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
| Spring Cloud Stream Binder | P1 | `streammq-spring-cloud-stream-binder` | 实现 Spring Cloud Stream Binder SPI |
| 云原生增强 | P2 | `streammq-cloud-k8s` | K8s Operator / HPA 指标 / 优雅上下线 / ConfigMap 热更新 |
| 分布式追踪增强 | P2 | `streammq-tracing` | OpenTelemetry 原生集成 + 消息拓扑图 |
| 消息画像与诊断 | P3 | `streammq-diagnostics` | 消息生命周期可视化 + 异常诊断 |

---

## 4. 功能需求详细设计

### 4.1 Spring Cloud Stream Binder（P1）

#### 4.1.1 目标

实现 Spring Cloud Stream Binder SPI，让 Spring Cloud Stream 用户零代码切换到 StreamMQ。

#### 4.1.2 使用方式

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

#### 4.1.3 实现范围

- `StreamMQMessageBinder` 实现 `Binder<MessageChannel, ConsumerProperties, ProducerProperties>`
- 支持Spring Cloud Stream 的 `@EnableBinding` 注解
- 支持函数式编程模型（`java.util.function.Function` / `Supplier` / `Consumer`）
- 支持批量消费 (`spring.cloud.stream.bindings.input.consumer.batch-mode=true`)

### 4.2 云原生增强（P2）

#### 4.2.1 Kubernetes Operator

- CRD 定义：`StreamMQCluster` / `StreamMQTopic` / `StreamMQConsumerGroup`
- Operator 自动管理 Consumer 实例的伸缩
- 与 K8s HPA 集成，基于消费延迟自动扩缩容

#### 4.2.2 优雅上下线

- Consumer 启动时注册到 K8s Endpoints
- Consumer 收到 SIGTERM 时：停止拉取 → 完成处理中消息 → ACK → 注销 → 退出
- 超时强制退出（`spring-boot-graceful-shutdown-timeout`）

#### 4.2.3 配置热更新

- ConfigMap 变更自动感知
- 支持运行时动态调整：maxReconsumeTimes / consumeThread / scanInterval
- 不支持运行时变更：backend type / namespace / serializer

### 4.3 分布式追踪增强（P2）

#### 4.3.1 OpenTelemetry 原生集成

- 自动注入 Trace Context（W3C TraceContext 标准）
- Span 信息：producer.send / consumer.consume / retry / dlq
- 与 Micrometer Tracing / Zipkin / Jaeger / SkyWalking 无缝对接

#### 4.3.2 消息拓扑图

- 可视化消息流转路径：Producer → Topic → ConsumerGroup → Consumer
- 消息生命周期时间线：发送 → 入队 → 消费 → ACK/重试/DLQ
- 异常消息诊断：失败原因 / 堆栈 / 处理耗时分布

### 4.4 消息画像与诊断（P3）

#### 4.4.1 消息画像

- 每条消息的完整生命周期记录
- 消息指纹：topic + tag + keys + bodyHash
- 消费轨迹：哪些 Consumer 消费过、耗时多少、结果如何

#### 4.4.2 异常诊断

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
| **v2.0.0** | 云原生 + 可观测性增强 | K8s Operator + OpenTelemetry + Spring Cloud Stream Binder | Q4 2026 |
| **v2.1.0** | 诊断 + 优化 | 消息画像 + 异常诊断 + 性能优化 | Q1 2027 |

---

## 7. 风险与依赖

### 7.1 技术风险

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|---------|
| K8s Operator 运维复杂度 | 中 | 中 | 参考 Strimzi Kafka Operator 设计，充分测试 |
| OpenTelemetry 集成性能开销 | 低 | 中 | 异步上报，可配置采样率 |

### 7.2 外部依赖

| 依赖 | 用途 | 风险 |
|------|------|------|
| Spring Cloud Stream 4.x | Binder | API 变更 |
| Kubernetes Client Java | Operator | K8s 版本兼容 |
| OpenTelemetry SDK | 分布式追踪 | API 变更 |

---

## 8. 附录

### 8.1 术语表

| 术语 | 定义 |
|------|------|
| StreamMQMessageBinder | Spring Cloud Stream Binder 实现 |
| MessageTrace | 消息追踪记录，记录消息生命周期事件 |
| TopologyGraph | 消息拓扑图，描述 Producer → Topic → Consumer 的流转关系 |
| MessageProfile | 消息画像，完整记录单条消息的生命周期 |

### 8.2 参考文档

- [Kafka Protocol Specification](https://kafka.apache.org/protocol.html)
- [Spring Cloud Stream Reference](https://docs.spring.io/spring-cloud-stream/reference/)
- [Strimzi Kafka Operator](https://strimzi.io/)
- [OpenTelemetry Specification](https://opentelemetry.io/docs/specs/otel/)

### 8.3 变更记录

| 版本 | 日期 | 变更 |
|------|------|------|
| v2.0-draft | 2026-07-10 | 初稿建立，含多后端、跨机房、线网协议、云原生、持久化、追踪增强规划 |
