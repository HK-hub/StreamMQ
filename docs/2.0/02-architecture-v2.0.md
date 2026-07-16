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
│  后端实现层 (streammq-redisson)                                  │
│  Redis Backend (Redisson)                                       │
├─────────────────────────────────────────────────────────────────┤
│  底层客户端 (Redisson)                                           │
└─────────────────────────────────────────────────────────────────┘

横切关注点：配置 / 可观测性 / 异常处理 / 安全 / 云原生
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

## 3. 模块划分

### 3.1 V2.0 Maven 模块树

```
streammq
├── streammq-bom                          # BOM（依赖版本管理）
├── streammq-core                         # Core 层
├── streammq-redisson                     # Redis 后端（Redisson）
├── streammq-spring-boot-starter          # Spring Boot Starter
├── streammq-spring-cloud-stream-binder  # ★ V2.0 新增：Spring Cloud Stream Binder
├── streammq-cloud-k8s                   # ★ V2.0 新增：K8s Operator
├── streammq-tracing                     # ★ V2.0 新增：追踪增强
├── streammq-diagnostics                 # ★ V2.0 新增：消息画像与诊断
├── streammq-samples                      # 示例代码
└── streammq-test                         # 测试工具
```

### 3.2 模块依赖关系

```
                    streammq-bom
                        |
                    streammq-core
                   /      |      \
         streammq-redisson|   streammq-test
              \           |
               \          |
    streammq-spring-boot-starter
         /        \       \
    cloud-k8s   tracing  cloud-stream-binder
```

---

## 5. 云原生架构

### 5.1 Kubernetes Operator 架构

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

### 5.2 优雅上下线流程

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
| **CompressionCodec** | ✅ | ✅ | 压缩编解码 |

---

## 10. 关键技术决策

| 编号 | 决策项 | 选定方案 | 理由 |
|------|--------|---------|------|
| D1 | K8s Operator | Java + Fabric8 K8s Client | 与 Spring Boot 生态一致 |
| D2 | 延时消息后端 | Redis ZSet | 成熟稳定，高性能 |
| D3 | 配置热更新 | Spring Cloud Config + ConfigMap | 标准 Spring 方案 |
| D4 | 追踪集成 | OpenTelemetry SDK | 行业标准 |
| D5 | 消息格式 | 保持 V1.x Message 格式 | 向后兼容 |
