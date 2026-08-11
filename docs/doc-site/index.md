# StreamMQ

> 基于 Redis Stream + Redisson 的高性能消息中间件 SDK —— **让 Redis 成为你的消息总线**。

![License](https://img.shields.io/badge/License-MIT-blue.svg)
![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.5-green.svg)
![Redisson](https://img.shields.io/badge/Redisson-3.34.1-red.svg)
![Jackson](https://img.shields.io/badge/Jackson-2.18.1-yellow.svg)
![Version](https://img.shields.io/badge/Version-0.1.0--SNAPSHOT-lightgrey.svg)
![Tests](https://img.shields.io/badge/Tests-651%20passing-brightgreen.svg)

**GitHub**：https://github.com/streammq/streammq · **License**：MIT · **Maven groupId**：`io.github.streammq`

---

## 为什么选择 StreamMQ

如果你已经在使用 Redis，又需要一个轻量、可靠、易用的消息中间件，StreamMQ 是你的理想选择。它将 Redis Stream 的原生能力封装为一套类似 RocketMQ 的、面向业务开发者友好的消息 API。

### 🚀 零额外部署

无需引入独立 MQ 集群（如 RocketMQ / Kafka），复用现有 Redis 基础设施即可获得完整消息中间件能力。**一条 Redis 连接 = 一个消息总线**。

### 📝 注解驱动消费

`@StreamMQConsumer` 声明式定义消费者，一行注解即可开启消息监听。无需手动管理 ConsumerGroup、无需手写拉取循环，Spring 容器启动即自动注册。

### 🎯 RocketMQ 风格 API

`StreamMessageTemplate` 对齐 `RocketMQTemplate` 体验：`syncSend` / `asyncSend` / `sendOneway` / `syncSendBatch` / `executeInTransaction`，从 RocketMQ 迁移几乎零学习成本。

### 🌱 Spring Boot 3 深度集成

基于 Spring Boot 3.3.5 自动装配（`AutoConfiguration.imports`），配置绑定、Actuator 端点、Micrometer 指标，与 Spring 生态无缝衔接。

### 🧩 丰富特性开箱即用

事务消息、延时消息、顺序消息、批量发送、死信队列、消息过滤、背压控制、消费超时——企业级 MQ 该有的特性，StreamMQ 都有。

### 🔧 SPI 全链路可扩展

12 个 SPI 扩展点覆盖序列化、转换、过滤、拦截、重试、重平衡、追踪、鉴权、压缩、死信策略，业务方可在不修改框架代码的前提下定制任何环节。

---

## 快速代码示例

发送与消费，仅需 ~20 行代码：

```java
// 1. 发送消息
@Service
public class OrderService {
    @Autowired
    private StreamMessageTemplate template;

    public void sendOrder() {
        template.syncSend(MessageBuilder.<String>withTopic("order-topic")
                .tag("created")
                .keys("order-123")
                .body("Hello StreamMQ")
                .build());
    }
}

// 2. 消费消息
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {
    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        System.out.println("收到消息: " + message.getBody());
        return ConsumeAction.SUCCESS;
    }
}
```

[查看完整快速开始 →](quickstart.md)

---

## 核心特性一览

| 特性 | 说明 | 状态 |
|------|------|------|
| **注解驱动消费** | `@StreamMQConsumer` 声明式消费者，一行注解开启监听 | ✅ |
| **Template 编程模型** | `StreamMessageTemplate` 统一发送入口，同步/异步/单向/批量/事务 | ✅ |
| **4 种消费模型** | 集群消费 / 广播消费 / 顺序消费 / DLQ 死信消费 | ✅ |
| **事务消息** | 半消息 + 本地事务 + 事务回查机制，保证最终一致性 | ✅ |
| **延时消息** | 18 级固定延时 + 任意毫秒延时 | ✅ |
| **顺序消息** | ShardingKey 分片顺序消费，同分区内严格有序 | ✅ |
| **批量发送** | `BatchMessage` 批量投递，充分利用 Redis Pipeline 提升吞吐 | ✅ |
| **背压控制** | InflightQueue 拉取-处理解耦，防止内存溢出 | ✅ |
| **消费超时** | 单条消息消费超时自动取消并进入重试队列 | ✅ |
| **死信队列（DLQ）** | 重试耗尽后自动进入 DLQ，支持人工干预与二级死信 | ✅ |
| **消息过滤** | Tag 表达式（`tag1 \|\| tag2`）+ SQL92 表达式（`a = 1 AND b > 2`） | ✅ |
| **消息压缩** | GZIP 压缩（`CompressionCodec` SPI），超阈值自动压缩 | ✅ |
| **可观测性** | Micrometer 指标 + MDC 结构化日志 + Trace 查询 API | ✅ |
| **SPI 扩展机制** | 12 个扩展点，全链路可定制 | ✅ |
| **管理 REST 端点** | 内置 Admin REST API，支持 Topic / Consumer / DLQ 管理 | ✅ |

[查看全部特性详解 →](features.md)

---

## 架构概览

```
┌─────────────────────────────────────────────────────────────────────┐
│                        业务应用（Spring Boot 3）                      │
│                                                                     │
│   ┌──────────────┐     ┌──────────────────┐     ┌───────────────┐   │
│   │  @StreamMQ   │     │ StreamMessage    │     │  Admin REST   │   │
│   │  Consumer    │     │ Template         │     │  Endpoints    │   │
│   │  (注解驱动)   │     │ (发送入口)        │     │  (运维管理)    │   │
│   └──────┬───────┘     └────────┬─────────┘     └───────┬───────┘   │
│          │                      │                       │           │
│   ┌──────▼──────────────────────▼───────────────────────▼───────┐   │
│   │                  streammq-spring-boot-starter                │   │
│   │            （自动装配 + 配置绑定 + Actuator 端点）              │   │
│   └──────────────────────────┬──────────────────────────────────┘   │
│                              │                                      │
│   ┌──────────────────────────▼──────────────────────────────────┐   │
│   │                     streammq-core                            │   │
│   │  （无 Spring 依赖的核心 API：消息/消费/事务/过滤/拦截/SPI）      │   │
│   └──────────────────────────┬──────────────────────────────────┘   │
│                              │                                      │
│   ┌──────────────────────────▼──────────────────────────────────┐   │
│   │                    streammq-redisson                         │   │
│   │            （Redisson 适配器：Redis Stream 实现）              │   │
│   └──────────────────────────┬──────────────────────────────────┘   │
└──────────────────────────────┼──────────────────────────────────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Redis 7.2+        │
                    │   (Stream / ZSet /  │
                    │    Hash / Pub-Sub)  │
                    └─────────────────────┘
```

### 模块说明

| 模块 | 说明 |
|------|------|
| `streammq-bom` | BOM（物料清单），统一管理版本 |
| `streammq-core` | 核心 API（无 Spring 依赖），消息/消费/事务/过滤/拦截/SPI |
| `streammq-redisson` | Redisson 适配器，Redis Stream 底层实现 |
| `streammq-spring-boot-starter` | Spring Boot 3 自动装配与配置绑定 |
| `streammq-test` | 测试工具包（嵌入式 Redis、断言、Mock） |
| `streammq-samples` | 示例工程（quickstart / transaction / delay / orderly / dlq / interceptor） |

---

## 适用场景

- ✅ **已有 Redis 基础设施**，希望复用为消息总线，避免引入独立 MQ 集群
- ✅ **中小规模业务**（单集群日消息量 < 1 亿）
- ✅ **需要事务消息 / 延时消息 / 顺序消息**能力但不想引入重量级 MQ
- ✅ **微服务架构**下基于 Spring Boot 3 的轻量级异步通信
- ✅ **电商订单状态流转**、支付回调、库存扣减、通知推送等业务场景
- ✅ **日志采集、监控告警、任务调度**等异步处理场景

### 反场景（不建议使用）

- ❌ 单集群日消息量 > 1 亿的超大规模场景（建议使用 Kafka）
- ❌ 需要严格顺序跨 Topic 的场景（建议使用 RocketMQ）
- ❌ 消息体单条 > 1MB 的大对象传输（Redis 不擅长大 Value）

---

## 快速链接

| 文档 | 说明 |
|------|------|
| 📖 [快速开始](quickstart.md) | 5 分钟上手 StreamMQ |
| 📖 [核心特性](features.md) | 全部特性详解与代码示例 |
| 📖 [核心概念](concepts.md) | Topic / ConsumerGroup / 消费模型等关键术语 |
| 📖 [API 文档](api.md) | 完整 API 参考 |
| 📖 [配置参考](configuration.md) | 全部配置项说明 |
| 📖 [部署指南](deploy.md) | 生产环境部署建议 |
| 📖 [FAQ](faq.md) | 常见问题解答 |
| 📖 [贡献指南](contributing.md) | 参与项目贡献 |

---

## 版本信息

| 项 | 值 |
|----|----|
| 当前版本 | **0.1.0** |
| JDK | 21+ |
| Spring Boot | 3.3.5 |
| Redisson | 3.34.1 |
| Jackson | 2.18.1 |
| Redis | 7.2+ |
| Maven groupId | `io.github.streammq` |
| License | MIT |
| 测试用例 | 651 个全部通过 |

---

## V2.0 路线图（规划中）

| 规划特性 | 说明 |
|----------|------|
| `BackendProvider` SPI | 支持 Redis / Kafka / RabbitMQ / Pulsar 多后端 |
| Kafka wire protocol | Kafka 线协议兼容 |
| 跨数据中心复制 | Cross-datacenter replication |
| K8s Operator | Kubernetes Operator 部署管理 |
| Spring Cloud Stream Binder | Spring Cloud Stream 集成 |

---

*StreamMQ · 让 Redis 成为你的消息总线。*
