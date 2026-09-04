<div align="center">

# StreamMQ

### 让 Redis 成为你的消息总线

基于 Redis Stream + Redisson 构建的高性能消息中间件 SDK，提供类 RocketMQ 的编程体验

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.java.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.x-green.svg)](https://spring.io/projects/spring-boot)
[![Redisson](https://img.shields.io/badge/Redisson-3.34.x-red.svg)](https://redisson.org/)
[![Version](https://img.shields.io/badge/version-0.1.1-blue.svg)](https://github.com/HK-hub/StreamMQ)
[![CI](https://github.com/HK-hub/StreamMQ/actions/workflows/ci.yml/badge.svg)](https://github.com/HK-hub/StreamMQ/actions/workflows/ci.yml)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-ff69b4.svg)](https://github.com/HK-hub/StreamMQ/pulls)
[![Stars](https://img.shields.io/github/stars/HK-hub/StreamMQ?style=social)](https://github.com/HK-hub/StreamMQ)

</div>

---

> **StreamMQ** 是一款基于 **Redis Stream** 与 **Redisson** 构建的开源消息中间件 SDK，以 MIT 协议发布。它将 Redis Stream 的原生能力封装为一套类 RocketMQ 的、面向业务开发者友好的消息 API，让你在无需引入重量级 MQ 集群的前提下，获得注解驱动消费、事务消息、延时消息、顺序消息等企业级特性。

### 为什么要求 JDK 21

StreamMQ 0.1.1 硬性依赖 **JDK 21+**（在 `pom.xml` 中由 `maven-enforcer-plugin` 与 `requireJavaVersion [21,)` 强制）。这是有意为之：

- **虚拟线程（JEP 444）**是消费循环的默认执行模型——`Executors.newVirtualThreadPerTaskExecutor()` 在 JDK 21 才是 GA 状态。我们拒绝回退到平台线程池，因为高并发消费者的线程数量会与 Redis 连接池产生 1:N 放大效应。
- **模式匹配 + Record 模式**简化了 `ConsumeLoopTask` / `ConsumeAction` 等核心胶水代码。
- 我们在 0.2.0 路线图中**不会**降级到 JDK 17——如果你目前在 JDK 17 LTS，请评估是否可以在该项目内使用 JDK 21。Spring Boot 3.3.x 同时支持 JDK 17 与 21，但 StreamMQ 选择把赌注压在 21 上以避免为旧 JDK 写两套线程模型。

---

## 演示视频

| 资源 | 链接 |
|------|------|
| 📺 YouTube 演示视频 | 即将上线 |
| 🎯 Product Hunt 展示 | 即将上线 |
| 📝 GIF 制作指南 | [docs/demo/demo-gif-guide.md](docs/demo/demo-gif-guide.md) |
| 🖼️ 截图素材清单 | [docs/demo/screenshots/README.md](docs/demo/screenshots/README.md) |
| 🚀 一键演示脚本 | [docs/demo/quickstart-demo.sh](docs/demo/quickstart-demo.sh) |

> 💡 一键演示脚本已内置发送演示消息（应用启动即自动发送），并在超时未检测到消费时以非零退出码失败，便于录屏一次通过。

---

## 目录

- [为什么选择 StreamMQ](#为什么选择-streammq)
- [架构总览](#架构总览)
- [与同类产品对比](#与同类产品对比)
- [性能基准测试](#性能基准测试)
- [快速开始](#快速开始)
- [核心特性](#核心特性)
- [广播消费的运维注意事项](#广播消费的运维注意事项)
- [消费者不消费时的排查路径](#消费者不消费时的排查路径)
- [模块结构](#模块结构)
- [配置参考](#配置参考)
- [SPI 扩展机制](#spi-扩展机制)
- [可观测性](#可观测性)
- [示例工程](#示例工程)
- [文档导航](#文档导航)
- [路线图](#路线图)
- [贡献指南](#贡献指南)
- [社区](#社区)
- [安全](#安全)
- [许可证](#许可证)

---

## 为什么选择 StreamMQ

### 零额外部署

已有 Redis？你已经拥有了消息中间件。StreamMQ 复用现有 Redis 基础设施，无需引入 NameServer、Broker、Zookeeper 等额外组件，**一个 Redis 即是一个 MQ 集群**。

### 类 RocketMQ API 体验

对齐 RocketMQ `RocketMQTemplate` / `@RocketMQMessageListener` 的编程模型，迁移成本低，学习曲线平缓。如果你熟悉 RocketMQ，你已经会使用 StreamMQ。

### 丰富的高级特性

事务消息、18 级延时消息、顺序消息、批量发送、死信队列、消息压缩、消息过滤——开箱即用的企业级能力，不输独立 MQ 集群。

### Spring Boot 3 深度集成

自动装配、配置绑定、Actuator 端点、Micrometer 指标——与 Spring 生态无缝衔接，`@EnableStreamMQ` 一键开启。

### 深度可扩展

序列化器、转换器、过滤器、拦截器、重试策略、重平衡策略、压缩编解码器、死信失败策略、管理鉴权器、链路追踪采集器——几乎一切可替换。0.1.1 提供 **10 个面向用户的扩展点**（外加 6 个内部装配点，总计 16 个可覆盖 Bean，详见 [SPI 扩展机制](#spi-扩展机制)）。

### 质量与发布姿态

单元测试 ≥ 780 个（由 `mvn test` 实际产生，surefire 报告可逐文件复现）， 集成测试 ≥ 80 个（由 `mvn verify` 在 Redis 可用时执行，CI 集成 tripwire 保证数量下限）—— surefire/failsafe 报告可逐文件复现。 覆盖核心消息能力、事务流程、延时投递、顺序消费、DLQ 处理、PEL 认领、广播消费等场景。

> **版本姿态（诚实声明）**：0.1.x 为**功能预览版**——核心能力、测试体系、文档齐全。
> 端到端消费吞吐 JMH 基准框架已就绪（见「性能基线」章节），具体数字需在你的目标硬件与 Redis 实例上实测。
> 生产容量规划请以自己环境的实测为准，并在试点期保持可回退（消息总线建议从非核心链路灰度）。

---

## 架构总览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        StreamMQ Architecture                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌───────────────────────────────────────────────────────────────────┐ │
│   │                    Spring Boot Application                       │ │
│   │  ┌─────────────┐  ┌──────────────┐  ┌──────────────────────────┐ │ │
│   │  │@EnableStreamMQ│ │@StreamMQConsumer│ │  StreamMessageTemplate  │ │ │
│   │  │  (自动装配)   │  │  (声明式消费)  │ │   (统一发送入口)         │ │ │
│   │  └──────┬──────┘  └──────┬───────┘  └───────────┬──────────────┘ │ │
│   └─────────┼─────────────────┼─────────────────────┼────────────────┘ │
│             │                 │                     │                  │
│   ┌─────────▼─────────────────▼─────────────────────▼────────────────┐ │
│   │                     streammq-core                                │ │
│   │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │ │
│   │  │ Message  │ │ Template │ │ Consumer │ │Producer  │ │Transaction│ │
│   │  │ Builder  │ │ Service  │ │ Listener │ │ Factory  │ │ Executor │ │
│   │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └────────┘ │ │
│   │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │ │
│   │  │ Filter   │ │Interceptor│ │ Retry    │ │ Rebalance│ │  DLQ   │ │ │
│   │  │ Chain    │ │ Chain    │ │ Policy   │ │ Strategy │ │Handler │ │ │
│   │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └────────┘ │ │
│   │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │ │
│   │  │Serializer│ │Converter │ │Codec     │ │ Trace    │ │Metrics │ │ │
│   │  │  (SPI)   │ │  (SPI)   │ │  (SPI)   │ │Collector │ │  (Mic.)│ │ │
│   │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └────────┘ │ │
│   └───────────────────────────────────────────────────────────────────┘ │
│             │                 │                     │                  │
│   ┌─────────▼─────────────────▼─────────────────────▼────────────────┐ │
│   │                   streammq-redisson                               │ │
│   │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │ │
│   │  │ Redisson │ │ Stream   │ │ Delay    │ │ PEL      │ │ Tx     │ │ │
│   │  │ Producer │ │ Listener │ │ Scheduler│ │ Claimer  │ │Scanner │ │ │
│   │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └────────┘ │ │
│   └───────────────────────────────────────────────────────────────────┘ │
│             │                                                           │
│   ┌─────────▼────────────────────────────────────────────────────────┐ │
│   │                       Redis 7.2+                                 │ │
│   │   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │ │
│   │   │  Stream  │  │   ZSet   │  │   Hash   │  │  Sorted  │       │ │
│   │   │ (消息存储)│  │(延时队列)│  │(事务状态)│  │   Set    │       │ │
│   │   └──────────┘  └──────────┘  └──────────┘  └──────────┘       │ │
│   └──────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 与同类产品对比

| 能力 | StreamMQ | Redisson RStream | Spring Data Redis Stream | RocketMQ | Kafka |
|------|----------|-----------------|-------------------------|----------|-------|
| 底层存储 | Redis Stream | Redis Stream | Redis Stream | NameServer+Broker | Broker+KRaft |
| 部署复杂度 | **低（仅 Redis）** | 低（仅 Redis） | 低（仅 Redis） | 高（独立集群） | 高（独立集群） |
| 注解声明式消费 | **支持** | 不支持 | 部分支持 | 支持 | 不支持 |
| Template 编程模型 | **支持** | 不支持 | 不支持 | 支持 | 支持（KafkaTemplate，spring-kafka 提供，非注解声明式） |
| 事务消息 | **支持** | 不支持 | 不支持 | 支持 | 不支持 |
| 延时消息 | **支持（18 级+任意）** | 不支持 | 不支持 | 支持（18 级） | 不支持 |
| 顺序消息 | **支持** | 不支持 | 不支持 | 支持 | 支持（分区内） |
| 死信队列 | **支持（含二级 DLQ）** | 不支持 | 不支持 | 支持 | 支持（spring-kafka DLT/@RetryableTopic，非注解式） |
| 消息过滤 | **Tag + SQL92** | 不支持 | 不支持 | 支持 | 不支持 |
| 消息压缩 | **支持（GZIP SPI）** | 不支持 | 不支持 | 支持 | 支持 |
| 背压控制 | **支持（InflightQueue）** | 不支持 | 不支持 | 支持 | 支持 |
| Spring Boot 3 集成 | **深度集成** | 一般 | 一般 | 一般（第三方） | 一般（第三方） |
| SPI 扩展点数量 | **16 个** | 0 | 0 | 少量 | 少量 |
| 管理接口 | **REST API + Actuator** | 无 | 无 | Dashboard | 无 |
| 链路追踪 | **支持（TraceCollector SPI）** | 不支持 | 不支持 | 支持 | 不支持 |
| 学习成本 | **低** | 中 | 中 | 中 | 中 |
| 适用规模 | 中小规模（< 1 亿/天） | 中小规模 | 中小规模 | 大规模 | 超大规模 |

---

## 性能基准测试

### 测试环境

| 项目 | 配置 |
|------|------|
| JDK | OpenJDK 21.0.11 (Eclipse Adoptium) |
| Spring Boot | 3.3.5 |
| Redisson | 3.34.1 |
| Redis | 7.x (本地单机, 无密码) |
| JMH | 1.37 |
| 操作系统 | Windows 11 |
| 连接池 | 16 连接, 4 最小空闲 |

### 性能基线（方法学声明）

> ⚠️ **重要：以下数字是 0.1.1 本地实测快照**（2026-09-02，localhost Redis，JDK 21，笔记本级硬件）：
> - 序列化基准已加入 JMH `Blackhole` 消费，防止 JIT 死码消除导致吞吐虚高
> - 消费基准已重写为「XREADGROUP 拉取 → 反序列化 → 业务回调 → XACK」完整端到端路径，并配合持续灌数
> - 此前 README 引用的 "Stream 消费吞吐 ~269,760 ops/s" 来自一个测量**空 XREADGROUP 网络往返**的破损基准，已移除
> - 误差栏为 99.9% CI；笔记本级硬件结果仅供参考，生产环境请以自己的实测为准

> 我们公开承认 v0.1.0 之前曾发布过有方法学缺陷的基准数字（死码消除、灌数耗尽、缺 ACK）。这种透明度比"假装没发过"更重要。**生产容量规划请以你自己环境的实测为准。**

### 序列化性能 (Throughput, ops/s) — 0.1.1 实测

测试 1KB 消息体的序列化/反序列化吞吐量（messageCount=1000，含 Blackhole 消费）。JMH fork=1，warmup=1×2s，measurement=2×3s。

| 序列化器 | Serialize (ops/s) | Deserialize (ops/s) | RoundTrip (ops/s) | 单次序列化 (ops/s) | 单次反序列化 (ops/s) |
|----------|-------------------|---------------------|-------------------|--------------------|----------------------|
| **Fury** | **~5,205,112** | **~4,542,655** | **~2,123,210** | **~5,215,574** | **~4,630,521** |
| Jackson  | ~401,806 | ~914,020 | ~192,823 | ~391,602 | ~912,513 |
| JDK      | ~455,704 | — | — | ~455,704 | — |

> **结论**: Fury 序列化吞吐量是 Jackson 的 **~7-13x**，是 JDK 的 **~10x**（数字会因 JDK/硬件/负载而漂移）。

### 消息发送性能 (Throughput, ops/s) — 0.1.1 实测

单实例同步/异步发送，直连 localhost Redis。JMH fork=1，warmup=1×2s，measurement=2×3s。

| 发送模式 | 100B 负载 (ops/s) | 1KB 负载 (ops/s) | 10KB 负载 (ops/s) |
|----------|-------------------|------------------|-------------------|
| **异步批量发送** (batch=100) | **~12,513** | **~11,780** | **~8,326** |
| 同步批量发送 (batch=10) | ~3,640 | ~3,765 | ~2,863 |
| 同步单条发送 | ~3,741 | ~3,610 | ~2,600 |

> **结论**: 异步发送性能约为同步的 **3~4 倍**（同样依赖硬件与 Redis 网络 RTT）。

### 消息消费性能 — 0.1.1 实测

端到端完整消费路径：XREADGROUP + 字段解码 + 回调 + XACK（含持续灌数）。JMH fork=1，warmup=1×2s，measurement=3×3s。

| 消费模式 | 说明 | 1KB (ops/s) | 10KB (ops/s) |
|----------|------|-------------|--------------|
| `consumeThroughput` | 完整消费路径（含网络往返、反序列化、ACK） | **~2,383** | **~2,018** |
| `serializationRoundTrip` | Jackson 序列化/反序列化回环（含网络） | ~270,705 | ~19,249 |
| `messageCreateAndConsume` | 纯内存消息构建 + 回调（无网络） | ~5,857,147 | ~6,134,699 |

> `consumeThroughput` 是 MQ 最关键的容量指标：它测量的是真实端到端消费路径（含 Redis 网络往返、
> 反序列化、业务回调、XACK 确认），而非空读往返。不同硬件、Redis 实例、网络延迟下数字会有显著差异。

> 自行运行基准：`mvn -B -Pbenchmark -pl streammq-benchmark exec:exec@benchmark-template exec:exec@benchmark-serialization exec:exec@benchmark-consumer -Dstreammq.benchmark.allowFlush=true`
> 或按 [`.github/workflows/benchmark.yml`](.github/workflows/benchmark.yml) 手动触发 CI 基准任务，
> 结果会以 JMH 产物形式回填。

### 性能优化建议

1. **序列化选择**: 默认 **Apache Fury**（`streammq.producer.serializer` 默认值即
   `io.github.streammq.adapter.redisson.serializer.FurySerializer`，该属性类型为 `Class<? extends MessageSerializer>`，
   需填写**全限定类名**），其吞吐量是 Jackson 的 7 倍以上；Fury 默认不强制类注册，任意 POJO 开箱即用，
   共享/多租户 Redis 建议开启类注册白名单（见 [反序列化安全](#反序列化安全)）。需要 JSON 可读性/跨语言互通时再切回 `JacksonJsonSerializer`
2. **发送策略**: 高吞吐场景使用 `asyncSend`，可提升 4~5 倍性能
3. **负载大小**: 10KB 大消息建议启用 GZIP 压缩（`MessageCompressor` SPI）
4. **连接池**: 默认 16 连接可满足多数场景，高并发可调至 32~64。**Sizing 经验**：
   - 公式：`(consumers × consumeThreadMin) + producers + scheduler_threads + 4 headroom`。
   - 100 个 consumer、`consumeThreadMin=4`：需 400+ 连接（虚拟线程会全部并发发起 XREADGROUP）。
   - 启动时监控 Redisson 活跃连接数 / 池大小，接近 80% 即扩容。
5. **批量消费**: 使用 `pullBatchSize`（注解）或 `streammq.consumer.batch-size`（全局配置）批量拉取，减少网络往返

---

## 快速开始

### 环境要求

| 组件 | 最低版本 | 推荐版本 |
|------|----------|----------|
| JDK | 21 | 21+ |
| Maven | 3.9 | 3.9+ |
| Redis | 7.2 | 7.2+ |
| Spring Boot | 3.3 | 3.3.5 |

> ⚠️ **`mvn verify` 需本地 Redis（`localhost:6379`）。** 该命令会运行集成测试，无 Redis 时自动跳过；CI 通过 Docker service 提供 Redis。

### 1. 引入依赖

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.github.streammq</groupId>
            <artifactId>streammq-bom</artifactId>
            <version>0.1.1</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <dependency>
        <groupId>io.github.streammq</groupId>
        <artifactId>streammq-spring-boot-starter</artifactId>
    </dependency>
    <dependency>
        <groupId>org.redisson</groupId>
        <artifactId>redisson-spring-boot-starter</artifactId>
    </dependency>
</dependencies>
```

> ⚠️ 必须同时引入 `redisson-spring-boot-starter` 以提供 `RedissonClient` Bean。
> StreamMQ **有意**把 `redisson` 声明为 `provided` scope——这样你可以自由决定 Redis 客户端版本，
> 代价是必须自己引入它。若忘记，启动时 StreamMQ 的 `FailureAnalyzer` 会拦截原本语焉不详的
> `NoSuchBeanDefinitionException`，直接给出上面这段依赖声明与配置示例。

### 2. 配置

```yaml
spring:
  application:
    name: streammq-demo

streammq:
  enabled: true
  namespace: streammq

redisson:
  singleServerConfig:
    address: "redis://127.0.0.1:6379"
    database: 0
```

### 3. 启用

```java
@SpringBootApplication
@EnableStreamMQ
public class DemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
```

> 💡 `@EnableStreamMQ` 是一个显式标记注解，**不会**触发额外装配——所有核心 Bean 都通过 `META-INF/spring/AutoConfiguration.imports` 在 starter 出现在 classpath 时自动注册。不写 `@EnableStreamMQ` 也能跑通，添加它仅为了在代码上明确表达"使用 StreamMQ"。

### 4. 发送消息（推荐：使用 `StreamMessageService` 门面）

```java
@Component
public class OrderService {

    // 推荐注入 StreamMessageService：业务友好的薄门面（topic + body + 元数据），
    // 等价于 StreamMessageTemplate 的简写形式。
    private final StreamMessageService messageService;

    public OrderService(StreamMessageService messageService) {
        this.messageService = messageService;
    }

    public SendResult sendOrder(String orderId, String content) {
        return messageService.send(
                "order-topic",
                content,
                MessageMetadataBuilder.create()
                        .tag("created")
                        .keys(orderId)
                        .withUserProperty("traceId", "t-001"));
    }
}
```

> **高级用户**：需要访问拦截器/过滤器/SPI 能力时，改为注入 `StreamMessageTemplate`（详见 [进阶用法](#进阶用法)）。

### 5. 消费消息

```java
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-consumer-group")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {

    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        System.out.println("收到订单：" + message.getKeys() + ", 内容：" + message.getBody());
        return ConsumeAction.SUCCESS;
    }
}
```

就这样！启动应用，发送一条消息，消费者会自动接收并处理。

---

## 核心特性

### 注解驱动消费

一行注解，声明式定义消费者，支持并发消费、顺序消费、广播消费、DLQ 消费四种模型。

```java
// 并发消费（默认）
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group")

// 顺序消费
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group",
                  messageModel = MessageModel.ORDERLY, shardCount = 8)

// 广播消费
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group",
                  consumeMode = ConsumeMode.BROADCASTING)

// DLQ 消费
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group", dlqMode = true)
```

> ⚠️ **广播消费会为每个容器实例创建一个独立的 Redis 消费者组，且组名随实例重启而变。**
> 这意味着组的总数约等于心跳超时窗口内「实例数 × 重启次数」的累积量，持续增长会占用 Redis 内存
> （每个组都有自己的 PEL）。生产使用广播模式前，请务必阅读
> [广播消费的运维注意事项](#广播消费的运维注意事项)。

### StreamMessageTemplate 编程模型（高级）

`StreamMessageTemplate` 是发送 API 的完整形态——所有拦截器 / 过滤器 / SPI 访问器都在这里暴露。**业务代码建议优先使用 `StreamMessageService` 门面**（见 [快速开始](#4-发送消息推荐使用-streammessageservice-门面)），仅在需要直接操作 SPI 时才注入 `StreamMessageTemplate`。

0.1.1 起 API 已收敛：每个发送模式仅保留一个 `SendOptions` 规范形，此前的 timeout / retry / callback 伸缩重载全部移除；零参便捷形式以 default 方法提供。

```java
public interface StreamMessageTemplate {
    // 规范形（唯一参数空间）
    <T> SendResult syncSend(Message<T> message, SendOptions options);
    <T> CompletableFuture<SendResult> asyncSend(Message<T> message, SendOptions options);
    <T> List<SendResult> syncSendBatch(BatchMessage<T> batch, SendOptions options);
    <T> void sendOneway(Message<T> message);          // fire-and-forget
    <T> SendResult executeInTransaction(Message<T> message, TransactionCallback<T> callback);

    // 便捷 default 方法
    <T> SendResult syncSend(Message<T> message);
    <T> CompletableFuture<SendResult> asyncSend(Message<T> message);
    <T> void asyncSend(Message<T> message, SendCallback callback);
    <T> void asyncSend(Message<T> message, SendOptions options, SendCallback callback);
    <T> List<SendResult> syncSendBatch(BatchMessage<T> batch);

    // SPI 访问器：拦截器/过滤器/转换器管理
}
```

`StreamMessageService` 门面同步收敛为三种正交维度：发送模式 × 载体形态（完整 Message 或
topic+body+`MessageMetadataBuilder`）× 参数（`SendOptions` / 元数据内联超时重试）。

### 事务消息

半消息 + 本地事务 + 回查机制，保证最终一致性。

```java
// 发送事务消息
TransactionCallback<String> callback = (message, ctx) -> {
    try {
        executeLocalTransaction(message.getBody());
        return LocalTransactionState.COMMIT_MESSAGE;
    } catch (Exception e) {
        return LocalTransactionState.ROLLBACK_MESSAGE;
    }
};
SendResult result = template.executeInTransaction(message, callback);

// 事务回查器
@Component
@StreamMQTransactionConsumer(transactionGroup = "default-tx-group")
public class TransactionCheckerImpl implements TransactionChecker<String> {
    @Override
    public LocalTransactionState check(Message<String> message, TransactionContext context) {
        return checkLocalTransactionStatus(context.getTransactionId());
    }
}
```

### 延时消息

内置 18 级固定延时，亦可自定义任意毫秒延时。

```java
// 固定延时（18 级）
Message<String> msg1 = MessageBuilder.<String>withTopic("delay-topic")
        .body("content")
        .delayLevel(DelayLevel.MINUTE_5)   // 延时 5 分钟
        .build();

// 任意延时毫秒
Message<String> msg2 = MessageBuilder.<String>withTopic("delay-topic")
        .body("content")
        .delayTimeMillis(15 * 60 * 1000L)  // 延时 15 分钟
        .build();
```

**延时级别对照表：**

| 级别 | 延时 | 级别 | 延时 | 级别 | 延时 |
|------|------|------|------|------|------|
| `SECOND_1` | 1s | `MINUTE_3` | 3m | `MINUTE_20` | 20m |
| `SECOND_5` | 5s | `MINUTE_4` | 4m | `MINUTE_30` | 30m |
| `SECOND_10` | 10s | `MINUTE_5` | 5m | `HOUR_1` | 1h |
| `SECOND_30` | 30s | `MINUTE_6` | 6m | `HOUR_2` | 2h |
| `MINUTE_1` | 1m | `MINUTE_7` | 7m | | |
| `MINUTE_2` | 2m | `MINUTE_8` | 8m | | |
| `MINUTE_9` | 9m | `MINUTE_10` | 10m | | |

### 顺序消息

基于 ShardingKey 的分片顺序消费，保证同一分片内严格有序。

> 实现：单 Stream + 分片分布式锁。消费失败时在当前线程内按 `maxReconsumeTimes` 重试，每次失败按
> `suspendCurrentQueueTimeMillis`（默认 1000ms）挂起，不越过失败消息继续消费（严格有序）；重试耗尽后直接进入 DLQ。
> 消费者实例崩溃后的消息由 PEL 认领调度器恢复重投（空闲阈值默认 60s，且会检查分片锁活性——
> 正在处理中的消息不会被认领）。
> 分片并发控制由分布式锁保证（获取等待上限默认 5s，超时转 RECONSUME_LATER 防止持有者挂死导致分片停摆）；
> `RebalanceStrategy` 提供分片分配元数据（assignment Hash + REBALANCE 通知，供管理端点观测），
> 手动重平衡见 `/actuator/streammq/rebalance/{group}`。

**消费超时（可选，推荐开启）：** 顺序消费默认不设消费超时——卡死的 handler 会持有分片锁并阻塞消费循环，直到进程重启。
通过 `@StreamMQConsumer(orderlyConsumeTimeout = 60000)`（毫秒）开启后，单次消费超过该时长即视为失败：
框架取消任务并按 `RECONSUME_LATER` 重试，消费循环不再被卡死 handler 阻塞；重试在 `maxReconsumeTimes` 次数内进行，
耗尽后消息进入 DLQ。注意：

- 若业务 handler 不响应线程中断，原消费线程仍可能继续运行，其分片锁会在 handler 返回时由任务自身的 finally 释放；
  该窗口内同分片的其它投递会被锁拒绝并重试，**不会破坏严格有序**；
- 重试是严格串行的，设置过小的超时会把慢消息快速送入 DLQ，建议按业务最慢耗时的 2 倍以上配置；
- 超时语义与并发消费一致：**业务层必须保证幂等**（原消费与重试副本可能并发执行）。

**全局开启：** 逐个消费者写注解很繁琐，可通过 `streammq.consumer.orderly-consume-timeout-millis`
一次性为所有顺序消费者设置默认值。优先级为「注解显式值（`> 0`）> 全局配置」，因此全局开启后
无法用注解单独关闭某个消费者——需要对该消费者放松保护时，请设置一个足够大的值：

```yaml
streammq:
  consumer:
    orderly-consume-timeout-millis: 60000   # 全局默认；0=不启用（默认）
```

**为什么不使用 `consumeTimeout`：** 两者语义与默认值都不同，不能合并：

| | `consumeTimeout`（并发） | `orderlyConsumeTimeout`（顺序） |
|---|---|---|
| 默认值 | 30000，**默认启用** | 0，**默认关闭** |
| 超时后果 | 单条消息重投，不影响其它消息 | 串行重试＋分片挂起，耗尽 `maxReconsumeTimes` 后进 **DLQ** |
| 保护目标 | 单消息吞吐 | 分片可用性 |

若顺序消费复用 `consumeTimeout` 的非零默认值，等于把所有存量顺序消费者的慢消息系统性送入 DLQ
（破坏性变更）。因此独立为 opt-in 属性，由业务按最慢耗时显式评估后开启。

```java
@Component
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-orderly-group",
    messageModel = MessageModel.ORDERLY,
    shardCount = 8
)
public class OrderOrderlyConsumer implements StreamMessageOrderlyConsumer<String> {
    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeOrderlyContext context) {
        processOrder(message.getBody());
        return ConsumeAction.SUCCESS;
    }
}

// 发送时指定 shardingKey
Message<String> message = MessageBuilder.<String>withTopic("order-topic")
        .shardingKey("user-123")
        .body("content")
        .build();
```

### 批量发送

`BatchMessage` 批量投递，充分利用 Redis Pipeline 提升吞吐。

```java
BatchMessage<String> batch = BatchMessage.<String>withTopic("order-topic")
        .add(msg1)
        .add(msg2)
        .add(msg3)
        .build();

List<SendResult> results = template.syncSendBatch(batch);
```

### 死信队列

消费重试耗尽后的消息自动进入 DLQ，支持二级 DLQ 与自定义失败策略。

```java
@Component
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    dlqMode = true
)
public class OrderDlqConsumer implements StreamMessageConcurrentlyConsumer<String> {
    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        handleDeadLetter(message);
        return ConsumeAction.SUCCESS;
    }
}
```

### 消息过滤

支持 Tag 表达式与 SQL92 表达式两种过滤模式。

```java
// Tag 过滤
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    selectorExpression = "tag1 || tag2"
)

// SQL92 过滤
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    selectorType = SelectorType.SQL92,
    selectorExpression = "a = 1 AND b > 2"
)
```

### 消息压缩

通过 `CompressionCodec` SPI 支持 GZIP 压缩，可配置压缩阈值。

```yaml
streammq:
  producer:
    compress-threshold: 1024    # 消息体超过 1KB 时自动压缩
```

---

## 广播消费的运维注意事项

**这是使用 `ConsumeMode.BROADCASTING` 前必须理解的一条实现语义。**

### 行为

Redis 的消费者组天然是"组内竞争消费"。要实现广播（每条消息投递给所有实例），StreamMQ 的做法是：

```
每个容器实例 → 一个独立的 Redis 消费者组（组名后缀为容器级随机标识）
```

该标识**跨重启不保证相同**（容器级 UUID，见
`DefaultStreamMQListenerContainer#instanceToken`）。因此：

- **每次重启都会产生一个新组**，旧组不会立即消失；
- 旧组由回收任务在心跳超时后清理（`RedissonStreamListener#sweepStaleBroadcastGroups`）；
- 清理前的窗口内，组的总数 = 心跳超时窗口内的「实例数 × 重启次数」；
- 每个组都持有自己的 PEL，**会占用 Redis 内存**。

### 容量估算

```
稳态组数量 ≈ 实例数
峰值组数量 ≈ 实例数 × (心跳超时窗口内的最大重启次数)
```

心跳超时由 `streammq.group.instance-timeout-ms` 控制（默认见 `StreamMQConstants`）。

### 需要监控的信号

| 指标 | 获取方式 | 异常含义 |
|------|----------|----------|
| 广播组条目数 | `GET /actuator/streammq` → `broadcastGroups` | 持续增长 = 实例崩溃循环，或心跳超时配置过长 |
| 单轮清理量 | 日志 `Swept N stale broadcast group(s): namespace=..., remaining=M` | N 长期为 0 但 `remaining` 持续增长 = 回收任务未生效 |
| Redis 内存 | `INFO memory` | 与上面两个数字交叉验证 |

### 建议

1. **不要对频繁重启的工作负载使用广播模式**（如 CI 环境、反复 OOM 的 Pod）。
2. 为 `broadcastGroups` 建立告警：超过「实例数 × 3」即排查。
3. 广播模式下消费者组**无法复用消费位点**——重启后新组从当前时间点开始消费，
   重启期间产生的消息**不会**被补投。若需要重启不丢消息，请使用集群消费
   （`ConsumeMode.CLUSTERING`）或自行实现持久化位点。

---

## 消费者不消费时的排查路径

消费者"注册成功但从不消费"是 StreamMQ 中最容易被误判为"消息丢了"的现象。按以下顺序排查：

1. **看健康状态**：`GET /actuator/health` → `streammq` 组件。
   若存在消费循环启动失败，会返回 `DOWN`，并在详情中给出
   `listenerContainer.consumeLoopFailures`（`loopKey → 失败原因`）。
2. **看总览**：`GET /actuator/streammq` → `status` 字段同样反映该状态。
3. **看日志**：`Failed to create consumer for listener` 的 ERROR 行含 topic/group 与根因
   ——最常见的是 Redis 凭据错误、消费者组名非法、命名空间不一致。
4. **看容器状态**：`/actuator/streammq/groups` 中的 `containerRunning` 字段。

> 消费循环启动失败**不会**自动重试。修复根因后需要重启应用（或调用管理端点的重平衡接口）。

---

## 模块结构

| 模块 | 说明 |
|------|------|
| **streammq-bom** | BOM（Bill of Materials），统一版本管理 |
| **streammq-core** | 核心抽象层，定义消息模型、API、SPI 接口（无 Spring 依赖） |
| **streammq-redisson** | Redisson 适配层，基于 Redis Stream 实现核心能力 |
| **streammq-spring-boot-starter** | Spring Boot 3 自动装配、配置绑定、Actuator 集成 |
| **streammq-tracing-opentelemetry** | OpenTelemetry 链路追踪集成 |
| **streammq-diagnostics** | 消息画像、慢消费、积压、DLQ 诊断 |
| **streammq-kubernetes** | K8s 健康检查、HPA、优雅停机、CRD Operator。**实验性预览**：默认关闭，且当前**不发布**到 Maven Central（`ConfigMapConfigRefresher` 默认实现为 no-op）。待功能完整后再纳入发布 |
| **streammq-spring-cloud-stream-binder** | Spring Cloud Stream Binder 实现（分区生产不支持） |
| **streammq-benchmark** | JMH 基准测试 |
| **streammq-test** | 测试工具包：容器化 Redis（基于 Testcontainers，**需要 Docker daemon**）、Redis 可用性探测（`RedisAvailability`，零外部依赖、随本模块一同发布）、断言工具、Mock 工具。请以 `test` scope 引入。注意 `testcontainers` 与 `com.redis:testcontainers-redis` 为 optional 依赖，需自行显式引入 |
| **streammq-samples** | 示例工程集合，覆盖快速开始、事务、延时、顺序、DLQ、拦截器、诊断、链路追踪 |

---

## 配置参考

### 完整配置示例

```yaml
streammq:
  enabled: true                        # 总开关
  namespace: streammq                  # Redis key 前缀命名空间

  # ── 生产者 ──────────────────────────────────────────────
  producer:
    group: default-producer
    send-message-timeout: 3000         # 同步发送超时（毫秒）
    retry-times: 2                     # 同步发送重试次数（0~MAX_SYNC_RETRY_TIMES）
    compress-threshold: 0              # 0=不压缩，>0 时超过该字节数的消息自动压缩
    serializer: io.github.streammq.adapter.redisson.serializer.FurySerializer  # 序列化器（全限定类名，默认 Fury）
    fury-require-class-registration: false  # Fury 是否强制类注册白名单（仅对 Fury 生效；共享/多租户 Redis 建议 true）
    stream-max-len: 0                  # Stream 最大长度（0=不限制）
    max-message-size: 10485760         # 单条消息最大字节数

  # ── 消费者 ──────────────────────────────────────────────
  consumer:
    batch-size: 32                     # 单次拉取批量大小（1~max-batch-size-limit；注解未声明时生效）
    poll-timeout: 2000ms               # 单次拉取阻塞超时（Duration 格式）
    pull-interval: 0                   # 拉取间隔（毫秒），0=不间隔（注解未声明时生效）
    max-batch-size-limit: 1000         # 拉取批量上界（注解/配置与底层校验均以此为准；不可超过）
    inflight-capacity: 0               # 背压队列容量（0=禁用；>0 时拉取与处理解耦，队列满则拉取阻塞）
    paused-sleep-millis: 100           # 暂停状态下的休眠间隔（毫秒）
    broker-error-backoff-millis: 1000  # Broker 异常退避间隔（毫秒）
    timeout-cancel-grace-millis: 100   # 消费超时取消宽限期（毫秒），缩小与重试副本的重叠窗口
    consume-timeout-millis: 30000      # 全局并发消费超时（毫秒），0=不设超时（注解未声明时生效）
    orderly-consume-timeout-millis: 0  # 全局顺序消费超时（毫秒），0=不启用（注解可 per-consumer 覆盖）
    consume-from-where: CONSUME_FROM_LAST  # 新消费者组起始位点（仅首次建组生效）：CONSUME_FROM_LAST=只消费组创建后消息；CONSUME_FROM_FIRST=重放全量历史

  # ── 重试 ────────────────────────────────────────────────
  retry:
    enabled: true
    policy: io.github.streammq.adapter.redisson.policy.FixedArrayRetryPolicy   # 重试策略（全限定类名）
    max-reconsume-times: 16            # 消费失败最大重试次数（注解未声明时生效；此前该值仅取注解、全局配置失效，已修复）
    scan-interval: 5s                  # 重试 ZSet 扫描间隔
    batch-size: 100                    # 单次扫描批量
    delay-array: ""                    # 自定义重试延时数组（逗号分隔毫秒，如 1000,5000,10000）
    stream-max-len: 0                  # retry Stream 最大长度（0=不限制）
    pel-claim-scan-interval: 5s        # PEL 认领扫描间隔（顺序消费专用）
    pel-claim-min-idle-ms: 60000       # PEL 认领空闲阈值（顺序消费专用，默认 60s；须 >= 35s 否则启动失败）
    failure-requeue-backoff-ms: 5000   # 转移失败后的回写退避间隔（毫秒）

  # ── 延时 ────────────────────────────────────────────────
  delay:
    enabled: true
    scan-interval: 5s                  # 延时 ZSet 扫描间隔
    batch-size: 100                    # 单次扫描批量
    failure-requeue-backoff-ms: 5000   # 转移失败后的回写退避间隔（毫秒）

  # ── 事务 ────────────────────────────────────────────────
  transaction:
    enabled: true
    default-group: streammq-tx        # 默认事务组名
    check-interval: 60s               # 回查间隔
    max-check-times: 15               # 最大回查次数

  # ── 死信队列 ────────────────────────────────────────────
  dlq:
    max-dlq-retry-attempts: 3         # DLQ 消费失败最大重试次数
    dlq-retry-delay-ms: 1000          # DLQ 重试间隔（毫秒）
    retry-max-delay-ms: 300000        # DLQ 重试最大退避（毫秒）
    min-retry-delay-ms: 1000          # DLQ 重试最小退避（毫秒）
    stream-max-len: 0                 # DLQ Stream 最大长度（0=不限制，默认）
    secondary-dlq-enabled: false      # 是否启用二级 DLQ（DLQ 再次失败时）
    secondary-dlq-key-prefix: streammq:dlq2   # 二级 DLQ key 前缀
    alert-threshold: 3                # DLQ 告警阈值
    # failure-strategy: io.github.streammq.adapter.redisson.dlq.LogAndDropDlqFailureStrategy  # DLQ 失败策略（全限定类名）

  # ── 消费者组（心跳与实例存活判定）────────────────────
  group:
    heartbeat-interval-ms: 10000      # 心跳上报间隔（毫秒）
    instance-timeout-ms: 30000        # 实例超时判定（毫秒，须 >= heartbeat-interval-ms）

  # ── 重平衡 ──────────────────────────────────────────────
  rebalance:
    strategy: io.github.streammq.adapter.redisson.rebalance.ConsistentHashRebalanceStrategy  # 策略（全限定类名）
    virtual-nodes: 200                # 虚拟节点数（仅一致性哈希生效）

  # ── 管理端点（Actuator 运维接口）──────────────────────
  admin:
    enabled: true                     # 与 health.enabled 解耦；false 时仅关闭管理 REST 端点
    list-page-size: 20                # 列表默认页大小
    max-pending-query-size: 1000      # pending 单次最大拉取条数
    failure-retry-cooldown-ms: 30000  # 写操作失败重试冷却期（毫秒），0=禁用
    startup-warn: true                # 启动暴露面提醒（-Dstreammq.admin.startup-warn=false 可关）
    trust-forwarded-headers: false    # 是否信任 X-Forwarded-For 用于失败限流来源聚合（安全默认 false）
    trusted-proxies:                  # 可信代理 CIDR（仅 trust-forwarded-headers=true 时生效）
      # - 10.0.0.0/8
      # - 192.168.1.0/24

  # ── 追踪（日志级别输出，默认关闭）─────────────────────
  tracing:
    enabled: false

  # ── 追踪存储与查询（v1.0+，默认关闭）──────────────────
  trace:
    enabled: false
    storage: none                     # REDIS=启用 Redis Stream 存储，其他值禁用
    max-read-count: 1000              # 单次追踪查询最大读取条数

  # ── 健康检查 ────────────────────────────────────────────
  health:
    enabled: true                     # 仅在 Actuator 在 classpath 时生效
```

> **说明**：`Class` 类型的配置项（`producer.serializer`、`retry.policy`、`dlq.failure-strategy`、`rebalance.strategy`）
> 一律填写**全限定类名**。全部键与校验规则以 `StreamMQProperties` 与
> `META-INF/spring-configuration-metadata.json` 为准（IDE 自动补全会给出描述与校验）。

### @StreamMQConsumer 属性速查

| 属性 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `topic` | String | - | 主题（必填） |
| `consumerGroup` | String | - | 消费组（必填） |
| `messageModel` | MessageModel | CONCURRENT | 消费模型：CONCURRENT / ORDERLY |
| `consumeMode` | ConsumeMode | CLUSTERING | 消费模式：CLUSTERING / BROADCASTING |
| `consumeThreadMin` | int | 1 | **并发消费循环数**（仅 CONCURRENT 集群消费生效；每循环独立 XREADGROUP 拉取，共享 consumer name 原子分配互不相交） |
| `consumeThreadMax` | int | 64 | 并发消费循环数上限（夹取上界） |
| `maxReconsumeTimes` | int | -1（=回落全局 `streammq.retry.max-reconsume-times`，默认 16） | 最大重试次数；-1 时取全局配置，>=0 时注解优先（0=消费失败不重试直接进 DLQ） |
| `consumeTimeout` | long | -1（=回落全局 `streammq.consumer.consume-timeout-millis`，默认 30000） | 并发消费超时（毫秒）；-1 取全局，>=0 注解优先（0=不设超时）。超时后按 RECONSUME_LATER 重试（业务层需幂等） |
| `orderlyConsumeTimeout` | long | -1（=回落全局 `streammq.consumer.orderly-consume-timeout-millis`，默认 0=不启用） | 顺序消费超时（毫秒）；-1 取全局，>=0 注解优先；**显式设 0 可单独关闭该消费者的顺序超时保护**（即使全局已开启） |
| `consumeFromWhere` | ConsumeFromWhere | CONSUME_FROM_LAST（=全局默认） | 新消费者组起始位点（仅首次建组生效）。由于枚举注解默认值无法用哨兵表达，<b>仅显式声明 `CONSUME_FROM_FIRST` 视为用户覆盖</b>；未声明或声明 `CONSUME_FROM_LAST` 一律采用全局 `streammq.consumer.consume-from-where`（默认 `CONSUME_FROM_LAST`）。`CONSUME_FROM_LAST`=只消费组创建后消息；`CONSUME_FROM_FIRST`=重放全量历史 |
| `pullBatchSize` | int | -1（=回落全局 `streammq.consumer.batch-size`，默认 32） | 单次拉取批量；-1 取全局，>0 注解优先。最终夹取到 1~max-batch-size-limit |
| `selectorExpression` | String | "*" | Tag/SQL92 过滤表达式 |
| `selectorType` | SelectorType | TAG | 过滤类型：TAG / SQL92 |
| `shardCount` | int | 4 | 顺序消费分片数 |
| `dlqMode` | boolean | false | 是否 DLQ 消费者 |
| `pullInterval` | long | -1（=回落全局 `streammq.consumer.pull-interval`，默认 0=不间隔） | 拉取间隔（毫秒）；-1 取全局，>=0 注解优先 |
| `streamMaxLen` | int | 0 | Stream 最大长度（0=不限制） |
| `retryStreamMaxLen` | int | 0 | 重试 Stream 最大长度 |
| `enableMsgTrace` | boolean | false | 是否启用消息追踪 |
| `serializer` | Class | MessageSerializer.class | 序列化器（默认全局） |
| `messageConverter` | Class | MessageConverter.class | 消息转换器（默认全局） |
| `retryPolicy` | Class | RetryPolicy.class | 重试策略（默认全局） |
| `rebalanceStrategy` | Class | RebalanceStrategy.class | 重平衡策略（默认全局） |
| `consumerFilter` | Class[] | {} | 消费者专属过滤器 |

> 完整配置参考请查看本文件「[配置参考](#配置参考)」章节与 [架构设计文档](docs/historical/02-architecture.md)（V1.0 起草稿，仅供考古）。

---

## SPI 扩展机制

StreamMQ 通过 SPI 提供丰富的扩展点，几乎一切可替换。0.1.1 提供 **16 个可覆盖点**，分两类：

- **12 个面向用户的扩展点**（业务方最常实现/替换）
- **4 个内部装配点**（容器内部组件，技术集成方按需覆盖）

| 类别 | SPI/可覆盖接口 | 作用 | 默认实现 |
|------|------|------|----------|
| 用户扩展 | `MessageSerializer` | 消息序列化/反序列化 | **`FurySerializer`（默认）** / `JacksonJsonSerializer` / `JdkSerializer` / `ProtostuffSerializer` / `ByteArraySerializer` / `StringSerializer` |
| 用户扩展 | `MessageConverter` | 消息体与业务对象转换 | `DefaultMessageConverter` / `CompactMessageConverter` / `PassThroughMessageConverter` |
| 用户扩展 | `ProducerFilter` | 生产者过滤器（过滤链） | `NoopProducerFilter` / `LoggingProducerFilter` |
| 用户扩展 | `ConsumerFilter` | 消费者过滤器（全局+per-consumer） | `TagSelectorFilter` / `SqlSelectorFilter`（共享接口 `ExpressionSelectorFilter`） |
| 用户扩展 | `ProducerInterceptor` | 生产者拦截器（拦截链） | `LoggingProducerInterceptor` |
| 用户扩展 | `ConsumerInterceptor` | 消费者拦截器（拦截链） | `LoggingConsumerInterceptor` |
| 用户扩展 | `RetryPolicy` | 重试策略 | `FixedArrayRetryPolicy` / `FixedIntervalRetryPolicy` / `ExponentialBackoffRetryPolicy` / `DecorrelatedJitterRetryPolicy` / `NoRetryPolicy` |
| 用户扩展 | `RebalanceStrategy` | 消费者重平衡策略 | `AverageRebalanceStrategy` / `ConsistentHashRebalanceStrategy` / `RangeRebalanceStrategy` |
| 用户扩展 | `CompressionCodec` | 消息压缩编解码 | `GzipCompressionCodec` / `Lz4CompressionCodec`（classpath 探测） |
| 用户扩展 | `ManagementAuthenticator` | 管理/诊断接口鉴权 | `AllowAllAuthenticator` / `BasicAuthAuthenticator` / `TokenAuthenticator` / `DenyAllAuthenticator` |
| 用户扩展 | `DlqFailureStrategy` | 死信消费失败策略 | `LogAndDropDlqFailureStrategy` / `LimitedRetryDlqFailureStrategy` / `SecondaryDlqFailureStrategy` |
| 用户扩展 | `ExpressionSelectorFilter` | SQL92/TAG 表达式选择器共享接口（自定义选择器扩展点） | 由 `SqlSelectorFilter` 实现 |
| 内部装配 | `TraceCollector` | 链路追踪上下文采集（默认关闭） | `NoopTraceCollector` / `Slf4jTraceCollector` / `RedisTraceCollector` |
| 内部装配 | `ConsumerFilterResolver` | per-consumer 过滤器解析器 | `ReflectiveConsumerFilterResolver`（默认反射）/ Spring 容器解析 |
| 内部装配 | `OrderlyShardLockManager` | 顺序消费分片分布式锁 | `RedissonOrderlyShardLockManager` |
| 内部装配 | `ConsumerGroupManager` | 消费组实例管理 | `RedissonConsumerGroupManager` |

> 说明：0.1.x 阶段 core 的 SPI 接口仍可能演进（多后端抽象将在 1.0 前定型），自定义 SPI 实现的
> 用户请以 0.2.x 版本为前提评估接口稳定性。

### 自定义 SPI 示例

```java
@Component
public class CustomMessageSerializer implements MessageSerializer {

    @Override
    public byte[] serialize(Object obj) throws SerializationException {
        // 自定义序列化逻辑
        return customSerialize(obj);
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> type) throws SerializationException {
        // 自定义反序列化逻辑
        return customDeserialize(bytes, type);
    }

    @Override
    public String name() {
        return "custom";
    }
}
```

```java
// 在注解中指定使用自定义 SPI
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    serializer = CustomMessageSerializer.class,
    consumerFilter = { CustomFilter.class }
)
```

---

## 可观测性

### Micrometer 指标

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `streammq.send.total` | Counter | 发送总数（tag：`success`） |
| `streammq.send.duration` | Timer | 发送耗时 |
| `streammq.consume.total` | Counter | 消费总数 |
| `streammq.consume.duration` | Timer | 消费耗时 |
| `streammq.retry.total` | Counter | 重试数 |
| `streammq.dlq.total` | Counter | 进入 DLQ 数 |
| `streammq.delay.total` | Counter | 延时投递数 |
| `streammq.transaction.commit.total` | Counter | 事务提交数 |
| `streammq.transaction.rollback.total` | Counter | 事务回滚数 |
| `streammq.transaction.check.total` | Counter | 事务回查数 |

### Actuator 端点

| 端点 | 说明 |
|------|------|
| `/actuator/health` | 健康检查（含 StreamMQ 组件状态） |
| `/actuator/metrics` | Micrometer 指标 |
| `/actuator/prometheus` | Prometheus 格式指标 |

### 管理 REST API

所有操作均注册在 `/actuator/streammq` 之下，按 HTTP 方法 + 路径段分发：

| 端点 | 方法 | 说明 |
|------|------|------|
| `/actuator/streammq` | GET | 总览（状态、消费组、Topic） |
| `/actuator/streammq/groups` | GET | 消费组列表 |
| `/actuator/streammq/topics` | GET | Topic 列表 |
| `/actuator/streammq/pending/{group}/{topic}` | GET | Pending 消息 |
| `/actuator/streammq/dlq/{group}` | GET | DLQ 消息 |
| `/actuator/streammq/dlq/{group}?messageId&targetTopic` | POST | DLQ 重新入队 |
| `/actuator/streammq/dlq/{group}/{messageId}` | DELETE | 删除 DLQ 消息 |
| `/actuator/streammq/stats/{group}/{topic}` | GET | 运行时统计 |
| `/actuator/streammq/ack/{group}/{topic}?messageId` | POST | 手动 ACK |
| `/actuator/streammq/rebalance/{group}` | POST | 触发重平衡 |
| `/actuator/streammq/topics?topic=` | POST | 创建 Topic |
| `/actuator/streammq/topics/{topic}` | DELETE | 删除 Topic |
| `/actuator/streammq/config/{group}` | POST | 更新消费组配置 |

> 所有操作均需通过 `ManagementAuthenticator` 鉴权；默认 `DenyAllAuthenticator` 拒绝所有访问（返回 401），需注册 `AllowAllAuthenticator` / `BasicAuthAuthenticator` / `TokenAuthenticator` Bean 后开放。管理端点可通过 `streammq.admin.enabled=false` 单独关闭。

> **访问前提（重要）：** `/actuator/streammq/**` 是标准 Actuator Web 端点，**受 `management.endpoints.web.exposure.*`
> 治理**——Spring Boot 默认仅暴露 `health` / `info`，因此必须显式放行才能访问：
>
>   ```yaml
>   management:
>     endpoints:
>       web:
>         exposure:
>           include: "health,info,metrics,prometheus,streammq"
>   ```

> **失败限流（内置）：** 管理/诊断端点在鉴权器外层统一包了失败限流（`RateLimitedAuthenticator`）：同一客户端
> 在 60s 窗口内鉴权失败超过 10 次即锁定 5 分钟，成功后复位。即使误用弱凭据，也难以被在线暴力破解。
> 限流对默认的 `DenyAll` 与 `AllowAll` 无副作用。
>
> **来源地址可信模型（安全关键）：** 客户端地址默认**仅取不可伪造的 `remoteAddr`** 聚合——`X-Forwarded-For`
> 完全由客户端可控，未经校验就采用它会让限流被一行请求头绕过。仅当端点部署在**受控反向代理**之后，才应开启
> `streammq.admin.trust-forwarded-headers=true`，并配合 `streammq.admin.trusted-proxies` 声明可信代理
> CIDR（直连对端命中该列表或为回环地址时，才采用 XFF 首值）：

>   ```yaml
>   streammq:
>     admin:
>       trust-forwarded-headers: true     # 默认 false：仅按 remoteAddr 聚合
>       trusted-proxies:                  # 受控代理 CIDR，直连对端需命中
>         - 10.0.0.0/8
>         - 192.168.1.0/24
>   ```
>
> 若代理网络不可信，XFF 仍可被伪造，请保持默认配置（限流按代理 IP 聚合，宁可误伤不可绕过）。

> ⚠️ **暴露面注意事项**：
>
> - `/actuator/streammq/**` 受 Actuator exposure 治理，`/streammq/diagnostics/**`（`streammq-diagnostics` 模块）
>   则是挂载在应用**主端口**的普通 MVC 端点，**不受** `management.endpoints.web.exposure.*` 治理——引入该模块后，
>   即使 Actuator 仅暴露 health，诊断端点仍随主端口可达。两类端点都请通过网络层（安全组/Ingress）限制访问来源，
>   并保持默认 `DenyAllAuthenticator`；
> - 若启用了 JMX 暴露，建议将 StreamMQ 端点从 JMX 排除，避免管理能力被二次暴露：
>
>   ```yaml
>   management:
>     endpoints:
>       jmx:
>         exposure:
>           exclude: "streammq"
>   ```

### 链路追踪

StreamMQ 提供两条互补的追踪路径，按需选择：

| 路径 | 机制 | 适用场景 |
|------|------|----------|
| `TraceCollector` SPI | 生产/消费上下文采集（Redis 存储 / Slf4j 日志 / Noop），支持 traceId 透传 | 轻量审计、消息级流转画像（配合诊断模块拓扑图） |
| `streammq-tracing-opentelemetry` | 标准 OTel `ProducerInterceptor` / `ConsumerInterceptor`，导出标准 Span | 已有 OpenTelemetry 栈（Collector/Jaeger/Tempo）的链路观测 |

两条路径独立生效、互不依赖；同一应用可同时启用（OTel Span 用于分布式追踪，TraceCollector 用于消息画像）。

**三条追踪开关对照表：**

| 开关 | 作用 | 产物 | 典型组合 |
|------|------|------|----------|
| `streammq.tracing.enabled` | TraceCollector SPI 总开关（消息级追踪采集，traceId 透传） | Slf4j 追踪日志 / 自定义 Collector 输出 | 轻量审计；配合诊断模块消息画像 |
| `streammq.trace.enabled`（+ `streammq.trace.storage=redis`） | 消息轨迹的持久化存储与查询 | Redis Stream 存储的轨迹数据（可经管理端点查询） | 需要事后排查消息流转路径时开启 |
| `streammq.tracing.otel.enabled` | OpenTelemetry 集成开关（拦截器注入 Span） | 标准 OTLP Span（Jaeger / Tempo / Collector 可视） | 已有 OTel 栈的分布式链路观测 |

```java
MDC.put("traceId", "t-001");
template.syncSend(message);  // traceId 自动透传到消费者
```

---

## 示例工程

| 示例 | 说明 |
|------|------|
| [streammq-sample-quickstart](streammq-samples/streammq-sample-quickstart) | 快速开始示例 |
| [streammq-sample-transaction](streammq-samples/streammq-sample-transaction) | 事务消息示例 |
| [streammq-sample-delay](streammq-samples/streammq-sample-delay) | 延时消息示例 |
| [streammq-sample-orderly](streammq-samples/streammq-sample-orderly) | 顺序消息示例 |
| [streammq-sample-dlq](streammq-samples/streammq-sample-dlq) | 死信队列示例 |
| [streammq-sample-interceptor](streammq-samples/streammq-sample-interceptor) | 拦截器示例 |
| [streammq-sample-diagnostics](streammq-samples/streammq-sample-diagnostics) | 诊断画像与慢消费示例 |
| [streammq-sample-tracing](streammq-samples/streammq-sample-tracing) | OpenTelemetry 链路追踪示例 |

---

## 文档导航

| 文档 | 说明 |
|------|------|
| [本 README](README.md) | 权威使用手册（功能 / 快速开始 / 配置 / SPI / 运维） |
| Javadoc | 随 Maven Central 发布的构件附带 sources/javadoc jar |
| [CHANGELOG](CHANGELOG.md) | 版本变更记录 |
| [CONTRIBUTING](CONTRIBUTING.md) | 贡献流程与开发规范 |
| [SECURITY](SECURITY.md) | 安全策略与漏洞披露 |

> ⚠️ `docs/historical/` 目录保存 V0.1/V1.0 起草期的设计稿（01-PRD / 02-architecture / 03-functional / 04-detailed），其中的类名、配置键与部分机制描述已随实现演进过时，仅供考古；当前权威参考是本 README 与代码 Javadoc。

---

## 路线图

### V1.0 功能里程碑（0.1.x 已实现）

> **说明**：以下功能已在 0.1.x 版本中实现并可用。项目当前版本为 **0.1.1**（功能预览版），
> API 在 1.0.0 之前仍可能根据社区反馈演进。生产使用前建议在非核心链路灰度验证。

- [x] 注解驱动消费（`@StreamMQConsumer`）
- [x] `StreamMessageTemplate` 编程模型（同步/异步/单向/批量/事务）
- [x] 集群消费 + 广播消费（广播模式支持持久化实例标识）
- [x] 顺序消费（ShardingKey 分片）
- [x] 事务消息（半消息 + 回查）
- [x] 延时消息（18 级 + 任意毫秒）
- [x] 死信队列（含二级 DLQ）
- [x] 消息过滤（Tag + SQL92）
- [x] 消息压缩（GZIP）
- [x] 背压控制（InflightQueue）
- [x] 消费超时自动取消
- [x] Micrometer 指标 + MDC 日志
- [x] 链路追踪（TraceCollector SPI）
- [x] 管理 REST API
- [x] 16 个可覆盖点（10 个用户扩展点 + 6 个内部装配点）
- [x] Spring Boot 3 自动装配 + Actuator 集成
- [x] Spring Cloud Stream Binder（实现 Spring Cloud Stream Binder SPI）
- [x] Kubernetes 集成（实验性预览：CRD 控制器 / HPA / 配置热更新，默认关闭；需显式开启 `streammq.cloud.k8s.enabled=true`）
- [x] 消息画像与拓扑图（可视化消息流转拓扑）
- [x] 分布式追踪增强（OpenTelemetry 集成）

### V2.0（规划中）

- [ ] **多后端抽象层**（BackendProvider SPI，支持 Redis / Kafka / RabbitMQ / Pulsar）
- [ ] **Kafka 后端实现**（基于 Kafka Client 的 BackendProvider）
- [ ] **跨机房复制**（异步复制，RPO ≤ 1s）
- [ ] **Kafka 线网协议兼容**（原生 Kafka Client 零代码接入）

---

## 贡献指南

欢迎参与 StreamMQ 开源建设！请阅读 [贡献指南](CONTRIBUTING.md) 了解详细信息。

### 快速贡献

```bash
# 1. Fork & Clone
git clone https://github.com/<your-username>/streammq.git
cd streammq

# 2. 创建分支
git checkout -b feature/your-feature

# 3. 编写代码 & 测试
mvn clean test

# 4. 提交（遵循 Conventional Commits）
git commit -m "feat: add your feature"

# 5. 发起 PR
```

### 贡献方式

- **Bug 报告**：提交 [Issue](https://github.com/HK-hub/StreamMQ/issues)，描述问题与复现步骤
- **功能请求**：提交 [Issue](https://github.com/HK-hub/StreamMQ/issues)，描述期望功能与使用场景
- **代码贡献**：提交 [Pull Request](https://github.com/HK-hub/StreamMQ/pulls)，关联相关 Issue
- **文档改进**：完善文档、修正错误、补充示例
- **问题解答**：在 [Discussions](https://github.com/HK-hub/StreamMQ/discussions) 中帮助其他用户

---

## 社区

- **GitHub Issues**：[https://github.com/HK-hub/StreamMQ/issues](https://github.com/HK-hub/StreamMQ/issues)
- **GitHub Discussions**：[https://github.com/HK-hub/StreamMQ/discussions](https://github.com/HK-hub/StreamMQ/discussions)
- **Pull Requests**：[https://github.com/HK-hub/StreamMQ/pulls](https://github.com/HK-hub/StreamMQ/pulls)

---

## 适用场景

### 推荐使用

- 已有 Redis 基础设施，希望复用为消息总线
- 中小规模业务（单集群日消息量 < 1 亿）
- 需要事务消息 / 延时消息 / 顺序消息能力但不想引入独立 MQ 集群
- 微服务架构下基于 Spring Boot 3 的轻量级异步通信
- 电商订单状态流转、支付回调、库存扣减、通知推送

### 不建议使用

- 超大规模流式数据处理（单集群日消息量 > 1 亿）—— 建议使用 Kafka
- 对消息吞吐要求极高且可容忍少量丢失 —— 建议使用 Kafka
- 需要复杂路由规则（topic 通配符、多级路由）—— 建议使用 RabbitMQ
- 已有成熟 MQ 集群且无 Redis 资源 —— 直接复用现有 MQ

---

## 技术栈

| 技术 | 版本 | 用途 |
|------|------|------|
| Java | 21+ | 运行时 |
| Spring Boot | 3.3.5 | 框架基础 |
| Redisson | 3.34.1 | Redis 客户端 |
| Jackson | 2.18.1 | JSON 序列化 |
| Fury | 0.9.0 | 高性能序列化（可选） |
| Protostuff | 1.8.0 | Protobuf 序列化（可选） |
| Lombok | - | 代码简化 |
| Micrometer | - | 指标收集 |
| SLF4J | - | 日志门面 |

---

## 安全

StreamMQ 重视您的安全。遵循以下最佳实践以确保安全部署：

### 密钥管理

- **凭据不落日志**：StreamMQ 从不将 Redis 密码等鉴权凭据输出到日志，并建议通过环境变量注入。
- **配置安全存储**：切勿将 Redis 密码硬编码在代码或公开的配置文件中。生产环境建议使用环境变量、配置中心（如 Nacos、Apollo）或密钥管理服务（如 Vault、AWS Secrets Manager）进行管理。
- **最小权限原则**：Redis 实例应使用具有最小必要权限的账号，避免直接复用管理员密码。

### 安全配置

```yaml
redisson:
  singleServerConfig:
    # 启用 TLS/SSL
    address: "rediss://127.0.0.1:6379"
    # 使用认证（推荐通过环境变量注入）
    password: ${REDIS_PASSWORD:}
```

### 反序列化安全

`FurySerializer`（默认序列化器）与 `JdkSerializer` 的安全姿态如下：

- `FurySerializer` 是**默认序列化器**（`streammq.producer.serializer` 默认值）。默认**不强制**类注册（宽松模式，`requireClassRegistration=false`），任意 POJO 开箱即用；但 Redis 中被写入的字节流可被反序列化为 classpath 上的任意类。共享/多租户 Redis 建议通过配置开启类注册白名单：
  ```yaml
  streammq:
    producer:
      fury-require-class-registration: true # 开启类注册白名单
  ```
  白名单模式下需预注册业务消息体类型（构造器注册或启动阶段调用 `register`/`registerAll`）：
  ```java
  FurySerializer<OrderCreated> serializer = new FurySerializer<>(OrderCreated.class);
  // 等价写法：new FurySerializer<>(true).register(OrderCreated.class);
  ```
  纯 Java 直接实例化时：`new FurySerializer()` 为宽松模式，`new FurySerializer(true)` 为强制白名单模式；宽松构造会打印 WARN 提醒，已确认风险的场景可用 `-Dstreammq.security.allowUnrestrictedSerializer=true` 抑制。
- `JdkSerializer` 内置 JEP 290 类名白名单过滤器（目标类型 + JDK 基础类型），反序列化前拦截未知类；
  第三方业务类型通过 `addAllowedClasses(...)` 显式放行。切勿使用 `JdkSerializer.unrestricted()`。

若 Redis 实例可能被不可信方写入（共享实例、多租户场景），请开启 Fury 类注册白名单以收窄反序列化攻击面。完整安全策略见 [SECURITY.md](SECURITY.md)。

### 安全策略

- **版本更新**：关注 [GitHub Security Advisories](https://github.com/HK-hub/StreamMQ/security/advisories) 及时获取安全公告。
- **依赖扫描**：StreamMQ 提供 OWASP Dependency-Check 配置（`mvn verify -Dowasp.skip=false` 触发），CI 中执行依赖漏洞扫描。
- **负责任披露**：如发现安全漏洞，请通过 [GitHub Security](https://github.com/HK-hub/StreamMQ/security) 页面私下报告，我们将在 48 小时内响应。

### 日志脱敏

StreamMQ 从不将 Redis 密码等鉴权凭据输出到日志，并建议通过环境变量注入。

---

## 许可证

本项目基于 [MIT License](LICENSE) 开源。

<div align="center">

---

**StreamMQ** · 让 Redis 成为你的消息总线

如果这个项目对你有帮助，欢迎给一个 ⭐ Star！

[![Star History](https://api.star-history.com/svg?repos=HK-hub/StreamMQ&type=Date)](https://github.com/HK-hub/StreamMQ)

</div>
