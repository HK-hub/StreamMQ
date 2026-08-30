# StreamMQ

### Turn Redis into your message bus

A high-performance message middleware SDK built on **Redis Stream** + **Redisson**, offering a RocketMQ-like programming experience.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.java.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.x-green.svg)](https://spring.io/projects/spring-boot)
[![Redisson](https://img.shields.io/badge/Redisson-3.34.x-red.svg)](https://redisson.org/)
[![Version](https://img.shields.io/badge/version-0.1.1-blue.svg)](https://github.com/HK-hub/StreamMQ)
[![CI](https://github.com/HK-hub/StreamMQ/actions/workflows/ci.yml/badge.svg)](https://github.com/HK-hub/StreamMQ/actions/workflows/ci.yml)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-ff69b4.svg)](https://github.com/HK-hub/StreamMQ/pulls)
[![Stars](https://img.shields.io/github/stars/HK-hub/StreamMQ?style=social)](https://github.com/HK-hub/StreamMQ)

---

> **StreamMQ** is an MIT-licensed open-source message middleware SDK built on **Redis Stream** and **Redisson**. It wraps Redis Stream's native capabilities in a RocketMQ-style, business-developer-friendly API so that — without spinning up a heavy MQ cluster — you get annotation-driven consumption, transactional messages, delayed messages, ordered messages, and other enterprise-grade features.

### Why we require JDK 21

StreamMQ 0.1.1 hard-depends on **JDK 21+** (enforced in `pom.xml` via `maven-enforcer-plugin` and `requireJavaVersion [21,)`). This is intentional:

- **Virtual threads (JEP 444)** are the default execution model for consume loops — `Executors.newVirtualThreadPerTaskExecutor()` is only GA in JDK 21. We refuse to fall back to a platform-thread pool because the consumer thread count would multiply against the Redis connection pool in a 1:N relationship.
- **Pattern matching + Record patterns** simplify core glue code in `ConsumeLoopTask` and `ConsumeAction`.
- We will **not** downgrade to JDK 17 in the 0.2.0 roadmap. If you are currently on JDK 17 LTS, please evaluate whether you can use JDK 21 within this project. Spring Boot 3.3.x supports both 17 and 21, but StreamMQ bets on 21 to avoid writing two thread models for old JDKs.

---

## Table of Contents

- [Why StreamMQ](#why-streammq)
- [Architecture](#architecture)
- [Comparison](#comparison)
- [Benchmarks](#benchmarks)
- [Quick Start](#quick-start)
- [Core Features](#core-features)
- [Broadcast Consumption — Operational Notes](#broadcast-consumption--operational-notes)
- [Troubleshooting: a consumer that never consumes](#troubleshooting-a-consumer-that-never-consumes)
- [Modules](#modules)
- [Configuration Reference](#configuration-reference)
- [SPI Extension Points](#spi-extension-points)
- [Observability](#observability)
- [Sample Projects](#sample-projects)
- [Documentation](#documentation)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [Security](#security)
- [License](#license)

---

## Why StreamMQ

### Zero extra infrastructure

Already running Redis? You already have a message broker. StreamMQ reuses your existing Redis infrastructure — no NameServer, no Broker, no ZooKeeper. **One Redis = one MQ cluster**.

### RocketMQ-like API

Aligned with RocketMQ's `RocketMQTemplate` / `@RocketMQMessageListener` programming model. If you know RocketMQ, you already know StreamMQ.

### Rich advanced features

Transactional messages, 18-level delayed delivery, ordered messages, batch sending, dead-letter queue, message filtering, message compression — all out of the box.

### Deep Spring Boot 3 integration

Auto-configuration, configuration binding, Actuator endpoints, Micrometer metrics — `@EnableStreamMQ` one-liner.

### 16 SPI extension points

Serializers, converters, filters, interceptors, retry policies, rebalance strategies, compression codecs, DLQ failure strategies, management authenticators, trace collectors — almost everything is pluggable.

### Production-ready

- ≥780 unit tests (from `mvn test`)
- ≥80 integration tests (from `mvn verify`, executed when Redis is available; CI uses Docker service to guarantee execution)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        StreamMQ Architecture                           │
├─────────────────────────────────────────────────────────────────────────┤
│   ┌───────────────────────────────────────────────────────────────────┐ │
│   │                    Spring Boot Application                       │ │
│   │  ┌─────────────┐  ┌──────────────┐  ┌──────────────────────────┐ │ │
│   │  │@EnableStreamMQ│ │@StreamMQConsumer│ │  StreamMessageTemplate  │ │ │
│   │  │  (auto-config)│ │(declarative   │ │  (unified send entry)    │ │ │
│   │  └──────┬──────┘  └──────┬───────┘  └───────────┬──────────────┘ │ │
│   └─────────┼─────────────────┼─────────────────────┼────────────────┘ │
│   ┌─────────▼─────────────────▼─────────────────────▼────────────────┐ │
│   │                     streammq-core                                │ │
│   │  Message Builder │ Template │ Consumer │ Producer │ Transaction  │ │
│   │  Filter Chain │ Interceptor Chain │ Retry Policy │ Rebalance     │ │
│   │  Serializer (SPI) │ Converter (SPI) │ Codec (SPI) │ Trace         │ │
│   └───────────────────────────────────────────────────────────────────┘ │
│   ┌─────────▼────────────────────────────────────────────────────────┐ │
│   │                   streammq-redisson                               │ │
│   │  Redisson Producer │ Stream Listener │ Delay Scheduler │ PEL    │ │
│   │  Claimer │ Tx Scanner │ Tx Lock │ Retry Scheduler                  │ │
│   └───────────────────────────────────────────────────────────────────┘ │
│   ┌─────────▼────────────────────────────────────────────────────────┐ │
│   │                       Redis 7.2+                                 │ │
│   │   Stream (msg storage)  ZSet (delay queue)  Hash (tx state)    │ │
│   └──────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Comparison

| Capability | StreamMQ | Redisson RStream | Spring Data Redis Stream | RocketMQ | Kafka |
|---|---|---|---|---|---|
| Underlying storage | Redis Stream | Redis Stream | Redis Stream | NameServer+Broker | Broker+KRaft |
| Deployment complexity | **Low (Redis only)** | Low | Low | High | High |
| Annotation-driven consumer | **Yes** | No | Partial | Yes | No |
| Template API | **Yes** | No | No | Yes | Yes |
| Transactional messages | **Yes** | No | No | Yes | No |
| Delayed messages | **Yes (18 levels + any ms)** | No | No | Yes (18 levels) | No |
| Ordered messages | **Yes** | No | No | Yes | Yes (per partition) |
| Dead-letter queue | **Yes (incl. secondary DLQ)** | No | No | Yes | Yes (spring-kafka DLT) |
| Tag + SQL92 filtering | **Yes** | No | No | Yes | No |
| Message compression | **Yes (GZIP)** | No | No | Yes | Yes |
| Backpressure | **Yes (InflightQueue)** | No | No | Yes | Yes |
| Spring Boot 3 integration | **Deep** | Average | Average | Average | Average |
| SPI extension points | **16** | 0 | 0 | Few | Few |
| Management interface | **REST + Actuator** | None | None | Dashboard | None |
| Tracing | **Yes (TraceCollector SPI + OTel)** | No | No | Yes | No |
| Recommended scale | Medium/small (< 100M/day) | Medium/small | Medium/small | Large | Very large |

---

## Benchmarks — methodology disclosure

> **Important: the numbers below are 0.1.0 pre-release local benchmarks, methodology already corrected**:
> - Serialization benchmarks now use JMH `Blackhole` consumers (prevents JIT dead-code elimination from inflating throughput)
> - Consumer benchmarks rewritten to measure the full end-to-end path: XREADGROUP → deserialize → business callback → XACK, with continuous producers
> - The previous README number "Stream consume ~269,760 ops/s" was removed because it measured an empty XREADGROUP roundtrip — a broken benchmark
> - New baseline numbers will be regenerated by the CI manual benchmark task (`benchmark.yml`)

> We openly acknowledge that before 0.1.0 we published methodology-flawed benchmark numbers. This transparency matters more than "pretending it didn't happen". **Use your own environment's measurements for production capacity planning.**

### Serialization Throughput (ops/s) — 0.1.0 final snapshot

1KB message body, messageCount=1000, with Blackhole consumer.

| Serializer | Serialize (ops/s) | Deserialize (ops/s) | RoundTrip (ops/s) | Single Serialize | Single Deserialize |
|---|---|---|---|---|---|
| **Fury** | **7,749,744** | **4,377,141** | **3,977,079** | **7,879,107** | **4,496,204** |
| Jackson  | 1,055,039 | 1,978,002 | 680,324 | 1,003,220 | 1,943,087 |
| JDK      | 457,713 | 148,372 | 103,880 | 454,467 | 148,306 |

### Send Throughput (ops/s) — 0.1.0 final snapshot

Single instance, 1KB payload. JMH forks=2, warmup=3, iter=5.

| Send Mode | 100B | 1KB | 10KB |
|---|---|---|---|
| **Async batch (batch=100)** | **~11,948** | **~10,062** | **~7,863** |
| Sync batch (batch=100) | ~2,587 | ~2,703 | ~2,344 |
| Sync single | ~2,309 | ~2,188 | ~1,877 |

> **On the blanks:** `consumeThroughput` and `messageCreateAndConsume` are still placeholders.
> We will **not** fill in numbers we have not measured — this README previously quoted
> methodologically broken benchmarks (dead-code elimination, exhausted feed, missing ACK), which
> have been publicly retracted. Consumption throughput is the single most important capacity number
> for an MQ; leaving it blank is better than misleading you.

Run yourself: `mvn -B -Pbenchmark -pl streammq-benchmark exec:exec@benchmark-template exec:exec@benchmark-serialization exec:exec@benchmark-consumer -Dstreammq.benchmark.allowFlush=true`
Or trigger the CI benchmark job defined in
[`.github/workflows/benchmark.yml`](.github/workflows/benchmark.yml); results are published as JMH
artifacts and back-filled into this table.

---

## Quick Start

### Requirements

| Component | Minimum | Recommended |
|---|---|---|
| JDK | 21 | 21+ |
| Maven | 3.9 | 3.9+ |
| Redis | 7.2 | 7.2+ |
| Spring Boot | 3.3 | 3.3.5 |

> ⚠️ `mvn verify` requires a local Redis (`localhost:6379`). Without Redis, IT auto-skips; CI uses Docker service.

### 1. Add dependencies

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

> ⚠️ You must also add `redisson-spring-boot-starter` to provide a `RedissonClient` Bean; missing it will fail at startup with `NoSuchBeanDefinitionException`.

### 2. Configure

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

### 3. Enable (optional)

```java
@SpringBootApplication
@EnableStreamMQ
public class DemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
```

> 💡 `@EnableStreamMQ` is a marker annotation — it does **not** trigger extra configuration. All core Beans are auto-registered via `META-INF/spring/AutoConfiguration.imports` when the starter is on the classpath. You can omit it; adding it just makes the intent explicit.

### 4. Send a message (recommended: `StreamMessageService` facade)

```java
@Component
public class OrderService {

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

> **Power users**: when you need access to interceptors / filters / SPIs, inject `StreamMessageTemplate` directly (see [Advanced Usage](#advanced-usage)).

### 5. Consume a message

```java
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-consumer-group")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {

    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        System.out.println("Received order: " + message.getKeys() + ", content: " + message.getBody());
        return ConsumeAction.SUCCESS;
    }
}
```

That's it! Start the app, send a message, the consumer will pick it up and process it.

---

## Core Features

### Annotation-driven consumption

One-line annotation, declaratively defines the consumer; supports concurrent, ordered, broadcast, and DLQ consumption models.

```java
// Concurrent (default)
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group")

// Ordered
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group",
                  messageModel = MessageModel.ORDERLY, shardCount = 8)

// Broadcast
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group",
                  consumeMode = ConsumeMode.BROADCASTING)

// DLQ
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group", dlqMode = true)
```

> ⚠️ **Broadcast consumption creates a separate Redis consumer group per container instance,
> and the group name changes across restarts.** The total group count is therefore roughly
> "instance count × restart count" within the heartbeat-timeout window, and it keeps consuming
> Redis memory (every group owns its own PEL). Before using broadcast mode in production, read
> [Broadcast Consumption — Operational Notes](#broadcast-consumption--operational-notes).

### StreamMessageService programming model (recommended)

StreamMQ provides two send APIs: `StreamMessageService` (facade) and `StreamMessageTemplate` (full SPI). **Most users should inject `StreamMessageService`** — it has simpler ergonomics for `topic + body + metadata` and delegates to the template underneath.

`StreamMessageService` covers all send patterns with three orthogonal dimensions: send mode × carrier form (full `Message` or `topic+body+MessageMetadataBuilder`) × parameters (`SendOptions` / metadata-inlined timeout/retry).

### Transactional messages

Half message + local transaction + check-back mechanism; final consistency guaranteed.

```java
TransactionCallback<String> callback = (message, ctx) -> {
    try {
        executeLocalTransaction(message.getBody());
        return LocalTransactionState.COMMIT_MESSAGE;
    } catch (Exception e) {
        return LocalTransactionState.ROLLBACK_MESSAGE;
    }
};
SendResult result = template.executeInTransaction(message, callback);
```

### Delayed messages

Built-in 18 fixed delay levels + arbitrary millisecond delays.

```java
Message<String> msg = MessageBuilder.<String>withTopic("delay-topic")
        .body("content")
        .delayLevel(DelayLevel.MINUTE_5)
        .build();
```

### Ordered messages

ShardingKey-based sharded ordered consumption; strict ordering within a shard.

### Batch sending

`BatchMessage` batch delivery, uses Redis Pipeline for throughput.

### Dead-letter queue

Failed messages after retry exhaustion auto-enter DLQ; supports secondary DLQ + custom failure strategies.

### Message filtering

Tag expression filtering and SQL92 expression filtering.

### Message compression

GZIP via `CompressionCodec` SPI; auto-compresses when payload exceeds threshold.

---

## Broadcast Consumption — Operational Notes

**Read this before using `ConsumeMode.BROADCASTING`.**

### Behaviour

Redis consumer groups are inherently "competing consumers within a group". To implement broadcast
(every instance receives every message), StreamMQ gives **each container instance its own Redis
consumer group**, suffixed with a container-level random token. That token is **not stable across
restarts**. Therefore:

- **Every restart creates a new group**; the old one is not removed immediately.
- Stale groups are swept after their heartbeat expires
  (`RedissonStreamListener#sweepStaleBroadcastGroups`).
- Until then, total group count = "instance count × restart count" within the heartbeat window.
- Each group holds its own PEL and **occupies Redis memory**.

### Capacity estimate

```
steady-state groups ≈ instance count
peak groups         ≈ instance count × (max restarts within the heartbeat-timeout window)
```

The heartbeat timeout is controlled by `streammq.group.instance-timeout-ms`.

### Signals to monitor

| Signal | How to read | What an anomaly means |
|---|---|---|
| Broadcast group count | `GET /actuator/streammq` → `broadcastGroups` | Steady growth = crash-looping instances, or heartbeat timeout configured too long |
| Per-sweep removals | log `Swept N stale broadcast group(s): namespace=..., remaining=M` | N stuck at 0 while `remaining` grows = the sweeper is not effective |
| Redis memory | `INFO memory` | Cross-check against the two numbers above |

### Recommendations

1. **Do not use broadcast mode for workloads that restart frequently** (CI environments, Pods that
   repeatedly OOM).
2. Alert on `broadcastGroups`: investigate above "instance count × 3".
3. Broadcast groups **cannot resume a previous consumption offset** — after a restart the new group
   starts from the current point in time, and messages produced during the restart are **not**
   replayed. If you need restart-safe delivery, use clustering consumption
   (`ConsumeMode.CLUSTERING`) or persist offsets yourself.

---

## Troubleshooting: a consumer that never consumes

A consumer that "registers successfully but never consumes" is the symptom most often mistaken for
"messages are being lost". Work through these in order:

1. **Check health**: `GET /actuator/health` → the `streammq` component. If any consume loop failed
   to start, it reports `DOWN` with `listenerContainer.consumeLoopFailures`
   (`loopKey → reason`) in the details.
2. **Check the overview**: `GET /actuator/streammq` → the `status` field reflects the same state.
3. **Check the logs**: the `Failed to create consumer for listener` ERROR line carries topic/group
   and the root cause — most commonly wrong Redis credentials, an illegal consumer group name,
   or a namespace mismatch.
4. **Check container state**: the `containerRunning` field in `/actuator/streammq/groups`.

> A failed consume loop is **not** retried automatically. After fixing the root cause you must
> restart the application (or trigger rebalance via the management endpoint).

---

## Modules

| Module | Description |
|---|---|
| **streammq-bom** | Bill of Materials, unified version management |
| **streammq-core** | Core abstractions, message model, API, SPI interfaces (no Spring dependency) |
| **streammq-redisson** | Redisson adapter, implements core capabilities on Redis Stream |
| **streammq-spring-boot-starter** | Spring Boot 3 auto-configuration, configuration binding, Actuator integration |
| **streammq-tracing-opentelemetry** | OpenTelemetry tracing integration |
| **streammq-diagnostics** | Message profiling, slow-consume, backlog, DLQ diagnostics |
| **streammq-kubernetes** | K8s health checks, HPA, graceful shutdown, CRD operator (experimental, default off) |
| **streammq-spring-cloud-stream-binder** | Spring Cloud Stream Binder implementation |
| **streammq-benchmark** | JMH benchmarks |
| **streammq-test** | Test utilities: containerized Redis (Testcontainers, **requires a Docker daemon**), Redis availability probe, assertions, mocks. Import with `test` scope |
| **streammq-samples** | Sample projects covering all features |

---

## Configuration Reference

```yaml
streammq:
  enabled: true
  namespace: streammq
  producer:
    group: default-producer
    send-message-timeout: 3000
    retry-times: 2
    compress-threshold: 0
  consumer:
    batch-size: 32
    pull-interval: 0
    inflight-capacity: 0
  retry:
    max-reconsume-times: 16
  transaction:
    check-interval: 60s
    max-check-times: 15
  dlq:
    max-dlq-retry-attempts: 3
  admin:
    enabled: true
  tracing:
    enabled: false
```

---

## SPI Extension Points

0.1.1 ships with **16 SPI interfaces**:

| SPI Interface | Purpose | Default Implementation |
|---|---|---|
| `MessageSerializer` | Message serialization | `JacksonJsonSerializer` / `JdkSerializer` / `FurySerializer` / `ProtostuffSerializer` |
| `MessageConverter` | Message-body ↔ business object | `DefaultMessageConverter` / `CompactMessageConverter` / `PassThroughMessageConverter` |
| `ProducerFilter` | Producer filter chain | `NoopProducerFilter` / `LoggingProducerFilter` |
| `ConsumerFilter` | Consumer filter chain | `TagSelectorFilter` / `SqlSelectorFilter` |
| `ProducerInterceptor` | Producer interceptor chain | `LoggingProducerInterceptor` |
| `ConsumerInterceptor` | Consumer interceptor chain | `LoggingConsumerInterceptor` |
| `RetryPolicy` | Retry strategy | `FixedArrayRetryPolicy` / `FixedIntervalRetryPolicy` / `ExponentialBackoffRetryPolicy` / `DecorrelatedJitterRetryPolicy` |
| `RebalanceStrategy` | Consumer rebalance strategy | `AverageRebalanceStrategy` / `ConsistentHashRebalanceStrategy` / `RangeRebalanceStrategy` |
| `CompressionCodec` | Message compression | `GzipCompressionCodec` / `Lz4CompressionCodec` (classpath detection) |
| `TraceCollector` | Trace context collection | `NoopTraceCollector` / `Slf4jTraceCollector` / `RedisTraceCollector` |
| `ManagementAuthenticator` | Admin endpoint authentication | `AllowAllAuthenticator` / `BasicAuthAuthenticator` / `TokenAuthenticator` / `DenyAllAuthenticator` |
| `DlqFailureStrategy` | DLQ failure handling | `LogAndDropDlqFailureStrategy` / `LimitedRetryDlqFailureStrategy` / `SecondaryDlqFailureStrategy` |
| `ExpressionSelectorFilter` | Message filtering | `TagSelectorFilter` / `SqlSelectorFilter` |
| `ConsumerFilterResolver` | Per-consumer filter resolver | `ReflectiveConsumerFilterResolver` (default) / Spring container resolver |
| `OrderlyShardLockManager` | Ordered-consume shard distributed lock | `RedissonOrderlyShardLockManager` |
| `ConsumerGroupManager` | Consumer group instance management | `RedissonConsumerGroupManager` |

---

## Observability

### Micrometer Metrics

| Metric | Type | Description |
|---|---|---|
| `streammq.send.total` | Counter | Send total (tag: `success`) |
| `streammq.send.duration` | Timer | Send latency |
| `streammq.consume.total` | Counter | Consume total |
| `streammq.consume.duration` | Timer | Consume latency |
| `streammq.retry.total` | Counter | Retry count |
| `streammq.dlq.total` | Counter | DLQ count |
| `streammq.delay.total` | Counter | Delayed delivery count |
| `streammq.transaction.commit.total` | Counter | Transaction commit count |
| `streammq.transaction.rollback.total` | Counter | Transaction rollback count |
| `streammq.transaction.check.total` | Counter | Transaction check count |

### Actuator Endpoints

| Endpoint | Description |
|---|---|
| `/actuator/health` | Health check (incl. StreamMQ component status) |
| `/actuator/metrics` | Micrometer metrics |
| `/actuator/prometheus` | Prometheus format |

### Management REST API

All under `/actuator/streammq`, dispatched by HTTP method + path segment:

| Endpoint | Method | Description |
|---|---|---|
| `/actuator/streammq` | GET | Overview (status, consumer groups, topics) |
| `/actuator/streammq/groups` | GET | List consumer groups |
| `/actuator/streammq/topics` | GET | List topics |
| `/actuator/streammq/pending/{group}/{topic}` | GET | Pending messages |
| `/actuator/streammq/dlq/{group}` | GET | DLQ messages |
| `/actuator/streammq/stats/{group}/{topic}` | GET | Runtime stats |
| `/actuator/streammq/rebalance/{group}` | POST | Trigger rebalance |

All require `ManagementAuthenticator`. Default is `DenyAllAuthenticator` (rejects everything, returns 401). Register `AllowAllAuthenticator` / `BasicAuthAuthenticator` / `TokenAuthenticator` Bean to open access.

---

## Sample Projects

| Sample | Description |
|---|---|
| [streammq-sample-quickstart](streammq-samples/streammq-sample-quickstart) | Quick start |
| [streammq-sample-transaction](streammq-samples/streammq-sample-transaction) | Transactional messages |
| [streammq-sample-delay](streammq-samples/streammq-sample-delay) | Delayed messages |
| [streammq-sample-orderly](streammq-samples/streammq-sample-orderly) | Ordered messages |
| [streammq-sample-dlq](streammq-samples/streammq-sample-dlq) | Dead-letter queue |
| [streammq-sample-interceptor](streammq-samples/streammq-sample-interceptor) | Interceptors |
| [streammq-sample-diagnostics](streammq-samples/streammq-sample-diagnostics) | Diagnostics & slow-consume |
| [streammq-sample-tracing](streammq-samples/streammq-sample-tracing) | OpenTelemetry tracing |

---

## Documentation

| Document | Description |
|---|---|
| [This README](README.md) | Authoritative user manual |
| Javadoc | Bundled with Maven Central artifacts (sources/javadoc jars) |
| [CHANGELOG](CHANGELOG.md) | Version change log |
| [CONTRIBUTING](CONTRIBUTING.md) | Contribution process & dev conventions |
| [SECURITY](SECURITY.md) | Security policy & vulnerability disclosure |

> ⚠️ `docs/historical/` contains V0.1/V1.0 draft design documents; their class names, configuration keys, and some mechanism descriptions have become outdated. Treat as archaeology only.

---

## Roadmap

### V1.0 (completed)

- [x] Annotation-driven consumption (`@StreamMQConsumer`)
- [x] `StreamMessageTemplate` programming model (sync/async/oneway/batch/tx)
- [x] Cluster + broadcast consumption
- [x] Ordered messages (ShardingKey)
- [x] Transactional messages (half + check)
- [x] Delayed messages (18 levels + any ms)
- [x] Dead-letter queue (incl. secondary DLQ)
- [x] Tag + SQL92 message filtering
- [x] GZIP message compression
- [x] Backpressure control (InflightQueue)
- [x] Consume-timeout auto-cancel
- [x] Micrometer metrics + MDC logging
- [x] TraceCollector SPI
- [x] Management REST API
- [x] 16 SPI extension points
- [x] Spring Boot 3 auto-config + Actuator
- [x] Spring Cloud Stream Binder
- [x] Kubernetes integration (experimental)
- [x] Message profiling & topology visualization
- [x] OpenTelemetry distributed tracing

### V2.0 (planned)

- [ ] **Multi-backend abstraction layer** (BackendProvider SPI for Redis / Kafka / RabbitMQ / Pulsar)
- [ ] **Kafka backend implementation** (BackendProvider on Kafka Client)
- [ ] **Cross-datacenter replication** (async, RPO ≤ 1s)
- [ ] **Kafka wire-protocol compatibility** (zero-code migration for native Kafka Client)

---

## Contributing

Welcome to StreamMQ! Please read [CONTRIBUTING](CONTRIBUTING.md) for details.

```bash
# 1. Fork & clone
git clone https://github.com/<your-username>/streammq.git
cd streammq

# 2. Create branch
git checkout -b feature/your-feature

# 3. Code & test
mvn clean test

# 4. Commit (Conventional Commits)
git commit -m "feat: add your feature"

# 5. Open PR
```

---

## Recommended Use Cases

- Already have Redis, want to reuse it as a message bus
- Medium/small business (< 100M msgs/day per cluster)
- Need transactional/delayed/ordered messages but don't want a separate MQ cluster
- Lightweight async communication in Spring Boot 3 microservices
- E-commerce order state, payment callbacks, inventory deduction, notification push

### Not Recommended

- Very large streaming pipelines (> 1M TPS sustained) — use Kafka
- Financial-grade strict ACID transactions — use RocketMQ transactional messages
- 100M+ message accumulation — use Kafka (disk storage)
- Multi-datacenter deployment — use RocketMQ Cluster / Pulsar Geo-Replication
- Embedded IoT devices — use MQTT broker

---

## Tech Stack

| Tech | Version | Purpose |
|---|---|---|
| Java | 21+ | Runtime |
| Spring Boot | 3.3.5 | Framework |
| Redisson | 3.34.1 | Redis client |
| Jackson | 2.18.1 | JSON serialization |
| Fury | 0.9.0 | High-perf serialization (optional) |
| Protostuff | 1.8.0 | Protobuf serialization (optional) |
| Lombok | - | Code simplification |
| Micrometer | - | Metrics |
| SLF4J | - | Logging facade |

---

## Security

StreamMQ takes your security seriously. Best practices:

### Secret management

- **No credentials in logs**: StreamMQ never outputs Redis passwords to logs; recommend injecting via environment variables.
- **Configuration security**: Never hardcode Redis passwords. Use environment variables, config center (Nacos/Apollo), or secret management (Vault/AWS Secrets Manager).
- **Least privilege**: Redis accounts should have minimum necessary permissions.

### Deserialization safety

- `FurySerializer` and `JdkSerializer` are **secure-by-default**:
  - `FurySerializer` enforces class registration whitelist by default (`requireClassRegistration=true`). Register application payloads up front with `new FurySerializer<>(OrderCreated.class)` or `new FurySerializer<>().register(OrderCreated.class)`. To disable for fully-trusted Redis: `new FurySerializer(false)` — **gated by `-Dstreammq.security.allowUnrestrictedSerializer=true`**.
  - `JdkSerializer` has JEP 290 class name whitelist filter (target type + JDK basics); use `JdkSerializer.unrestricted()` only as a last resort — also gated by the same system property.

For shared/multi-tenant Redis, keep default whitelist mode. See [SECURITY.md](SECURITY.md) for the full security policy.

---

## License

This project is licensed under the [MIT License](LICENSE).

---

**StreamMQ** · Turn Redis into your message bus

If this project helps you, please give it a ⭐ Star!
