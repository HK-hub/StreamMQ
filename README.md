# StreamMQ

### Turn Redis into your message bus

A high-performance message middleware SDK built on **Redis Stream** + **Redisson**, offering a RocketMQ-like programming experience.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.java.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.x-green.svg)](https://spring.io/projects/spring-boot)
[![Redisson](https://img.shields.io/badge/Redisson-3.34.x-red.svg)](https://redisson.org/)
[![Version](https://img.shields.io/badge/version-0.1.2-blue.svg)](https://github.com/HK-hub/StreamMQ)
[![CI](https://github.com/HK-hub/StreamMQ/actions/workflows/ci.yml/badge.svg)](https://github.com/HK-hub/StreamMQ/actions/workflows/ci.yml)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-ff69b4.svg)](https://github.com/HK-hub/StreamMQ/pulls)
[![Stars](https://img.shields.io/github/stars/HK-hub/StreamMQ?style=social)](https://github.com/HK-hub/StreamMQ)

---

> **StreamMQ** is an MIT-licensed open-source message middleware SDK built on **Redis Stream** and **Redisson**. It wraps Redis Stream's native capabilities in a RocketMQ-style, business-developer-friendly API so that — without spinning up a heavy MQ cluster — you get annotation-driven consumption, transactional messages, delayed messages, ordered messages, and other enterprise-grade features.

### Why we require JDK 21

StreamMQ 0.1.2 hard-depends on **JDK 21+** (enforced in `pom.xml` via `maven-enforcer-plugin` and `requireJavaVersion [21,)`). This is intentional:

- **Virtual threads (JEP 444)** are the default execution model for consume loops — `Executors.newVirtualThreadPerTaskExecutor()` is only GA in JDK 21. We refuse to fall back to a platform-thread pool because the consumer thread count would multiply against the Redis connection pool in a 1:N relationship.
- **Pattern matching + Record patterns** simplify core glue code in `ConsumeLoopTask` and `ConsumeAction`.
- We will **not** downgrade to JDK 17 in the 0.2.0 roadmap. If you are currently on JDK 17 LTS, please evaluate whether you can use JDK 21 within this project. Spring Boot 3.3–3.5 support both 17 and 21, but StreamMQ bets on 21 to avoid writing two thread models for old JDKs.

---

## Table of Contents

- [Why StreamMQ](#why-streammq)
- [Architecture](#architecture)
- [Comparison](#comparison)
- [Benchmarks](#benchmarks--methodology-disclosure)
- [Quick Start](#quick-start)
- [Core Features](#core-features)
- [Broadcast Consumption — Operational Notes](#broadcast-consumption--operational-notes)
- [Troubleshooting: a consumer that never consumes](#troubleshooting-a-consumer-that-never-consumes)
- [Modules](#modules)
- [Configuration Reference](#configuration-reference)
- [Minimum dependency matrix](#minimum-dependency-matrix)
- [Extension Points](#extension-points)
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

Auto-configuration, configuration binding, Actuator endpoints, Micrometer metrics — just add the starter.

### 18 Extension Points (user-facing + internal assembly)

Serializers, converters, filters, interceptors, retry policies, rebalance strategies, compression codecs, DLQ failure strategies, management authenticators, trace collectors — almost everything is pluggable. They are resolved via the annotation's `Class` attribute or a Spring Bean override — **not** Java `ServiceLoader`.

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
│   │  │ auto-config │ │@StreamMQConsumer│ │  StreamMessageTemplate  │ │ │
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
| Extension points | **18** (user-facing + internal assembly) | 0 | 0 | Few | Few |
| Management interface | **REST + Actuator** | None | None | Dashboard | None |
| Tracing | **Yes (TraceCollector SPI + OTel)** | No | No | Yes | No |
| Recommended scale | Medium/small (< 100M/day) | Medium/small | Medium/small | Large | Very large |

> **Backpressure is OFF by default**: `streammq.consumer.inflight-capacity` defaults to `0` (disabled). Set a positive value to enable throttling (the in-flight queue decouples fetch from processing and blocks fetches when full).

> **Why not Redisson `RTopic` / `RReliableTopic`?** Topics are pub/sub: no consumer group, no per-group
> offset, no PEL/recovery, no ordered or delayed delivery, and a slow subscriber either drops messages or grows an
> unbounded listener queue. StreamMQ is built on Redis **Streams** precisely to get at-least-once delivery with
> replay, DLQ, retry, ordering and transactions.
>
> **Why not plain `XADD` + `XREADGROUP`?** That is exactly what StreamMQ does underneath; the SDK adds what the raw
> commands leave to you — consumer-group lifecycle & rebalance, ACK/PEL orphan recovery, retry with backoff, DLQ
> (including a secondary DLQ), delayed messages, transaction half-messages with check-back, tag/SQL92 filtering,
> backpressure, metrics, tracing and a management API. If you need none of those and want zero dependencies, raw
> `XADD`/`XREADGROUP` is a perfectly reasonable choice.


---

## Benchmarks — methodology disclosure

> **Important: the numbers below are 0.1.2 locally measured benchmarks** (2026-09-02, localhost Redis, JDK 21, laptop-grade hardware):
> - Serialization benchmarks now use JMH `Blackhole` consumers (prevents JIT dead-code elimination from inflating throughput)
> - Consumer benchmark drives the **raw Redisson read path** (XREADGROUP → field decode → callback → **batched** XACK, 1 ACK per 100 messages) with continuous producers. It deliberately **bypasses the listener container**, so the filter/interceptor chains, metrics, retry/DLQ handling and per-message ACK of the production path are **not** included — expect real container throughput to be lower. A container-driven benchmark is tracked as follow-up work.
> - The previous README number "Stream consume ~269,760 ops/s" was removed because it measured an empty XREADGROUP roundtrip — a broken benchmark
> - Error bars are 99.9% CI; laptop-grade results are for reference only — measure on your own production hardware

> We openly acknowledge that before 0.1.0 we published methodology-flawed benchmark numbers. This transparency matters more than "pretending it didn't happen". **Use your own environment's measurements for production capacity planning.**
>
> **Note (0.1.2 defaults changed):** the numbers below were measured under the 0.1.1-era defaults (Fury as default serializer, concurrent consume-timeout = 30s). 0.1.2 flips the **default serializer to `JacksonJsonSerializer`** and **disables the per-message consume-timeout by default** (PEL-reclaim fallback keeps at-least-once). Both change the absolute throughput figures — re-run `mvn -Pbenchmark` in your environment for current numbers.

### Serialization Throughput (ops/s) — 2026-09-17 measured

1KB message body, `messageCount=1000`, with Blackhole consumer. JMH profile **declared in `SerializationBenchmark`**: `@Fork(3, warmups = 2)`, `@Warmup(3×2s)`, `@Measurement(5×2s)`, `@BenchmarkMode(Throughput, SampleTime)`; the table below was collected with the CLI overrides `-f 1 -wi 4 -i 5 -w 2s -r 2s -bm thrpt` (single-fork run — see §2 of the [full report](docs/benchmarks/serialization-2026-09-17.md) for the exact command). All six built-in serializers, **including the new `FlatBuffersSerializer` (FlexBuffers) and `SbeSerializer` (SBE envelope)**. Full report: [`docs/benchmarks/serialization-2026-09-17.md`](docs/benchmarks/serialization-2026-09-17.md).

| Serializer | Serialize (ops/s) | Deserialize (ops/s) | RoundTrip (ops/s) | Single Serialize | Single Deserialize | Size (bytes) |
|---|---|---|---|---|---|---|
| **Fury** | **~3,483,891** | **~3,800,231** | **~1,880,645** | **~4,036,015** | **~3,995,683** | 1,094 |
| Protostuff | ~351,231 | ~3,945,355 | ~320,724 | ~342,823 | ~3,717,589 | 1,050 |
| FlatBuffers (FlexBuffers) | ~657,099 | ~763,086 | ~347,640 | ~645,424 | ~785,224 | 1,150 |
| SBE (envelope) | ~358,310 | ~765,708 | ~246,984 | ~354,743 | ~817,930 | 1,104 |
| Jackson (default) | ~416,872 | ~893,418 | ~266,364 | ~410,731 | ~875,660 | 1,092 |
| JDK | ~442,764 | — | — | ~459,824 | — | — |

> **Reading the numbers.**
> - **Fury** is fastest overall (~8× Jackson on serialize, ~4.3× on deserialize) but requires the class-registration whitelist (safety) and pulls Guava.
> - **Protostuff** deserializes very fast (~4.4× Jackson) yet serializes slowly (~0.85× Jackson) — good for read-heavy paths.
> - **FlatBuffers** (FlexBuffers, schema-less, safe — pure data, no gadget RCE surface) is balanced. Its zero-copy read applies *per field*; because `FlatBuffersSerializer` still materializes a POJO via reflection, end-to-end deserialize (~763K) is comparable to Jackson, not dramatically faster.
> - **SBE** (envelope mode) is bounded by its inner Jackson codec (~0.86× Jackson both ways) plus the fixed 8-byte header + `varData` framing (~10–16 B). Its value is the framed, schema-versioned container; true low-latency gains need schema-first bodies (codegen readers), not the envelope.
> - **JDK** deserialize is not measured: `JdkSerializer` enforces a deserialization filter (security) that rejects this payload.
> - `Size` is dominated by the 1KB string; differences are within ±10%. Laptop-grade hardware with an IDE running → reference only.

### Send Throughput (ops/s) — 0.1.2 measured

Single instance, localhost Redis. JMH profile declared in `StreamMessageTemplateBenchmark`: `@Fork(3, warmups = 2)`, `@Warmup(3×2s)`, `@Measurement(5×2s)`, `@BenchmarkMode(Throughput, SampleTime)`.
**Disclosure:** the 0.1.2 numbers below were collected with an earlier, lighter profile (fork=1, warmup=1×2s, measurement=2×3s — as recorded in `streammq-benchmark/BENCHMARK_REPORT.md` §4) and were not re-measured after the profile was tightened; treat them as indicative and re-run the reproducible command at the end of this section for full-profile numbers.

| Send Mode | 100B | 1KB | 10KB |
|---|---|---|---|
| **Async batch (batch=100)** | **~12,513** | **~11,780** | **~8,326** |
| Sync batch (batch=10) | ~3,640 | ~3,765 | ~2,863 |
| Sync single | ~3,741 | ~3,610 | ~2,600 |

### Consume Throughput (ops/s) — 0.1.2 measured

Raw read path: XREADGROUP + field decode + callback + batched XACK (1 ACK per 100 messages, with continuous feed). JMH fork=1, warmup=1×2s, measurement=3×3s. **Not** the container path — see the note above.

| Benchmark | Description | 1KB | 10KB |
|---|---|---|---|
| `consumeThroughput` | Full path (network RTT, deserialization, ACK) | **~2,383** | **~2,018** |
| `serializationRoundTrip` | Jackson round-trip (with network) | ~270,705 | ~19,249 |

> `consumeThroughput` measures the consume path (Redis network round-trip, deserialization, business callback, batched XACK), not an empty read roundtrip — but it is a **lower bound of the SDK container path**, since it omits the container's per-message ACK and its filter/interceptor/metrics chains. Numbers vary significantly across hardware, Redis instances, and network latency.

Run yourself (same reproducible command as the benchmark report — run the JMH CLI directly so `test-compile` is not skipped and stale bytecode cannot be measured):

```bash
mvn -q -pl streammq-benchmark test-compile dependency:build-classpath \
    -Dmdep.includeScope=test -Dmdep.outputFile=target/cp.txt
cd streammq-benchmark
java -Djmh.ignoreLock=true \
     -cp "target/test-classes:target/classes:$(cat target/cp.txt)" \
     org.openjdk.jmh.Main "SerializationBenchmark" \
     -f 1 -wi 4 -i 5 -w 2s -r 2s -bm thrpt \
     -rf json -rff target/jmh-serialization.json
```

Send/consume benchmarks run the same way with `org.openjdk.jmh.Main "StreamMessageTemplateBenchmark"` / `"StreamConsumerBenchmark"`
(the classpath separator is `:` on Linux/macOS and `;` on Windows).
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
| Spring Boot | 3.3 – 3.5 | 3.5.16 |

> ⚠️ `mvn verify` requires a local Redis (`localhost:6379`). Without Redis, IT auto-skips; CI uses Docker service.
> ⚠️ **Build prerequisites:** JDK **21+** is required (`requireJavaVersion [21,)`) and Maven **3.9+**. The build runs `spotless:check` at the `verify` phase — run `mvn spotless:apply` first, or skip with `-Dspotless.check.skip=true`.
> ⚠️ **Default serializer is `JacksonJsonSerializer` (strict types, no gadget RCE surface).** As of 0.1.2 the default flipped from Fury (unrestricted mode) to Jackson for **safe-by-default** publishing: a library's default deserializer must not expose an RCE surface to every downstream app. **For higher throughput**, opt in to `FurySerializer` (`streammq.producer.serializer=io.github.streammq.adapter.redisson.serializer.FurySerializer`, then pre-register your payload types (`FurySerializer` enforces the class-registration whitelist by default)), or `ProtostuffSerializer`. Fory (`org.apache.fory:fory-core`, >= 1.1.0 — the underlying library is Apache Fory, formerly Apache Fury) and Protostuff are `optional` dependencies of `streammq-redisson` — add them to your classpath only when you use them, so Guava/Protostuff are not force-pulled into every app. See [SECURITY.md](SECURITY.md).

### 1. Add dependencies

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.github.streammq</groupId>
            <artifactId>streammq-bom</artifactId>
            <version>0.1.2</version>
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

> ⚠️ You must also add `redisson-spring-boot-starter` to provide a `RedissonClient` Bean; missing it will fail at startup with `NoSuchBeanDefinitionException`. See [Minimum dependency matrix](#minimum-dependency-matrix) for which coordinates each usage path must supply itself.

### 2. Configure

```yaml
spring:
  application:
    name: streammq-demo
  # Redis connection: the Redisson Spring Boot Starter builds the client from spring.data.redis.*
  data:
    redis:
      host: 127.0.0.1
      port: 6379
      database: 0
      # password: ${REDIS_PASSWORD:}   # uncomment when authentication is required

streammq:
  enabled: true
  namespace: streammq
```

> **Note:** `redisson.singleServerConfig.*` has **no property binding** in this starter (it only binds
> `spring.redis.redisson.config` / `spring.redis.redisson.file`), so writing it has no effect — do not use it.
> For cluster / sentinel and other advanced topologies, supply Redisson's native configuration through
> `spring.redis.redisson.config`.

### 3. Enable (automatic)

Just add the starter dependency — StreamMQ auto-configures all core Beans via `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` when the starter is on the classpath (and `streammq.enabled=true`, the default). No `@Enable*` annotation is required.

```java
@SpringBootApplication
public class DemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
```

> 💡 The historical `@EnableStreamMQ` marker annotation (empty, no `@Import`) did not trigger extra configuration and was removed in 0.1.2. Auto-configuration is standalone.

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
                        .userProperty("traceId", "t-001"));
    }
}
```

> **Power users**: when you need access to interceptors / filters / SPIs, inject `StreamMessageTemplate` directly (see [Extension Points](#extension-points)).

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
> and the group name is derived from a persistent instance identity (0.1.2+).** With a persistent
> identity the group name is stable across typical restarts and the PEL is preserved; only the
> random-UUID fallback (Redis and the local identity file both unavailable) produces a new group
> per restart. Before using broadcast mode in production, read
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

> ⚠️ **Delay ceiling: 7 days.** `delayTimeMillis` must not exceed
> `StreamMQConstants.MAX_DELAY_TIME_MILLIS` (7 days); a larger value **fails fast at send time**
> (`StreamMQException`), it is not silently truncated. The 18 fixed levels are far below this ceiling
> (max `HOUR_2` = 2h); **arbitrary millisecond delays are bounded by the same 7-day ceiling**.

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
consumer group** identified by a persistent instance token (`BroadcastInstanceIdResolver`,
introduced in 0.1.2). The token is resolved via a five-level fallback chain:

1. Explicit configuration: `streammq.consumer.broadcast-instance-id` (or system property
   `streammq.instance.id` / environment variable `STREAMMQ_INSTANCE_ID`)
2. Local persistent file: `${user.home}/.streammq/instance-id-<namespace>_<group>` — **scoped per
   application/consumer group** (multiple StreamMQ apps under the same OS user never share an
   identity file). The file stores one record per process (`id pid timestamp`), so a restart reuses
   the identity of an **exited** process and never overwrites the identity of a still-running one.
   Path overridable via `streammq.consumer.broadcast-instance-id-file` (a single file for all
   consumers; `none`/`false` disables the local file entirely)
3. Redis broadcast registry **reclaiming this host's previous slot** (persists across restarts, keeps the PEL)
4. Redis broadcast registry **allocating a fresh slot** for a brand-new instance
5. Random UUID (last resort, non-persistent — the pre-0.1.2 behaviour)

This means **the broadcast group name is stable across typical restarts** when any of the
persistent identity sources (1–4) are configured or available. However, **if only the
random fallback applies**, the old behaviour applies:

- **With persistent identity**: restarted instances resume the same consumer group → PEL is
  preserved, messages produced during downtime are delivered on reconnect, **no new group created**.
  Stale groups still exist only when instances are decommissioned permanently (slot reclaimed and
  eventually swept after the reclaim grace period, default `streammq.consumer.broadcast-reclaim-grace` = 7d).
- **Without persistent identity (random UUID)**: every restart creates a new group; the abandoned
  groups (a group is "stale" once its heartbeat stops) are destroyed by
  `RedissonBroadcastGroupRegistry` after a **fixed 10-minute TTL**
  (`BROADCAST_GROUP_STALE_TTL_MS`, not configurable) — identity slots, which carry the PEL, are
  reclaimed only after the 7-day reclaim grace.
  Total group count ≈ instance-count × restarts within that 10-minute window.
- Each group holds its own PEL and **occupies Redis memory**.

### Recommendations

1. **Configure a persistent identity source in production** (explicit `broadcast-instance-id`, a writable
   local file, or a reachable Redis at startup) — this ensures restart-safe offset persistence; only a
   random UUID fallback produces a new group per restart.
2. **Do not use broadcast mode for workloads that restart frequently** (CI environments, Pods that
   repeatedly OOM) when relying on the UUID fallback.
3. Alert on `broadcastGroups`: investigate above "instance count × 3".
4. Broadcast groups **without persistent identity cannot resume a previous consumption offset** —
   after a UUID-fallback restart the new group starts from the current point in time.

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

> A failed consume loop is **not** retried automatically, and the rebalance endpoint **cannot** recover it:
> `POST /actuator/streammq/rebalance/{group}` only re-assigns shards for **already-running ORDERLY containers**
> (`StreamMQAdminEndpoint#triggerRebalance` → `container.rebalanceGroup(group)`), so it does nothing when the
> consume loop never started. After fixing the root cause you **must restart the application**.

---

## Modules

> ⚠️ In 0.1.x only **`streammq-bom`, `streammq-core`, `streammq-redisson`, `streammq-spring-boot-starter`** (plus
> `streammq-test`) are published to Maven Central. Rows marked *source-only* are compiled and tested in this
> repository but **cannot be resolved from Central** — build them from source (`mvn install`) if you need them.

| Module | Description |
|---|---|
| **streammq-bom** | Bill of Materials, unified version management |
| **streammq-core** | Core abstractions, message model, API, SPI interfaces (no Spring dependency) |
| **streammq-redisson** | Redisson adapter, implements core capabilities on Redis Stream |
| **streammq-spring-boot-starter** | Spring Boot 3 auto-configuration, configuration binding, Actuator integration |
| **streammq-tracing-opentelemetry** | OpenTelemetry tracing integration — *source-only in 0.1.x* |
| **streammq-diagnostics** | Message profiling, slow-consume, backlog, DLQ diagnostics — *source-only in 0.1.x* |
| **streammq-kubernetes** | K8s health checks, HPA, graceful shutdown, CRD operator (experimental, default off) — *source-only in 0.1.x* |
| **streammq-spring-cloud-stream-binder** | Spring Cloud Stream Binder implementation — *source-only in 0.1.x* |
| **streammq-benchmark** | JMH benchmarks — *source-only in 0.1.x* |
| **streammq-test** | Test utilities: containerized Redis (Testcontainers, **requires a Docker daemon**), Redis availability probe, assertions, mocks. Import with `test` scope |
| **streammq-samples** | Sample projects covering all features — *source-only in 0.1.x* |

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

### Two keys that are easy to miss

- **`streammq.diagnostics.enabled`** (default `false`) — turns on the `streammq-diagnostics` module
  (message profiling, slow-consume, backlog and DLQ diagnostics). It lives in a **separate module**:
  you must add `streammq-diagnostics` to the classpath *and* set the key to `true`; a
  `StreamMQTraceService` bean is also required for the analyzers to bind (enable it via
  `streammq.trace.enabled=true` + `streammq.trace.storage=redis`; without a Redisson client the
  backlog probe silently falls back to trace-window estimation).
  **If the module is not on the classpath, Spring silently ignores `streammq.diagnostics.*`** — no
  warning, no failure: nothing binds the key (an unknown property is ignored by default). The same
  applies to `streammq.tracing.otel.*` (needs `streammq-tracing-opentelemetry`) and to any other
  module-scoped key. **Troubleshooting when the key seems to have no effect:** confirm the coordinate is
  really on the classpath (`mvn dependency:tree | grep streammq-diagnostics`) and that `enabled=true` took
  effect (the auto-configuration is gated by `@ConditionalOnProperty(..., matchIfMissing=false)`).
- **`@StreamMQConsumer(enable = false)`** (annotation attribute, default `true`) — the consumer is
  **skipped entirely at registration time**: `StreamMQListenerRegistrar` logs
  `Skip disabled @StreamMQConsumer: bean=..., topic=...`, no listener/consume loop is created, and the
  `(topic, group)` registration key is **not** occupied (so another bean may claim it meanwhile). The
  consumer Bean itself is still created by Spring. Useful for gating a consumer behind an
  environment/flag (e.g. feature toggle) without removing the class. It is per-consumer only; there is
  no global counterpart. (The same attribute exists on `@StreamMQDlqConsumer`.)

---

## Extension Points

0.1.2 ships with **18 extension points** (user-facing + internal assembly points):

| Extension Point | Purpose | Default Implementation |
|---|---|---|
| `MessageSerializer` | Message serialization | **`JacksonJsonSerializer` (default, strict types / no gadget RCE surface)** / `FurySerializer` (opt-in, high throughput) / `ProtostuffSerializer` / `FlatBuffersSerializer` (FlexBuffers, zero-copy field reads) / `SbeSerializer` (fixed-header envelope) / `JdkSerializer` / `StringSerializer` / `ByteArraySerializer` |
| `MessageConverter` | Message-body ↔ business object | `DefaultMessageConverter` / `CompactMessageConverter` / `PassThroughMessageConverter` |
| `ProducerFilter` | Producer filter chain | none built-in — implement it and register a Bean |
| `ConsumerFilter` | Consumer filter chain | `TagSelectorFilter` / `SqlSelectorFilter` |
| `ProducerInterceptor` | Producer interceptor chain | `TraceContextProducerInterceptor` / `OpenTelemetryProducerInterceptor` |
| `ConsumerInterceptor` | Consumer interceptor chain | `TraceContextConsumerInterceptor` / `OpenTelemetryConsumerInterceptor` |
| `RetryPolicy` | Retry strategy | `FixedArrayRetryPolicy` / `FixedIntervalRetryPolicy` / `ExponentialBackoffRetryPolicy` / `NoRetryPolicy` |
| `RebalanceStrategy` | Consumer rebalance strategy | `AverageRebalanceStrategy` / `ConsistentHashRebalanceStrategy` / `RangeRebalanceStrategy` |
| `CompressionCodec` | Message compression | `GzipCompressionCodec` / `Lz4CompressionCodec` (classpath detection) |
| `TraceCollector` | Trace context collection | `NoopTraceCollector` / `Slf4jTraceCollector` / `RedisTraceCollector` |
| `ManagementAuthenticator` | Admin endpoint authentication | `AllowAllAuthenticator` / `BasicAuthAuthenticator` / `TokenAuthenticator` / `DenyAllAuthenticator` |
| `DlqFailureStrategy` | DLQ failure handling | `LogAndDropDlqFailureStrategy` / `LimitedRetryDlqFailureStrategy` / `SecondaryDlqFailureStrategy` |
| `ExpressionSelectorFilter` | Message filtering | `TagSelectorFilter` / `SqlSelectorFilter` |
| `ConsumerFilterResolver` | Per-consumer filter resolver | `ReflectiveConsumerFilterResolver` (default) / Spring container resolver |
| `OrderlyShardLockManager` | Ordered-consume shard distributed lock | `RedissonOrderlyShardLockManager` |
| `ConsumerGroupManager` | Consumer group instance management | `RedissonConsumerGroupManager` |
| `BroadcastInstanceRegistry` | Broadcast instance identity registry (stable slot + reclaim/lease, 0.1.2) | `RedisBroadcastInstanceRegistry` |
| `CompressionCodecRegistry` | Codec registry (register/lookup by name; consumers resolve the codec to decompress with) | `DefaultCompressionCodecRegistry` (GZIP built in, LZ4 by classpath detection) |

---

## Minimum dependency matrix

`streammq-core` and `streammq-redisson` declare their adapters/optional libraries as `provided` / `optional`
on purpose, so you can pick the client version. That also means **you must supply some coordinates yourself**:

| Path | Coordinates you must add yourself | Scope facts (verified in the module POMs) |
|---|---|---|
| **core-only** (framework-agnostic use) | `org.slf4j:slf4j-api` | `streammq-core` declares SLF4J as `provided` (the logging facade is not shipped transitively); Jackson is `optional` in core and core's runtime path does not use it |
| **redisson-only** (no Spring) | `org.redisson:redisson` (add the Jackson/SLF4J coordinates explicitly only if you override or exclude them) | `redisson` is `provided`; `org.slf4j:slf4j-api`, `com.fasterxml.jackson.core:jackson-databind` and `com.fasterxml.jackson.datatype:jackson-datatype-jsr310` are **compile** dependencies of `streammq-redisson` and therefore arrive transitively (the two Jackson artifacts back the default `JacksonJsonSerializer` and the `props` field codec). Serializers that are `optional` are needed only when you opt in: `org.apache.fory:fory-core` (>= 1.1.0) / `io.protostuff:protostuff-core`+`protostuff-runtime` / `com.google.flatbuffers:flatbuffers-java` / `org.agrona:agrona` |
| **starter** (Spring Boot 3) | `org.redisson:redisson-spring-boot-starter` (its version is managed by `streammq-bom`) + your own Spring Boot parent/BOM | the starter declares `redisson-spring-boot-starter` and `spring-boot-starter` as `provided`; `spring-boot-starter-actuator` and `micrometer-core` are `optional` (add them for health checks/metrics); `spring-boot-autoconfigure` is a regular compile dependency |

Optional-but-recommended extras: `io.micrometer:micrometer-registry-prometheus` for `/actuator/prometheus`,
and `streammq-diagnostics` / `streammq-tracing-opentelemetry` (source-only in 0.1.x) when you want their endpoints.

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
| `/actuator/prometheus` | Prometheus format — **requires `io.micrometer:micrometer-registry-prometheus` on the classpath** (not a StreamMQ dependency; the starter only brings `micrometer-core`). Add it yourself or the endpoint returns 404 |

> **Exposure note:** `/actuator/streammq` (and the management REST API below) is a Spring Boot `@WebEndpoint`. Spring Boot only exposes `health` and `info` by default, so without the setting below the endpoint returns **404**. Explicitly include it:
>
> ```yaml
> management:
>   endpoints:
>     web:
>       exposure:
>         include: streammq
> ```

### Management REST API

All under `/actuator/streammq`, dispatched by HTTP method + path segment:

| Endpoint | Method | Description |
|---|---|---|
| `/actuator/streammq` | GET | Overview (status, consumer groups, topics) |
| `/actuator/streammq/groups` | GET | List consumer groups |
| `/actuator/streammq/topics` | GET | List topics |
| `/actuator/streammq/pending/{group}/{topic}` | GET | Pending messages |
| `/actuator/streammq/dlq/{group}` | GET | DLQ messages |
| `/actuator/streammq/dlq/{group}?messageId&targetTopic` | POST | Requeue a DLQ message |
| `/actuator/streammq/dlq/{group}/{messageId}` | DELETE | Delete a DLQ message (`confirm={messageId}` required) |
| `/actuator/streammq/stats/{group}/{topic}` | GET | Runtime stats |
| `/actuator/streammq/ack/{group}/{topic}?messageId` | POST | Manual ACK |
| `/actuator/streammq/rebalance/{group}` | POST | Trigger rebalance |
| `/actuator/streammq/topics?topic=` | POST | Create a topic |
| `/actuator/streammq/topics/{topic}` | DELETE | Delete a topic (`confirm={topic}` required) |
| `/actuator/streammq/config/{group}` | POST | Update consumer-group config at runtime |

All require `ManagementAuthenticator`. Default is `DenyAllAuthenticator` (rejects everything, returns 401). Register `AllowAllAuthenticator` / `BasicAuthAuthenticator` / `TokenAuthenticator` Bean to open access. Authentication failures are rate-limited (10 failures per 60s → 5-minute lockout per client), and clients are aggregated by the non-spoofable `remoteAddr` by default. `X-Forwarded-For` is **not** trusted unless you opt in with `streammq.admin.trust-forwarded-headers=true` **and** declare `streammq.admin.trusted-proxies` (IPv4/IPv6 CIDR, e.g. `10.0.0.0/8`; loopback is trusted by default) — only a direct peer that matches the list may contribute the first XFF value, because an unvalidated XFF lets a single header bypass the lockout. Two more exposure caveats:
>
> - `/streammq/diagnostics/**` (the `streammq-diagnostics` module's plain MVC endpoints, mounted on the **application's main port**) is **not** governed by `management.endpoints.web.exposure.*` — once the module is on the classpath and enabled, those endpoints stay reachable even if Actuator only exposes `health`.
> - If JMX exposure is on, exclude the StreamMQ endpoints from it (`management.endpoints.jmx.exposure.exclude: streammq`).
>
> Restrict both endpoint families by network layer (security group / Ingress) and see [SECURITY.md](SECURITY.md) for the full policy.

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
| [Configuration Reference](docs/configuration-reference.md) | Every `streammq.*` key with its real default value |
| Javadoc | Bundled with Maven Central artifacts (sources/javadoc jars) |
| [CHANGELOG](CHANGELOG.md) | Version change log |
| [CONTRIBUTING](CONTRIBUTING.md) | Contribution process & dev conventions |
| [SECURITY](SECURITY.md) | Security policy & vulnerability disclosure |

> ⚠️ `docs/archived-historical/` contains V0.1/V1.0 draft design documents; their class names, configuration keys, and some mechanism descriptions have become outdated. Treat as archaeology only.

---

## Roadmap

### V1.0 Feature Milestone (implemented in 0.1.x)

> **Note**: The following features are available in the 0.1.x releases. The current version is **0.1.2**
> (feature preview). APIs may still evolve before 1.0.0 based on community feedback. Production use is
> recommended after gray-scale validation on non-critical paths.

- [x] Annotation-driven consumption (`@StreamMQConsumer`)
- [x] `StreamMessageTemplate` programming model (sync/async/oneway/batch/tx)
- [x] Cluster + broadcast consumption (broadcast supports persistent instance identity)
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
- [x] 18 extension points (user-facing + internal assembly points)
- [x] Spring Boot 3 auto-config + Actuator
- [x] Spring Cloud Stream Binder (source-only in 0.1.x)
- [x] Kubernetes integration (experimental; source-only in 0.1.x)
- [x] Message profiling & topology visualization (source-only in 0.1.x)
- [x] OpenTelemetry distributed tracing (source-only in 0.1.x)

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
git clone https://github.com/HK-hub/StreamMQ.git
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
| Spring Boot | 3.5.16 | Framework |
| Redisson | 3.34.1 | Redis client |
| Jackson | 2.18.10 | JSON serialization (default serializer; upgraded from 2.17.2 for GHSA-r7wm-3cxj-wff9 / GHSA-72hv-8253-57qq — the `jackson-bom` import is declared **before** the Spring Boot BOM so Boot's managed 2.17.2 cannot override it) |
| Apache Fory (formerly Apache Fury) | 1.7.3 | High-perf serialization (optional; opt-in for throughput; `org.apache.fory:fory-core`, required >= 1.1.0 — CVE-2026-50076) |
| Protostuff | 1.8.0 | Protobuf serialization (optional alternative) |
| FlatBuffers | 24.3.25 | FlexBuffers dynamic binary serialization (optional; `com.google.flatbuffers:flatbuffers-java`, for `FlatBuffersSerializer`) |
| Agrona | 1.17.1 | Runtime library for SBE generated code (optional; `org.agrona:agrona`) |
| SBE (sbe-tool) | 1.18.0 | Build-time code generation for the SBE envelope (`uk.co.real-logic:sbe-tool`, runs at `generate-sources`) |
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

- `FurySerializer` (opt-in, high throughput; `streammq.producer.serializer` must be explicitly set; underlying library **Apache Fory (formerly Apache Fury) 1.7.3**, Maven coordinates `org.apache.fory:fory-core`, >= 1.1.0 required — earlier versions are affected by CVE-2026-50076) — **enforces class registration by default** (`requireClassRegistration=true`): only explicitly registered types can be deserialized and unregistered POJOs are rejected. Pre-register your payload types:
  ```yaml
  streammq:
    producer:
      fury-require-class-registration: true
      fury-registered-classes: com.acme.OrderCreated,com.acme.Payment
  ```
  Register application payloads up front with `new FurySerializer<>(OrderCreated.class)` or `new FurySerializer().register(OrderCreated.class)`. From plain Java: `new FurySerializer()` **is the whitelist mode** (same as `new FurySerializer(true)`); `new FurySerializer(false)` is the unrestricted mode and requires `-Dstreammq.security.allowUnrestrictedSerializer=true` (otherwise it throws `SecurityException`; with the property set it still logs a WARN). Note the post-deserialize `isInstance` check only validates the result type — it cannot stop gadget execution, so the whitelist is the real defense.
- When the default serializer (`JacksonJsonSerializer`, nested-object safe) is in effect, the underlying Redisson client codec must still be secured separately — see [SECURITY.md](SECURITY.md) for the full security surface.
- `JdkSerializer` has a JEP 290 class name whitelist filter (target type + JDK basics); use `JdkSerializer.unrestricted()` only as a last resort — gated by `-Dstreammq.security.allowUnrestrictedSerializer=true`.

For shared/multi-tenant Redis, keep the `FurySerializer` class-registration whitelist enabled (it is the default). See [SECURITY.md](SECURITY.md) for the full security policy.

---

## License

This project is licensed under the [MIT License](LICENSE).

---

**StreamMQ** · Turn Redis into your message bus

If this project helps you, please give it a ⭐ Star!
