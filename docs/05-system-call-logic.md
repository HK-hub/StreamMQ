# StreamMQ 系统完整调用逻辑文档

> 本文档基于 `streammq-core`、`streammq-redisson-adapter`、`streammq-spring-boot-starter` 模块的源码梳理而成，描述从用户调用 API 到 Redis Stream 底层读写的完整调用链路、组件生命周期与关键时序。
>
> 所有引用的类名、方法签名均来自当前代码库（截至 2026-07-03）。

---

## 目录

1. [系统架构概览](#1-系统架构概览)
2. [核心组件清单](#2-核心组件清单)
3. [完整的消息发送→消费链路](#3-完整的消息发送消费链路)
4. [单独的消息发送逻辑](#4-单独的消息发送逻辑)
5. [单独的消费逻辑](#5-单独的消费逻辑)
6. [生命周期管理](#6-生命周期管理)
7. [关键时序图](#7-关键时序图)

---

## 1. 系统架构概览

### 1.1 模块组成

StreamMQ 采用多模块 Maven 结构，职责严格分离：

| 模块 | 职责 | 当前状态 |
|------|------|----------|
| `streammq-core` | 核心 SPI 接口与值对象定义（`StreamMessageService`、`StreamMessageTemplate`、`Message`、`MessageBuilder`、拦截器、策略接口、枚举、异常） | 完整实现 |
| `streammq-redisson-adapter` | 基于 Redisson 的实现层（`RedissonStreamProducer`、`RedissonStreamListener`、`DefaultStreamMQListenerContainer`、调度器、转换器、序列化器、重试策略等） | 完整实现 |
| `streammq-spring-boot-starter` | Spring Boot 自动装配层（5 个 `AutoConfiguration` + 2 个 `SmartLifecycle` + `StreamMQListenerRegistrar`） | 完整实现 |
| `streammq-native` | 原生 API（无 Spring 依赖）入口 | 占位（仅 `package-info.java`） |
| `streammq-kafka-compat` | Kafka 协议兼容层 | 占位（仅 `package-info.java`） |
| `streammq-amqp-compat` | AMQP 协议兼容层 | 占位（仅 `package-info.java`） |
| `streammq-test` | 共享测试工具 | 占位（仅 `package-info.java`） |
| `streammq-samples` | 4 个示例：`quickstart` / `orderly` / `delay` / `transaction` | 完整示例 |
| `streammq-bom` | 物料清单（BOM），统一版本管理 | 完整 |

### 1.2 分层架构

StreamMQ 采用 5 层架构，自顶向下依赖：

```
┌─────────────────────────────────────────────────────────────────┐
│  Spring Boot 自动装配层                                          │
│  StreamMQAutoConfiguration → 5 个子配置 + 2 个 SmartLifecycle   │
└──────────────────────────────┬──────────────────────────────────┘
                               │ 依赖
┌──────────────────────────────▼──────────────────────────────────┐
│  API 层（用户入口）                                              │
│  StreamMessageService ← DefaultStreamMessageService             │
└──────────────────────────────┬──────────────────────────────────┘
                               │ 委派
┌──────────────────────────────▼──────────────────────────────────┐
│  模板层（编排拦截器 + 重试 + 事务）                              │
│  StreamMessageTemplate ← DefaultStreamMessageTemplate           │
└──────────────────────────────┬──────────────────────────────────┘
                               │ 选择 Producer / Listener
┌──────────────────────────────▼──────────────────────────────────┐
│  适配层（Redisson 实现）                                         │
│  Producer / Listener / Container / Scheduler / 策略类           │
└──────────────────────────────┬──────────────────────────────────┘
                               │ 底层调用
┌──────────────────────────────▼──────────────────────────────────┐
│  Redis Stream (XADD / XREADGROUP / XACK / ZSet / Hash)          │
└─────────────────────────────────────────────────────────────────┘
```

依赖方向严格自顶向下；`streammq-core` 不依赖任何实现模块，所有扩展点均通过 SPI 接口暴露。

---

## 2. 核心组件清单

### 2.1 服务层（API 层）

| 组件 | 模块 | 职责 | 创建时机 | 销毁时机 |
|------|------|------|----------|----------|
| `StreamMessageService`（接口） | core | 用户级发送 API，封装 Template，支持 `send` / `asyncSend` / `sendOneway` / `sendBatch` / `sendDelay` / `sendTransaction` | — | — |
| `DefaultStreamMessageService`（实现） | core | 通过 `MessageBuilder` 构造 `Message`，委派 `StreamMessageTemplate` | `StreamMQCoreAutoConfiguration#streamMQService` 装配时（Spring 容器启动） | Spring 容器关闭 |

### 2.2 模板层

| 组件 | 模块 | 职责 | 创建时机 | 销毁时机 |
|------|------|------|----------|----------|
| `StreamMessageTemplate`（接口，继承 `TransactionExecutor`） | core | 核心生产者 API：`syncSend` / `asyncSend` / `sendOneway` / `syncSendBatch` / `executeInTransaction`；维护 `ProducerInterceptor` 链 | — | — |
| `DefaultStreamMessageTemplate`（实现） | redisson-adapter | 调度拦截器 `beforeSend`/`afterSend`/`onException` 链；按 group 解析 Producer；MDC 上下文注入；事务消息编排；批量转发 | `StreamMQCoreAutoConfiguration#streamMQTemplate` 装配时 | Spring 容器关闭 |

### 2.3 生产者层

| 组件 | 模块 | 职责 | 创建时机 | 销毁时机 |
|------|------|------|----------|----------|
| `StreamMessageProducer`（接口） | core | 底层发送抽象：`syncSend` / `asyncSend` / `sendOneway` / `syncSendBatch` / `close` | — | — |
| `StreamMessageProducerFactory`（接口） | core | 按 `ProducerConfig` 创建 / 缓存 Producer | — | — |
| `RedissonStreamProducer`（实现） | redisson-adapter | 调用 `RStream.add` / `RBatch` 完成 XADD；延时消息写入 ZSet + payload Hash；异步发送使用虚拟线程执行器 | `RedissonStreamProducerFactory#createProducer` 首次按 group 调用时（lazy） | `Factory.close()` 或 `Producer.close()` |
| `RedissonStreamProducerFactory`（实现） | redisson-adapter | 按 group 缓存 Producer（`ConcurrentHashMap.computeIfAbsent`）；共享 RedissonClient | `StreamMQCoreAutoConfiguration#streamMQProducerFactory` 装配时 | `Factory.close()`（容器关闭时由 Bean 销毁回调） |

### 2.4 消费者层（用户实现的回调接口）

| 组件 | 模块 | 职责 |
|------|------|------|
| `StreamMessageConsumer<T>`（接口） | core | 通用消费者标记接口，提供 `consumeMessage` 默认方法 |
| `StreamMessageConcurrentlyConsumer<T>` | core | 并发消费回调：`ConsumeAction onMessage(Message<T>, ConsumeContext)`；默认 `consumeMessage` 实现：调用 `onMessage`，返回 `SUCCESS` 时调用 `context.markAcked()` |
| `StreamMessageOrderlyConsumer<T>` | core | 顺序消费回调：`OrderlyAction onMessage(Message<T>, ConsumeOrderlyContext)` |

> 这些接口由用户实现并由 `@StreamMQConsumer` 注解标注，**非框架创建**。创建时机为 Spring 容器扫描注解后通过 `StreamMQListenerRegistrar` 注册到容器。

### 2.5 监听器层（底层 PULL 抽象）

| 组件 | 模块 | 职责 | 创建时机 | 销毁时机 |
|------|------|------|----------|----------|
| `StreamMQListener`（接口） | core | 底层拉取与确认：`pull` / `pullBlock` / `ack` / `ackBatch` / `close` | — | — |
| `StreamMQListenerFactory`（接口） | core | 按 `ListenerConfig` 创建 Listener | — | — |
| `RedissonStreamListener`（实现） | redisson-adapter | 调用 `RStream.readGroup`（XREADGROUP）拉取；`RStream.ack`（XACK）确认；消费者组首次 lazy 创建（`createGroup` + `BUSYGROUP` 容错）；DLQ 模式与跨平台 body 类型回退 | `DefaultStreamMQListenerContainer#consumeLoop` 启动时通过 `consumerFactory.createListener(config)` 创建 | `RedissonStreamListenerFactory#close()`（容器 stop 时） |
| `RedissonStreamListenerFactory`（实现） | redisson-adapter | 持有共享 RedissonClient 与 Converter；维护已创建 Listener 集合 | `StreamMQCoreAutoConfiguration#streamMQListenerFactory` 装配时 | `Factory.close()` |

### 2.6 容器层

| 组件 | 模块 | 职责 | 创建时机 | 销毁时机 |
|------|------|------|----------|----------|
| `StreamMQListenerContainer`（接口） | core | 注册并发 / 顺序 / DLQ Listener；管理 start / stop / pause / resume；返回 `ConsumerMetadata` 集合 | — | — |
| `DefaultStreamMQListenerContainer`（实现） | redisson-adapter | 注册表 + 虚拟线程消费循环（`Executors.newVirtualThreadPerTaskExecutor`）；编排拦截器链 / RetryAndDlqHandler / OrderlyShardLockManager 三大策略；状态机 `INIT→STARTING→RUNNING→STOPPING→STOPPED` | `StreamMQListenerContainerAutoConfiguration#streamMQListenerContainer` 装配时 | `container.stop()`（由 `StreamMQListenerContainerLifecycle` 触发） |

### 2.7 调度器层

| 组件 | 模块 | 职责 | 创建时机 | 销毁时机 |
|------|------|------|----------|----------|
| `StreamMQScheduler`（接口） | core | 统一调度器接口：`start` / `stop` / `isRunning` | — | — |
| `DelayMessageScheduler` | redisson-adapter | 周期扫描各 `DelayLevel` 的 ZSet（`streammq:{ns}:delay:{level}`），将到期 msgId 通过 `ZREM` 原子获取，读取 payload Hash 后 XADD 到目标 Stream | `StreamMQSchedulerAutoConfiguration#streamMQDelayMessageScheduler` 装配时 | `stop()`（由 `StreamMQSchedulerLifecycle` 触发） |
| `RetryScheduler` | redisson-adapter | 周期扫描重试 ZSet（`streammq:{ns}:retry:{topic}:{group}`），按 `retryCount` 决策转投目标 Stream 或 DLQ Stream | `StreamMQSchedulerAutoConfiguration#streamMQRetryScheduler` 装配时 | 同上 |
| `TransactionScanner` | redisson-adapter | 周期扫描事务回查 ZSet（`streammq:{ns}:txcheck:{txGroup}`），对超时半消息触发 `TransactionChecker.check`；提供 `registerHalfMessage` / `markCommit` / `markRollback` | `StreamMQSchedulerAutoConfiguration#streamMQTransactionScanner` 装配时 | 同上 |

### 2.8 策略类

| 组件 | 模块 | 职责 |
|------|------|------|
| `ConsumerInterceptorChain`（接口） / `DefaultConsumerInterceptorChain` | core / redisson-adapter | 管理 `ConsumerInterceptor` 链，按 `order()` 升序执行 `beforeConsume` / `afterConsume` / `onException` |
| `RetryAndDlqHandler`（接口） / `DefaultRetryAndDlqHandler` | core / redisson-adapter | 路由消费动作：`SUCCESS`→XACK；`RECONSUME_LATER`→写 retry ZSet + payload Hash 后 XACK，超限则转投 DLQ Stream |
| `OrderlyShardLockManager`（接口） / `RedissonOrderlyShardLockManager` | core / redisson-adapter | 为顺序消费创建 shard 级 `RLock` 数组；按 `shardingKey.hashCode() % shardCount` 路由加锁执行 |

### 2.9 SPI 组件

| SPI 接口 | 模块 | 实现类（redisson-adapter） | 默认装配 |
|----------|------|---------------------------|----------|
| `MessageSerializer` | core | `JacksonJsonSerializer`（默认）、`JdkSerializer`、`FurySerializer`、`ProtostuffSerializer`、`StringSerializer`、`ByteArraySerializer` | `JacksonJsonSerializer`（可由 `streammq.producer.serializer` 配置覆盖） |
| `MessageConverter` | core | `DefaultMessageConverter`（默认，Base64+序列化器）、`CompactMessageConverter`、`PassThroughMessageConverter` | `DefaultMessageConverter` |
| `RetryPolicy` | core | `FixedArrayRetryPolicy`（对齐 RocketMQ 16 级）、`ExponentialBackoffRetryPolicy`、`FixedIntervalRetryPolicy`、`DecorrelatedJitterRetryPolicy`、`NoRetryPolicy` | 由 `streammq.retry.policy` 配置指定类名，反射实例化 |
| `RebalanceStrategy` | core | `AverageRebalanceStrategy`、`RangeRebalanceStrategy`、`ConsistentHashRebalanceStrategy` | 未在 AutoConfiguration 默认装配（预留扩展点） |
| `ManagementAuthenticator` | core | `DenyAllAuthenticator`（默认安全兜底）、`AllowAllAuthenticator`、`BasicAuthAuthenticator`、`TokenAuthenticator` | `DenyAllAuthenticator` |
| `TraceCollector` | core | `NoopTraceCollector`（默认）、`Slf4jTraceCollector`（`streammq.tracing.enabled=true` 时） | `NoopTraceCollector` |
| `ProducerInterceptor` | core | `TraceContextProducerInterceptor`（tracing 启用时）、`LoggingProducerInterceptor` | 由 Template 通过 `addProducerInterceptor` 注入 |
| `ConsumerInterceptor` | core | `TraceContextConsumerInterceptor`（tracing 启用时）、`LoggingConsumerInterceptor` | 由 Container 通过 `addConsumerInterceptor` 注入 |

### 2.10 指标

| 组件 | 模块 | 职责 | 创建时机 |
|------|------|------|----------|
| `StreamMQMetrics`（接口） | core | 指标记录 SPI：`recordSend` / `recordConsume` / `recordRetry` / `recordDlq` / `recordDelay` / `recordTxCommit` / `recordTxRollback` / `recordTxCheck` | — |
| `MicrometerStreamMQMetrics`（实现） | redisson-adapter | 基于 `MeterRegistry` 输出 `streammq.send.total` / `streammq.consume.total` / `streammq.retry.total` / `streammq.dlq.total` / `streammq.delay.total` / `streammq.transaction.*` 等 Counter/Timer | `StreamMQMetricsAutoConfiguration#streamMQMetrics`（仅当 classpath 存在 `MeterRegistry`） |

### 2.11 Spring Boot 装配层

| 组件 | 模块 | 职责 |
|------|------|------|
| `StreamMQAutoConfiguration` | starter | 装配主入口，`@Import` 5 个子配置；触发条件 `streammq.enabled=true`（默认）且 classpath 存在 `RedissonClient` 与 `StreamMessageTemplate` |
| `StreamMQCoreAutoConfiguration` | starter | 装配 `RedissonClient`（兜底）+ 序列化器 + 转换器 + 重试策略 + Producer/Listener 工厂 + Template + Service + Trace/Auth 拦截器 |
| `StreamMQSchedulerAutoConfiguration` | starter | 装配 3 个调度器 + `StreamMQSchedulerLifecycle`（`phase = Integer.MAX_VALUE - 100`） |
| `StreamMQListenerContainerAutoConfiguration` | starter | 装配 `DefaultStreamMQListenerContainer` + `StreamMQListenerRegistrar` + `StreamMQListenerContainerLifecycle`（`phase = Integer.MAX_VALUE - 200`） |
| `StreamMQHealthAutoConfiguration` | starter | 装配 `StreamMQHealthIndicator`（Redis ping + 容器状态） |
| `StreamMQMetricsAutoConfiguration` | starter | 装配 `MicrometerStreamMQMetrics`（条件：classpath 存在 `MeterRegistry`） |
| `StreamMQListenerRegistrar` | starter | 实现 `SmartInitializingSingleton`，在所有单例 Bean 就绪后扫描 `@StreamMQConsumer` / `@StreamMQTransactionConsumer` 注解并注册；支持 `${}` 占位符与 `#{}` SpEL 表达式 |
| `StreamMQSchedulerLifecycle` | starter | `SmartLifecycle`，按列表顺序启动调度器，反向停止 |
| `StreamMQListenerContainerLifecycle` | starter | `SmartLifecycle`，启动 / 停止 Listener 容器 |

### 2.12 辅助工具

| 组件 | 模块 | 职责 |
|------|------|------|
| `BodyTypeResolver` | core | 通过反射解析 Consumer 实现类上的泛型 `T`，作为跨平台反序列化回退类型 |
| `MessageBuilder<T>` | core | Builder 模式构造 `Message`，支持 topic / body / tag / keys / shardingKey / delayLevel / properties 等 |
| `MessageMetadataBuilder` | core | 封装附加元数据（tag/keys/shardingKey/delay/properties），通过 `applyTo(MessageBuilder)` 应用到消息 |
| `BatchMessage<T>` | core | 批量消息载体，封装同 Topic 的 `List<Message<T>>` |
| `ConsumerMdcTrace` | redisson-adapter | 消费侧 MDC 上下文注入 / 清理（topic / group / msgId / shardingKey / reconsumeTimes） |
| `DefaultConsumeContextConsume` | redisson-adapter | `ConsumeOrderlyContext` 实现，提供消息元数据访问与手动 ACK 能力，封装 `AtomicBoolean acked` |
| `DefaultAcknowledgment` | redisson-adapter | `Acknowledgment` 实现：`acknowledge()` 调 `listener.ack` + `context.markAcked`；`nack()` / `defer()` 留在 PEL |
| `StreamMQKeys` | redisson-adapter | Redis Key 命名工具，统一 `streammq:{ns}:{type}:{...}` 拼接规则 |
| `MdcKeys` | redisson-adapter | MDC 字段名常量 |

---

## 3. 完整的消息发送→消费链路

以用户调用 `streamMessageService.send("orders", order)` 为起点，到消费者 `onMessage` 被调用的完整链路：

### 3.1 发送阶段（同步发送）

```text
用户代码
  └─ StreamMessageService.send(topic, body)                          [API 层]
       └─ DefaultStreamMessageService.send(topic, body)
            ├─ MessageBuilder.withTopic(topic).body(body).build()    构造 Message
            └─ StreamMessageTemplate.syncSend(message)               [模板层]
                 └─ DefaultStreamMessageTemplate.syncSend(msg, timeout=3000, retry=2)
                      ├─ injectProducerMdc(message)                  MDC 注入
                      ├─ applyInterceptorsBefore(message)            拦截器 beforeSend 链
                      │     └─ 任一返回 false → 构造 SEND_FAILED → applyInterceptorsAfter → 返回
                      ├─ resolveProducer(topic)
                      │     └─ producerFactory.createProducer(defaultConfig)
                      │           └─ RedissonStreamProducerFactory.createProducer
                      │                 └─ ConcurrentHashMap.computeIfAbsent(group, g -> builder().build())
                      ├─ for (attempt=0; attempt<=retry; attempt++):  重试循环
                      │     ├─ StreamMessageProducer.syncSend(msg, timeout)   [适配层]
                      │     │    └─ RedissonStreamProducer.syncSend
                      │     │         ├─ ensureOpen()
                      │     │         ├─ message.isDelayMessage() ? sendDelayMessage : 普通发送
                      │     │         │    普通发送:
                      │     │         │    ├─ converter.toStreamFields(message)     MessageConverter
                      │     │         │    │    └─ DefaultMessageConverter.toStreamFields
                      │     │         │    │         ├─ serializer.serialize(body, bodyType)   body 序列化
                      │     │         │    │        ├─ Base64 编码 body
                      │     │         │    │        └─ 组装 Map: body/bodyType/tag/keys/shardingKey/props/bornTs/...
                      │     │         │    ├─ StreamMQKeys.topicStream(namespace, topic)       Key 拼接
                      │     │         │    └─ appendStream(streamKey, fields, timeoutMillis)
                      │     │         │         └─ RStream.addAsync(args).get(timeout, MS)    [Redis XADD]
                      │     │         │               (maxLen>0 时附加 trimNonStrict MAXLEN)
                      │     │         │    延时发送:
                      │     │         │    ├─ DelayLevel closestAbove(delayTimeMillis)
                      │     │         │    ├─ zset.add(deliverAt, msgId)                       [Redis ZADD]
                      │     │         │    ├─ payloadMap.putAll(fields + targetTopic + deliverAt)  [Redis HSET]
                      │     │         │    └─ 返回合成的 SendResult（不写入 Stream）
                      │     │         ├─ message.setMessageId(new MessageId(streamId))
                      │     │         └─ 返回 SendResult
                      │     ├─ 成功 → applyInterceptorsAfter(message, result) → 返回 result
                      │     └─ StreamMQException → notifyProducerException → 继续重试
                      ├─ 全部失败 → applyInterceptorsAfter(failedResult) → 抛 StreamMQException
                      └─ clearProducerMdc()                         MDC 清理
```

### 3.2 消费阶段

```text
Redis Stream (有新 entry)
  ↑ 消费循环主动拉取（非推送）
  │
DefaultStreamMQListenerContainer.consumeLoop(reg)                  [容器层]
  ├─ createConsumerFor(reg)
  │    └─ consumerFactory.createListener(ListenerConfig)           [监听器层]
  │         └─ RedissonStreamListenerFactory.createListener
  │              └─ RedissonStreamListener.builder().build()
  ├─ while (state == RUNNING):
  │    ├─ if (paused) sleep(PAUSED_SLEEP_MILLIS=100ms); continue
  │    ├─ listener.pullBlock(batchSize=32, Duration=1s)            [StreamMQListener]
  │    │    └─ RedissonStreamListener.pullBlock
  │    │         ├─ ensureGroup()                                  首次 lazy 创建消费者组
  │    │         │    └─ RStream.createGroup(name(group).makeStream().id(0-0))
  │    │         │       (BUSYGROUP 视为已存在)
  │    │         └─ doRead(batchSize, timeout)
  │    │              ├─ RStream.readGroup(group, consumerName,    [Redis XREADGROUP > COUNT n BLOCK ms]
  │    │              │     StreamReadGroupArgs.neverDelivered().count(n).timeout(t))
  │    │              ├─ 对每个 entry:
  │    │              │    toMessage(streamId, fields)
  │    │              │      ├─ 解析 bodyType（fields.bodyType 或 targetBodyType 或 String）
  │    │              │      ├─ converter.fromStreamFields(fields, bodyType)  反序列化
  │    │              │      │    └─ DefaultMessageConverter.fromStreamFields
  │    │              │      │         ├─ Base64 解码 body → serializer.deserialize(bytes, type)
  │    │              │      │         ├─ 回填 tag/keys/shardingKey/props/bornTs/retryTimes/txId
  │    │              │      └─ applyTopic / applyMessageId 回填
  │    │              └─ 返回 List<Message>
  │    ├─ for (message : messages):
  │    │    └─ handleMessage(message, reg, listener)
  │    │         ├─ new DefaultConsumeContextConsume(message, reg, listener)
  │    │         ├─ ConsumerMdcTrace.inject(message, reg)
  │    │         ├─ interceptorChain.applyBefore(message)          [拦截器 beforeConsume 链]
  │    │         │    └─ 任一返回 false → retryDlqHandler.handleAction(SUCCESS) → return
  │    │         ├─ if (reg.type == ORDERLY):
  │    │         │    └─ shardLockManager.consumeWithShardLock(message, reg, ctx, orderly)
  │    │         │         ├─ shardIndex = abs(shardingKey.hashCode()) % shardCount
  │    │         │         ├─ lock.lock(consumeTimeoutMillis, MS)  [Redis SETNX + TTL]
  │    │         │         ├─ orderly.onMessage(message, ctx)      [用户业务回调]
  │    │         │         └─ lock.unlock()
  │    │         │    else (并发):
  │    │         │    └─ consumer.onMessage(message, ctx)          [用户业务回调]
  │    │         │         └─ 返回 ConsumeAction (SUCCESS / RECONSUME_LATER)
  │    │         ├─ 根据 ackMode + 返回值:
  │    │         │    AUTO 模式 → retryDlqHandler.handleAction(action, ...)
  │    │         │    MANUAL 模式 → 检查 ctx.isAcked() 决定 SUCCESS 或 RECONSUME_LATER
  │    │         ├─ 异常 → interceptorChain.notifyException(EXECUTING)
  │    │         │        → retryDlqHandler.handleAction(RECONSUME_LATER)
  │    │         └─ finally:
  │    │              ├─ interceptorChain.applyAfter(message, finalAction)
  │    │              └─ ConsumerMdcTrace.clear()
  │    └─ 空批次 → sleep(pullIntervalMillis) 后继续
  └─ 状态非 RUNNING → 退出循环
```

### 3.3 ACK / 重试 / DLQ 路由（`DefaultRetryAndDlqHandler.handleAction`）

```text
handleAction(action, message, reg, listener):
  ├─ SUCCESS → listener.ack(messageId)                           [Redis XACK]
  └─ RECONSUME_LATER:
       ├─ if (reg.dlqMode):                                       DLQ 消费者失败
       │    └─ listener.ack(messageId)                            直接 ACK 丢弃，避免死循环
       └─ else: handleReconsumeLater(...)
            ├─ fields = converter.toStreamFields(message)
            ├─ delay = retryPolicy.nextRetryDelay(retryCount, message)
            ├─ if (delay == null):                                不再重试
            │    ├─ routeToDlq(message, reg, messageId, "maxRetry")
            │    │    ├─ fields.put("dlqReason", "maxRetry")
            │    │    ├─ fields.put("originalMessageId", streamEntryId)
            │    │    ├─ dlqStream.add(StreamAddArgs.entries(fields))  [Redis XADD 到 DLQ Stream]
            │    │    └─ 返回 true
            │    └─ if (成功) → listener.ack(messageId)
            └─ else:                                              安排重试
                 ├─ nextRetryAt = now + delay.toMillis()
                 ├─ payloadMap.putAll(fields + retryCount + targetTopic)  [Redis HSET]
                 ├─ zset.add(nextRetryAt, msgIdStr)                [Redis ZADD 到 retry ZSet]
                 └─ listener.ack(messageId)                       [Redis XACK 原消息]
```

### 3.4 数据流转

| 阶段 | 数据形态 | 关键字段 |
|------|----------|----------|
| 用户调用 | `(topic, body)` | 原始 POJO |
| Service 层 | `Message<T>` | topic + body + tag/keys/shardingKey |
| 模板层 | `Message<T>` + 拦截器可能修改 | 同上 |
| Producer | `Map<String, String>` fields | body(Base64) + bodyType + tag + keys + shardingKey + props(JSON) + bornTs + ... |
| Redis Stream | Entry ID → fields | 同上 |
| Listener 拉取 | `Map<StreamMessageId, Map<String, String>>` | 同上 + Entry ID |
| Container | `Message<?>` | 反序列化的 body + 回填的 topic/messageId |
| Consumer | `Message<T>` + `ConsumeContext` | 同上 |

### 3.5 线程模型

| 阶段 | 线程 | 阻塞点 |
|------|------|--------|
| 同步发送 | 调用方线程 | `RStream.addAsync(args).get(timeout, MS)` |
| 异步发送 | `RedissonStreamProducer.asyncExecutor`（虚拟线程池） | 同上，但调用方不阻塞 |
| `sendOneway` | 同上虚拟线程池 | 调用方不阻塞，异常仅记日志 |
| 消费循环 | `DefaultStreamMQListenerContainer.consumeExecutor`（虚拟线程池，每个 Listener 一个虚拟线程） | `RStream.readGroup` BLOCK 1s |
| 调度器扫描 | 各调度器自有 `ScheduledThreadPoolExecutor`（1 个守护线程） | `scheduleAtFixedRate` |
| 顺序消费加锁 | 消费虚拟线程 | `RLock.lock(consumeTimeout, MS)` |

---

## 4. 单独的消息发送逻辑

### 4.1 同步发送 `syncSend`

**调用链**：`Service.send` → `Template.syncSend` → `Producer.syncSend` → `RStream.addAsync().get(timeout)`

**核心逻辑**（`DefaultStreamMessageTemplate#syncSend`，line 103-151）：

1. 参数校验：`message` 非空，`timeoutMillis <= 0` 时回退到 `DEFAULT_SEND_TIMEOUT_MILLIS`（3000ms），`retryTimes < 0` 时置 0。
2. `injectProducerMdc(message)`：注入 `topic` / `producerGroup` / `msgId` / `shardingKey` 到 SLF4J MDC。
3. `applyInterceptorsBefore(message)`：按 `order()` 升序遍历 `ProducerInterceptor.beforeSend`，任一返回 false 则构造 `SEND_FAILED` 结果，调用 `afterSend` 后直接返回。
4. `resolveProducer(topic)`：调用 `producerFactory.createProducer(defaultConfig)`，按 group 缓存命中或新建 `RedissonStreamProducer`。
5. **重试循环** `for (attempt = 0; attempt <= retryTimes; attempt++)`：
   - 调用 `producer.syncSend(message, timeoutMillis)`。
   - 成功 → `applyInterceptorsAfter` → 返回 `SendResult`。
   - 抛 `StreamMQException` → `notifyProducerException(EXECUTING)` → 记日志 → 继续下一次尝试。
   - 抛其他 `RuntimeException` → `notifyProducerException` → 重新抛出。
6. 全部重试失败 → `applyInterceptorsAfter(failedResult)` → 抛出最后一个 `StreamMQException`。
7. `finally` 块清理 MDC。

**Producer 层**（`RedissonStreamProducer#syncSend`，line 106-137）：

- `ensureOpen()` 校验未关闭。
- `message.isDelayMessage()` 为 true（`delayLevel != null` 或 `delayTimeMillis != null`）→ 走 `sendDelayMessage` 分支（见 §4.6）。
- `converter.toStreamFields(message)` 调用 `DefaultMessageConverter`：
  - `serializer.serialize(body, bodyType)` 序列化 body 为 byte[]。
  - `Base64.getEncoder().encodeToString(bodyBytes)` 编码。
  - 写入 `bodyType`（类全名）、`tag`、`keys`、`shardingKey`、`props`（系统+用户属性合并的 JSON）、`bornTs`、`bornHost`、`retryTimes`、`txId`。
  - **注意**：`topic` 不写入 fields，由 Stream Key 本身表示。
- `StreamMQKeys.topicStream(namespace, topic)` 拼接 Key 为 `streammq:{ns}:msg:{topic}`。
- `appendStream(streamKey, fields, timeoutMillis)`：
  - `redisson.getStream(streamKey)` 获取 `RStream<String, String>`。
  - `StreamAddArgs.entries(fields)` 构造参数；`maxLen > 0` 时附加 `trimNonStrict().maxLen(maxLen).noLimit()`（对应 `XADD ... MAXLEN ~ N`）。
  - **`stream.addAsync(args).get(timeoutMillis, MS)`**：异步提交 + 超时等待，确保 `timeoutMillis` 真正生效。
  - `TimeoutException` → 抛 `ProducerTimeoutException`；`ExecutionException` → 包装为 `StreamMQBrokerException`。
- 返回 `SendResult(messageId, topic, tag, bornTimestamp)`，`message.setMessageId` 回填。

### 4.2 异步发送 `asyncSend`

存在两个重载：

#### 4.2.1 返回 `CompletableFuture<SendResult>`

**`Template.asyncSend(message)`**（line 154-178）：

1. MDC 注入。
2. `applyInterceptorsBefore`，被中止 → 返回 `CompletableFuture.failedFuture(StreamMQException)`。
3. `producer.asyncSend(message)`：
   - `RedissonStreamProducer.asyncSend`（line 140-144）：`CompletableFuture.supplyAsync(() -> syncSend(message, defaultTimeoutMillis), asyncExecutor)`。
   - **异步执行器**：`Executors.newVirtualThreadPerTaskExecutor()`（每个发送任务一个虚拟线程）。
4. `whenComplete` 回调：成功 → `applyInterceptorsAfter`；异常 → `notifyProducerException(EXECUTING)`。
5. `finally` 清理 MDC（同步清理，因为异步任务在另一线程，MDC 上下文不传递）。

#### 4.2.2 回调通知 `asyncSend(message, callback, timeout)`

**`Template.asyncSend(message, callback, timeoutMillis)`**（line 186-212）：

1. MDC 注入 + `applyInterceptorsBefore`（被中止 → `callback.onException`）。
2. `producer.asyncSend(message).whenComplete((result, ex) -> {...})`：
   - 成功 → `applyInterceptorsAfter` + `callback.onSuccess(result)`。
   - 失败 → `notifyProducerException` + `callback.onException(ex)`。

> Service 层的 `asyncSend(topic, body, timeoutMillis)` 通过 `template.asyncSend(...).orTimeout(timeoutMillis, MS)` 实现超时控制。

### 4.3 单向发送 `sendOneway`

**`Template.sendOneway(message)`**（line 215-232）：

1. MDC 注入 + `applyInterceptorsBefore`（**不检查返回值**，仅做拦截记录）。
2. `producer.sendOneway(message)`：
   - `RedissonStreamProducer.sendOneway`（line 147-157）：`asyncExecutor.submit(() -> { try { syncSend(message, defaultTimeoutMillis); } catch (RuntimeException ex) { LOG.warn(...); } })`。
   - **fire-and-forget**：提交到虚拟线程池后立即返回，不等待结果，异常仅记录日志。

### 4.4 批量发送 `syncSendBatch`

**`Template.syncSendBatch(batch)`**（line 235-266）：

1. 校验 `batch` 非空。
2. 对 `batch.getMessages()` 中每条消息执行 `applyInterceptorsBefore`，任一被中止 → 抛 `StreamMQException`。
3. `resolveProducer(batch.getTopic())`。
4. `producer.syncSendBatch(batch.getMessages())`：
   - `RedissonStreamProducer.syncSendBatch`（line 160-214）：
     - 校验所有消息同 Topic。
     - 任一为延时消息 → 降级为单条 `syncSend` 循环。
     - **正常批量**：`RBatch batch = redisson.createBatch()`，对每条消息 `converter.toStreamFields` + `batch.getStream(streamKey).addAsync(args)`，最后 `batch.execute()`（Pipeline 一次性提交）。
     - 由于 `RBatch` 不返回每条 ID，为每条生成 UUID 哈希占位 ID。
5. 对每条结果 `applyInterceptorsAfter`，返回 `List<SendResult>`。

### 4.5 拦截器链执行顺序

**生产者拦截器**（`DefaultStreamMessageTemplate`）：

| 时机 | 方法 | 行为 |
|------|------|------|
| 发送前 | `applyInterceptorsBefore` | 按 `order()` 升序遍历；任一返回 false 中止发送；异常 → `notifyProducerException(BEFORE)` + 中止 |
| 发送后 | `applyInterceptorsAfter` | 按 `order()` 升序遍历；异常 → `notifyProducerException(AFTER)`，不影响主流程 |
| 异常时 | `notifyProducerException(timing)` | 按 `order()` 升序遍历 `onException`；拦截器自身异常被忽略 |

拦截器链由 `CopyOnWriteArrayList` 持有，支持运行时动态添加（`addProducerInterceptor` 按序插入）。

### 4.6 延时消息发送（`sendDelayMessage`）

**`RedissonStreamProducer#sendDelayMessage`**（line 222-259）：

1. 生成 `msgId = UUID.randomUUID()`。
2. 确定延时级别：`message.getDelayLevel()` 优先；否则 `DelayLevel.closestAbove(delayTimeMillis)`。
3. 计算 `deliverAt = now + level.toMillis()`。
4. `converter.toStreamFields(message)` 后追加 `targetTopic` 与 `deliverAt` 字段。
5. **两个 Redis 操作**：
   - `zset.add(deliverAt, msgId)` → `streammq:{ns}:delay:{level}` ZSet [ZADD]。
   - `payloadMap.putAll(fields)` → `streammq:{ns}:delay:payload:{msgId}` Hash [HSET]。
6. 返回合成的 `SendResult`（不写入目标 Stream，由 `DelayMessageScheduler` 延迟转投）。

### 4.7 MessageConverter 与 MessageSerializer 调用点

| 调用点 | 方法 | 用途 |
|--------|------|------|
| `RedissonStreamProducer.syncSend` | `converter.toStreamFields(message)` | Message → Stream fields |
| `RedissonStreamProducer.syncSendBatch` | 同上 | 批量转换 |
| `RedissonStreamProducer.sendDelayMessage` | 同上 | 延时 payload 转换 |
| `DefaultRetryAndDlqHandler.handleReconsumeLater` | `converter.toStreamFields(message)` | 重试 payload 转换 |
| `DefaultRetryAndDlqHandler.routeToDlq` | 同上 | DLQ 转换 |
| `RedissonStreamListener.toMessage` | `converter.fromStreamFields(fields, bodyType)` | Stream fields → Message |
| `TransactionScanner.readHalfMessage` | 同上 | 半消息回查时反序列化 |
| `DefaultMessageConverter.toStreamFields` | `serializer.serialize(body, bodyType)` | body → byte[] |
| `DefaultMessageConverter.fromStreamFields` | `serializer.deserialize(bytes, targetType)` | byte[] → body |

### 4.8 Redis Stream XADD 具体调用

| 场景 | Redisson API | Redis 命令 |
|------|--------------|-----------|
| 同步发送 | `RStream.addAsync(StreamAddArgs.entries(fields)).get(timeout, MS)` | `XADD streammq:{ns}:msg:{topic} * field1 v1 field2 v2 ...` |
| 批量发送 | `RBatch.getStream(key).addAsync(args)` + `batch.execute()` | Pipeline 内多条 `XADD` |
| 延时转投 | `RBatch.getStream(targetStreamKey).addAsync(args)` | `XADD` 到目标 Stream |
| 重试转投 | `RStream.add(StreamAddArgs.entries(fields))` | `XADD` 到目标 Stream |
| DLQ 路由 | `RStream.add(StreamAddArgs.entries(fields))` | `XADD` 到 DLQ Stream |
| 半消息注册 | `RStream.add(StreamAddArgs.entries(fields))` | `XADD` 到 half Stream |
| 事务 COMMIT | `RStream.add(StreamAddArgs.entries(fields))` | `XADD` 半消息到目标 Stream |

`maxLen > 0` 时附加 `trimNonStrict().maxLen(maxLen).noLimit()`，对应 Redis 近似切剪 `XADD ... MAXLEN ~ N`。

---

## 5. 单独的消费逻辑

### 5.1 消费者注册流程

#### 5.1.1 注解扫描

**`StreamMQListenerRegistrar.afterSingletonsInstantiated`**（由 Spring 在所有单例 Bean 初始化完成后调用）：

1. `registerStreamMQListeners()`：
   - `applicationContext.getBeansWithAnnotation(StreamMQConsumer.class)` 获取所有标注 Bean。
   - 对每个 Bean，`AnnotationUtils.findAnnotation` 获取注解。
   - **属性解析**：`resolveStreamMQListener(annotation)` 通过 `SpringAnnotationAttributeResolver` 解析 `${}` 占位符与 `#{}` SpEL 表达式，构造动态代理注解覆盖 `topic` / `consumerGroup` / `namespace` / `selectorExpression` / `dlqConsumerGroup` / `dlqOriginalGroup`。
   - `enable == false` → 跳过。
   - **分支**：
     - `messageModel == ORDERLY` 且实现 `StreamMessageOrderlyConsumer` → `listenerContainer.registerOrderlyConsumer`。
     - 实现 `StreamMessageConcurrentlyConsumer` → `listenerContainer.registerConsumer`（含 DLQ 场景）。
     - 其他 → 警告并忽略。

2. `registerTransactionListeners()`：
   - 获取 `TransactionScanner` Bean（不存在则跳过）。
   - 扫描 `@StreamMQTransactionConsumer` 标注的 Bean（需实现 `TransactionChecker`）。
   - `transactionScanner.registerChecker(txGroup, checker)`。

3. `registerRetryTargetsIfPossible()`：
   - 获取 `RetryScheduler` Bean（不存在则跳过）。
   - `listenerContainer.registerRetryTargets(scheduler)`：遍历所有非 DLQ Listener，调用 `scheduler.registerRetryTarget(topic, group, maxReconsumeTimes)`。

#### 5.1.2 容器注册（`DefaultStreamMQListenerContainer`）

**`registerConsumer`**（line 185-237）：

1. `checkBeforeStart()`：状态必须为 `INIT`，否则抛异常。
2. `BodyTypeResolver.resolve(consumer)`：反射解析泛型 T。
3. DLQ 模式判断：`dlqConsumerGroup` 非空 → `dlqMode = true`，`effectiveGroup = dlqConsumerGroup`，`dlqOriginalGroup` 取 `dlqOriginalGroup` 或回退到 `consumerGroup`。
4. 构造 `ListenerRegistration`（`type = AUTO_ACK`），`resolveNamespace(defaultNamespace)`。
5. `registrations.put(reg.key(), reg)`（key 为 `topic:group`，DLQ 模式加 `dlq:` 前缀）。

**`registerOrderlyConsumer`**（line 240-280）：

- 额外步骤：`shardLockManager.createShardLocks(defaultNamespace, topic, group, namespace, shardCount)` 创建 `RLock[]`。
- `type = ORDERLY`。

### 5.2 消费循环（`consumeLoop`）

**`DefaultStreamMQListenerContainer#consumeLoop`**（line 385-430）：

1. `createConsumerFor(reg)`：构造 `ListenerConfig`，调用 `consumerFactory.createListener(config)` 创建 `RedissonStreamListener`。失败则记录错误并 return（不消费）。
2. **主循环** `while (state.get() == ContainerState.RUNNING)`：
   - `paused == true` → `sleepQuietly(PAUSED_SLEEP_MILLIS=100ms)`，continue。
   - `listener.pullBlock(reg.getPullBatchSize(), Duration.ofMillis(reg.getPullBlockTimeoutMillis()))`：
     - 默认 `batchSize = 32`，`blockTimeout = 1s`。
     - 内部调用 `RStream.readGroup(group, consumerName, args)`，args 含 `neverDelivered().count(n).timeout(t)`（对应 `XREADGROUP GROUP group consumer COUNT n BLOCK ms >`）。
   - 空批次 → `sleepQuietly(pullIntervalMillis)`（默认 0，不睡眠）。
   - 非空 → 对每条消息 `handleMessage(message, reg, listener)`。
   - `StreamMQBrokerException` → 记录 + `sleepQuietly(BROKER_ERROR_BACKOFF_MILLIS=500ms)`。
   - 其他 `RuntimeException` → 记录 + 退避。
3. 状态变为非 RUNNING → 退出。

### 5.3 Redis Stream XREADGROUP 调用

**`RedissonStreamListener#doRead`**（line 228-251）：

```java
RStream<String, String> stream = getStream();
StreamReadGroupArgs args;
if (timeout != null && !timeout.isZero() && !timeout.isNegative()) {
    args = StreamReadGroupArgs.neverDelivered().count(batchSize).timeout(timeout);
} else {
    args = StreamReadGroupArgs.neverDelivered().count(batchSize);
}
Map<StreamMessageId, Map<String, String>> result = stream.readGroup(group, consumerName, args);
```

- `neverDelivered()` 对应 `>` 标识符（仅读取未投递给当前消费者的消息）。
- `getStream()` 在 DLQ 模式下返回 `streammq:{ns}:dlq:{topic}:{dlqOriginalGroup}`，否则返回 `streammq:{ns}:msg:{topic}`。
- `ensureGroup()`：首次调用时 `RStream.createGroup(StreamCreateGroupArgs.name(group).makeStream().id(new StreamMessageId(0, 0)))`；捕获 `BUSYGROUP` 错误视为已存在。

### 5.4 消息反序列化（`fromStreamFields`）

**`RedissonStreamListener#toMessage`**（line 253-277）：

1. 读取 `fields.get("bodyType")`，`Class.forName` 加载。失败 → 回退到 `targetBodyType`，再回退到 `String.class`。
2. `converter.fromStreamFields(fields, bodyType)`：
   - `DefaultMessageConverter.fromStreamFields`（line 128-188）：
     - `bodyStr = fields.get("body")`。
     - `deserializeBody(bodyStr, bodyTypeField, targetType)`：
       - `bodyTypeField` 非空（SDK 路径）→ `Base64.getDecoder().decode(bodyStr)` → `serializer.deserialize(bytes, targetType)`。
       - `bodyTypeField` 为空（跨平台路径）→ `targetType == String` 直接返回字符串；否则将原始字符串 UTF-8 字节交给序列化器。
     - 回填 `tag` / `keys` / `shardingKey` / `bornHost` / `txId`。
     - 解析 `props` JSON 写入 `userProperties`。
     - 解析 `bornTs` / `retryTimes`。
3. `DefaultMessageConverter.applyTopic(message, topic)`：从 Stream Key 解析的 topic 回填。
4. `DefaultMessageConverter.applyMessageId(message, streamId.toString())`：回填 Stream Entry ID。

### 5.5 拦截器链执行（`DefaultConsumerInterceptorChain`）

| 时机 | 方法 | 行为 |
|------|------|------|
| 消费前 | `applyBefore(message)` | 按 `order()` 升序遍历 `beforeConsume`；任一返回 false 视为消费中止（直接 SUCCESS）；异常 → `notifyException(BEFORE)` 但**不中止** |
| 消费后 | `applyAfter(message, action)` | 按 `order()` 升序遍历 `afterConsume`；异常 → `notifyException(AFTER)` |
| 异常时 | `notifyException(message, ex, timing)` | 按 `order()` 升序遍历 `onException`；拦截器自身异常被忽略 |

### 5.6 ACK / 重试 / DLQ 路由（`DefaultRetryAndDlqHandler`）

见 §3.3。关键点：

- **`SUCCESS`**：`listener.ack(messageId)` → `RStream.ack(group, streamId)` [XACK]。
- **`RECONSUME_LATER`（普通消费者）**：
  - `retryPolicy.nextRetryDelay(retryCount, message)` 返回 null → 路由到 DLQ Stream 后 ACK。
  - 返回非 null → 写入 `retry ZSet` + `retry payload Hash` 后 ACK 原消息。
- **`RECONSUME_LATER`（DLQ 消费者）**：直接 ACK 丢弃，避免死信死循环。
- **顺序消费 `SUSPEND_CURRENT_QUEUE_A_MOMENT`**：由容器直接处理，不进入 `RetryAndDlqHandler`，消息留在 PEL 等待下次 `XREADGROUP` 或 `XAUTOCLAIM`。

### 5.7 顺序消费分片锁（`RedissonOrderlyShardLockManager`）

**`createShardLocks`**（line 45-56）：

- `shardCount <= 0` → 返回 null。
- 否则构造 `RLock[shardCount]`，每个 lock 对应 Key `streammq:{ns}:shardlock:{topic}:{group}:{i}`。

**`consumeWithShardLock`**（line 72-91）：

1. 无分片锁 → 直接调用 `orderly.onMessage`。
2. `shardIndex = Math.abs(shardingKey.hashCode()) % shardCount`（`shardingKey == null` 视为 `""`）。
3. `lock.lock(consumeTimeoutMillis, MS)`：获取分布式锁，租约时间 = 消费超时。
4. `orderly.onMessage(message, ctx)`：在锁内执行业务。
5. `finally`：`lock.isHeldByCurrentThread()` 为 true 时 `unlock`。

> 这保证同一 `shardingKey` 的消息串行消费，不同 shard 并行。

### 5.8 手动 ACK 模式（`Acknowledgment` / `ConsumeContext`）

由 `AcknowledgeMode.MANUAL` 配置驱动（注解 `acknowledgeMode = MANUAL`），不区分 Listener 类型，仍走 `AUTO_ACK` 或 `ORDERLY` 分支。

**`DefaultConsumeContextConsume`**：

- 持有 `AtomicBoolean acked`，初始 false。
- `acknowledge()` 返回新的 `DefaultAcknowledgment(message, listener, this)`。
- `markAcked()` / `isAcked()`：标记 / 查询 ACK 状态。

**`DefaultAcknowledgment`**：

- `acknowledge()`：`listener.ack(messageId)` + `context.markAcked()`。
- `nack()`：不 ACK，消息留 PEL 等待 `XAUTOCLAIM` 补偿（仅记录日志）。
- `defer(Duration)`：不 ACK，由 `RetryScheduler` 调度重投（仅记录日志）。

**容器处理**（`handleMessage` line 460-501）：

- `MANUAL` 模式下忽略 `onMessage` 返回值，检查 `ctx.isAcked()`：
  - true → `handleAction(SUCCESS)`。
  - false → 记录日志 + `handleAction(RECONSUME_LATER)`。

---

## 6. 生命周期管理

### 6.1 创建阶段（Spring Boot 自动装配顺序）

由 `StreamMQAutoConfiguration` 通过 `@Import` 触发，Spring 容器按依赖关系装配（核心顺序）：

| 顺序 | Bean | 来源配置 | 依赖 |
|------|------|----------|------|
| 1 | `RedissonClient`（兜底） | `StreamMQCoreAutoConfiguration#redissonClient` | 仅当用户未注册时 |
| 2 | `MessageSerializer` | `StreamMQCoreAutoConfiguration#streamMQMessageSerializer` | `StreamMQProperties` |
| 3 | `MessageConverter` | `StreamMQCoreAutoConfiguration#streamMQMessageConverter` | `MessageSerializer` |
| 4 | `RetryPolicy` | `StreamMQCoreAutoConfiguration#streamMQRetryPolicy` | `StreamMQProperties`（反射实例化） |
| 5 | `StreamMessageProducerFactory` | `StreamMQCoreAutoConfiguration#streamMQProducerFactory` | `RedissonClient` + `MessageConverter` |
| 6 | `StreamMQListenerFactory` | `StreamMQCoreAutoConfiguration#streamMQListenerFactory` | `RedissonClient` + `MessageConverter` |
| 7 | `StreamMessageTemplate` | `StreamMQCoreAutoConfiguration#streamMQTemplate` | `ProducerFactory` + `Converter` + `Properties` |
| 8 | `StreamMessageService` | `StreamMQCoreAutoConfiguration#streamMQService` | `StreamMessageTemplate` |
| 9 | `RetryScheduler` | `StreamMQSchedulerAutoConfiguration#streamMQRetryScheduler` | `RedissonClient` + `Properties` |
| 10 | `DelayMessageScheduler` | `StreamMQSchedulerAutoConfiguration#streamMQDelayMessageScheduler` | 同上 |
| 11 | `TransactionScanner` | `StreamMQSchedulerAutoConfiguration#streamMQTransactionScanner` | `RedissonClient` + `Converter` + `Properties` |
| 12 | `StreamMQSchedulerLifecycle` | `StreamMQSchedulerAutoConfiguration#streamMQSchedulerLifecycle` | 三个调度器（`ObjectProvider` 可选注入） |
| 13 | `DefaultStreamMQListenerContainer` | `StreamMQListenerContainerAutoConfiguration#streamMQListenerContainer` | `RedissonClient` + `ListenerFactory` + `Converter` + `RetryPolicy` + `Properties` |
| 14 | `StreamMQListenerRegistrar` | `StreamMQListenerContainerAutoConfiguration#streamMQListenerRegistrar` | `DefaultStreamMQListenerContainer` |
| 15 | `StreamMQListenerContainerLifecycle` | `StreamMQListenerContainerAutoConfiguration#streamMQListenerContainerLifecycle` | `DefaultStreamMQListenerContainer` |
| 16 | `StreamMQHealthIndicator` | `StreamMQHealthAutoConfiguration`（条件：Actuator 在 classpath） | `RedissonClient` + 可选 `Container` |
| 17 | `StreamMQMetrics` | `StreamMQMetricsAutoConfiguration`（条件：`MeterRegistry` 在 classpath） | `MeterRegistry` |

> **注解扫描时机**：`StreamMQListenerRegistrar` 实现 `SmartInitializingSingleton`，其 `afterSingletonsInstantiated` 在所有单例 Bean（含 `DefaultStreamMQListenerContainer` 自身）创建完成后由 Spring 调用，因此注册阶段发生在所有 Bean 实例化完毕、但 `SmartLifecycle.start()` 尚未触发之前。

### 6.2 启动阶段（`SmartLifecycle.start()` 顺序）

Spring 按 `getPhase()` 降序启动 `SmartLifecycle` Bean：

| Phase | Bean | 启动动作 |
|-------|------|----------|
| `Integer.MAX_VALUE - 100` | `StreamMQSchedulerLifecycle` | 按列表顺序调用 `scheduler.start()`：`RetryScheduler` → `DelayMessageScheduler` → `TransactionScanner` |
| `Integer.MAX_VALUE - 200` | `StreamMQListenerContainerLifecycle` | 调用 `listenerContainer.start()` |

**调度器启动**（`StreamMQSchedulerLifecycle#start`）：

- 按构造时传入的列表顺序逐个 `scheduler.start()`。
- 单个启动失败不影响其他，记录警告后继续。
- 即使部分失败也设 `running = true`。

**各调度器 `start`**：

- `RetryScheduler.start`：`scanExecutor.scheduleAtFixedRate(this::scanAllTargets, 0, scanIntervalMs, MS)`（默认 1000ms 间隔）。
- `DelayMessageScheduler.start`：`scheduleAtFixedRate(this::scanAllLevels, 0, scanIntervalMs, MS)`。
- `TransactionScanner.start`：`scheduleAtFixedRate(this::scanAllGroups, 0, checkIntervalMs, MS)`（默认 60000ms 间隔）。

**容器启动**（`DefaultStreamMQListenerContainer#start`）：

1. `state.compareAndSet(INIT, STARTING)`，失败则抛 `IllegalStateException`。
2. `state.set(RUNNING)`。
3. `doStartListeners()`：
   - 遍历 `registrations`，对每个 `ListenerRegistration`：
     - `consumeExecutor.submit(() -> consumeLoop(reg))` 提交到虚拟线程池。
     - `consumeFutures.put(reg.key(), future)`。
4. 记录日志。

> **关键设计**：调度器先启动，确保 Listener 容器开始消费时重试 / 延时 / 事务回查机制已就绪。

### 6.3 运行阶段

#### 6.3.1 消费循环运行机制

- 每个 `ListenerRegistration` 对应一个虚拟线程，独立运行 `consumeLoop`。
- 单次 `pullBlock` 超时 1s，空批次按 `pullIntervalMillis` 间隔继续。
- 异常时退避 500ms。
- 暂停（`pause()`）后所有循环 sleep 100ms 让出 CPU，但不退出。

#### 6.3.2 调度器扫描循环

**`DelayMessageScheduler.scanExpired(level)`**：

1. `zset.valueRange(0, true, now, true, 0, batchSize - 1)` [ZRANGEBYSCORE]。
2. 对每个 `msgId`：`zset.remove(msgId)` [ZREM]，返回 false 表示被其他实例获取，跳过。
3. 读取 `payloadHash.readAllMap()`，移除 `targetTopic` / `deliverAt` 元字段。
4. `RBatch`：`getStream(targetStreamKey).addAsync(args)` + `getMap(payloadKey).deleteAsync()`，达到 batchSize 阈值时 `execute()`。
5. 异常 → `zset.add(now, msgId)` 重新入队，避免丢失。

**`RetryScheduler.scanRetryEntries(target)`**：

1. `zset.valueRange(0, true, now, true, 0, batchSize - 1)`。
2. `zset.remove(msgId)` 获取所有权。
3. `transferOne`：
   - 读取 payload，解析 `retryCount`，移除元字段，递增 `retryTimes`。
   - `retryCount >= maxReconsumeTimes` → XADD 到 DLQ Stream + 字段 `dlqReason=maxRetry` + `originalRetryCount`。
   - 否则 → XADD 到目标 Stream。
   - 删除 payload Hash。
4. 异常 → 重新入 ZSet。

**`TransactionScanner.scanTimeoutHalf(txGroup)`**：

1. `zset.valueRange(0, true, now, true, 0, batchSize - 1)`。
2. 对每个 `txId` 调用 `triggerCheck`：
   - 读取 `txstate` Hash 当前状态：已终态 → 清理；异常状态 → 强制 ROLLBACK。
   - 无 checker → ROLLBACK。
   - 读取半消息，调用 `checker.check(halfMessage, ctx)`：
     - `COMMIT_MESSAGE` → `markCommit`（XADD 到目标 Stream + XDEL 半消息）。
     - `ROLLBACK_MESSAGE` → `markRollback`（XDEL 半消息）。
     - `UNKNOW` → 检查 `checkCount`，超过 `maxCheckTimes` 则强制 ROLLBACK；否则递增计数 + 重新调度。

### 6.4 停止阶段（`SmartLifecycle.stop()` 顺序）

Spring 按 `getPhase()` 升序停止（与启动相反）：

| Phase | Bean | 停止动作 |
|-------|------|----------|
| `Integer.MAX_VALUE - 200` | `StreamMQListenerContainerLifecycle` | `listenerContainer.stop()` |
| `Integer.MAX_VALUE - 100` | `StreamMQSchedulerLifecycle` | 反向遍历列表 `scheduler.stop()` |

**容器 stop**（`DefaultStreamMQListenerContainer#stop`，line 324-347）：

1. 状态非 `STOPPED` / `INIT` 才执行。
2. `state.set(STOPPING)`。
3. 遍历 `consumeFutures`，`future.cancel(true)` 中断消费虚拟线程。
4. `consumeFutures.clear()`。
5. `consumerFactory.close()`：关闭所有 `RedissonStreamListener`。
6. `consumeExecutor.shutdown()` + `awaitTermination(5s)`，超时则 `shutdownNow()`。
7. `state.set(STOPPED)`。

**调度器 stop**（`StreamMQSchedulerLifecycle#stop`）：

- 反向遍历（`for (i = size-1; i >= 0; i--)`）调用 `scheduler.stop()`。
- 各调度器 `stop`：`scanExecutor.shutdown()` + `awaitTermination(5s)`。

### 6.5 退出阶段

- `RedissonStreamProducer.close()`：`asyncExecutor.shutdown()` + `awaitTermination(5s)`。
- `RedissonStreamProducerFactory.close()`：遍历所有 Producer `close()`，清空缓存。
- `RedissonStreamListenerFactory.close()`：遍历所有 Listener `close()`。
- `RedissonClient`：由 `@Bean(destroyMethod = "shutdown")` 在 Spring 容器关闭时销毁。

---

## 7. 关键时序图

### 7.1 发送时序：Service → Template → Interceptors → Producer → Redisson → Redis

以同步发送为例：

1. 用户调用 `streamMessageService.send("orders", order)`。
2. `DefaultStreamMessageService.send` 调用 `MessageBuilder.withTopic("orders").body(order).build()` 构造 `Message`。
3. 调用 `template.syncSend(message)`，进入 `DefaultStreamMessageTemplate.syncSend`。
4. `injectProducerMdc(message)` 注入 MDC 上下文（topic / producerGroup / msgId / shardingKey）。
5. `applyInterceptorsBefore(message)`：按 `order()` 升序遍历 `ProducerInterceptor.beforeSend`。
6. 任一拦截器返回 false → 构造 `SEND_FAILED` 结果，跳到步骤 11。
7. `resolveProducer(topic)` → `producerFactory.createProducer(defaultConfig)` → 命中缓存或新建 `RedissonStreamProducer`。
8. 进入重试循环（默认 2 次重试）：
   - 9. 调用 `producer.syncSend(message, timeoutMillis)`。
   - 10. `RedissonStreamProducer.syncSend`：
     - 10.1 `ensureOpen()`。
     - 10.2 非延时消息：`converter.toStreamFields(message)`（内部 `serializer.serialize` + Base64）。
     - 10.3 `StreamMQKeys.topicStream(namespace, topic)` 拼 Key `streammq:{ns}:msg:orders`。
     - 10.4 `RStream.addAsync(StreamAddArgs.entries(fields)).get(3000, MS)` → Redis 执行 `XADD`。
     - 10.5 `message.setMessageId(new MessageId(streamId))`。
     - 10.6 返回 `SendResult`。
   - 11. 成功 → `applyInterceptorsAfter(message, result)` → 返回。
   - 12. 抛 `StreamMQException` → `notifyProducerException(EXECUTING)` → 继续下一次尝试。
13. 全部重试失败 → `applyInterceptorsAfter(failedResult)` → 抛异常。
14. `clearProducerMdc()` 清理 MDC。
15. 结果返回给用户。

### 7.2 消费时序：Redis → Redisson → Listener → Container → Interceptors → Consumer.onMessage → ACK

1. `DefaultStreamMQListenerContainer.consumeLoop(reg)` 在虚拟线程中运行。
2. `createConsumerFor(reg)` 通过 `consumerFactory.createListener(config)` 创建 `RedissonStreamListener`。
3. 进入 while 循环（`state == RUNNING`）：
   - 4. 检查 `paused`，true 则 sleep 100ms 后 continue。
   - 5. 调用 `listener.pullBlock(batchSize=32, Duration.ofMillis(1000))`。
   - 6. `RedissonStreamListener.pullBlock`：
     - 6.1 `ensureOpen()` + `validateBatchSize` + `ensureGroup()`（首次 `XGROUP CREATE`，`BUSYGROUP` 容错）。
     - 6.2 `RStream.readGroup(group, consumerName, StreamReadGroupArgs.neverDelivered().count(32).timeout(1s))` → Redis 执行 `XREADGROUP GROUP group consumer COUNT 32 BLOCK 1000 >`。
     - 6.3 对每个 entry：`toMessage(streamId, fields)` → `converter.fromStreamFields(fields, bodyType)` 反序列化 body，回填 topic / messageId。
     - 6.4 返回 `List<Message>`。
   - 7. 空批次 → sleep `pullIntervalMillis`，回到步骤 3。
   - 8. 非空 → 对每条消息调用 `handleMessage(message, reg, listener)`：
     - 9. `new DefaultConsumeContextConsume(message, reg, listener)`。
     - 10. `ConsumerMdcTrace.inject(message, reg)`。
     - 11. `interceptorChain.applyBefore(message)`：按序执行 `ConsumerInterceptor.beforeConsume`。
     - 12. 任一返回 false → `retryDlqHandler.handleAction(SUCCESS)` → 跳到步骤 18。
     - 13. 顺序消费：`shardLockManager.consumeWithShardLock`：
       - 13.1 `shardIndex = abs(shardingKey.hashCode()) % shardCount`。
       - 13.2 `lock.lock(consumeTimeoutMillis, MS)`。
       - 13.3 `orderly.onMessage(message, ctx)` 返回 `OrderlyAction`。
       - 13.4 `lock.unlock()`。
     - 14. 并发消费：`consumer.onMessage(message, ctx)` 返回 `ConsumeAction`。
     - 15. 异常 → `interceptorChain.notifyException(EXECUTING)` → `handleAction(RECONSUME_LATER)`。
     - 16. 根据 `ackMode` 与返回值决定 `finalAction`，调用 `retryDlqHandler.handleAction`：
       - 16.1 `SUCCESS` → `listener.ack(messageId)` → Redis `XACK`。
       - 16.2 `RECONSUME_LATER`：
         - 计算 `delay = retryPolicy.nextRetryDelay(retryCount, message)`。
         - `delay == null` → `routeToDlq`（Redis `XADD` 到 DLQ Stream） + `listener.ack`。
         - `delay != null` → 写入 retry ZSet（`ZADD`）+ payload Hash（`HSET`）+ `listener.ack`。
     - 17. MANUAL 模式：忽略返回值，检查 `ctx.isAcked()`。
     - 18. `interceptorChain.applyAfter(message, finalAction)`。
     - 19. `ConsumerMdcTrace.clear()`。
   - 20. 处理完本批次 → 回到步骤 3。
4. `state != RUNNING` → 退出循环，记录日志。

### 7.3 事务消息时序：半消息发送 → 本地事务 → commit/rollback → 回查

> **当前实现说明**：`DefaultStreamMessageTemplate.executeInTransaction`（line 269-328）采用**简化实现**：直接发送业务消息到目标 Stream，再执行本地事务回调，按返回状态决定结果。完整的半消息 + 回查机制由 `TransactionScanner` 提供（`registerHalfMessage` / `markCommit` / `markRollback`），但当前 Template 未调用这些方法。以下描述**完整设计时序**（含 `TransactionScanner` 提供的能力）。

#### 7.3.1 简化实现（当前 Template 行为）

1. 用户调用 `streamMessageService.sendTransaction(topic, body, callback)`。
2. `DefaultStreamMessageService.sendTransaction` → `template.executeInTransaction(message, callback)`。
3. `executeInTransaction`：
   - 3.1 校验 `transactionGroup` 非空。
   - 3.2 生成 `transactionId = UUID`，`message.setTransactionId(transactionId)`。
   - 3.3 `syncSend(message)` 直接发送业务消息到目标 Stream。
   - 3.4 失败 → 抛 `TransactionException`。
   - 3.5 构造 `TransactionContext`。
   - 3.6 调用 `callback.execute(message, ctx)` 执行本地事务，返回 `LocalTransactionState`。
   - 3.7 异常 → 抛 `TransactionException`。
   - 3.8 根据状态：
     - `COMMIT_MESSAGE` → 返回 `sendResult`（消息已发送）。
     - `ROLLBACK_MESSAGE` → 返回 `SEND_FAILED`（消息可能需人工清理）。
     - `UNKNOW` → 返回 `sendResult`（等待回查）。

#### 7.3.2 完整半消息机制（`TransactionScanner` 提供，待 Template 接入）

**发送阶段**：

1. Template 调用 `transactionScanner.registerHalfMessage(txId, txGroup, targetTopic, fields)`：
   - 1.1 `XADD` 半消息到 `streammq:{ns}:half:{txGroup}` Stream，得到 `halfId`。
   - 1.2 `HSET` 事务状态：`{txId}=PREPARE` + `{txId}.target=targetTopic` + `{txId}.halfId=halfId`。
   - 1.3 `ZADD` 回查 ZSet：`streammq:{ns}:txcheck:{txGroup}` score=`now + checkInterval`。
2. 执行本地事务 `callback.execute`。
3. `COMMIT_MESSAGE` → `transactionScanner.markCommit(txId, txGroup)`：
   - 3.1 读取 half Stream entry。
   - 3.2 `XADD` 到目标 Stream `streammq:{ns}:msg:{targetTopic}`。
   - 3.3 `XDEL` half Stream 中的半消息。
   - 3.4 `HSET {txId}=COMMIT` + `ZREM` 回查 ZSet + 清理 `.target` / `.halfId` 字段。
4. `ROLLBACK_MESSAGE` → `transactionScanner.markRollback(txId, txGroup)`：
   - 4.1 `XDEL` half Stream 中的半消息。
   - 4.2 `HSET {txId}=ROLLBACK` + `ZREM` + 清理。
5. `UNKNOW` → 不调用 `markCommit` / `markRollback`，等待回查调度器处理。

**回查阶段**（`TransactionScanner.scanTimeoutHalf`，周期 60s）：

1. `ZRANGEBYSCORE` 回查 ZSet，获取超时 `txId` 列表。
2. 对每个 `txId` 调用 `triggerCheck`：
   - 2.1 读取 `txstate` Hash 当前状态：
     - 已 `COMMIT` / `ROLLBACK` → 清理 ZSet。
     - 非 `PREPARE` / `UNKNOWN` → 强制 `markRollback`。
   - 2.2 无 `checker` → `markRollback`。
   - 2.3 `XRANGE` 读取半消息 → `messageConverter.fromStreamFields` 还原 `Message`。
   - 2.4 调用 `checker.check(halfMessage, ctx)`：
     - 异常 → 视为 `UNKNOW`。
   - 2.5 根据返回状态：
     - `COMMIT_MESSAGE` → `markCommit`。
     - `ROLLBACK_MESSAGE` → `markRollback`。
     - `UNKNOW`：
       - `checkCount >= maxCheckTimes`（默认 15） → 强制 `markRollback`。
       - 否则 → `HSET {txId}=UNKNOWN` + 递增 counter + `ZADD` 重新调度（score=`now + checkInterval`）。

### 7.4 延时消息时序：发送 → ZSet 存储 → 调度器扫描 → 转投目标 Stream → 消费

#### 7.4.1 发送阶段

1. 用户调用 `streamMessageService.sendDelay("notifications", body, DelayLevel.LEVEL_5)`。
2. `DefaultStreamMessageService.sendDelay` → `MessageBuilder.withTopic("notifications").body(body).delayLevel(LEVEL_5).build()` → `template.syncSend(message)`。
3. `DefaultStreamMessageTemplate.syncSend` → `producer.syncSend(message, timeout)`。
4. `RedissonStreamProducer.syncSend`：
   - 4.1 `message.isDelayMessage()` 为 true（`delayLevel != null`）→ 进入 `sendDelayMessage`。
   - 4.2 生成 `msgId = UUID`。
   - 4.3 `level = message.getDelayLevel()`（LEVEL_5）。
   - 4.4 `deliverAt = now + level.toMillis()`。
   - 4.5 `converter.toStreamFields(message)` + 追加 `targetTopic=notifications` + `deliverAt`。
   - 4.6 `ZADD streammq:{ns}:delay:LEVEL_5 {deliverAt} {msgId}`。
   - 4.7 `HSET streammq:{ns}:delay:payload:{msgId} field1 v1 field2 v2 ...`。
   - 4.8 返回合成的 `SendResult`（消息尚未进入目标 Stream）。

#### 7.4.2 调度器扫描阶段（`DelayMessageScheduler`，默认 1s 间隔）

1. `scheduleAtFixedRate(scanAllLevels, 0, 1000, MS)`。
2. `scanAllLevels`：遍历所有 `DelayLevel`，调用 `scanExpired(level)`。
3. `scanExpired(LEVEL_5)`：
   - 3.1 `ZRANGEBYSCORE streammq:{ns}:delay:LEVEL_5 0 now LIMIT 0 99`（默认 batchSize=100）。
   - 3.2 对每个 `msgId`：
     - 3.3 `ZREM` 原子获取（返回 false 表示被其他实例获取，跳过）。
     - 3.4 `HREADALL streammq:{ns}:delay:payload:{msgId}` 读取 payload。
     - 3.5 校验 `targetTopic` 非空。
     - 3.6 移除 `targetTopic` / `deliverAt` 元字段。
     - 3.7 `RBatch`：
       - `XADD streammq:{ns}:msg:notifications * fields...`（转投到目标 Stream）。
       - `DEL streammq:{ns}:delay:payload:{msgId}`（清理 payload）。
     - 3.8 达到 batchSize 阈值 → `batch.execute()`。
     - 3.9 异常 → `ZADD streammq:{ns}:delay:LEVEL_5 {now} {msgId}` 重新入队。

#### 7.4.3 消费阶段

转投到目标 Stream 后，消息进入正常消费流程（见 §7.2），由 `DefaultStreamMQListenerContainer` 的消费循环拉取并分发给 `StreamMessageConcurrentlyConsumer.onMessage`。

> **关键设计**：延时消息与重试消息共用 `streammq:{ns}:delay:payload:{msgId}` 这个 payload Hash Key 命名空间（`RetryScheduler.transferOne` 也调用 `StreamMQKeys.delayPayloadHash`），但 ZSet Key 不同（`delay:{level}` vs `retry:{topic}:{group}`），因此不会冲突。

---

## 附录：关键源码文件索引

| 组件 | 文件路径 |
|------|----------|
| `StreamMessageService` | `streammq-core/src/main/java/io/github/streammq/core/service/StreamMessageService.java` |
| `DefaultStreamMessageService` | `streammq-core/src/main/java/io/github/streammq/core/service/DefaultStreamMessageService.java` |
| `StreamMessageTemplate` | `streammq-core/src/main/java/io/github/streammq/core/template/StreamMessageTemplate.java` |
| `DefaultStreamMessageTemplate` | `streammq-redisson-adapter/src/main/java/io/github/streammq/adapter/redisson/template/DefaultStreamMessageTemplate.java` |
| `RedissonStreamProducer` | `streammq-redisson-adapter/src/main/java/io/github/streammq/adapter/redisson/producer/RedissonStreamProducer.java` |
| `RedissonStreamProducerFactory` | `streammq-redisson-adapter/src/main/java/io/github/streammq/adapter/redisson/producer/RedissonStreamProducerFactory.java` |
| `RedissonStreamListener` | `streammq-redisson-adapter/src/main/java/io/github/streammq/adapter/redisson/listener/RedissonStreamListener.java` |
| `RedissonStreamListenerFactory` | `streammq-redisson-adapter/src/main/java/io/github/streammq/adapter/redisson/listener/RedissonStreamListenerFactory.java` |
| `DefaultStreamMQListenerContainer` | `streammq-redisson-adapter/src/main/java/io/github/streammq/adapter/redisson/container/DefaultStreamMQListenerContainer.java` |
| `DefaultRetryAndDlqHandler` | `streammq-redisson-adapter/src/main/java/io/github/streammq/adapter/redisson/container/DefaultRetryAndDlqHandler.java` |
| `DefaultConsumerInterceptorChain` | `streammq-redisson-adapter/src/main/java/io/github/streammq/adapter/redisson/container/DefaultConsumerInterceptorChain.java` |
| `RedissonOrderlyShardLockManager` | `streammq-redisson-adapter/src/main/java/io/github/streammq/adapter/redisson/container/RedissonOrderlyShardLockManager.java` |
| `DefaultAcknowledgment` | `streammq-redisson-adapter/src/main/java/io/github/streammq/adapter/redisson/container/DefaultAcknowledgment.java` |
| `DefaultConsumeContextConsume` | `streammq-redisson-adapter/src/main/java/io/github/streammq/adapter/redisson/container/DefaultConsumeContextConsume.java` |
| `ConsumerMdcTrace` | `streammq-redisson-adapter/src/main/java/io/github/streammq/adapter/redisson/container/ConsumerMdcTrace.java` |
| `DelayMessageScheduler` | `streammq-redisson-adapter/src/main/java/io/github/streammq/adapter/redisson/scheduler/DelayMessageScheduler.java` |
| `RetryScheduler` | `streammq-redisson-adapter/src/main/java/io/github/streammq/adapter/redisson/scheduler/RetryScheduler.java` |
| `TransactionScanner` | `streammq-redisson-adapter/src/main/java/io/github/streammq/adapter/redisson/scheduler/TransactionScanner.java` |
| `DefaultMessageConverter` | `streammq-redisson-adapter/src/main/java/io/github/streammq/adapter/redisson/converter/DefaultMessageConverter.java` |
| `StreamMQKeys` | `streammq-redisson-adapter/src/main/java/io/github/streammq/adapter/redisson/support/StreamMQKeys.java` |
| `StreamMQAutoConfiguration` | `streammq-spring-boot-starter/src/main/java/io/github/streammq/spring/boot/autoconfigure/StreamMQAutoConfiguration.java` |
| `StreamMQCoreAutoConfiguration` | `streammq-spring-boot-starter/src/main/java/io/github/streammq/spring/boot/autoconfigure/StreamMQCoreAutoConfiguration.java` |
| `StreamMQSchedulerAutoConfiguration` | `streammq-spring-boot-starter/src/main/java/io/github/streammq/spring/boot/autoconfigure/StreamMQSchedulerAutoConfiguration.java` |
| `StreamMQListenerContainerAutoConfiguration` | `streammq-spring-boot-starter/src/main/java/io/github/streammq/spring/boot/autoconfigure/StreamMQListenerContainerAutoConfiguration.java` |
| `StreamMQListenerRegistrar` | `streammq-spring-boot-starter/src/main/java/io/github/streammq/spring/boot/autoconfigure/StreamMQListenerRegistrar.java` |
| `StreamMQSchedulerLifecycle` | `streammq-spring-boot-starter/src/main/java/io/github/streammq/spring/boot/autoconfigure/StreamMQSchedulerLifecycle.java` |
| `StreamMQListenerContainerLifecycle` | `streammq-spring-boot-starter/src/main/java/io/github/streammq/spring/boot/autoconfigure/StreamMQListenerContainerLifecycle.java` |
| `StreamMQConstants` | `streammq-core/src/main/java/io/github/streammq/core/StreamMQConstants.java` |
