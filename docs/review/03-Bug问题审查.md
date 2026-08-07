# StreamMQ Bug 问题审查报告

> 审查视角：以开源项目发布标准，全面否定性审查项目中潜在的 Bug 和缺陷
> 审查日期：2026-07-21
> 审查结论：**发现 15 项 Bug/缺陷（5 项严重 / 6 项重要 / 4 项一般）**

---

## 一、严重 Bug（必须修复）

### 1.1 兜底 RedissonClient 在生产环境造成数据安全隐患

**位置**：`streammq-spring-boot-starter/.../StreamMQCoreAutoConfiguration.java:49-55`

```java
@Bean(destroyMethod = "shutdown")
@ConditionalOnMissingBean(RedissonClient.class)
public RedissonClient redissonClient() {
    LOG.warn("No RedissonClient bean found, creating default localhost:6379 instance...");
    Config config = new Config();
    config.useSingleServer().setAddress("redis://localhost:6379").setDatabase(0);
    return Redisson.create(config);
}
```

**Bug 描述**：
当用户忘记配置 RedissonClient 时，框架自动创建一个连接 `localhost:6379` 的实例。这意味着：
1. 在 K8s/Docker 环境中，会连接到容器内部的 localhost（通常没有 Redis），导致启动失败但错误信息不明确
2. 如果恰好有一个 Redis 实例在 localhost，会静默地向其写入数据，造成数据污染
3. 虽然有 WARN 日志，但在生产环境中日志可能被忽略

**修复建议**：
- 移除兜底 Bean，改为启动时检测并抛出明确的异常
- 或至少使用 `@Profile("dev")` 限制仅在开发环境生效

---

### 1.2 TransactionScanner 回查调度器的并发安全问题

**位置**：`streammq-redisson/.../scheduler/TransactionScanner.java`

**Bug 描述**：
`TransactionScanner` 中的事务状态检查使用 `RBucket` 存储半消息状态，但 `checkAndCommit` 方法中存在 TOCTOU（Time-of-check-to-time-of-use）竞争条件：

```java
// 伪代码 - 实际检查逻辑
String state = bucket.get();  // 检查状态
if (state == HALF) {
    // 此时另一个实例可能已经修改了状态
    LocalTransactionState result = checker.check(message, context);
    bucket.set(result);  // 覆盖可能已被修改的状态
}
```

在多实例部署时，两个实例可能同时检查同一个半消息并尝试提交/回滚。

**影响**：
- 消息可能被重复提交（重复消费）
- 消息可能被错误回滚

---

### 1.3 消费超时取消机制可能丢失消息

**位置**：`streammq-redisson/.../container/DefaultStreamMQListenerContainer.java`

**Bug 描述**：
消费超时通过 `CompletableFuture.orTimeout()` 实现，超时后 Future 被取消。但此时 Redis Stream 中的消息已经通过 `XREADGROUP` 被消费者获取，处于 PENDING 状态。如果超时后消费者线程实际上还在执行业务逻辑，当它最终完成时：

1. 消息已经从 PENDING 列表中被 `XACK`（由超时处理逻辑触发）
2. 业务逻辑的副作用（如数据库写入）已经执行
3. 但消息被认为已成功消费

**影响**：
- 超时取消后消息既被 ACK 又可能被重新投递（如果超时处理触发了 XACK 但业务仍在执行）
- 或者消息永远不会被 ACK（如果超时处理不触发 XACK），导致 PENDING 列表堆积

---

### 1.4 广播消费模式的 Consumer Group 泄漏

**位置**：`streammq-redisson/.../container/DefaultStreamMQListenerContainer.java`

**Bug 描述**：
广播消费模式下，每个消费者实例应创建独立的 Consumer Group。但 `DefaultStreamMQListenerContainer` 中：

1. 广播模式下创建的 Consumer Group 名称基于实例 ID
2. 实例重启后，旧的 Consumer Group 不会被清理
3. Redis Stream 的 Consumer Group 不会自动过期

**影响**：
- Redis 中积累大量无用的 Consumer Group
- 长期运行后可能达到 Redis 的 Group 数量限制

---

### 1.5 DefaultMessageConverter 的压缩解压可能失败

**位置**：`streammq-redisson/.../converter/DefaultMessageConverter.java`

**Bug 描述**：
`DefaultMessageConverter` 在序列化时检查消息大小是否超过 `compressThreshold`，超过则压缩。但反序列化时：

```java
// 伪代码
if (isCompressed(bytes)) {
    bytes = compressionCodec.decompress(bytes);
}
return serializer.deserialize(bytes, type);
```

如果序列化使用了 `GzipCompressionCodec`，但反序列化时 `compressionCodec` 未注入（或注入了不同的实现），会导致解压失败。

**影响**：
- 消息在生产者和消费者使用不同配置时无法反序列化
- 错误信息不够明确，难以排查

---

## 二、重要 Bug（应该修复）

### 2.1 Health Check 使用 AtomicLong.get() 产生副作用

**位置**：`streammq-spring-boot-starter/.../StreamMQHealthAutoConfiguration.java:91`

```java
long val = redisson.getAtomicLong(HEALTH_CHECK_KEY).get();
```

**Bug 描述**：
`AtomicLong.get()` 对于不存在的 key 返回 0，不会创建 key。但这行代码的语义是"检查 Redis 连通性"，更合适的做法是使用 `ping()` 命令。当前实现：
1. 如果 Redis 有内存限制，频繁调用可能产生不必要的开销
2. 没有真正的"活性检查"——即使 Redis 返回缓存数据也能通过检查

---

### 2.2 StreamMQConsumer 注解的 proxy 属性解析不完整

**位置**：`streammq-spring-boot-starter/.../StreamMQListenerRegistrar.java`

**Bug 描述**：
`resolveStreamMQListener` 方法只为 `topic`、`consumerGroup`、`namespace`、`selectorExpression` 创建了代理，但注解中还有很多其他属性需要解析（如 `serializer`、`rebalanceStrategy`、`dlqFailureStrategy` 等）。

```java
InvocationHandler handler = (proxy, method, args) -> {
    String name = method.getName();
    switch (name) {
        case "topic": return resolvedTopic;
        case "consumerGroup": return resolvedGroup;
        // ... 只处理了 4 个属性
        default: return method.invoke(original, args);
    }
};
```

**影响**：
- 注解中使用 `${...}` 占位符的 serializer、rebalanceStrategy 等属性不会被解析
- 用户配置 `${my.serializer}` 会直接传入原始字符串

---

### 2.3 消息 ID 生成策略可能导致冲突

**位置**：`streammq-core/.../message/Message.java`

**Bug 描述**：
`MessageId` 的生成逻辑在 `MessageBuilder.build()` 中：

```java
if (StringUtils.isEmpty(this.messageId)) {
    this.messageId = MessageId.generate();
}
```

`MessageId.generate()` 的实现如果基于时间戳+随机数，在高并发场景下（同毫秒内大量消息）可能存在冲突。

**影响**：
- 消息去重逻辑可能失效
- 日志中难以区分不同消息

---

### 2.4 RetryScheduler 的重试目标注册时机问题

**位置**：`streammq-spring-boot-starter/.../StreamMQListenerRegistrar.java`

**Bug 描述**：
`registerRetryTargetsIfPossible()` 在 `afterSingletonsInstantiated()` 中调用，此时 RetryScheduler 可能尚未被创建（因为它也是通过 `@ConditionalOnMissingBean` 延迟创建的）。

```java
private void registerRetryTargetsIfPossible() {
    try {
        RetryScheduler retryScheduler = applicationContext.getBean(RetryScheduler.class);
        // ...
    } catch (NoSuchBeanDefinitionException ex) {
        LOG.debug("RetryScheduler not present, skip retry target registration");
    }
}
```

**影响**：
- 重试目标可能永远不会被注册到 RetryScheduler
- 消费失败的消息无法被自动重试

---

### 2.5 MessageConverter 的序列化器不一致风险

**位置**：`streammq-spring-boot-starter/.../StreamMQCoreAutoConfiguration.java`

**Bug 描述**：
`MessageSerializer` 和 `MessageConverter` 是两个独立的 Bean，但它们之间存在隐式依赖：

```java
@Bean
@ConditionalOnMissingBean(MessageConverter.class)
public MessageConverter streamMQMessageConverter(MessageSerializer<?> serializer, ...) {
    DefaultMessageConverter converter = new DefaultMessageConverter(serializer);
    // ...
}
```

如果用户自定义了 `MessageConverter` 但没有使用与 `MessageSerializer` 相同的序列化方式，消息将无法正确反序列化。

---

### 2.6 延时消息的 ScoredSortedSet 方案缺乏清理机制

**位置**：`streammq-redisson/` 模块

**Bug 描述**：
使用 `ScoredSortedSet` 实现任意毫秒延时时，消息被存储在 ZSet 中，score 为触发时间戳。但项目中没有：
- 定期清理已投递的 ZSet 条目
- ZSet 内存占用监控
- ZSet 大小限制

**影响**：
- 长期运行后 ZSet 会无限增长
- Redis 内存持续消耗

---

## 三、一般 Bug（建议修复）

### 3.1 日志输出包含敏感信息

**位置**：多处

**Bug 描述**：
多处日志输出了消息的完整内容：

```java
LOG.info("Received message: topic={}, body={}", message.getTopic(), message.getBody());
```

在生产环境中，消息体可能包含用户的敏感数据（如手机号、身份证号），直接输出到日志存在安全风险。

---

### 3.2 关闭资源时的异常吞没

**位置**：`DefaultStreamMQListenerContainer.stop()`

**Bug 描述**：
容器停止时，如果某个 Listener 的关闭失败，异常被 catch 后仅记录日志，不影响其他 Listener 的关闭。但调用方无法知道关闭是否完全成功。

---

### 3.3 Consumer Filter 的 order() 可能导致数组越界

**位置**：`streammq-core/.../filter/ConsumerFilter.java`

**Bug 描述**：
过滤器的 `order()` 返回值用于排序，但没有范围校验。如果返回负数或极大值，可能在排序或数组操作时出现问题。

---

### 3.4 批量发送中单条消息失败的处理不明确

**位置**：`streammq-core/.../template/StreamMessageTemplate.java`

**Bug 描述**：
`syncSendBatch` 返回 `List<SendResult>`，但如果批量中某条消息发送失败，是抛出异常还是返回失败的 SendResult？当前实现的语义不明确。

---

## 四、Bug 统计汇总

| 严重级别 | 数量 | 影响范围 |
|---------|------|---------|
| 严重（Critical） | 5 | 数据丢失/安全/可靠性 |
| 重要（Major） | 6 | 功能异常/性能问题 |
| 一般（Minor） | 4 | 代码质量/日志/异常处理 |
| **总计** | **15** | |

---

## 五、发布前必须验证的 Bug 场景

| # | 场景 | 验证方式 | 风险等级 |
|---|------|---------|---------|
| 1 | 未配置 Redis 时启动是否报明确错误 | 移除 Redis 配置后启动 | 高 |
| 2 | 多实例部署时事务消息是否正确 | 启动 2 个实例同时发送事务消息 | 高 |
| 3 | 消费超时后消息是否被正确处理 | 设置 1 秒超时，消费逻辑 sleep 5 秒 | 高 |
| 4 | 广播消费多实例是否都能收到消息 | 启动 2 个实例，发送 1 条消息 | 中 |
| 5 | 压缩消息的生产者/消费者使用不同配置 | 生产者用 GZIP，消费者不配置压缩 | 中 |
| 6 | 消息体超过 1KB 时压缩是否正常工作 | 发送 2KB 消息 | 中 |
| 7 | 顺序消息在消费者重启后是否保持顺序 | 重启消费者后发送多条同 shardingKey 消息 | 中 |
| 8 | DLQ 消息是否能被 DLQ 消费者正常消费 | 发送无法处理的消息触发 DLQ | 中 |
