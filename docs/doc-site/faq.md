# FAQ · 常见问题

> 本文档汇总 StreamMQ 使用过程中的常见问题、故障排查、性能优化与迁移指南，帮助开发者快速定位与解决问题。

---

## 目录

- [基础概念](#基础概念)
- [消息模型](#消息模型)
- [可靠性与持久化](#可靠性与持久化)
- [监控与运维](#监控与运维)
- [故障排查](#故障排查)
- [性能优化](#性能优化)
- [迁移指南](#迁移指南)

---

## 基础概念

### Q1: StreamMQ 与 RocketMQ、Kafka 有什么区别？如何选型？

**A:** 三者的核心差异如下：

| 维度          | StreamMQ                       | RocketMQ                       | Kafka                          |
| ------------- | ------------------------------ | ------------------------------ | ------------------------------ |
| 底层存储      | Redis Stream                   | NameServer + Broker（自研）    | Broker + ZK/KRaft              |
| 部署复杂度    | **低（仅需 Redis）**           | 高（独立集群）                  | 高（独立集群）                  |
| API 风格      | 类 RocketMQ                    | RocketMQ 原生                  | Kafka 原生                      |
| 注解消费      | **支持**                       | 支持                            | 不支持                          |
| Template 编程 | **支持**                       | 支持                            | 不支持                          |
| 事务消息      | **支持**                       | 支持                            | 不支持（仅幂等生产者）          |
| 延时消息      | **支持（18 级 + 任意）**       | 支持（18 级）                   | 不支持                          |
| 顺序消息      | **支持**                       | 支持                            | 支持（分区内）                  |
| 适用规模      | 中小规模（< 1 亿/日）          | 大规模                          | 超大规模                        |

**选型建议：**
- 已有 Redis 基础设施，希望复用为消息总线 → **StreamMQ**
- 日消息量 > 1 亿，需要超大规模吞吐 → **Kafka**
- 需要复杂路由、多级路由、灵活的过滤 → **RocketMQ**
- 已有成熟 MQ 集群且无 Redis 资源 → 直接复用现有 MQ

### Q2: StreamMQ 支持哪些消息类型？

**A:** StreamMQ 当前支持以下消息类型：

| 消息类型   | 发送方式                                                  | 适用场景                       |
| ---------- | --------------------------------------------------------- | ------------------------------ |
| 普通消息   | `syncSend` / `asyncSend` / `sendOneway`                   | 一般业务通信                   |
| 批量消息   | `syncSendBatch(BatchMessage)`                             | 高吞吐场景                     |
| 事务消息   | `executeInTransaction(Message, TransactionCallback)`      | 最终一致性场景                 |
| 延时消息   | `delayLevel(DelayLevel)` 或 `delayTimeMillis(long)`       | 定时任务、延迟处理             |
| 顺序消息   | `shardingKey(String)` + `MessageModel.ORDERLY`            | 同 key 严格有序场景            |
| 死信消息   | DLQ 消费者（`dlqMode = true`）                            | 失败消息人工干预               |

### Q3: StreamMQ 是否支持消息去重 / 幂等？

**A:** StreamMQ **不提供内置幂等机制**，业务侧需自行实现。原因：
- Redis Stream 的 `XADD` 默认自动生成 ID，不支持天然去重
- 业务幂等键的选择（业务 ID / 消息 ID）由业务决定
- Redis Stream 在重试时可能重复投递，必须实现幂等

**推荐做法：**

```java
@Override
public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
    String idempotentKey = message.getKeys();   // 使用业务 key 作为幂等键
    if (isDuplicate(idempotentKey)) {
        return ConsumeAction.SUCCESS;            // 幂等：直接返回成功
    }
    try {
        process(message.getBody());
        markProcessed(idempotentKey);
        return ConsumeAction.SUCCESS;
    } catch (Exception e) {
        return ConsumeAction.RECONSUME_LATER;
    }
}
```

幂等存储可选：Redis SETNX、数据库唯一索引、本地布隆过滤器等。

---

## 消息模型

### Q4: 延时消息的精度如何？支持任意延时吗？

**A:** StreamMQ 延时消息基于 Redis ZSet 实现，支持两种方式：

1. **固定延时级别（18 级）：** 与 RocketMQ 完全对齐

   | 级别 | 延时 | 级别 | 延时 |
   |------|------|------|------|
   | LEVEL_1 | 1s | LEVEL_10 | 6m |
   | LEVEL_2 | 5s | LEVEL_11 | 7m |
   | LEVEL_3 | 10s | LEVEL_12 | 8m |
   | LEVEL_4 | 30s | LEVEL_13 | 9m |
   | LEVEL_5 | 1m | LEVEL_14 | 10m |
   | LEVEL_6 | 2m | LEVEL_15 | 20m |
   | LEVEL_7 | 3m | LEVEL_16 | 30m |
   | LEVEL_8 | 4m | LEVEL_17 | 1h |
   | LEVEL_9 | 5m | LEVEL_18 | 2h |

2. **任意延时毫秒：** 通过 `delayTimeMillis(long)` 设置

   ```java
   Message<String> msg = MessageBuilder.<String>withTopic("delay-topic")
           .body("content")
           .delayTimeMillis(15 * 60 * 1000L)   // 延时 15 分钟
           .build();
   ```

**精度：** 默认轮询间隔为 1 秒，实际精度为 **秒级**，最大偏差约 1 秒。如需更高精度，可调整后台调度器的扫描间隔。

### Q5: 顺序消息如何保证顺序？有什么限制？

**A:** StreamMQ 顺序消息基于 **ShardingKey 分片** 实现：

**保证机制：**
1. 生产端：相同 `shardingKey` 的消息路由到同一 Redis Stream 分片
2. 消费端：每个分片由独立线程串行消费，确保分片内严格有序
3. 失败重试：返回 `SUSPEND_CURRENT_QUEUE_A_MOMENT` 后挂起当前分片，不消费后续消息

**示例：**

```java
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-orderly-group",
    messageModel = MessageModel.ORDERLY,
    shardCount = 8
)
public class OrderOrderlyConsumer implements StreamMessageOrderlyConsumer<String> {
    @Override
    public OrderlyAction onMessage(Message<String> message, ConsumeOrderlyContext context) {
        // 相同 userId 的订单创建、支付、发货消息会严格按顺序消费
        processOrder(message.getBody());
        return OrderlyAction.SUCCESS;
    }
}
```

**限制：**
- `shardCount` 决定并行度上限，建议 ≤ 消费者实例数 × 单实例线程数
- `shardCount` 一经确定，已存在的消息仍按原分片路由，变更需谨慎
- 顺序消费吞吐低于并发消费，建议仅对真正需要顺序的场景使用
- 业务消费失败会阻塞该分片后续消息，需合理设置 `suspendCurrentQueueTimeMillis`

### Q6: 如何实现消息过滤？支持 SQL92 吗？

**A:** StreamMQ 支持两种过滤方式：

#### Tag 过滤（推荐）

通过 `selectorExpression` 配置：

```java
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    selectorExpression = "tag1 || tag2"   // 匹配 tag1 或 tag2
)
```

支持的表达式：
- `*` — 匹配所有
- `tag1` — 精确匹配 tag1
- `tag1 || tag2` — 匹配 tag1 或 tag2
- `tag1 && tag2` — 匹配 tag1 且 tag2

#### SQL92 过滤（高级）

通过 `SelectorType.SQL92` 配置，可基于消息属性进行复杂过滤：

```java
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "vip-group",
    selectorType = SelectorType.SQL92,
    selectorExpression = "age > 18 AND region = 'CN'"
)
```

> **注意：** SQL92 过滤在客户端侧执行，仍会拉取全量消息，不适合高过滤比例的场景。如需服务端过滤，建议按 Topic 拆分。

### Q7: 消息压缩如何启用？支持哪些算法？

**A:** StreamMQ 内置 GZIP 压缩支持，通过 `CompressionCodec` SPI 扩展点实现。

**启用压缩：**

```java
Message<String> msg = MessageBuilder.<String>withTopic("big-topic")
        .body(largeContent)
        .compress(true)        // 启用 GZIP 压缩
        .build();
```

**适用场景：**
- 消息体 > 1KB 时收益明显
- 消息体 < 256B 时压缩可能反而增加体积
- 已压缩的二进制内容（如图片、视频）不建议再次压缩

**自定义压缩算法：** 实现 `CompressionCodec` SPI 即可支持 LZ4、Snappy、Zstd 等算法。

### Q8: StreamMQ 支持的最大消息大小是多少？

**A:** StreamMQ 本身无固定限制，但受 Redis 与 Redisson 约束：

| 层级        | 限制                                                        |
| ----------- | ----------------------------------------------------------- |
| Redis Stream| 单个 Stream entry 最大 512MB（但不推荐）                    |
| Redisson    | 默认命令大小限制可配置                                      |
| 实践推荐    | 单消息 ≤ **64KB**，超过 1MB 需评估 Redis 内存与网络压力     |

**建议：**
- 单消息 ≤ 64KB：直接发送
- 64KB - 1MB：启用 GZIP 压缩
- > 1MB：考虑拆分消息或将大对象存入对象存储，消息中只传递引用

### Q9: 多租户隔离如何实现？

**A:** 通过 `streammq.namespace` 配置全局命名空间，所有 Redis key 会自动添加该前缀：

```yaml
streammq:
  namespace: tenant-a    # 所有 key 形如 streammq:tenant-a:topic:order-topic
```

**隔离维度：**
- 不同 namespace 的 Stream、消费组、DLQ 完全隔离
- 同一 Redis 实例可承载多个 namespace，互不干扰
- 配合 Redis ACL 可实现更细粒度的权限隔离

**生产建议：** 关键业务建议使用独立 Redis 实例；非关键业务可在同实例下通过 namespace 隔离。

---

## 可靠性与持久化

### Q10: StreamMQ 如何保证消息持久化？

**A:** StreamMQ 基于 Redis Stream，持久化能力继承自 Redis：

| Redis 配置                | 持久化保证                                  | 推荐场景          |
| ------------------------- | ------------------------------------------- | ----------------- |
| `appendonly yes` + `appendfsync always`   | 不丢消息，性能最差           | 金融级关键业务    |
| `appendonly yes` + `appendfsync everysec` | 最多丢 1 秒数据，性能较好    | **生产推荐**      |
| `appendonly yes` + `appendfsync no`       | 依赖 OS flush，可能丢较多    | 不推荐            |
| 仅 RDB                    | 可能丢失最近几分钟数据        | 仅适合缓存场景    |
| 无持久化                  | Redis 重启即丢失              | **绝对不推荐**    |

**Stream 长度控制：** StreamMQ 会在创建 Stream 时设置 `MAXLEN`，避免无限增长。生产环境建议显式配置：

```yaml
streammq:
  producer:
    max-stream-length: 100000   # 单 Stream 保留最近 10 万条
```

### Q11: 消费者宕机后未处理的消息怎么办？

**A:** StreamMQ 基于 Redis Stream 的 PEL（Pending Entry List）实现消息可靠性：

1. **消息拉取：** 消费者通过 `XREADGROUP` 拉取消息，消息进入 PEL
2. **消费成功：** 业务返回 `ConsumeAction.SUCCESS`，StreamMQ 调用 `XACK` 移除
3. **消费失败：** 返回 `RECONSUME_LATER`，消息留在 PEL，进入重试流程
4. **消费者宕机：** 消息保留在 PEL，重启后通过 `XAUTOCLAIM` 重新认领
5. **重试耗尽：** 消息进入 DLQ

**关键配置：**

```yaml
streammq:
  consumer:
    max-reconsume-times: 16     # 最大重试次数
    consume-timeout: 30000      # 消费超时（ms），超时后自动重新分配
```

### Q12: 死信队列（DLQ）的消息如何处理？

**A:** StreamMQ 在消息重试次数耗尽后自动将其投递到 DLQ，DLQ 与原 Topic 一一对应。

**消费 DLQ：**

```java
@Component
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    dlqMode = true     // 标记为 DLQ 消费者
)
public class OrderDlqConsumer implements StreamMessageConcurrentlyConsumer<String> {
    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        // 1. 记录告警日志
        log.error("DLQ 消息: topic={}, keys={}, reconsumeTimes={}",
            message.getTopic(), message.getKeys(),
            context.getReconsumeTimes());
        // 2. 通知人工干预
        alertService.notifyDeadLetter(message);
        // 3. 持久化到数据库备查
        deadLetterRepository.save(message);
        return ConsumeAction.SUCCESS;
    }
}
```

**DLQ 失败策略（SPI）：** 通过 `DlqFailureStrategy` SPI 可自定义 DLQ 处理逻辑，例如：
- 重试 N 次后丢弃
- 转发到其他系统
- 触发告警但不消费

---

## 监控与运维

### Q13: 如何监控 StreamMQ 的运行状态？

**A:** StreamMQ 提供三层可观测能力：

#### 1. Micrometer 指标

| 指标名                              | 类型      | 说明                     |
| ----------------------------------- | --------- | ------------------------ |
| `streammq.send.total`      | Counter   | 发送消息总数             |
| `streammq.send.total (tag success=true)`    | Counter   | 发送成功数               |
| `streammq.send.total (tag success=false)`     | Counter   | 发送失败数               |
| `streammq.send.duration`   | Timer     | 发送耗时分布             |
| `streammq.consume.total`   | Counter   | 消费消息总数             |
| `streammq.consume.duration`| Timer     | 消费耗时分布             |
| `streammq.retry.total`     | Counter   | 重试消息数               |
| `streammq.dlq.total`       | Counter   | 进入死信队列数           |

#### 2. Actuator 端点

```bash
curl http://localhost:8080/actuator/prometheus   # Prometheus 格式
curl http://localhost:8080/actuator/health       # 健康检查
curl http://localhost:8080/actuator/streammq     # StreamMQ 管理端点
```

#### 3. MDC 结构化日志

StreamMQ 自动在 MDC 中注入 traceId、topic、consumerGroup、messageId，配合 ELK/Loki 可实现链路追踪。

### Q14: 如何调整消费者线程数？

**A:** 通过 `@StreamMQConsumer` 注解或全局配置：

**注解级别（推荐）：**

```java
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    consumeThreadMin = 8,
    consumeThreadMax = 64
)
```

**全局级别：**

```yaml
streammq:
  consumer:
    consume-thread-min: 4
    consume-thread-max: 64
```

**调优建议：**

| 业务类型            | consumeThreadMax | 备注                       |
| ------------------- | ---------------- | -------------------------- |
| CPU 密集型          | CPU 核数 × 2     | 避免上下文切换             |
| IO 密集型（DB/RPC） | CPU 核数 × 10    | 充分利用 IO 等待           |
| 顺序消费            | shardCount       | 单分片串行                 |

### Q15: 连接池如何配置？

**A:** Redisson 连接池参数在 `redisson.*` 下配置：

```yaml
redisson:
  singleServerConfig:
    connectionPoolSize: 64            # 连接池大小
    connectionMinimumIdleSize: 24     # 最小空闲连接
    idleConnectionTimeout: 10000      # 空闲连接超时（ms）
    connectTimeout: 10000             # 建连超时（ms）
    timeout: 3000                     # 命令超时（ms）
    retryAttempts: 3                  # 重试次数
    retryInterval: 1500               # 重试间隔（ms）
    pingConnectionInterval: 30000     # 心跳间隔（ms）
```

**经验值：**
- 单实例中等并发：`connectionPoolSize=64`, `connectionMinimumIdleSize=24`
- 顺序消费：`connectionPoolSize=shardCount×2`
- 高并发场景需对应调整 Redis 的 `maxclients`

### Q16: 如何实现优雅停机？

**A:** StreamMQ 默认支持优雅停机，配置如下：

```yaml
server:
  shutdown: graceful

spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s
```

> **注意**：StreamMQ 消费者容器停机时，等待正在处理的消息完成的最长时间为固定 5s（0.1.0 暂不可配置，不存在 `streammq.consumer.shutdown-timeout` 配置项）。

**K8s 配合：**

```yaml
spec:
  terminationGracePeriodSeconds: 60
  containers:
    - name: streammq-app
      lifecycle:
        preStop:
          exec:
            command: ["sh", "-c", "sleep 10"]
```

优雅停机流程：
1. 收到 SIGTERM 信号
2. 停止拉取新消息
3. 等待正在处理的消息完成（最长 5 秒，0.1.0 固定值，不可配置）
4. 提交消费进度（XACK）
5. 关闭 Redisson 连接
6. 应用退出

---

## 故障排查

### 消息发送失败

**可能原因：**
- Redis 连接异常 / 网络问题
- 发送超时
- Redis 内存达到 `maxmemory` 限制
- 命令被 Redis 拒绝（ACL 权限不足）

**排查步骤：**

```bash
# 1. 检查 Redis 连通性
redis-cli -h <host> -p <port> -a <password> ping

# 2. 检查 Redis 内存
redis-cli INFO memory | grep used_memory_human

# 3. 检查 Redis 慢日志
redis-cli SLOWLOG GET 10

# 4. 检查应用日志
grep "send failed" app.log | tail -50

# 5. 检查消费者组状态
redis-cli XINFO STREAM streammq:order-topic
redis-cli XINFO GROUPS streammq:order-topic
```

**解决方案：**
- 增大 `send-timeout` 与连接池
- 优化 Redis 内存使用，必要时扩容
- 检查 ACL 配置是否正确

### 消费失败

**可能原因：**
- 业务逻辑异常
- 消费超时
- 反序列化失败
- 消费者组配置错误

**排查步骤：**

```bash
# 1. 查看消费者日志中的异常堆栈
grep -A 30 "onMessage failed" app.log

# 2. 检查消费组 PEL 中的积压消息
redis-cli XPENDING streammq:order-topic order-group

# 3. 查看具体积压消息
redis-cli XRANGE streammq:order-topic <min-id> <max-id>

# 4. 检查 DLQ
redis-cli XLEN streammq:dlq:order-topic:order-group
```

**解决方案：**
- 修复业务异常，避免持续失败
- 调整 `consume-timeout` 与 `max-reconsume-times`
- 检查 `selectorExpression` 是否匹配消息 Tag
- 对反序列化失败的消息直接 ACK 并记录 DLQ

### 消息堆积

**可能原因：**
- 消费速度慢于生产速度
- 消费者实例数不足
- 业务处理耗时过长
- 顺序消费分片阻塞

**排查步骤：**

```bash
# 1. 查看积压量
redis-cli XLEN streammq:order-topic
redis-cli XPENDING streammq:order-topic order-group

# 2. 查看消费者组详情
redis-cli XINFO GROUPS streammq:order-topic
redis-cli XINFO CONSUMERS streammq:order-topic order-group

# 3. 查看 StreamMQ 指标
curl http://localhost:8080/actuator/metrics/streammq.consume.total
```

**解决方案：**
- 增加消费者实例数（集群消费模式）
- 优化业务处理逻辑（异步化、批量化）
- 调整 `pull-batch-size`
- 检查是否单分片热点导致顺序消费阻塞

### 事务消息回查频繁

**可能原因：**
- 本地事务执行时间过长
- 本地事务返回 `UNKNOW` 状态
- 网络抖动导致回查请求失败
- `TransactionChecker` 实现异常

**排查步骤：**

```bash
# 1. 检查回查日志
grep "transaction check" app.log | tail -50

# 2. 检查本地事务执行时长
grep "executeLocalTransaction cost" app.log

# 3. 检查半消息数量
redis-cli XLEN streammq:half:tx-topic
```

**解决方案：**
- 优化本地事务执行速度，避免返回 `UNKNOW`
- 调整 `check-interval-ms` 与 `max-check-times`
- 检查 `TransactionChecker` 实现是否健壮
- 网络不稳定时适当增加超时

### Redis 连接异常

**可能原因：**
- Redis 服务不可用
- 网络分区
- 连接池耗尽
- ACL 权限不足

**排查步骤：**

```bash
# 1. 检查 Redis 进程
redis-cli ping

# 2. 检查客户端连接数
redis-cli INFO clients

# 3. 检查慢命令
redis-cli SLOWLOG GET 10

# 4. 检查 ACL 配置
redis-cli ACL WHOAMI
redis-cli ACL LIST
```

**解决方案：**
- 排查 Redis 进程与网络
- 增大 `connectionPoolSize`
- 检查 ACL 用户与权限
- 优化慢命令

---

## 性能优化

### 发送性能优化

| 优化手段                  | 说明                                                    | 适用场景          |
| ------------------------- | ------------------------------------------------------- | ----------------- |
| 使用 `sendOneway`         | 不等待响应，最高吞吐                                    | 日志、监控类消息  |
| 使用 `syncSendBatch`      | 批量发送，减少 RTT                                      | 高吞吐业务        |
| 异步发送 `asyncSend`      | 非阻塞，配合 `CompletableFuture`                        | 一般业务          |
| 调整 `send-timeout`       | 避免过短超时误判失败                                    | 网络抖动场景      |
| 增大 Redisson 连接池      | `connectionPoolSize=128`+                               | 高并发            |
| 启用 Pipeline             | Redisson 默认启用                                       | 始终推荐          |
| 控制 Message 体积         | < 64KB，必要时启用 GZIP                                 | 大消息场景        |

### 消费性能优化

| 优化手段                    | 说明                                                  | 适用场景          |
| --------------------------- | ----------------------------------------------------- | ----------------- |
| 增大 `consume-thread-max`   | 充分利用多核                                          | IO 密集型         |
| 增大 `pull-batch-size`      | 单次拉取更多消息，减少 RTT                            | 高吞吐            |
| 调整 `inflight-capacity`    | 增大队列容量                                          | 业务处理波动大    |
| 业务异步化                  | 消息接收后异步处理，快速返回 SUCCESS                  | 允许少量丢失      |
| 业务批量化                  | 累积 N 条后批量处理                                   | 数据库写入场景    |
| 使用虚拟线程（JDK 21）      | 显著降低线程切换开销                                  | 高并发 IO 密集型  |
| 调整 `pull-interval`        | 控制拉取频率                                          | 资源有限场景      |

### Redis 优化

| 优化手段                          | 说明                                              |
| --------------------------------- | ------------------------------------------------- |
| 关闭 RDB 或降低 RDB 频率          | RDB fork 会阻塞，大内存实例尤其明显               |
| 使用 `appendfsync everysec`       | 平衡性能与可靠性                                  |
| 设置合理的 `maxmemory-policy`     | **必须 `noeviction`**，避免 Stream 被驱逐         |
| 设置 Stream `MAXLEN`              | 避免单 Stream 无限增长                            |
| 使用 Redis Cluster                | 突破单机内存与吞吐上限                            |
| 优化 `client-output-buffer-limit` | 防止慢消费者拖垮 Redis                            |
| 启用 `keepalive`                  | 减少连接建立开销                                  |
| 使用更快的存储（SSD/NVMe）        | 加速 AOF fsync                                    |

---

## 迁移指南

### 从 RocketMQ 迁移

StreamMQ 的 API 设计对齐 RocketMQ，迁移成本低。

#### 依赖替换

```xml
<!-- 移除 -->
<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-spring-boot-starter</artifactId>
</dependency>

<!-- 引入 -->
<dependency>
    <groupId>io.github.streammq</groupId>
    <artifactId>streammq-spring-boot-starter</artifactId>
    <version>0.1.0</version>
</dependency>
<dependency>
    <groupId>org.redisson</groupId>
    <artifactId>redisson-spring-boot-starter</artifactId>
</dependency>
```

#### API 映射表

| RocketMQ                            | StreamMQ                              | 说明                       |
| ----------------------------------- | ------------------------------------- | -------------------------- |
| `@RocketMQMessageListener`          | `@StreamMQConsumer`                   | 消费者注解                 |
| `RocketMQTemplate`                  | `StreamMessageTemplate`               | 发送模板                   |
| `RocketMQMessage`                   | `Message`                             | 消息体                     |
| `MessageBuilder.withPayload`        | `MessageBuilder.withTopic`            | 消息构建器                 |
| `ConsumeConcurrentlyStatus`         | `ConsumeAction`                       | 消费结果                   |
| `ConsumeOrderlyStatus`              | `OrderlyAction`                       | 顺序消费结果               |
| `MessageModel.CLUSTERING`           | `ConsumeMode.CLUSTERING`              | 集群消费                   |
| `MessageModel.BROADCASTING`         | `ConsumeMode.BROADCASTING`            | 广播消费                   |
| `TransactionListener`               | `TransactionCallback` + `TransactionChecker` | 事务消息            |
| `MessageDelayLevel`                 | `DelayLevel`                          | 延时级别                   |
| `LocalTransactionState`             | `LocalTransactionState`               | 事务状态（命名一致）       |

#### 迁移步骤

1. 替换 Maven 依赖
2. 修改 `@RocketMQMessageListener` → `@StreamMQConsumer`
3. 修改 `RocketMQTemplate` → `StreamMessageTemplate`
4. 修改 `MessageBuilder.withPayload` → `MessageBuilder.withTopic`
5. 修改 `ConsumeConcurrentlyStatus` → `ConsumeAction`
6. 调整配置文件（`rocketmq.*` → `streammq.*` + `redisson.*`）
7. 部署 Redis 7.2+
8. 移除 RocketMQ NameServer / Broker 集群

#### 注意事项

- RocketMQ 的 `keys` 在 StreamMQ 中作为业务键保留
- RocketMQ 的 `shardingKey` 在 StreamMQ 中保留同名
- RocketMQ 的 `userProperty` 在 StreamMQ 中保留同名
- RocketMQ 的 SQL92 过滤语法在 StreamMQ 中保持兼容

### 从 Kafka 迁移

**当前状态：** StreamMQ V2.0 计划提供 Kafka wire protocol 兼容能力，当前版本（0.1.0）暂不支持直接迁移。

**V2.0 规划：**

- `BackendProvider` SPI 支持 Redis / Kafka / RabbitMQ / Pulsar 等多种后端
- Kafka wire protocol 兼容层，允许 Kafka 客户端直接连接
- 现有 StreamMQ API 可无缝切换到 Kafka 后端

**当前版本迁移建议：**

| Kafka 概念        | StreamMQ 对应                          |
| ----------------- | -------------------------------------- |
| Topic             | Topic                                  |
| Partition         | Shard（顺序消费分片）                  |
| Consumer Group    | ConsumerGroup                          |
| Offset            | Stream ID                              |
| Producer          | `StreamMessageTemplate.syncSend`       |
| Consumer          | `@StreamMQConsumer`                    |
| Key               | `keys`                                 |
| Headers           | `userProperty`                         |

需手动重写 Producer / Consumer 代码，迁移至 StreamMQ API。

### 从 RabbitMQ 迁移

**当前状态：** StreamMQ V2.0 计划提供 AMQP 兼容能力，当前版本暂不支持。

**V2.0 规划：**

- AMQP 兼容层支持 RabbitMQ 客户端直连
- Exchange / Queue / RoutingKey 概念映射

**当前版本迁移建议：**

| RabbitMQ 概念     | StreamMQ 对应                          |
| ----------------- | -------------------------------------- |
| Exchange          | Topic                                  |
| Queue             | ConsumerGroup + Topic                  |
| RoutingKey        | Tag / `selectorExpression`             |
| Producer          | `StreamMessageTemplate.syncSend`       |
| Consumer          | `@StreamMQConsumer`                    |

需手动重写业务代码，使用 StreamMQ API。

### 从 Spring Data Redis Stream 迁移

从原生 `RedisTemplate.opsForStream()` 迁移到 StreamMQ 可获得注解驱动、Template 编程、事务消息、延时消息等高级能力。

**迁移前（Spring Data Redis Stream）：**

```java
// 发送
redisTemplate.opsForStream().add(StreamRecords.newRecord()
    .ofObject(map)
    .withStreamKey("order-topic"));

// 消费
@Bean
public Subscription consumer(StreamListener<String, MapRecord<String, String, String>> listener) {
    return StreamMessageListenerContainer.create(...)
        .register(StreamMessageListenerContainer.StreamReadRequest
            .builder()
            .stream("order-topic")
            .consumer(Consumer.from("order-group", "consumer-1"))
            .build(), listener);
}
```

**迁移后（StreamMQ）：**

```java
// 发送
template.syncSend(MessageBuilder.<String>withTopic("order-topic").body(content).build());

// 消费
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {
    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        process(message.getBody());
        return ConsumeAction.SUCCESS;
    }
}
```

**迁移收益：**

| 维度          | Spring Data Redis Stream | StreamMQ                            |
| ------------- | ------------------------ | ----------------------------------- |
| 注解驱动      | 不支持                    | **支持**                            |
| Template API  | 无                        | **支持（类 RocketMQ）**             |
| 事务消息      | 不支持                    | **支持**                            |
| 延时消息      | 不支持                    | **支持（18 级 + 任意）**            |
| 顺序消息      | 不支持                    | **支持**                            |
| 死信队列      | 不支持                    | **支持**                            |
| 监控指标      | 不支持                    | **支持（Micrometer）**              |
| 重试机制      | 需自行实现                | **内置**                            |

---

## V2.0 迁移计划

### V2.0 规划概览

StreamMQ V2.0 计划引入以下能力，帮助用户从其他 MQ 平滑迁移：

| 能力                          | 说明                                                 | 迁移价值                  |
| ----------------------------- | ---------------------------------------------------- | ------------------------- |
| **BackendProvider SPI**       | 抽象存储后端，支持 Redis / Kafka / RabbitMQ / Pulsar | 现有代码零修改切换后端    |
| **Kafka wire protocol 兼容**  | Kafka 客户端可直接连接 StreamMQ                       | 从 Kafka 平滑迁移         |
| **AMQP 兼容层**               | RabbitMQ 客户端可直接连接                            | 从 RabbitMQ 平滑迁移      |
| **跨数据中心复制**            | 多机房消息同步                                       | 容灾场景                  |
| **K8s Operator**              | 自动化部署与运维                                     | 云原生场景                |
| **Spring Cloud Stream Binder**| 集成 Spring Cloud Stream                              | 微服务生态                |

### 迁移路径建议

**当前阶段（0.1.0）：**
- 已有 Redis 基础设施的用户：直接采用 StreamMQ
- 从 RocketMQ 迁移：API 高度对齐，迁移成本低
- 从 Kafka / RabbitMQ 迁移：需手动重写代码

**V2.0 阶段：**
- 从 Kafka / RabbitMQ 迁移：通过兼容层平滑迁移，客户端无需修改
- 跨数据中心：通过复制能力实现多机房部署
- 云原生部署：通过 K8s Operator 简化运维

> **注意：** V2.0 仍在规划阶段，特性可能调整。当前版本（0.1.0）不支持上述能力，请勿在生产环境中依赖未实现的特性。

---

## 其他常见问题

### Q17: StreamMQ 是否支持广播消费？

**A:** 支持。通过 `consumeMode = ConsumeMode.BROADCASTING` 配置：

```java
@StreamMQConsumer(
    topic = "notification-topic",
    consumerGroup = "notification-group",
    consumeMode = ConsumeMode.BROADCASTING
)
```

广播模式下，每个消费者实例都会消费全量消息。注意：广播消费不支持 ACK 与重试，业务侧需自行保证处理成功。

### Q18: 如何查看消息轨迹？

**A:** StreamMQ 提供 Trace 查询 API（通过 `TraceCollector` SPI），启用方式：

```yaml
streammq:
  tracing:
    enabled: true
```

通过管理端点查询：

```bash
curl "http://localhost:8080/actuator/streammq/trace?messageId=<messageId>"
```

默认实现 `Slf4jTraceCollector` 将 Trace 记录到日志，可通过 SPI 替换为 Zipkin / Jaeger 等实现。

### Q19: StreamMQ 如何与 Spring Cloud 集成？

**A:** StreamMQ 当前版本（0.1.0）通过 `streammq-spring-boot-starter` 与 Spring Boot 3 深度集成。Spring Cloud Stream Binder 集成在 V2.0 路线图中规划。

当前可在 Spring Cloud 项目中直接使用 StreamMQ Starter，与 Spring Cloud Config / Discovery / Gateway 等组件无冲突。

### Q20: 如何贡献代码或反馈问题？

**A:** 请参考 [贡献指南](contributing.md)：

- **Bug 反馈 / 功能建议：** 提交 [GitHub Issue](https://github.com/streammq/streammq/issues)
- **代码贡献：** Fork 仓库 → 创建分支 → 提交 PR
- **讨论交流：** 使用 [GitHub Discussions](https://github.com/streammq/streammq/discussions)
- **安全漏洞：** 私下联系维护者，请勿公开 Issue

---

## 参考资源

- [StreamMQ GitHub](https://github.com/streammq/streammq)
- [快速开始](quickstart.md)
- [核心特性](features.md)
- [配置参考](configuration.md)
- [API 文档](api.md)
- [部署指南](deploy.md)
- [贡献指南](contributing.md)
- [Redis Stream 文档](https://redis.io/docs/data-types/streams/)
- [Redisson 文档](https://github.com/redisson/redisson/wiki)

---

*StreamMQ · 让 Redis 成为你的消息总线。*
