# 配置参考

StreamMQ 通过 `application.yml` 进行配置，所有配置项统一以 `streammq.*` 为前缀。本文档覆盖全部配置项、Redisson 配置、消费模式、过滤器语法及多环境配置。

> **版本**：`0.1.0` ｜ Spring Boot 3.3.5 ｜ Redisson 3.34.1
> **配置前缀**：`streammq`

---

## 目录

- [完整配置示例](#完整配置示例)
- [全局配置](#全局配置)
- [生产者配置](#生产者配置)
- [消费者配置](#消费者配置)
- [事务配置](#事务配置)
- [DLQ 配置](#dlq-配置)
- [可观测性配置](#可观测性配置)
- [管理配置](#管理配置)
- [Redisson 配置](#redisson-配置)
- [消费模式配置](#消费模式配置)
- [顺序消费配置](#顺序消费配置)
- [Tag 过滤配置](#tag-过滤配置)
- [SQL92 过滤配置](#sql92-过滤配置)
- [环境特定配置](#环境特定配置)
- [环境变量覆盖](#环境变量覆盖)

---

## 完整配置示例

```yaml
streammq:
  enabled: true
  namespace: streammq

  # 生产者配置
  producer:
    group: default-producer-group
    send-message-timeout: 3000        # 发送超时（毫秒）
    retry-times: 2                    # 同步发送重试次数
    compress-threshold: 0             # 0 = 关闭压缩，>0 时超过阈值自动压缩

  # 消费者配置（单消费者当前串行处理，无线程池配置）
  consumer:
    batch-size: 32                    # 单次拉取批量大小
    pull-interval: 0                  # 拉取间隔（毫秒），0 = 不间隔

  # 重试配置
  retry:
    max-reconsume-times: 16           # 消费失败最大重试次数

  # 事务消息配置
  transaction:
    check-interval: 60s               # 回查间隔
    max-check-times: 15               # 最大回查次数

  # 死信队列配置
  dlq:
    max-dlq-retry-attempts: 3         # DLQ 消费失败最大重试次数
    # failure-strategy: io.github.streammq.adapter.redisson.dlq.LogAndDropDlqFailureStrategy  # 可选：DLQ 失败策略

  # 可观测性配置（指标开关由 streammq.enabled 控制）
  tracing:
    enabled: false
```

---

## 全局配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `streammq.enabled` | boolean | `true` | 是否启用 StreamMQ |
| `streammq.namespace` | string | `""` | 全局命名空间，用于多租户 / 多环境隔离 |

> `namespace` 非空时，所有 Redis Key 统一带 `streammq:{namespace}:` 前缀。

```yaml
streammq:
  enabled: true
  namespace: streammq-dev
```

---

## 生产者配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `streammq.producer.group` | string | `default-producer-group` | 默认生产者组名 |
| `streammq.producer.send-message-timeout` | long | `3000` | 发送超时（毫秒） |
| `streammq.producer.retry-times` | int | `2` | 同步发送重试次数（0 = 不重试） |
| `streammq.producer.compress-threshold` | int | `0` | body 压缩阈值（字节），`0` = 关闭压缩 |

### 压缩配置说明

当 `compress-threshold > 0` 且 body 序列化后的字节数超过阈值时，框架自动压缩消息体，减少网络传输与 Redis 内存占用。

```yaml
streammq:
  producer:
    compress-threshold: 1024        # 超过 1KB 的 body 压缩
```

| 压缩算法 | 说明 |
|----------|------|
| `gzip` | GZIP 压缩（默认，mandatory） |
| `lz4` | LZ4 压缩（optional，需引入 LZ4 依赖） |

> 消费端框架会自动检测压缩标记并解压，无需额外配置。

---

## 消费者配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `streammq.consumer.batch-size` | int | `32` | 单次拉取批量大小 |
| `streammq.consumer.pull-interval` | long | `0` | 拉取间隔（毫秒），`0` = 不间隔 |

> 消费线程数不可配置：0.1.0 中单消费者当前串行处理；背压队列（InflightQueue）暂未开放配置。
> 消费超时、最大重试次数为注解属性（`@StreamMQConsumer.consumeTimeout` / `@StreamMQConsumer.maxReconsumeTimes`）或 `streammq.retry.max-reconsume-times`。

### 拉取间隔

`pull-interval` 控制两次拉取之间的间隔，用于限速场景：
- `0`（默认）：拉取完成后立即下一次拉取
- `> 0`：两次拉取之间等待指定毫秒

```yaml
streammq:
  consumer:
    batch-size: 64             # 增大批量提升吞吐
    pull-interval: 0           # 拉取间隔（毫秒）

---

## 事务配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `streammq.transaction.check-interval` | duration | `60s` | 事务回查间隔 |
| `streammq.transaction.max-check-times` | int | `15` | 最大回查次数 |

> 半消息发送后，框架按 `check-interval` 间隔扫描超时未确认的半消息并触发回查。连续 `max-check-times` 次仍为 `UNKNOW`，框架强制 `ROLLBACK_MESSAGE`。

```yaml
streammq:
  transaction:
    check-interval: 30s          # 30s 回查一次
    max-check-times: 20          # 最多回查 20 次
```

---

## DLQ 配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `streammq.dlq.failure-strategy` | class | `LogAndDropDlqFailureStrategy` | DLQ 消费失败处理策略实现类 |
| `streammq.dlq.max-dlq-retry-attempts` | int | `3` | DLQ 消费失败最大重试次数 |

```yaml
streammq:
  dlq:
    max-dlq-retry-attempts: 5
```

> DLQ Stream Key 格式：`streammq:{namespace}:dlq:{consumerGroup}`
> DLQ 消费失败的精细策略通过 `@StreamMQDlqConsumer` 注解或 `DlqFailureStrategy` SPI 配置。

---

## 可观测性配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `streammq.tracing.enabled` | boolean | `false` | 是否启用消息追踪 |

> Micrometer 指标无独立开关，由 `streammq.enabled` 控制（`streammq.enabled=false` 时不再注册指标收集器）。

```yaml
streammq:
  tracing:
    enabled: true                   # 启用追踪收集器与追踪拦截器
```

> 启用 `tracing` 后，框架注册 `Slf4jTraceCollector` 与追踪拦截器，自动埋点发送 / 消费事件。
> 也可通过 `@EnableStreamMQ(tracingEnabled = true)` 或 `@StreamMQConsumer(enableMsgTrace = true)` 单独启用。

---

## 管理配置

StreamMQ 的运维管理端点通过 Spring Boot Actuator 暴露（`@WebEndpoint`，路径 `/actuator/streammq`），不提供 `streammq.management.*` 配置项。

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,streammq
```

> 管理端点鉴权通过 `ManagementAuthenticator` SPI（注册为 Spring Bean）配置，内置实现：`AllowAllAuthenticator`、`BasicAuthAuthenticator`、`TokenAuthenticator`、`DenyAllAuthenticator`（默认，拒绝一切访问，返回 401）。

---

## Redisson 配置

StreamMQ 底层依赖 Redisson 连接 Redis。Redisson 配置独立于 `streammq.*`，通过 `redisson.*` 前缀或独立配置文件加载。

### 单机模式（singleServerConfig）

适用于开发 / 测试环境。

```yaml
redisson:
  singleServerConfig:
    address: "redis://127.0.0.1:6379"
    database: 0
    connectionPoolSize: 64
    connectionMinimumIdleSize: 24
    idleConnectionTimeout: 10000
    connectTimeout: 10000
    timeout: 3000
    retryAttempts: 3
    retryInterval: 1500
    password: null
    subscriptionsPerConnection: 5
    subscriptionConnectionPoolSize: 50
```

### 集群模式（clusterServersConfig）

适用于生产环境，支持多节点分片与高可用。

```yaml
redisson:
  clusterServersConfig:
    nodeAddresses:
      - "redis://127.0.0.1:7001"
      - "redis://127.0.0.1:7002"
      - "redis://127.0.0.1:7003"
      - "redis://127.0.0.1:7004"
      - "redis://127.0.0.1:7005"
      - "redis://127.0.0.1:7006"
    readMode: "SLAVE"               # 读取节点：SLAVE / MASTER / MASTER_SLAVE
    subscriptionMode: "SLAVE"
    loadBalancer: "org.redisson.connection.balancer.RoundRobinLoadBalancer"
    slaveConnectionMinimumIdleSize: 24
    slaveConnectionPoolSize: 64
    masterConnectionMinimumIdleSize: 24
    masterConnectionPoolSize: 64
    idleConnectionTimeout: 10000
    connectTimeout: 10000
    timeout: 3000
    retryAttempts: 3
    retryInterval: 1500
    password: null
    scanInterval: 1000
```

### 哨兵模式（sentinelServersConfig）

适用于基于 Sentinel 的高可用部署。

```yaml
redisson:
  sentinelServersConfig:
    masterName: "mymaster"
    sentinelAddresses:
      - "redis://127.0.0.1:26379"
      - "redis://127.0.0.1:26380"
      - "redis://127.0.0.1:26381"
    readMode: "SLAVE"
    database: 0
    password: null
    sentinelPassword: null
    slaveConnectionMinimumIdleSize: 24
    slaveConnectionPoolSize: 64
    masterConnectionMinimumIdleSize: 24
    masterConnectionPoolSize: 64
    idleConnectionTimeout: 10000
    connectTimeout: 10000
    timeout: 3000
    retryAttempts: 3
    retryInterval: 1500
```

### 独立配置文件

也可使用独立 Redisson 配置文件（如 `redisson.yml`），通过 `redisson.config` 指定路径：

```yaml
spring:
  redis:
    redisson:
      config: classpath:redisson.yml
```

---

## 消费模式配置

消费模式通过 `@StreamMQConsumer.consumeMode` 注解配置，决定 ConsumerGroup 内消息分发方式。

### 集群消费（CLUSTERING，默认）

同一 ConsumerGroup 内每条消息仅被其中一个 Consumer 实例消费，自动负载均衡。

```java
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    consumeMode = ConsumeMode.CLUSTERING   // 默认值，可省略
)
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {
    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        process(message.getBody());
        return ConsumeAction.SUCCESS;
    }
}
```

### 广播消费（BROADCASTING）

同一 Topic 的每条消息会被所有订阅的 Consumer 实例各处理一次。框架为每个 Consumer 实例创建独立 ConsumerGroup（基于 instanceId 拼接）。

```java
@StreamMQConsumer(
    topic = "config-topic",
    consumerGroup = "config-group",
    consumeMode = ConsumeMode.BROADCASTING
)
public class ConfigBroadcastConsumer implements StreamMessageConcurrentlyConsumer<String> {
    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        refreshLocalCache(message.getBody());
        return ConsumeAction.SUCCESS;
    }
}
```

| 模式 | 行为 | 适用场景 |
|------|------|----------|
| `CLUSTERING` | 每条消息被组内一个实例消费 | 绝大多数业务场景 |
| `BROADCASTING` | 每条消息被所有实例消费 | 配置广播、缓存刷新 |

---

## 顺序消费配置

顺序消费通过 `messageModel = MessageModel.ORDERLY` 启用，按 `shardingKey` 路由到固定 shard，shard 内单线程串行消费。

```java
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    messageModel = MessageModel.ORDERLY,
    shardCount = 8,                                   // 分片数
    suspendCurrentQueueTimeMillis = 1000              // 挂起时长（毫秒）
)
public class OrderlyConsumer implements StreamMessageOrderlyConsumer<String> {
    @Override
    public OrderlyAction onMessage(Message<String> message, ConsumeOrderlyContext context) {
        process(message.getBody());
        return OrderlyAction.SUCCESS;
    }
}
```

| 配置项 | 注解属性 | 默认值 | 说明 |
|--------|----------|--------|------|
| 分片数 | `shardCount` | `4` | 顺序消费分区数，仅 `ORDERLY` 生效 |
| 挂起时长 | `suspendCurrentQueueTimeMillis` | `1000` | `SUSPEND_CURRENT_QUEUE_A_MOMENT` 时暂停时长（毫秒） |

发送时通过 `shardingKey` 指定分片依据：

```java
Message<String> message = MessageBuilder.<String>withTopic("order-topic")
        .shardingKey("user-123")   // 相同 user 的消息路由到同一 shard
        .body("content")
        .build();
```

---

## Tag 过滤配置

Tag 过滤通过 `@StreamMQConsumer.selectorExpression` 配置，`selectorType` 默认为 `TAG`。

### 表达式语法

| 表达式 | 含义 |
|--------|------|
| `*` | 匹配所有 Tag（默认） |
| `tag1` | 仅匹配 Tag 为 `tag1` 的消息 |
| `tag1 \|\| tag2` | 匹配 `tag1` 或 `tag2` |
| `tag1 && tag2` | 匹配同时含 `tag1` 且 `tag2`（注：单消息仅一个 Tag，实际用 `\|\|`） |
| `!tag1` | 匹配 Tag 不为 `tag1` 的消息 |

```java
// 单 Tag
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    selectorExpression = "created"
)

// 多 Tag（或）
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    selectorExpression = "created || paid || shipped"
)
```

发送时通过 `MessageBuilder.tag()` 指定 Tag：

```java
Message<String> message = MessageBuilder.<String>withTopic("order-topic")
        .tag("created")
        .body("content")
        .build();
```

---

## SQL92 过滤配置

SQL92 过滤通过 `selectorType = SelectorType.SQL92` 启用，支持基于消息属性的复杂表达式。

```java
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    selectorType = SelectorType.SQL92,
    selectorExpression = "age > 18 AND region = 'CN'"
)
```

### 表达式语法

SQL92 表达式可引用消息的 `userProperties` 与系统字段，支持以下语法：

| 语法 | 示例 | 说明 |
|------|------|------|
| 比较运算 | `age > 18` | 数值比较 |
| 等值 | `region = 'CN'` | 字符串等值 |
| 逻辑与 | `a > 1 AND b < 10` | 逻辑与 |
| 逻辑或 | `a = 1 OR a = 2` | 逻辑或 |
| 逻辑非 | `NOT (a = 1)` | 逻辑非 |
| IN | `status IN ('A', 'B', 'C')` | 集合包含 |
| BETWEEN | `age BETWEEN 18 AND 60` | 范围 |
| IS NULL | `tag IS NULL` | 空判断 |

发送时通过 `withUserProperty()` 设置可过滤属性：

```java
Message<String> message = MessageBuilder.<String>withTopic("order-topic")
        .tag("created")
        .withUserProperty("age", "25")
        .withUserProperty("region", "CN")
        .withUserProperty("status", "A")
        .body("content")
        .build();
```

> SQL92 过滤在消费端执行，框架先拉取消息再过滤，不匹配的消息会被自动 ACK 跳过。

---

## 环境特定配置

StreamMQ 推荐使用 Spring Profile 区分不同环境的配置。

### 开发环境（dev）

```yaml
# application-dev.yml
streammq:
  namespace: streammq-dev
  producer:
    send-message-timeout: 5000
    retry-times: 1
  retry:
    max-reconsume-times: 5
  tracing:
    enabled: false

redisson:
  singleServerConfig:
    address: "redis://127.0.0.1:6379"
    database: 0
```

### 预发环境（staging）

```yaml
# application-staging.yml
streammq:
  namespace: streammq-staging
  producer:
    send-message-timeout: 3000
    retry-times: 2
  retry:
    max-reconsume-times: 16
  tracing:
    enabled: true

redisson:
  sentinelServersConfig:
    masterName: "mymaster"
    sentinelAddresses:
      - "redis://staging-redis-1:26379"
      - "redis://staging-redis-2:26379"
      - "redis://staging-redis-3:26379"
```

### 生产环境（prod）

```yaml
# application-prod.yml
streammq:
  namespace: streammq-prod
  producer:
    group: prod-producer-group
    send-message-timeout: 3000
    retry-times: 3
    compress-threshold: 1024        # 生产环境开启压缩
  consumer:
    batch-size: 64
    pull-interval: 0
  retry:
    max-reconsume-times: 16
  transaction:
    check-interval: 60s
    max-check-times: 15
  dlq:
    max-dlq-retry-attempts: 5
  tracing:
    enabled: true

redisson:
  clusterServersConfig:
    nodeAddresses:
      - "redis://prod-redis-1:7001"
      - "redis://prod-redis-2:7001"
      - "redis://prod-redis-3:7001"
      - "redis://prod-redis-4:7001"
      - "redis://prod-redis-5:7001"
      - "redis://prod-redis-6:7001"
    readMode: "SLAVE"
    password: ${REDIS_PASSWORD}
```

### 主配置文件

```yaml
# application.yml
spring:
  profiles:
    active: dev                      # 默认激活 dev 环境
  application:
    name: streammq-app

streammq:
  enabled: true
```

通过启动参数切换环境：

```bash
java -jar app.jar --spring.profiles.active=prod
```

---

## 环境变量覆盖

StreamMQ 基于 Spring Boot 配置机制，所有配置项均可通过环境变量覆盖。规则：将配置点号 `.` 替换为下划线 `_`，全大写，并以 `STREAMMQ_` 为前缀。

### 覆盖规则

| 配置项 | 环境变量 |
|--------|----------|
| `streammq.enabled` | `STREAMMQ_ENABLED` |
| `streammq.namespace` | `STREAMMQ_NAMESPACE` |
| `streammq.producer.group` | `STREAMMQ_PRODUCER_GROUP` |
| `streammq.producer.send-message-timeout` | `STREAMMQ_PRODUCER_SEND_MESSAGE_TIMEOUT` |
| `streammq.producer.retry-times` | `STREAMMQ_PRODUCER_RETRY_TIMES` |
| `streammq.producer.compress-threshold` | `STREAMMQ_PRODUCER_COMPRESS_THRESHOLD` |
| `streammq.consumer.batch-size` | `STREAMMQ_CONSUMER_BATCH_SIZE` |
| `streammq.consumer.pull-interval` | `STREAMMQ_CONSUMER_PULL_INTERVAL` |
| `streammq.retry.max-reconsume-times` | `STREAMMQ_RETRY_MAX_RECONSUME_TIMES` |
| `streammq.transaction.check-interval` | `STREAMMQ_TRANSACTION_CHECK_INTERVAL` |
| `streammq.transaction.max-check-times` | `STREAMMQ_TRANSACTION_MAX_CHECK_TIMES` |
| `streammq.dlq.max-dlq-retry-attempts` | `STREAMMQ_DLQ_MAX_DLQ_RETRY_ATTEMPTS` |
| `streammq.tracing.enabled` | `STREAMMQ_TRACING_ENABLED` |

### 使用示例

```bash
# 通过环境变量覆盖
export STREAMMQ_NAMESPACE=streammq-prod
export STREAMMQ_PRODUCER_SEND_MESSAGE_TIMEOUT=5000
export STREAMMQ_RETRY_MAX_RECONSUME_TIMES=20
export STREAMMQ_DLQ_MAX_DLQ_RETRY_ATTEMPTS=5

# 通过 JVM 系统属性覆盖
java -Dstreammq.namespace=streammq-prod \
     -Dstreammq.producer.send-message-timeout=5000 \
     -Dstreammq.retry.max-reconsume-times=20 \
     -jar app.jar

# 通过命令行参数覆盖
java -jar app.jar \
     --streammq.namespace=streammq-prod \
     --streammq.producer.send-message-timeout=5000 \
     --streammq.retry.max-reconsume-times=20
```

### 配合 .env / Docker / Kubernetes

```yaml
# docker-compose.yml
services:
  app:
    image: streammq-app:latest
    environment:
      - STREAMMQ_NAMESPACE=streammq-prod
      - STREAMMQ_PRODUCER_SEND_MESSAGE_TIMEOUT=5000
      - STREAMMQ_RETRY_MAX_RECONSUME_TIMES=20
      - STREAMMQ_DLQ_MAX_DLQ_RETRY_ATTEMPTS=5
```

```yaml
# kubernetes deployment env
env:
  - name: STREAMMQ_NAMESPACE
    value: "streammq-prod"
  - name: STREAMMQ_PRODUCER_SEND_MESSAGE_TIMEOUT
    value: "5000"
  - name: STREAMMQ_RETRY_MAX_RECONSUME_TIMES
    value: "20"
```

---

## 配置优先级

Spring Boot 配置优先级（从高到低）：

1. 命令行参数（`--streammq.*`）
2. JVM 系统属性（`-Dstreammq.*`）
3. 环境变量（`STREAMMQ_*`）
4. `application-{profile}.yml`
5. `application.yml`
6. 默认值

> 同一配置项在高优先级来源中定义时，覆盖低优先级来源的值。

---

## 配置速查表

| 模块 | 关键配置 | 推荐值（生产） |
|------|----------|----------------|
| 生产者 | `send-message-timeout` | `3000` |
| 生产者 | `retry-times` | `2-3` |
| 生产者 | `compress-threshold` | `1024`（开启压缩） |
| 消费者 | `batch-size` | `32-64` |
| 消费者 | `pull-interval` | `0` |
| 重试 | `max-reconsume-times` | `16` |
| 事务 | `check-interval` | `60s` |
| 事务 | `max-check-times` | `15` |
| DLQ | `max-dlq-retry-attempts` | `3` |
| 追踪 | `tracing.enabled` | `true` |
