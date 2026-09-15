# StreamMQ 配置参考（Configuration Reference）

> 适用版本：`0.1.2` · 绑定前缀：`streammq` · 实现：`StreamMQProperties`
>
> 所有键支持 Spring Boot 宽松绑定（`camelCase` / `kebab-case` / `snake_case` 均可），下表统一用 `kebab-case`。
> 标注 **⚠️ 安全** 的项请务必按部署环境确认。

## 快速示例

```yaml
streammq:
  enabled: true
  namespace: streammq
  producer:
    group: default-producer
    send-message-timeout: 3000
    retry-times: 2
    stream-max-len: 10000
    serializer: io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer
  consumer:
    poll-timeout: 1s
    batch-size: 32
    inflight-capacity: 0          # 背压默认关闭
    consume-timeout-millis: 0     # 并发消费超时默认关闭
    orderly-consume-timeout-millis: 0
  group:
    heartbeat-interval-ms: 5000
    instance-timeout-ms: 20000
  retry:
    policy: io.github.streammq.adapter.redisson.retry.FixedArrayRetryPolicy
    max-reconsume-times: 16
  delay:
    enabled: true
    scan-interval: 1s
    batch-size: 100
  dlq:
    min-retry-delay-ms: 1000
  transaction:
    default-group: default-tx-group
    check-interval: 60s
    max-check-times: 15
  health:
    enabled: true
  admin:
    list-page-size: 100
    max-pending-query-size: 1000
```

---

## 根配置

| Key | 默认 | 说明 |
|---|---|---|
| `streammq.enabled` | `true` | 是否启用 StreamMQ 自动装配 |
| `streammq.namespace` | `""` | 命名空间（多租户/多环境隔离），所有 Redis Key 前缀 |
| `streammq.instance-id` | `""`（自动推导） | 实例唯一标识。优先级：系统属性 `streammq.instance.id` → 环境变量 `STREAMMQ_INSTANCE_ID` → 主机名 → UUID |

---

## `streammq.producer.*` 生产者

| Key | 默认 | 说明 |
|---|---|---|
| `group` | `default-producer` | 默认生产者组名（仅字母/数字/`-`/`_`，≤128 字符） |
| `send-message-timeout` | `3000` | 发送超时（毫秒），必须 > 0 |
| `retry-times` | `2` | 同步发送重试次数（≤ `MAX_SYNC_RETRY_TIMES=10`） |
| `stream-max-len` | `0` | Stream 最大长度，`0`=不限制 |
| `serializer` | `JacksonJsonSerializer` | 消息体序列化器全限定类名。**默认 Jackson（0.1.2 起）**：安全严格类型；高吞吐可 opt-in `FurySerializer` / `ProtostuffSerializer`（需自备 classpath） |
| `fury-require-class-registration` | `true` | 仅 Fury 生效：是否强制类注册白名单（默认开启；关闭即宽松模式，扩大反序列化 RCE 面） |
| `compress-threshold` | `0` | 压缩阈值（字节），`0`=禁用 |
| `max-message-size` | `536870912`（512MB） | 单条消息最大字节，发送时校验（推荐 ≤1MB） |

---

## `streammq.consumer.*` 消费者

| Key | 默认 | 说明 |
|---|---|---|
| `poll-timeout` | `1s` | 单次拉取阻塞超时，必须 > 0 |
| `batch-size` | `32` | 单次拉取批量，必须 > 0 |
| `pull-interval` | `0` | 拉取间隔（毫秒），`0`=不间隔 |
| `paused-sleep-millis` | `100` | 暂停休眠间隔（毫秒），必须 > 0 |
| `broker-error-backoff-millis` | `500` | Broker 异常退避间隔（毫秒），必须 > 0 |
| `max-batch-size-limit` | `1000` | 最大拉取批量上界，必须 > 0 |
| `inflight-capacity` | `0` | **背压队列容量**：`>0` 启用拉取/处理解耦（队列满时拉取阻塞），`0` 禁用（默认关闭） |
| `timeout-cancel-grace-millis` | `2000` | 消费超时取消后的宽限期（毫秒），用于缩小与重试副本的重叠窗口 |
| `orderly-consume-timeout-millis` | `0` | 全局顺序消费超时（毫秒），`0`=不启用；注解显式 `>0` 优先 |
| `consume-timeout-millis` | `0` | 全局并发消费超时（毫秒），`0`=不启用（默认关闭，避免每条消息走 `Future.get` 的固定成本；卡死由 PEL 认领兜底） |
| `consume-from-where` | `CONSUME_FROM_LAST` | 新消费者组起始位点：`CONSUME_FROM_LAST` / `CONSUME_FROM_FIRST`（仅首次建组生效） |
| `broadcast-instance-id` | `""` | 广播消费实例身份（K8s StatefulSet 可绑定 Pod 名获得确定性组名） |
| `broadcast-instance-id-file` | `""`（`${user.home}/.streammq/instance-id`） | 广播实例身份本地持久化路径；`none`/`false` 禁用 |
| `broadcast-lease-timeout` | `20s` | 广播实例租约超时：空闲超此时长的槽位进入可回收窗口 |
| `broadcast-reclaim-grace` | `7d` | 广播实例回收宽限期：自最后心跳起超过后被销毁（覆盖滚动发布/停机） |

---

## `streammq.group.*` 消费者组管理

| Key | 默认 | 说明 |
|---|---|---|
| `heartbeat-interval-ms` | `5000` | 心跳上报间隔（毫秒），必须 > 0 |
| `instance-timeout-ms` | `20000` | 实例超时（毫秒），必须 ≥ `heartbeat-interval-ms` |

---

## `streammq.dlq.*` 死信队列

| Key | 默认 | 说明 |
|---|---|---|
| `failure-strategy` | `LogAndDropDlqFailureStrategy` | DLQ 消费失败处理策略实现类 |
| `max-dlq-retry-attempts` | `3` | DLQ 消费失败后最大重试次数（≥0） |
| `dlq-retry-delay-ms` | `10000` | DLQ 重试延迟（毫秒，≥0） |
| `secondary-dlq-enabled` | `false` | 是否启用二级死信队列 |
| `secondary-dlq-key-prefix` | `dlq2` | 二级死信 Stream Key 前缀段 |
| `alert-threshold` | `1` | 告警阈值 |
| `retry-backoff-multiplier` | `1.0` | 重试退避倍数（`1.0`=固定延迟） |
| `retry-max-delay-ms` | `300000` | 重试延迟上限（毫秒） |
| `min-retry-delay-ms` | `1000` | 重试延迟下限（毫秒），必须 > 0 |
| `stream-max-len` | `0` | DLQ Stream 最大长度，`0`=不限制 |

---

## `streammq.retry.*` 重试

| Key | 默认 | 说明 |
|---|---|---|
| `enabled` | `true` | 重试功能开关 |
| `policy` | `FixedArrayRetryPolicy` | 重试策略实现类 |
| `max-reconsume-times` | `16` | 最大重试次数（≥0） |
| `scan-interval` | `1s` | 重试 ZSet 扫描间隔，必须 > 0 |
| `batch-size` | `100` | 单次扫描批量，必须 > 0 |
| `delay-array` | `""` | 自定义重试延时数组（逗号分隔毫秒，如 `1000,5000,10000`） |
| `stream-max-len` | `0` | retry Stream 最大长度，`0`=不限制 |
| `pel-claim-scan-interval` | `5s` | PEL 认领扫描间隔（顺序消费），必须 > 0 |
| `pel-claim-min-idle-ms` | `60000` | PEL 认领空闲阈值（顺序消费），必须 ≥ `MIN_PEL_CLAIM_MIN_IDLE_MS`（否则仍处理中的消息会被误判为孤儿重投） |
| `failure-requeue-backoff-ms` | `5000` | 转移失败后的回写退避间隔（毫秒，必须 > 0），避免 Redis 故障时热循环 |

---

## `streammq.delay.*` 延时消息

| Key | 默认 | 说明 |
|---|---|---|
| `enabled` | `true` | 延时消息调度器开关 |
| `scan-interval` | `1s` | 扫描间隔，必须 > 0 |
| `batch-size` | `100` | 单次扫描批量，必须 > 0 |
| `failure-requeue-backoff-ms` | `5000` | 转移失败后的回写退避间隔（毫秒，必须 > 0） |

---

## `streammq.transaction.*` 事务消息

| Key | 默认 | 说明 |
|---|---|---|
| `enabled` | `true` | 事务消息开关 |
| `default-group` | `default-tx-group` | 默认事务组名 |
| `check-interval` | `60s` | 事务回查间隔，必须 > 0 |
| `max-check-times` | `15` | 最大回查次数，必须 > 0 |

---

## `streammq.health.*` 健康检查

| Key | 默认 | 说明 |
|---|---|---|
| `enabled` | `true` | 是否启用 `/actuator/health` 组件状态（需 Actuator 在 classpath） |

---

## `streammq.rebalance.*` 重平衡

| Key | 默认 | 说明 |
|---|---|---|
| `strategy` | `ConsistentHashRebalanceStrategy` | 重平衡策略实现类 |
| `virtual-nodes` | `160` | 一致性哈希虚拟节点数 |

---

## `streammq.tracing.*` / `streammq.trace.*` 追踪

| Key | 默认 | 说明 |
|---|---|---|
| `tracing.enabled` | `false` | 日志级追踪输出开关 |
| `trace.enabled` | `false` | 追踪数据存储与查询服务开关 |
| `trace.storage` | `NONE` | 存储方式（`REDIS` 启用 Redis Stream 存储，其他值禁用） |
| `trace.max-read-count` | `10000` | 单日单次追踪查询最大读取条数 |

---

## `streammq.admin.*` 管理端点（Actuator 运维接口）

| Key | 默认 | 说明 |
|---|---|---|
| `enabled` | `true` | 管理/运维 REST 端点开关（与 `health.enabled` 解耦） |
| `list-page-size` | `100` | 列表默认页大小，必须 > 0 |
| `max-pending-query-size` | `1000` | pending 列表单次最大拉取条数，必须 > 0 |
| `failure-retry-cooldown-millis` | `5000` | 写操作失败后的重试冷却期（毫秒，≥0） |
| `trust-forwarded-headers` | `false` | **⚠️ 安全** 是否信任 `X-Forwarded-For` 用于限流来源聚合。默认 `false`；仅受控代理后才开启 |
| `trusted-proxies` | `[]`（仅回环） | **⚠️ 安全** 可信代理 CIDR 列表（仅 `trust-forwarded-headers=true` 时生效），如 `10.0.0.0/8`、`2001:db8::/32` |

---

## Actuator 暴露（非 `streammq.*`）

管理端点 `/actuator/streammq` 是 Spring Boot `@WebEndpoint`，默认只暴露 `health`/`info`，需显式声明：

```yaml
management:
  endpoints:
    web:
      exposure:
        include: streammq
```

---

## 常见校验失败

自动装配时对以下值做合法性校验（抛 `IllegalArgumentException`）：所有 `>0`/`>=0` 约束（见上表）、`group.instance-timeout-ms >= heartbeat-interval-ms`、`retry.pel-claim-min-idle-ms >= 60s`、`producer.retry-times <= 10`、`admin.trusted-proxies` 必须是合法 CIDR。
