# StreamMQ 配置参考（Configuration Reference）

> 适用版本：`0.1.2` · 绑定前缀：`streammq` · 实现：`StreamMQProperties`
>
> 所有键支持 Spring Boot 宽松绑定（`camelCase` / `kebab-case` / `snake_case` 均可），下表统一用 `kebab-case`。
> 标注 **⚠️ 安全** 的项请务必按部署环境确认。
>
> **覆盖范围**：本篇枚举由 `StreamMQProperties`（starter 内）绑定的全部 `streammq.*` 键；
> **独立模块的前缀**（`streammq.diagnostics.*` 见文末章节、`streammq.tracing.otel.*`）由各自模块的
> `@ConfigurationProperties` 绑定，**未引入对应模块时 Spring 会静默忽略这些键**（未知属性默认不报错、不告警）。

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
| `retry-times` | `2` | 同步发送重试次数（≤ `MAX_SYNC_RETRY_TIMES=16`） |
| `stream-max-len` | `0` | Stream 最大长度，`0`=不限制 |
| `serializer` | `JacksonJsonSerializer` | 消息体序列化器全限定类名。**默认 Jackson（0.1.2 起）**：安全严格类型；高吞吐 / 低时延可 opt-in：`FurySerializer`（底层库 Apache Fory 1.7.3，坐标 `org.apache.fory:fory-core`，要求 >= 1.1.0）、`ProtostuffSerializer`（需自备 classpath）、`FlatBuffersSerializer`（FlexBuffers 动态格式，坐标 `com.google.flatbuffers:flatbuffers-java` 24.3.25，免代码生成；内层为纯数据、逐字段读取并复制，读取侧有单字段长度上限 64MB 与 64 层深度上限）、`SbeSerializer`（金融级确定性时延信封模式，坐标 `org.agrona:agrona` 1.17.1；`sbe-tool` 1.18.0 仅构建期生成桩；读取侧校验信封头与声明长度）。后两者在**内层格式层面**不含类名、无内层多态 gadget，但 SDK 外层仍会按目标类型反射物化对象，整体安全性取决于目标类型是否可信（见下方说明） |
| `fury-require-class-registration` | `true` | 仅 `FurySerializer`（底层库 Apache Fory）生效：是否强制类注册白名单（默认开启；关闭即宽松模式，扩大反序列化 RCE 面） |
| `fury-registered-classes` | `[]`（空） | 仅 `FurySerializer`（底层库 Apache Fory）白名单模式生效：预注册的业务消息体类型（全限定类名列表，逗号分隔），如 `com.acme.Order,com.acme.Payment`；未注册类型反序列化将被拒绝 |
| `compress-threshold` | `0` | 压缩阈值（字节），`0`=禁用 |
| `max-message-size` | `536870912`（512MB） | 单条消息最大字节，发送时校验（推荐 ≤1MB） |

> **序列化器统一 null 契约（0.1.2 起）**：6 个内置实现（Jackson/JDK/Fury/Protostuff/FlatBuffers/SBE）一致——`serialize(null)` 返回 `null`；`deserialize(null | 空数组)` 返回 `null`（不抛异常）。消息体为 null 时不会写入 `body` 字段。
>
> **载荷驱动目标类型（建议显式声明，勿依赖载荷）**：消费者未声明显式类型时，SDK 会按载荷的 `bodyType` / `bodyTypeName` 解析反序列化目标类，并统一经过 `PayloadTypeSafety` 护栏（**对所有序列化器生效**，不只 JDK）。该护栏是**命名空间拒绝式黑名单（纵深防御，不是完整性边界）**：拒绝 `java.*` / `javax.*` / `jdk.*` / `sun.*` / `com.sun.*` / Spring 及已知 gadget 命名空间（Commons-Collections/IO、fastjson、Xalan、SnakeYAML、Groovy、cglib、Javassist、Struts2 等）。**生产环境应为每个消费者显式声明泛型类型（注解泛型或 `targetBodyType`）**，把目标类固定在代码里；显式声明的类型不经过该护栏。

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
| `broadcast-instance-id-file` | `""`（`${user.home}/.streammq/instance-id-<namespace>_<group>`） | 广播实例身份本地持久化路径。默认**按 namespace+group 分片**（多应用同机互不共享；文件按 `id pid timestamp` 多记录，重启复用已退出进程的身份）；显式指定时为全实例共用的单一文件。`none`/`false` 禁用 |
| `broadcast-lease-timeout` | `20s` | 广播实例租约超时：空闲超此时长的槽位进入可回收窗口。必须 > 0 且 `broadcast-reclaim-grace >= broadcast-lease-timeout`（非法值启动期失败） |
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
| `pel-claim-min-idle-ms` | `60000` | PEL 认领空闲阈值（顺序消费）。必须 ≥ max(`MIN_PEL_CLAIM_MIN_IDLE_MS`=35000ms, 3 × `group.heartbeat-interval-ms`)——判活窗口小于心跳间隔时活跃慢消费者会被误判为死亡并复制重投，重试耗尽后已成功处理的消息会进 DLQ |
| `failure-requeue-backoff-ms` | `5000` | 转移失败后的回写退避间隔（毫秒，必须 > 0），避免 Redis 故障时热循环 |

---

## `streammq.delay.*` 延时消息

| Key | 默认 | 说明 |
|---|---|---|
| `enabled` | `true` | 延时消息调度器开关 |
| `scan-interval` | `1s` | 扫描间隔，必须 > 0 |
| `batch-size` | `100` | 单次扫描批量，必须 > 0 |
| `failure-requeue-backoff-ms` | `5000` | 转移失败后的回写退避间隔（毫秒，必须 > 0） |

> **延时上界：7 天（`StreamMQConstants.MAX_DELAY_TIME_MILLIS`）。** 无论走 18 级固定延时
> （`delayLevel`，最大 `HOUR_2` = 2 小时）还是任意毫秒延时（`delayTimeMillis`），**都受同一 7 天上限约束**：
> 超限在**发送期即快速失败**（`StreamMQException`），不会静默截断，也不会写入调度队列
> （payload 的 TTL 亦以 7 天为基准，`DEFAULT_DELAY_PAYLOAD_TTL_MS`）。

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
| `startup-warn` | `true` | 启动时输出管理端点暴露面提醒（仅日志、零行为影响；`-Dstreammq.admin.startup-warn=false` 可抑制） |
| `trust-forwarded-headers` | `false` | **⚠️ 安全** 是否信任 `X-Forwarded-For` 用于限流来源聚合。默认 `false`；仅受控代理后才开启 |
| `trusted-proxies` | `[]`（仅回环） | **⚠️ 安全** 可信代理 CIDR 列表（仅 `trust-forwarded-headers=true` 时生效），如 `10.0.0.0/8`、`2001:db8::/32` |

---

## `streammq.diagnostics.*` 诊断模块（独立模块前缀）

绑定类：`streammq-diagnostics` 模块的 `StreamMQDiagnosticsProperties`（前缀 `streammq.diagnostics`），
自动装配由 `@ConditionalOnProperty(streammq.diagnostics.enabled=true, matchIfMissing=false)` 门控。

**前置条件（缺一不可）：**

1. classpath 上存在 `streammq-diagnostics`（0.1.x 不发布到 Maven Central，需从源码 `mvn install`）；
2. `streammq.diagnostics.enabled=true`（**默认 `false`**）；
3. 存在 `StreamMQTraceService` Bean（`streammq.trace.enabled=true` + `streammq.trace.storage=redis`）
   —— 分析器（SlowConsume / Backlog / Dlq）与诊断服务才装配；无 Redisson 客户端时积压探针退化为按追踪窗口估算。

> **排查提示：未引入该模块时，`streammq.diagnostics.*` 会被 Spring 静默忽略**——没有任何配置类绑定该前缀，
> 未知属性默认既不报错也不告警，表现为"配了但毫无效果"。依次确认：`mvn dependency:tree | grep streammq-diagnostics`
> 有该构件、`enabled=true` 已生效、`StreamMQTraceService` Bean 存在。诊断 REST 端点
> `/streammq/diagnostics/**` 挂在应用**主端口**（普通 MVC 端点，不受 `management.endpoints.web.exposure.*` 治理）。

| Key | 默认 | 说明 |
|---|---|---|
| `enabled` | `false` | 诊断模块自动装配开关（`matchIfMissing=false`，不显式配置即关闭） |
| `namespace` | `""` | 命名空间，用于真实积压探测（XLEN/XPENDING）的 Redis Key 前缀，需与 `streammq.namespace` 保持一致 |
| `recent-window-ms` | `300000`（5 分钟） | 近期诊断时间窗口 |
| `dlq-window-ms` | `3600000`（1 小时） | DLQ 诊断时间窗口 |
| `slow-consume-threshold-ms` | `5000` | 慢消费耗时阈值（毫秒），超过判定为慢消费 |
| `backlog-warning-threshold` | `1000` | 积压 WARNING 阈值 |
| `backlog-critical-threshold` | `10000` | 积压 CRITICAL 阈值 |
| `dlq-topic-marker` | `dlq` | DLQ 主题标识关键字（小写匹配） |
| `dlq-max-retry-count` | `3` | DLQ 最大重试次数阈值（`DEFAULT_DLQ_MAX_RETRY_ATTEMPTS`） |
| `max-profile-query-size` | `1000` | 单次画像查询最大消息数（防止大范围查询 OOM） |

> 同理，**`streammq.tracing.otel.*`**（`enabled` 默认 `false`、`otlp-endpoint`、`service-name`、`exporter-interval-ms`）
> 由 `streammq-tracing-opentelemetry` 模块的 `StreamMQTracingProperties` 绑定；**未引入该模块时这些键被静默忽略**。
> 该模块另有独立开关 `streammq.tracing.enabled`（TraceCollector SPI，由 starter 绑定，见上文追踪章节）。

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

自动装配时对以下值做合法性校验（抛 `IllegalArgumentException`）：所有 `>0`/`>=0` 约束（见上表）、`group.instance-timeout-ms >= heartbeat-interval-ms`、`retry.pel-claim-min-idle-ms >= max(35000ms, 3 × group.heartbeat-interval-ms)`（低于该值仍处理中的消息可能被误判为孤儿重投）、`producer.retry-times <= 16`（`MAX_SYNC_RETRY_TIMES`）、`admin.trusted-proxies` 必须是合法 CIDR。
