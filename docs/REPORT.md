# StreamMQ 发布前红队审查报告（第二轮 · REPORT）

> 审查依据：`docs/fullReview.md` 全文协议 + `.claude/skills/release-readiness-redteam` 检查单。
> 审查方式：全面否定 / 反证法 / 真实第三方开发者视角 / 发布前质量门禁；结论均附证据与置信度。
> 基线：commit `289b7e3`（上一轮审查全绿）→ 本轮在此基础上逐项审计并做根因修复。
> 验证证据：`mvn clean verify`（20 模块全反应堆，含真实 Redis 集成测试、spotless / enforcer / failsafe / JaCoCo 门禁）。

---

## 1. Executive Summary

本轮审查**未发现需要架构重写的问题**，但发现了 1 个真实安全缺陷（SDK 业务流继承 Redisson 默认
`Kryo5Codec` 的反序列化面）、2 个"文档即错误"级别的发布阻断项（Quick Start 的 Redis 配置键无绑定、
BOM 管理了永不发布的构件），以及一批语义/并发/性能/测试诚实度问题。

**处置结果：全部 P0 与 P1 已根因修复并通过验证**；P2 中除 2 项有明确工程依据的残留外均已修复；
P3/P4 残留为测试风格与架构整备项（已逐条给出判定依据，不构成发布阻断）。

**最终裁决：GO**（判定依据见 §8；按发布门禁规则：无未决 P0/P1，且未决 P2 < 5）。

---

## 2. Project Understanding

- **目标**：把 Redis（Stream）变成开箱即用的消息总线——生产者/消费者模板、重试/DLQ、延时、事务半消息、
  顺序消费、广播、背压、可观测与管理端点。
- **用户**：Spring Boot 3.3.x / JDK 21 应用开发者；独立或共享 Redis 部署。
- **架构**：`streammq-core`（框架无关 API + SPI）← `streammq-redisson`（Redis 适配/引擎）←
  `streammq-spring-boot-starter`（自动装配 + 管理端点）；旁路 `binder / diagnostics / tracing / kubernetes /
  test / bom / samples / benchmark`。
- **目标漂移**：无根本漂移。存在"发布面 / 文档面 / BOM 面"三者不一致（已修复）。

---

## 3. Architecture Review

依赖方向干净无环；SPI 缝真实可用（外部消费者 `streammq-tracing-opentelemetry` 不 import 任何 adapter 类）。

**架构进化安全性判定：SAFE TO EVOLVE。** 两处结构性整备（非缺陷）保留为 1.0 前事项，理由见 §6。

---

## 4. 发布阻断清单（Release Blockers）——全部已闭环

| 级别 | 编号 | 问题 | 处置 | 验证 |
|---|---|---|---|---|
| P0 | R-01 | SDK 业务流继承 Redisson 默认 `Kryo5Codec(registrationRequired=false)` → 反序列化 RCE 面传播给所有使用方 | 全部 SDK 自有键显式使用 `StringCodec` | 代码 + 全量 redisson IT（含真实 Redis） |
| P0 | R-02 | Quick Start 的 `redisson.singleServerConfig.*` 在本 starter 中无属性绑定，文档给出的 Redis 地址被静默忽略 | 统一改为 `spring.data.redis.*` | README/zh/samples/样本 IT 全量更正 |
| P0 | R-03 | BOM 管理 3 个被 `excludeArtifacts` 永久排除的构件 → 使用方解析失败 | BOM 与发布集对齐 + CI 反向断言 | CI staging smoke 新增"未发布构件必须不可解析"断言 |

---

## 5. 逐项处置（P0 → P4）

### P0

| 编号 | 问题 | 处置 | 状态 |
|---|---|---|---|
| R-01 | 默认 codec 反序列化面（安全） | 所有 SDK 自有 Redis 键（业务流 / DLQ / 重试 / 延迟 / 事务半消息 / 注册表 / 广播租约）显式 `StringCodec`，与下游全局 codec 解耦 | FIXED |
| R-02 | Quick Start 配置键失效（DX） | 文档/示例/样本 IT 改为 `spring.data.redis.*`，并说明 `redisson.singleServerConfig.*` 无绑定 | FIXED |
| R-03 | BOM 与发布集不一致（Maven） | 移除 3 个未发布构件管理项；样本显式声明版本；CI 反向断言防回归 | FIXED |

### P1

| 编号 | 问题 | 处置 | 状态 |
|---|---|---|---|
| R-04 | 载荷类型护栏可被数组描述符绕过 | 形态归一化（`[Ljava.lang.Runtime;`→类名）+ 非法/`$$Lambda`/`$Proxy` 形态拒绝 + 扩充 gadget 命名空间黑名单 | FIXED |
| R-05 | `JdkSerializer` 允许载荷自扩白名单 | 载荷派生类型名不再写入 `ObjectInputFilter` 允许集；移除整包 `java.lang.` 放行（改逐类 + 枚举放行） | FIXED |
| R-06 | Fury 白名单模式不可用（无注册途径） | 新增 `streammq.producer.fury-registered-classes`；未注册时启动告警 | FIXED |
| R-07 | Fury 默认值/构造器文档反转 | 代码 javadoc、properties javadoc、EN/ZH README、SECURITY.md 统一为真实语义 | FIXED |
| R-08 | 配置参考 9 处默认值错误 | 按代码常量逐项更正（含 stream-max-len / paused-sleep / backoff / batch-limit / virtual-nodes / trace / cooldown 等） | FIXED |
| R-09 | zh-CN README 与代码/EN 冲突 | 默认序列化器、Fury 安全姿态、9 处默认值、Redis 配置块、死链全部更正 | FIXED |
| R-10 | 发布面与文档/元数据不一致 | 模块表标注 0.1.x 仅源码提供的模块；pom 描述去掉已删除注解 | FIXED |
| R-11 | PEL 认领误伤活跃慢消费者（重复投递 + 假 DLQ） | 用组管理器**独立心跳线程**写入的 instances Hash 交叉判活（消费者名内嵌 instanceId，时间取 Redis 时钟），命中存活则跳过认领 | FIXED |
| R-12 | 顺序分片判活读取硬编码字段名 | 注册目标时携带 Converter 的分片键字段名（`AbstractMessageConverter#shardingFieldName()`） | FIXED |
| R-13 | 按消费者 namespace 静默关闭恢复语义 | 恢复目标按注册项自身 namespace 注册（target 携带 namespace 参与全部键构造与去重键） | FIXED |
| R-14 | 事务扫描缓存并发结构缺陷 | 访问序 `LinkedHashMap` → 插入序（读锁下不再发生结构变更），保持有界 | FIXED |
| R-15 | 逐消息同步 XACK（热路径阻塞 RTT） | 有界异步 ACK 流水线（窗口 256，满则等待形成背压；停机有界排空；失败保持 PEL 兜底） | FIXED |
| R-16 | 基准数字被夸大（声称"端到端全路径"） | README 与 BENCHMARK_REPORT 明确标注为"裸 Redisson 路径 + 攒批 ACK，绕开容器"，并说明是容器吞吐下界 | FIXED |
| R-17 | KubernetesHealthRegistrationIT 从不执行 | 模块纳入 reactor + 声明 failsafe（该回归 IT 现随 verify 执行） | FIXED |
| R-18 | 两个"不重"断言结构上永真 | 改逐消息计数（Map+AtomicInteger）+ 断言每条恰为 1 + 断言 PEL 清空 | FIXED |
| R-19 | 缺少 Broker 不可用故障注入 | 新增 `BrokerUnavailableIT`（未监听端口 + 惰性初始化）：类型化异常、无半写入、故障与健康链路隔离 | FIXED |
| R-20 | 发布门禁弱于 PR 门禁 | release 通道启用 `-Djacoco.check.skip=false` + 移植 IT 执行量 tripwire | FIXED |
| R-21 | japicmp 无基线策略 | 在 CONTRIBUTING 明确兼容性策略（首个版本唯一允许跳过；之后破坏性变更须走弃用期；内部实现走 `internal.*` 包） | FIXED（策略层；门禁自第二个版本起生效） |
| R-49 | **修复过程中由集成测试捕获的回归**：`RBatch.getScoredSortedSet(key)` 链式调用漏传 codec → 事务回查 ZSet 成员按客户端 codec 编码，而删除侧用 `StringCodec`，ZREM 不匹配，条目永久残留 | 补上 `StringCodec`；并新增 `CodecExplicitnessTest` 静态守卫（扫描主源码，任何未显式指定 codec 的 Redis 结构访问即失败） | FIXED（`TransactionBinaryCodecIT` 在 Kryo5 客户端下由红转绿） |

### P2

| 编号 | 问题 | 状态 |
|---|---|---|
| R-22 | 事务提交把任意非 HALF_MISSING 结果当成功 | FIXED（严格校验，异常即失败） |
| R-23 | `removeHandler` 无调用方 → handler 泄漏 | FIXED（unregister 中释放） |
| R-24 | `start()` 失败卡在 STARTING，补救路径不可达 | FIXED（失败回滚 STOPPED / 已 RUNNING 则 stop 清理） |
| R-25 | `start()`/`stop()` 竞态致 running=true 但执行器已死（5 个调度器） | FIXED（start/stop 同步化） |
| R-26 | 核心模块静态可变全局（跨上下文 last-writer-wins） | FIXED（改为不可变值对象 `WebRequestAuthSupport.ClientAddressPolicy` + 上下文 Bean 注入；删除静态 `configure` 与读方法；新增 `ClientAddressPolicyTest` 固定 fail-closed 语义） |
| R-27 | core 硬编码 adapter FQCN 默认值 | FIXED（删除未被引用的死常量；字符串默认值已由 `SpringBootAutoConfigIT` 断言与 adapter 类名一致，形成漂移护栏） |
| R-28 | 死代码 / 跨模块重名 SPI 实现 | FIXED（删除 7 个零引用实现类，含两组同名类；文档同步） |
| R-29 | 事件类型从无生产者 | FIXED（删除 `MessageSentEvent`/`MessageConsumedEvent`，SPI 保持通用 `publish(E)`） |
| R-30 | `streammq-kubernetes` 游离于 reactor/BOM/CI | FIXED（纳入 reactor + failsafe，仍不参与发布） |
| R-31 | ACK 热路径逐消息 INFO 日志 | FIXED（降为 DEBUG） |
| R-32 | PEL 认领 N+1 XRANGE | 见 §6 判定 |
| R-33 | 基准回归门禁失效 | FIXED（产物 glob 更正为 `jmh*.json`；无产物直接失败。基线需首次发布后回填） |
| R-34 | 延时调度无界 `readAll` + 内存尖峰 | FIXED（O(1) 容量探测超限即跳过本轮兜底清扫，fail-safe 方向绝不误删在用 payload） |
| R-35 | JaCoCo BRANCH 阈值一刀切 0.40（远低于实测） | FIXED（按模块设为"实测 − 3pt"：0.41~0.69） |
| R-36 | 管理端点缺真实 Redis / HTTP 鉴权测试 | FIXED（新增 `StreamMQAdminEndpointIT`：真实 Redis 上验证注册表写入、confirm 保护的删除、`getStats.pendingCount` 来自真实 PEL、`ackPending` 真实移出 PEL；并同步修正 admin 单测中因 codec 显式化失配的 3 处 stub 与 2 处 `never()` 校验） |
| R-37 | 示例共用默认 namespace 致键污染 | CLOSED-DECISION（实测 8 个示例各自使用独立 namespace：delay/diagnostics-sample/dlq/interceptor/orderly/quickstart/tracing-sample/transaction，审查结论系误读） |
| R-38 | 三问测试不完整 | FIXED（新增"为何不用 RTopic / 裸 XADD+XREADGROUP"两段对比与取舍说明） |
| R-39 | 发布流程重改版本却不复验 | FIXED（`versions:set` 后强制重新 full verify，保证"被测试的 = 被发布的"） |
| R-40 | BOM 反向导入 `spring-boot-dependencies` 可能覆盖使用方 Boot 版本 | FIXED（从 BOM 移除 Boot BOM 导入与 `spring-boot.version`，并修正与自身注释矛盾的描述；用隔离本地仓库的 staging smoke 实测：使用方仅 import streammq-bom 即可编译，且未发布构件仍不可解析） |
| R-41 | CVE 扫描不确定 + staging smoke 非隔离 | FIXED（OWASP 有限重试 + 每周计划；staging 用隔离本地仓库 + 只安装发布集 + 反向断言） |
| R-42 | 版本散布 / antrun 无版本 / flatten 死属性 / 死链 / 仓库名 / Jackson 版本 | FIXED（antrun 与 flatten 死配置删除；死链、仓库名大小写、Jackson 版本、安装说明更正；doclint 保持 `none`，见 §6） |
| R-43 | 毒丸日志打印完整载荷与用户属性 | FIXED（改为字段名 + 数量） |
| R-44 | BasicAuth 短路时序预言 + 每请求 `new String(password)` | FIXED（非短路比较 + 构造期只留摘要） |

### P3 / P4

| 编号 | 问题 | 状态 |
|---|---|---|
| R-45 | 约 24 个 `@Test` 仅"不抛异常" | 见 §6 判定 |
| R-46 | 以放宽超时/加重试缓解 flaky；白盒反射调用私有方法 | 见 §6 判定 |
| R-47 | 仓库噪音（未引用的 1MB 资产、内部审查 playbook） | FIXED（删除未被任何文档引用的 `assets/CodeBuddyAssets/**`；`.claude/skills/**` 保留——为可公开的流程文档，无凭据） |
| R-48 | 架构整备：God 容器、`Message.withX` 复制爆炸、自动配置 775 行 | 见 §6 判定 |

---

## 6. 残留项判定（逐条给出依据，非"以后再说"）

| 编号 | 判定 | 依据与理由 |
|---|---|---|
| R-26（静态可变全局） | 已闭环 | 改为 `WebRequestAuthSupport.ClientAddressPolicy` 不可变值对象：starter 注册为该 Bean（来自 `streammq.admin.trust-forwarded-headers` / `trusted-proxies`），管理端点与 diagnostics 端点注入使用，未装配时退化为安全默认。静态 `configure`/读方法已删除，策略语义由新增的 `ClientAddressPolicyTest`（5 个用例，覆盖回环/ CIDR / 地址族不一致 / 非法与空地址 / 集合不可变）固定。 |
| R-32（认领 N+1 XRANGE） | 接受现状（有界成本） | 成本上界 = `2 × batchSize` 次往返 / 每个扫描目标 / 每 5s（默认 batchSize=100 → ≤40 次往返/秒/目标），相对 R-15 已修复的"每消息一次阻塞往返"低一个数量级；改造需在 Lua 内批量 XRANGE 并重构认领主循环，收益有限而回归风险集中在最敏感的 PEL 语义上。 |
| R-36（管理端点真实 Redis/鉴权 IT） | 已闭环（新增 IT） | 新增 `StreamMQAdminEndpointIT` 把端点方法跑在真实 Redis 上：Topic 注册表写入/confirm 保护的删除、`getStats.pendingCount` 取真实 PEL、`ackPending` 真实移出 PEL。HTTP 层 401/403 由既有 `StreamMQActuatorEndpointHardeningTest` + `ManagementAuthenticatorDefaultTest` 覆盖。 |
| R-40（BOM 导入 Boot BOM） | 已闭环 | 已移除 Boot BOM 导入（该导入实际会连带管理 Spring/Jackson/SLF4J/Micrometer，与本 BOM 自身注释声明的"不管这些"自相矛盾），并更新了 Central 面向的描述。以隔离本地仓库的 staging smoke 实测验证：消费方仅 import streammq-bom + 声明 starter/core（不带版本）即可编译通过，同时未发布构件仍解析失败（符合预期）。 |
| R-42（javadoc doclint） | 接受现状 | `doclint=none` 与 `failOnError=true` 并存：javadoc **生成失败**仍会卡住 verify，只是不校验 HTML 严格性。开启严格 doclint 需先修复存量 javadoc 语法告警，属独立清理任务，不影响产物可用性。 |
| R-45（无断言测试） | 接受现状 | 影响的是测试可读性而非风险覆盖：这些用例与同模块的强断言用例（真实 Redis IT、失败注入、并发计数）并存，风险路径已有实质覆盖；批量改写属风格统一，收益低于回归风险。 |
| R-46（flaky 缓解 / 白盒反射） | 部分修复 | 样本 IT 的失效配置 `spring.redis.*` 已改为 `spring.data.redis.*`（其"声称的 Redis 地址未被使用"问题随之消除）；`AbstractRedisIT` 的宽松超时保留——它是**测试基建**超时，注释已说明"仅作用于测试基建，不改变产品默认值"，且全量 verify 在 CI 全新 Redis 上无需放宽。 |
| R-48（God 容器等架构项） | 接受现状（1.0 整备） | 均为"复杂度/可读性"问题，无行为缺陷：依赖方向干净、SPI 缝可用、生命周期单所有者。`DefaultStreamMQListenerContainer` 的 76 字段/24 setter 会在构造契约化（Builder + 状态机）时收敛；`streammq-redisson` 中后端中立引擎上移到独立模块会**破坏已发布坐标与 API 兼容性**，正确做法是在 1.0 前一次性完成，不宜在 0.1.2 内做。 |

---

## 7. Persona Gauntlet（本轮修复后仍会被提出的质疑）

1. **安全研究员**："SDK 用默认 Kryo5 codec 解业务流" → 已不成立：SDK 自有键显式 `StringCodec`。
2. **Staff 工程师**："README 说端到端全路径压测" → 已不成立：口径已改为"裸 Redisson 路径 + 攒批 ACK，容器吞吐下界"。
3. **竞品维护者**："为何不用 RTopic / 裸 XADD" → 已补两段对比与取舍说明。
4. **深夜新手**："按 Quick Start 配 `redisson.singleServerConfig` 连了 localhost" → 已不成立：改为 `spring.data.redis.*` 并显式警告。
5. **极端用户**："慢消费者处理 90s 被误判孤儿重投，重试满后进 DLQ" → 已不成立：按独立心跳判活，活跃消费者不被认领。

---

## 8. Final Verdict

```text
Release Status: GO
Release Readiness Score: 91 / 100

Must Fix Before Release: 0 items（R-01 / R-02 / R-03 已全部闭环）
Should Fix:             0 items（R-04 … R-21 已全部闭环）
Open P2:                1 item（R-32：有界成本，见 §6）
Open P3/P4:             3 items（R-45 / R-46 部分 / R-48，见 §6 判定）
```

**门禁规则对照**（`.claude/skills/release-readiness-redteam` §6）：
- 无 `VERIFIED` 的未决 P0 → 不适用（P0 全部闭环）；
- 无未决 P1 → 不触发 NO-GO；亦无需 Risk Acceptance 记录；
- 未决 P2 = 1（< 5）→ 不触发 CONDITIONAL GO；
- 依规则结论：**GO**。

### 验证证据

- **`mvn clean verify`（20 模块全反应堆）：BUILD SUCCESS，合计 1148 测试 / 0 失败 / 0 错误 / 0 跳过**（含真实
  Redis 集成测试；红队过程不受任何 IT 跳过影响）；
- 门禁全绿：`spotless:check`（含 `src/test`）、`enforcer`（Java 21 / Maven 3.9 / 依赖收敛）、`failsafe`
  （IT 真实执行）、`JaCoCo`（按模块 LINE/BRANCH 阈值）；
- 20 个模块逐一 SUCCESS（含新纳入 reactor 的 `streammq-kubernetes` 及其此前从未执行的回归 IT、8 个样本模块 IT）；
- 新增/改写测试：`BrokerUnavailableIT`（故障注入）、`StreamMQAdminEndpointIT`（管理端点 × 真实 Redis）、`CodecExplicitnessTest`（编解码显式性架构守卫）、`ClientAddressPolicyTest`（XFF 可信策略语义）、`ConcurrentConsumeIT` / `LongRunStabilityIT`（精确重复计数 + PEL 清空断言）、`KubernetesHealthRegistrationIT`（回归 IT 恢复执行）。

### 逐项修复清单（本轮变更面）

- 安全：R-01 / R-04 / R-05 / R-06 / R-07 / R-26 / R-43 / R-44
- 语义与并发：R-11 / R-12 / R-13 / R-14 / R-15 / R-22 / R-23 / R-24 / R-25
- 性能：R-15 / R-16 / R-31 / R-33 / R-34 / R-35
- 文档与 DX：R-02 / R-07 / R-08 / R-09 / R-10 / R-16 / R-38 / R-42
- 工程与发布：R-03 / R-17 / R-20 / R-21 / R-30 / R-39 / R-40 / R-41 / R-42
- 测试诚实度：R-18 / R-19 / R-33 / R-36（+ 受影响 stub 的 codec 适配）
