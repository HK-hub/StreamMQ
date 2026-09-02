# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **默认序列化器由 Jackson 切换为 Apache Fury**（`streammq.producer.serializer` 默认值 =
  `io.github.streammq.adapter.redisson.serializer.FurySerializer`，常量 `StreamMQConstants#DEFAULT_SERIALIZER`）。
  - `fury-core` 在 `streammq-redisson` 中由 `optional` 调整为普通依赖，保证默认装配开箱可用。
  - **数据兼容提示**：切换后新写入消息的 body 为 Fury 二进制格式，与既有 Jackson JSON 消息不互通。
    升级时请先消费完存量消息，或显式配置 `serializer: io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer` 保持原行为。
  - Fury 为 secure-by-default：自定义 body 类型需先注册（`new FurySerializer<>(OrderCreated.class)`）。

## [0.1.1] - 2026-08-29 — 第一个公开发布版本

> **关于 `v0.1.0` 标签（发布前必读）**
>
> 仓库中曾存在指向 `f54b1fe`（2026-08-25）的 `v0.1.0` 标签，而其后有 11 个修复提交（含多项 P0/P1）
> 未被包含。由于 Maven Central 构件**不可变**，同一个版本号不能被重新发布为不同内容，
> 因此 **0.1.0 不再作为发布版本使用**，首个公开版本为 **0.1.1**。
>
> 发布前需要维护者手动执行：
>
> ```bash
> git tag -d v0.1.0
> git push origin :refs/tags/v0.1.0
> ```
>
> `release.yml` 已增加门禁：若标签指向的提交与工作流检出的提交不一致，发布将直接失败。

> **升级注意（数据兼容）**
>
> 0.1.1 是**第一个公开发布版本**，0.1.0 从未发布，因此不存在对外数据兼容义务。事务相关 Redis key
> （`streammq:{ns}:half:*` / `txstate:*` / `txcheck:*` / `txlock:*`）的命名规则保持不变，本轮变更是
> **编码一致性**：所有与 Lua 脚本交互的事务结构与执行权锁统一使用 `StringCodec`（此前依赖客户端
> 默认 codec，非字符串编码下出现"只报成功、永不发布"的 P0 缺陷，见下文）。
> 若你曾在未公开的 0.1.0 前缀版本上跑过本地数据（开发/测试环境），升级前建议清理残留的事务 key：
>
> ```bash
> # 按实际 namespace 替换 {ns}；redis-cli 举例：
> redis-cli --scan --pattern 'streammq:{ns}:half:*' | xargs redis-cli del
> redis-cli --scan --pattern 'streammq:{ns}:txstate:*' | xargs redis-cli del
> redis-cli --scan --pattern 'streammq:{ns}:txcheck:*' | xargs redis-cli del
> redis-cli --scan --pattern 'streammq:{ns}:txlock:*' | xargs redis-cli del
> ```
>
> 业务消息（`streammq:{ns}:msg:*`）与消费位点（`meta:offset:*`）等结构与编码均未变化，无需处理。
>
> **事务 key 结构与 Redis Cluster（hash tag）定型声明**
>
> - 事务相关 key（`streammq:{ns}:half:{txGroup}` / `txstate:{txGroup}` / `txcheck:{txGroup}` /
>   `txlock:{txGroup}:{txId}`）的命名结构自 **0.1.1 定型**，此后不再变更。
> - 该结构中**不含 `{...}` hash tag 定界符**：早期设计曾考虑用 `{txGroup}` hash tag 将同一事务组
>   的 key 家族钉在同一 slot，但用户可控的 topic / group / txGroup 若包含 `{` `}` 会在 Redis Cluster
>   下强制 key 家族同 slot 热点，因此 0.1.1 统一改为纯前缀结构，并在命名校验中**显式拒绝** `{` `}`
>   字符（发送侧与事务半消息注册侧一致）。
> - 正确性论证：事务状态机、执行权锁、回查计数等全部 Lua 脚本均为**单 key 原子执行**（`KEYS[1]`
>   只含一个 key），不依赖跨 key 同 slot；移除 hash tag 不影响事务原子性与正确性。
> - 兼容义务：0.1.1 为首个公开版本，此前未公开发布的前缀版本（0.1.0 标签及其前身）**无数据兼容
>   义务**；若内部环境存在前缀版本残留数据，按上文清理命令处理即可。

> **发布前红队审查（第二批）修复** — 以下为本轮针对发布就绪性的审查结果，全部在 0.1.1 发布前落地。

### Fixed (P0)

- **BOM 构件缺失发布声明（使用方 import 即失败）**：`streammq-bom` 此前未声明任何发布插件，
  `excludeArtifacts` 排除清单也不含它——发布流水线不会上传 BOM，使用方 `import` BOM 时
  Central 上根本不存在该坐标。本轮为 BOM 显式声明发布插件并纳入发布清单（P0-1）。
- **`@{jacoco.argLine}` 字面量透传致 fork VM 崩溃**：surefire/failsafe 的 `argLine` 采用
  `@{jacoco.argLine}` 延迟绑定，但未引入 jacoco 的模块（`streammq-test`、`streammq-samples/*`、
  `streammq-benchmark`）会将该字面量原样当作 JVM 参数传给 java，触发
  `The forked VM terminated without properly saying goodbye`（Tests run: 0，BUILD FAILURE）。
  本轮在根 POM 声明空默认值 `<jacoco.argLine/>`，启用 jacoco 的模块由 prepare-agent 运行时覆盖。

### Fixed (P1)

- **创建 Topic 会向消费者投递一条 `body == null` 的占位消息**：`createTopic` 旧实现通过向业务 Stream
  XADD 一条 `__placeholder` 消息（依赖"Stream 首次写入自动创建"副作用）实现——该占位消息被所有消费者
  当作真实消息投递，业务 handler 中直接 NPE。本轮改为独立注册表 Set（`streammq:{ns}:meta:topics`）登记
  Topic 元数据，业务 Stream 仍由首次真实发送自然创建，两者解耦（P1-1）。
- **`/actuator/streammq/stats/{group}/{topic}` 是"永为空 map 的死端点"**：旧实现只查询 Redis 中不存在的
  统计 key，任何环境都返回空。本轮新增进程内统计登记表 `RuntimeStatsRegistry`，消费成功/失败
  （`DefaultMessageProcessor`）与重试/死信（`DefaultRetryAndDlqHandler`）真实上报，管理端点聚合
  consumeTotal / avgConsumeMillis / retried / dlq / pendingCount 等字段（P1-3）。
- **`updateGroupConfig` 只写配置、不作用于运行态**：旧实现把组配置写入 Redis Hash 即返回成功，但容器
  运行态从不读取——运维以为已暂停/扩容，实际毫无效果。本轮改为逐 key 真实运行时变更
  （paused → 暂停/恢复容器、inflightCapacity / pausedSleepMillis 等 → 调用对应 setter），
  不支持或非法 key 显式拒绝并报告（P1-4）。
- **消费循环运行期持续失败对健康检查失明**：启动期失败已在上轮上报，但运行期连续失败（如 Redis 持续
  不可用）仍只打 ERROR 日志，健康检查一直 UP。本轮在 `ConsumeLoopTask` 增加连续失败计数：达到
  `RUNTIME_FAILURE_REPORT_THRESHOLD`（10 次）后经 `LoopFailureReporter` 上报（HealthIndicator DOWN），
  任一成功拉取即复位并调用 `LoopFailureCleaner` 清除健康条目——"持续失败 → DOWN、恢复 → UP"闭环（P1-6）。
- **`DefaultMessageProcessor.processMessage` 补 Throwable 兜底**：业务 `Exception` 已由内部管线路由，
  但逃逸的 `Error`（OOM / StackOverflowError）与路由本身二次故障（Redis 彻底不可用）此前会使消息
  从内存队列消失且无人认领。本轮新增 `handleFailure` 统一按 `RECONSUME_LATER` 路由，路由再次失败时
  消息留在 PEL 由 `PelClaimScheduler` 重投（P1-6/P1-8）。
- **InflightSink 泵捕获处理器异常后不再静默吞掉消息**：异常已 `poll` 出队，若只记日志，消息既不在内存
  队列也不在重试 ZSet，只能等 PEL 空闲阈值（默认 30s+）重投。本轮在泵的兜底分支显式调用
  `processor.handleFailure` 把消息交回重试/DLQ 路由（P1-8）。
- **并发消费启动排空"偷取"在途消息致重复投递**：并发消费（`consumeThreadMin>1`）时所有循环共享同一
  消费者名（`{group}-{instanceToken}`），主循环启动排空（`XREADGROUP id=0` 按消费者名读取整段 PEL）
  会持续读取其它并发循环刚读入、尚未 ACK 的在途消息——同一消息被两条循环各处理一次。全量复核实测：
  一次 3000 条压测中排空循环额外"恢复" 176 条，重启场景额外 160/74 条。本轮在 `hookDrainOwnPending`
  增加并发度门控：并发度 &gt; 1 时跳过启动排空，遗留未 ACK 消息由 `PelClaimScheduler` 按 group 级
  空闲阈值（默认 60s）认领重投，at-least-once 语义不变（P1-9，随 0.1.1 复核发现）。

### Fixed (P2)

- **`DELETE /actuator/streammq/topics/{topic}` 无防误删保护**：删除 Topic 是不可逆操作，任何持有
  admin 权限的调用方传错 topic 即永久销毁数据。本轮要求显式 `confirm={topic}` 匹配才执行删除，
  confirm 缺失或不匹配返回 400 且不下探后端（P2）。
- **管理端点暴露面说明修正**：`AdminEndpointExposureStartupWarner` 此前声称管理端点"不受
  `management.endpoints.web.exposure.*` 治理"，实际它是标准 Actuator `@WebEndpoint`——默认配置
  （仅暴露 health/info）下 `/actuator/streammq/**` 根本不可达，运维照旧文档配置会永久 404。
  本轮修正提示文案并补充 diagnostics MVC 端点（挂主端口、不受 Actuator 治理）的网络层限制建议。
- **默认 SPI 实现移除 `@Component`**：`LoggingProducerFilter` / `LoggingConsumerInterceptor` 不再依赖
  框架注解，可在纯 Java 应用直接 `new` 使用；Spring 应用中仍可注册为 Bean（Javadoc 同步更新）。

### Added

- **`RuntimeStatsRegistry`**：进程内运行时统计登记表（按 group/topic 维度），为
  `/actuator/streammq/stats` 提供真实数据源；随带并发安全（`LongAdder` 累加 + `AtomicLong` 耗时汇总）。
- **`streammq.admin.trust-forwarded-headers` / `streammq.admin.trusted-proxies` 配置项**：客户端地址可信
  策略——默认**不信任** `X-Forwarded-For`（该头完全由客户端可控，直接采用会让失败限流被一行请求头绕过），
  仅当端点部署在受控代理之后、且配置可信代理 CIDR 白名单时才解析 XFF 首值；`StreamMQProperties` 启动时
  校验 CIDR 合法性。`WebRequestAuthSupport` 相应新增 CIDR 校验/匹配与 Basic 凭据解析工具函数（安全默认值：
  fail-closed）。
- **CI 新增 `coverage` job（P3-13）**：仅针对已发布模块启用 JaCoCo 覆盖率门禁（LINE ≥ 30% / BRANCH ≥
  15%，防灾难性回退而非考核线）；提供 Redis service 运行 verify，让集成测试贡献覆盖率
  （redisson 实测：仅单测约 33% 行覆盖，含 IT 达 90%+）。
- **CI 新增 `staging-smoke` job（P2-8）**：发布预检——全部构件 install 到本地仓库（模拟 staging）后，
  以"使用方视角"最小工程 import `streammq-bom` 并编译引用公开 API，直接验证 BOM 与发布构件可解析。
- **发布流水线新增 japicmp API 兼容性门禁（P2-13）**：探测 Central 上一发布版本，非首个版本时对已发布
  模块做二进制/源码兼容对比，发现破坏性变更阻断发布（首个公开版本自动跳过）。
- 新增测试：`WebRequestAuthSupportTest`（CIDR 校验/匹配、Basic 解析、XFF 默认 fail-closed）、
  `RuntimeStatsRegistryTest`（维度隔离、平均耗时、并发上报）、`ConsumeLoopTaskTest`（持续失败上报阈值 /
  恢复清除闭环）、`StreamMQAdminEndpointTest`（Topic 注册表、delete confirm、运行时统计、组配置运行时应用）、
  `StreamMQActuatorEndpointHardeningTest` 新增 delete confirm 用例、`MessageSinkTest` P1-8 失败路由回归。

### Fixed (P0)

- **事务消息在非 StringCodec 默认编码下「只报成功、永不发布」**：新增
  `TransactionBinaryCodecIT`（以 Kryo 二进制默认 codec 运行）作为回归防护；此前
  `AbstractRedisIT` 一律显式 `StringCodec`，恰好掩盖了本缺陷。
  `TransactionScanner#casState` 等
  Lua 脚本用 `StringCodec` 读写 txstate Hash 的字段，而 Hash 本身由 `redisson.getMap()` 以
  **客户端默认 codec**（redisson-spring-boot-starter 默认为 Kryo 类二进制 codec，字符串 key/value
  带二进制前缀）写入。字段编码不一致导致 Lua `HGET` 永远 miss（返回 `MISSING`），
  `markCommit`/`markRollback` 据此静默返回——但 `executeInTransaction` 仍打印
  「Transaction committed」并把发送结果标记为成功。真实后果：目标 Stream 从未写入任何条目、
  半消息与 PREPARE 状态永久残留，消费端永远收不到事务消息。该缺陷只在默认 codec 非字符串时
  出现（用户恰好配置 StringCodec/JsonJackson 则不可见），具有极强的环境相关性。
  同类隐患一并修复（Lua 与 Java 侧 codec 统一为 StringCodec）：
  - txstate Hash：`TransactionScanner`（注册/提交/回滚/回查/降级/清理）、`TransactionCommitExecutor`
    （原子批置 COMMIT）、`TransactionRetentionSweeper`（保留期清理）的 `getMap()` 均显式
    `StringCodec.INSTANCE`；
  - 回查计数 Hash：`incrementCheckCount`（Lua `HINCRBY` 写入明文字段）与 `getCheckCount`/
    `removeCheckEntry`（`RMap` 读取/删除）此前编码不一致，导致 `maxCheckTimes` 有界回查永不触发；
  - 事务执行权锁与延时/重试转移 claim：`RBucket` 以默认 codec 写入持有者标识，Lua
    compare-and-delete 却以明文比对，导致锁/claim 永远释放不掉（仅靠 TTL 兜底）——
    `TransactionLockManager#tryAcquire`、`RetryScheduler`、`DelayMessageScheduler` 的
    `getBucket()` 均显式 `StringCodec.INSTANCE`。
- **诊断模块在普通应用上下文启动失败（`SlowConsumeAnalyzer` Bean 缺失）**：
  `StreamMQDiagnosticsAutoConfiguration` 的 `streamMQDiagnosticsService` 依赖
  `SlowConsumeAnalyzer`/`BacklogAnalyzer`/`DlqAnalyzer` 三个 `@Component` Bean，但三者位于
  `io.github.streammq.diagnostics` 包——普通应用（未额外 `@ComponentScan` 该包）永远扫不到，
  于是任意依赖诊断模块的应用在启动即抛 `NoSuchBeanDefinitionException`，且
  `streammq-sample-diagnostics` 的集成测试在完整 verify 之前从未真正跑过，缺陷被长期隐藏。
  修复：三个分析器改由自动装配显式 `@Bean` 注册（`@ConditionalOnMissingBean` 兜底，应用自行
  扫描该包时不会重复实例化），服务 Bean 通过它们完成装配。
- **`streammq-test` 发布构件存在无法解析的运行时依赖**：`StreamMQTestBase` 在运行期调用
  `RedisAvailability`，而后者所在的 `streammq-test-support` 同时满足两个致命条件——在
  `streammq-test` 中被声明为 `<optional>`（不传递），又被 `excludeArtifacts` 排除发布。
  结果是：外部用户引入 `streammq-test` 后会得到 `NoClassDefFoundError`，且**无法通过补依赖自救**
  （该坐标在中央仓库根本不存在）。
  本轮修复：`streammq-test-support` 纳入发布；`streammq-test` 对
  `streammq-test-support` / `streammq-core` / `slf4j-api` / `redisson` 改为可传递的普通 compile 依赖
  （测试框架与 `streammq-redisson` 仍保持 optional，交由使用方决定版本）。

### Fixed (P1)

- **消费者创建失败从此不再是静默故障**：消费循环在创建监听器失败时（Redis 认证失败、消费者组非法、
  配置错误）此前只打一条 ERROR 日志就退出——消费者在 `/actuator/streammq/groups` 仍然可见、
  健康检查仍然 UP。本轮新增 `LoopFailureReporter` 上报通道，容器登记失败原因并纳入：
  - `DefaultStreamMQListenerContainer#getConsumeLoopFailures()`
  - 健康检查（`HealthIndicator` 在存在启动失败时返回 DOWN，详情含 loopKey → 原因）
  - 管理端点总览 `status` 字段
  `start()` / `stop()` 会清空登记表，避免历史失败影响下一轮判定。
- **广播消费组累积可被观测**：新增 `RedissonStreamListener#countBroadcastGroups()`、
  sweep 汇总日志（`Swept N stale broadcast group(s): remaining=M`），并通过管理端点总览的
  `broadcastGroups` 字段暴露。此前该数字只能靠直接查 Redis 才能看到。
- **执行器替换未同步给 `DefaultMessageProcessor`（潜在的"消费者静默不消费"）**：
  `DefaultMessageProcessor` 在构造时捕获执行器引用且字段为 `final`，而容器可在 INIT 阶段被
  `setConsumeExecutor` 换掉执行器——两者不一致时消费回调会抛 `RejectedExecutionException`。
  本轮把 `executor` 改为 `volatile` 并在 `MessageProcessor` 接口新增 `setExecutor`，
  容器替换执行器时先同步给协作类再关闭旧执行器（顺序颠倒会直接抛拒绝执行异常）。
- **`StreamMQTracingIT` 跨测试污染导致偶发失败**：所有 `@Nested` 测试类共享同一个 TOPIC、
  消费者组与消费者 Bean，上一测试的消息会在 `@BeforeEach` 的 `clear()` 之后才投递完成，
  混入当前测试队列。断言却依赖 `getReceived().get(0)` / `hasSize(n)` / `getLastMessageId()`
  等位置与数量——曾观测到期望 `hello-tracing` 却拿到 `topology-msg`。
  本轮改为按内容匹配（`awaitBody` / `traceparentsOf` / `messageIdOf`），消除 flaky。

### Fixed (P2)

- `DefaultStreamMQListenerContainer#setConsumeExecutor` 现在会关闭构造器字段初始化时创建的
  内部执行器（此前每注入一次泄漏一个），语义与 `DefaultStreamMessageTemplate#setAsyncSendExecutor`
  保持一致：谁创建谁关闭。
- `StreamMQCoreAutoConfiguration` 的 `ExecutorService` 参数补上 `@Qualifier("streammqExecutor")`
  （此前依赖 Spring 的参数名兜底匹配，与同类装配写法不一致）。
- `TokenAuthenticator` 修复长度预言机：与 `BasicAuthAuthenticator` 统一改为先 SHA-256 再常量时间
  比较（`MessageDigest#isEqual` 在长度不等时立即返回，直接比较原始字节会泄露 token 长度）。
  两者共用新增的包内私有 `SecureCredentialMatcher`，并在 Javadoc 中明确说明"摘要仅用于长度归一化，
  不是口令散列加固"。
- ACK 失败日志从 WARN 提升为 ERROR 并说明后果（消息留在 PEL 中，将在超过 PEL min-idle 阈值后被
  `PelClaimScheduler` 重投，消费端必须幂等）——此前这条日志完全看不出会引发重复消费。
- `PelClaimScheduler` 字段 Javadoc 修正：此前声称"触发 XAUTOCLAIM"，实际实现是
  `XPENDING` + idle 过滤 + 「XADD 副本 + ACK 旧条目」；并补充了大 PEL 下的恢复延迟特性说明。
- 删除 `StreamMQCoreAutoConfiguration` 中一段复制粘贴残留的孤儿 Javadoc。
- `TokenAuthenticator` / `BasicAuthAuthenticator` Javadoc 补充"缺少失败重试限流"与
  "管理端点挂在主端口、需在网络层限制访问来源"的安全边界说明。

### Added

- **`RedissonClientMissingFailureAnalyzer`**：缺少 `RedissonClient` Bean 时，把语焉不详的
  `NoSuchBeanDefinitionException` 替换为含完整依赖声明与配置示例的启动失败报告。
- **`RedissonStreamListener#countBroadcastGroups()`** 与管理端点总览的 `broadcastGroups` 字段。
- 新增测试：`TokenAuthenticatorTest`（含长度预言机回归用例）、
  `DefaultStreamMQListenerContainerTest`（执行器所有权、失败登记、INIT-only 约束）、
  `EnumsTest#doesNotContainMisspelledAlias`（守卫 `UNKNOW` 不得重新引入）。
- README / README.en 新增「广播消费的运维注意事项」与「消费者不消费时的排查路径」两节（中英同步）。

### Removed

- `LocalTransactionState.UNKNOW`（拼写错误的弃用别名，详见上方 Changed 条目）。
- `DefaultStreamMessageTemplate#executeInTransactionInline`（全仓库零引用的死代码，
  且其 Javadoc 描述的降级行为与 CHANGELOG 承诺、与实际抛异常的语义三方矛盾）。
- `streammq-kubernetes` 从 Maven Central 发布清单中移除（实验性预览、无模块依赖、
  核心的 `ConfigMapConfigRefresher` 默认实现为 no-op），避免在功能完整前就形成 API 兼容承诺。

### Changed

- **BOM 收敛**：`streammq-bom` 现在只管理 StreamMQ 自身构件 + `redisson` /
  `redisson-spring-boot-starter`，不再覆盖 Jackson / SLF4J / Micrometer / Fury / Protostuff /
  Spring Cloud Stream / Spring Integration / OpenTelemetry 的版本。
  原因：BOM 的 import 顺序通常在使用方的 `spring-boot-dependencies` 之后，即 StreamMQ 的声明会
  **静默覆盖**使用方的版本。对 redisson 这是有意的（README 快速开始片段省略版本号，需要 BOM 兜底）；
  但对 Jackson 等"用户大概率已在用、且与 StreamMQ 无关"的依赖，覆盖属于越权。
  已验证 reactor 全量构建（含 enforcer `dependencyConvergence`）通过。
- **项目版本 0.1.0 → 0.1.1**：见本节开头关于 `v0.1.0` 标签的说明。

### Fixed

- **Fury registration API**: exposed safe `register(Class<?>)` / `registerAll(Class<?>...)`
  methods and constructor-based registration so secure-by-default serializers are usable
  without accessing Fury internals. Serialization errors now point to the public API.
- **Template executor lifecycle**: `DefaultStreamMessageTemplate` now implements
  `AutoCloseable`, shuts down only executors it owns, and releases its internal virtual
  thread executor when a caller injects an external pool. Async sends fail fast after close.

- **Fury registration API**: exposed safe `register(Class<?>)` / `registerAll(Class<?>...)`
  methods and constructor-based registration so secure-by-default serializers are usable
  without accessing Fury internals. Serialization errors now point to the public API.
- **Template executor lifecycle**: `DefaultStreamMessageTemplate` now implements
  `AutoCloseable`, shuts down only executors it owns, and releases its internal virtual
  thread executor when a caller injects an external pool. Async sends fail fast after close.

### Security (P0)

- **Fury / JDK 反序列化 foot-gun 加固**：
  - `FurySerializer(false)` 构造与 `JdkSerializer.unrestricted()` 静态工厂均被门控为
    `-Dstreammq.security.allowUnrestrictedSerializer=true`，否则抛 `SecurityException`。
    缺省路径永远安全；用户必须显式声明"我已知悉 RCE 风险"才能关闭白名单。
  - `FurySerializer` 序列化/反序列化失败时携带"如何注册类 / 切到 Jackson"的可操作错误消息。

### Fixed (P0)

- **README QuickStart 与示例代码 API 错位**：README 之前示例使用 `StreamMessageTemplate`，
  而 canonical sample (`streammq-sample-quickstart`) 实际使用 `StreamMessageService`——
  用户首 5 分钟即遇编译/运行错误。README 现在明确推荐 `StreamMessageService` 门面，
  并对 `StreamMessageTemplate` 标注"高级用法"，避免首次接触的认知割裂。
- **README benchmark 数字与方法学声明**：将"269,760 ops/s"等被确认破损的基准从文档移除，
  并显式承认 v0.1.0 之前曾发布过方法学有缺陷的数字（"we openly acknowledge..."）。
  下次发布时由 CI `benchmark.yml` 任务重新生成。
- **PRD 与 README 版本冲突**：`docs/01-PRD.md`（仍标注 "v0.1-draft 起草中"）移至
  `docs/historical/01-PRD-v0.1-draft.md`，避免新人先读到过期文档。
- **JDK 21 要求未文档化**：README 新增"为什么要求 JDK 21"一节，明确这是有意为之（虚拟线程、模式匹配），
  而不是疏漏。

### Changed (P1)

- **God class 进一步拆分**：
  - 新增 `ListenerContainerFilterCoordinator`（filter/interceptor 链管理）、
    `ListenerContainerMetadata`（元数据查询 / scheduler target 绑定），
    从 `DefaultStreamMQListenerContainer` 抽离。容器仍保留编排职责，但单文件 public 方法数从 41 降至 ~30，
    复杂度下降 25% 以上。
- **`DefaultStreamMessageTemplate` 仍为编排层**：暂未做二次拆分（已识别为 0.2.0 路线图项）。
- **`executeInTransaction` 明确为「缺失 Scanner 即快速失败」**：未注入 `TransactionScanner` 时抛出
  `TransactionException`（错误信息含如何启用 Scanner 的可操作指引），**不**再声称会降级为
  "同步本地事务 + 即时发送/回滚"。
  背景：CHANGELOG 曾承诺存在该降级路径，但代码里从未调用（对应实现 `executeInTransactionInline`
  是全仓库零引用的死代码），读文档的用户会误以为不配 Scanner 也能用。本轮删除死代码并统一为
  快速失败——对事务消息而言，静默降级为低一致性语义比直接报错危险得多（JVM 崩溃时半消息永久悬挂）。
- **MDC 跨虚拟线程透传修复**：`asyncSend` 现在捕获调用线程的 MDC 快照并在虚拟线程内恢复，
  修复 README 文档承诺 "MDC.put('traceId', 't-001'); template.asyncSend(message); traceId 自动透传"
  实际失效的问题。
- **`UNKNOW` 拼写错误彻底移除**：`LocalTransactionState` 只保留拼写正确的 `UNKNOWN`。
  背景：此前 `@Deprecated` 的 `UNKNOW` 别名以"兼容 0.0.x 早期用户"为由保留，但本项目从未发布过
  0.0.x（`git tag -l` 仅 `v0.1.0`），该理由不成立；更糟的是生产代码被迫用
  `"UNKNOW".equals(state.name())` 字符串比较来绕过 `-Werror` 下的弃用告警——一个编译参数在决定
  生产 API 设计。首个公开版本是移除它的唯一窗口，故本轮删除常量、字符串比较分支与相关 Javadoc。
- **重试次数硬上限**：`StreamMQConstants.MAX_SYNC_RETRY_TIMES = 16` 夹取 `retryTimes` 配置，
  防止 `Integer.MAX_VALUE` 等误配导致无限重试、业务线程阻塞数十分钟。
- **MessageId 碰撞修复**：`buildFailedResult` 使用 UUID 后缀，替代碰撞风险的 `currentTimeMillis() + "-0"`。
- **`syncSendBatch` 部分失败语义**：区分"单条失败"与"整批失败"，单条失败的 partial result
  正常透传；仅在重试耗尽时把所有消息标记为失败。
- **调度线程统一 daemon**：`TransactionScanner` / `DelayMessageScheduler` /
  `PelClaimScheduler` / `RetryScheduler` 的扫描线程全部设为 daemon，
  修复"JVM 因调度器非 daemon 线程挂死"的潜在问题。
- **Fury / JdkSerializer 错误消息可操作化**：序列化失败时附带"如何修复"指南。

### Added (P1)

- **英文 README**：`README.en.md`，覆盖所有主要章节，机械翻译为主、关键术语校对。
- **POM 修正说明**：README 顶部新增 "Why we require JDK 21" 章节。
- **`AuthenticatorStartupLogger`**：启动时若 `DenyAllAuthenticator` 处于激活态且 admin 启用，
  输出一行 INFO 提示用户如何注册其他 authenticator，避免 401 死锁。
- **集成测试跳过警告**：`AbstractRedisIT.setUpRedis()` 在 Redis 不可用时输出
  显眼 stderr 警告（之前是 `Assumptions.assumeTrue` 静默跳过）。

### Removed

- 无。

### Security Defaults

- `FurySerializer(false)` 与 `JdkSerializer.unrestricted()` 需要显式系统属性
  `-Dstreammq.security.allowUnrestrictedSerializer=true` 才会生效，否则抛 `SecurityException`。
  这是<b>默认安全</b>取向：缺省路径永远启用类白名单，用户必须显式声明"我已知悉 RCE 风险"才能关闭。

### Added

- 启动时管理端点暴露面 WARN：`AdminEndpointExposureStartupWarner` 在 `ApplicationReadyEvent` 阶段检测 `/actuator/streammq/**` 是否在主应用端口（不受 `management.endpoints.web.exposure.*` 治理），启用且未隔离时输出安全提醒；可通过 `-Dstreammq.admin.startup-warn=false` 关闭。
- Maven `maven-antrun-plugin` 在 `generate-test-resources` 阶段生成 `target/it-list.txt`：全项目 `*IT.java` 集成测试清单，作为 CI 工件 `integration-tests.txt` 上传，配套 verify tripwire 防止 Redis 静默失效。
- Spring Cloud Stream Binder 模块级 `package-info.java` 增加依赖与限制说明（Redisson 传递依赖、分区生产不支持、DenyAll 鉴权器默认）。
- `DefaultPerConsumerSpiResolver` 全局默认 `RebalanceStrategy` 回退路径：`streammq.rebalance.strategy` 配置（默认 `ConsistentHashRebalanceStrategy`）现真实生效——`@StreamMQConsumer` 注解未显式指定 `rebalanceStrategy` 时优先使用全局配置。
- 集成测试 `DefaultPerConsumerSpiResolverRebalanceTest`：覆盖三种回退路径（无全局 / 全局为 ConsistentHash / per-consumer 覆盖全局）。
- `TransactionLockManager` / `TransactionCommitExecutor` / `TransactionRetentionSweeper` / `TransactionMetricsRecorder` 四个事务协作类（拆分自 `TransactionScanner` god class），均可在隔离单元测试中独立验证。
- `CONTRIBUTING.md` 新增「Cutting a Release」章节：Central Portal 发布流程、首次人工 Publish 步骤、autoPublish 翻转 checklist、凭据配置、发布门禁。
- 4 个 SPI 默认实现，消除「无默认实现致 NPE」的 README 误导：
  - `NoopProducerFilter`（接受所有消息）
  - `LoggingProducerFilter`（按 tag/key 记录 DEBUG 日志）
  - `LoggingProducerInterceptor`（发送前/后/异常 INFO/WARN/ERROR 日志）
  - `LoggingConsumerInterceptor`（消费前/后/异常 DEBUG/INFO/ERROR 日志）
- LZ4 压缩 codec 真实现：`Lz4CompressionCodec` + `Lz4CompressionCodecFactory`（条件性注册，classpath 无 lz4-java 时降级为不可用）— 修正此前 Javadoc 漂移
- `StreamMQDiagnosticsService`（909 行 god class）拆分为 3 个独立 analyzer + 1 个 facade：
  - `SlowConsumeAnalyzer`（247 行）+ 单元测试
  - `BacklogAnalyzer`（211 行）+ 单元测试
  - `DlqAnalyzer`（294 行）+ 单元测试
  - facade `StreamMQDiagnosticsService` 缩为 220 行（薄壳，仅做依赖注入+委托）

### Changed

- 集成测试在无 Redis 环境统一自动跳过（含 Spring Boot 自动装配 IT），保证 `mvn verify` 在任意环境可复现
- 调度器（Retry/Delay/Transaction/PelClaim）SmartLifecycle 相位调整为先于消费容器启动、晚于其停止
- 事务消息：未注入 TransactionScanner 时快速失败（不再提供"先投递再回滚"的假事务回退路径）
- 诊断 REST 报告增加 locale-neutral `code` 字段，message 文本改为英文；移除伪造的线程池活跃度指标
- `OrderProducer`（streammq-sample-quickstart）从 308 行精简为 4 个核心方法：保留 `createOrder` / `createOrderWithBuilder` / `createOrderAsync` / `createOrdersBatch`；更复杂的 `oneway / callback / metadataBuilder / timeout-retry` 模式迁移至 `streammq-sample-interceptor` 与 `streammq-sample-delay`。
- 文档导航：`docs/02-architecture.md` / `03-functional-design.md` / `04-detailed-design.md` 移入 `docs/historical/`，README 文档导航表只保留 `docs/01-PRD.md` 与 Javadoc，提示历史设计稿仅供考古。
- README「环境要求」新增提示：`mvn verify` 需要本地 Redis（`localhost:6379`），无 Redis 时 IT 自动跳过，CI 通过 Docker service 提供。
- 7 个工具类改用 Lombok `@UtilityClass` 注解，删除手写 `private Xxx() {}`：StringUtils / CollectionUtils / SpiResolver / BodyTypeResolver / WebRequestAuthSupport / StreamMQKeys / MdcKeys
- `ConsumeLoopTask` 的 `PAUSED_SLEEP_MILLIS` / `BROKER_ERROR_BACKOFF_MILLIS` 从 `static final` 改为实例字段（构造器注入），允许 `streammq.consumer.paused-sleep-millis` 与 `streammq.consumer.broker-error-backoff-millis` 真正生效

### Removed

- 移除未生效的配置项：`streammq.event.*`、`streammq.thread-name-prefix`、`streammq.tracing.collector`、`streammq.tracing.trace-topic`（自定义 TraceCollector 请直接声明 Spring Bean）
- 移除 Kubernetes 模块中无控制器的 StreamMQTopic / StreamMQConsumerGroup CRD 与模型
- 移除不可拉取的默认镜像名；`spec.image` 现为必填
- 移除 README 旧的「827 单测 / 197 IT」硬编码数字（不实）；改为「≥780 单测（mvn test 实际产出）+ IT 由 CI tripwire 保证 ≥80 实际执行」。

### Fixed

#### 本轮修复（2026-08-27）

- **`streammq.rebalance.strategy` 全局配置此前被静默忽略**——`DefaultPerConsumerSpiResolver.resolveRebalanceStrategy` 在注解未指定时硬编码回退 `AverageRebalanceStrategy`，导致配置了 `ConsistentHashRebalanceStrategy` 的用户实际拿不到一致性哈希分片。本轮将全局配置提升为第一优先级，添加 3 个回归测试。
- **`TransactionScanner` 仍为 god class**——本轮拆出 4 个协作类（lock / commit / retention / metrics），共 508 行从 1238 行主类中下放；保留编排职责（生命周期、注册、扫描循环、状态机迁移）。
- **`docs/02-04` 仍被 README「文档导航」表推荐**——已确认与代码脱节，本轮移入 `docs/historical/` 并降级导航，避免新人先读过期设计稿。
- **Quickstart 示例 `mvn verify` 是否需要本地 Redis 未在 README 提示**——本轮在「环境要求」节加粗提示。
- **README:644 链接断链**——「完整配置参考」链接指向已移走的 `docs/02-architecture.md`，本轮改为 `docs/historical/02-architecture.md`（V1.0 起草稿，仅供考古）。
- **LZ4 文档漂移**——`CompressionCodec` Javadoc 声称 `Lz4CompressionCodec` 是 built-in 但实际不存在，本轮修正文档说明 LZ4 需用户自行引入依赖并注册 Bean。
- **2 个配置键被静默忽略**（config 未用项审计发现）：
  - `streammq.producer.max-message-size` 此前从未读入 `ProducerConfig`，本轮在 `StreamMQCoreAutoConfiguration.streamMQTemplate` 注入到 `ProducerConfig.maxMessageSize` 字段
  - `streammq.cloud.k8s.config-refresh-enabled` 此前对 `ConfigMapConfigRefresher` Bean 无效，本轮加 `@ConditionalOnProperty` 门控
- **`streammq.consumer.paused-sleep-millis` / `streammq.consumer.broker-error-backoff-millis` 真实生效**——`ConsumeLoopTask` 改用实例字段 + 构造器注入；`StreamMQListenerContainerAutoConfiguration` 从 `properties.getConsumer()` 注入到 `tuning`，再传到 `ConsumeLoopTask` 构造器
- **`streammq.producer.retry-times` 真实生效**——`ProducerConfig` 新增 `retryTimes` 字段（默认 `DEFAULT_SYNC_RETRY_TIMES`），`StreamMQCoreAutoConfiguration.streamMQTemplate` 注入；`DefaultStreamMessageTemplate.syncSend` 在调用方未传 `SendOptions` 或 `SendOptions` 使用默认值时优先采用 `defaultConfig.getRetryTimes()`
- **LZ4 文档漂移已修复**——Javadoc 声称 `Lz4CompressionCodec` 是 built-in 但实际不存在，本轮通过 `Lz4CompressionCodecFactory`（反射检测 classpath）真正实现条件性 LZ4 codec：classpath 有 `org.lz4:lz4-java` 时启用，否则 `tryCreate()` 返回 null、`isAvailable()` 返回 false

#### 发布前最终审计修复（本轮）

##### 核心修复

- MessageConverter SPI default 方法互递归修复：最小实现不再触发 StackOverflowError，3 参 `fromStreamFields(Map, Class, String)` 为唯一必须覆写点
- BodyTypeResolver 支持泛型基类继承链类型变量替换（`class Child extends Base<T>` 不再静默降级为 String）
- 容器状态机竞态消除（生命周期并发迁移下的非法状态跳变，STARTING→RUNNING 仅在合法迁移时成立）
- InflightSink 泵健壮性加固（处理器任意 Throwable 不再杀死泵线程；泵 Future 按 loopIndex 全量登记可取消；dispatch 自旋尊重 running 标志）
- 时钟源统一（cleanupStaleGroups 与写入侧一致使用 Redis TIME，免疫实例时钟偏移误删活跃 peer）
- Selector / ConsumerFilter / ProducerFilter 求值异常显式向上传播并进入失败路径（重试/DLQ），彻底消灭"求值失败被降级为放行/丢弃"的静默语义
- own-PEL 启动排空的 XREADGROUP 历史 ID 由非法 `-` 修正为 `0-0`（此前每次排空必然 ERR 并 WARN 刷屏）
- 广播消费者暂停期间持续心跳（暂停超过组回收阈值不再导致组被回收、恢复后全量重放）
- 事务回查器按 group 串行化并带超时看门狗（单个慢/挂死 checker 不再拖死全部事务组扫描）

##### Metrics 装配

- Micrometer 指标自动装配排序修复：从父配置嵌套 @Import 中移除，改经 `.imports` 排序在 Boot 注册 MeterRegistry 之后求值——标准 Boot 应用中指标 Bean 此前静默缺失

##### K8s 集成修复

- 循环依赖根因修正：ConfigMapConfigRefresher 自身实现 StreamMQConfigRefresher，工厂方法创建期 getIfAvailable 会把"创建中的自己"当候选；改为 ObjectProvider 延迟到首次 refresh 回调解析
- liveness/readiness 探针端点与 HealthIndicator、GracefulShutdownHandler 正式注册为 Bean（此前为从未装配的死代码，照文档配探针将永久 404）
- HpaAutoScaler 必需注入的 HpaMetricsProvider 补充默认 Bean 注册
- gracefulShutdownTimeoutMs 配置值真实生效（移除 1000ms 硬上限）；CRD 清单收敛为已实现的 StreamMQCluster；Operator 支持 watch-namespaces 定向与镜像漂移调和

##### PEL 恢复

- retry/DLQ 流 PEL 认领恢复（新增 RETRY/DLQ 两类认领目标：滞留条目尾部复制重投或超限转投 DLQ，实例崩溃重启后不再永久搁浅）

##### close 语义

- `RedissonStreamProducer.close` 语义修正：注入的外部执行器不再被 awaitTermination 空等或 shutdownNow 强杀（所有权归提供方），内部创建的执行器照常回收

##### 安全加固

- GZIP 解压上限（解压炸弹防护，超限受控失败进入毒丸隔离路径）
- Trace 数据 MAXLEN/TTL 约束（追踪存储不再无限增长）
- 移除安慰剂配置项 `streammq.access-key` / `streammq.secret-key`（从未参与任何鉴权逻辑，存在误导性）
- 管理 API 加固（topic/group/messageId 入参校验、DLQ requeue Lua 原子化、group config 写入配额、未知子路径返回 404）
- Spring ExecutorService 注入条件收窄为命名 Bean `streammqExecutor`（不再吞并用户无关的 ExecutorService Bean）

##### 文档勘误

- 配置元数据勘误（删除幻影键提示、namespace 默认值与 DLQ 策略默认标签对齐代码事实）
- SECURITY.md Fury 序列化安全默认描述与代码对齐（默认强制白名单）；NOTICE 底层依赖表述修正（Netty）
- README 测试数量改为可复现口径、对比表 Kafka 两处错误修正、补齐 diagnostics 治理/JMX/追踪开关矩阵说明
- 一键演示脚本重写：真实发送消息并在超时未消费时非零退出；示例工程死配置键修正
- CHANGELOG 重复 `[0.1.0]` 头合并；CONTRIBUTING 增加 DCO 签署要求；PR 模板乱码行修复

##### 工程/测试强化

- 新增零依赖叶子模块 `streammq-test-support`：RedisAvailability 以 PING/+PONG 协议握手探测并从 core 生产构件迁出；live-Redis 集成套件更名为 `CoreRedisIntegrationIT` 归入 failsafe
- 基准测试加入 JMH `Blackhole` 消费防 JIT 死码消除；flushdb 增加 `-Dstreammq.benchmark.allowFlush=true` 防误删守卫；新增手动触发 benchmark 工作流
- 测试强化：重复投递检测、故障注入用例、CI 集成测试数量下限 tripwire

#### streammq-redisson（既有修复）

- 并发消费组新增 PEL 启动排空 + PelClaim 认领覆盖，修复实例崩溃后消息永久滞留 PEL 的问题
- 毒丸消息逐条隔离进入 DLQ，不再拖垮整批已投递消息
- 延时消息改为「先写 payload 后写调度」+ 批量失败回补 ZSet，消除两处崩溃丢消息窗口
- 事务消息引入执行权锁（SETNX+TTL）串行化发布临界区，消除 COMMITTING 状态双实例重复发布
- PelClaim DLQ 分支调整为「先写 DLQ 后 ACK」，消除崩溃丢失窗口
- 同步发送仅在"确定未送达"的异常上重试；超时后已确认成功的结果直接返回，避免模板重试导致重复消息
- 重试/DLQ 调度改为单原子批次写入并附带 payload TTL；二级 DLQ 路由失败时保留 PEL 不再静默丢弃
- Rebalance 信号量初始化修复（此前许可从未初始化、注册期误占用）
- 延时消息补齐 maxMessageSize 校验

#### streammq-spring-boot-starter（既有修复）

- 修复 Micrometer 指标自动装配失效（未注册为顶层 AutoConfiguration 导致排序失序）
- AOP 代理消费者的注解解析改用 target class，修复代理 Bean 无法注册消费的问题
- `@StreamMQDlqConsumer` 支持 `${}` 占位符解析

#### streammq-tracing-opentelemetry

- 修复异步发送场景 Producer Span 泄漏（跨线程 ThreadLocal 配对失效），改为有界消息级注册表
- 实现 OTLP gRPC 导出器：配置 `otlp-endpoint` 即构建真实 SDK 导出链路（此前仅 no-op 且静默忽略端点配置）
- 消费 Span 增加 makeCurrent 作用域，业务侧 `Span.current()` 可正确挂接

#### 其他模块（既有修复）

- Kubernetes 控制器 phase 由 Deployment 就绪副本推导（对齐 CRD enum）；HPA 无指标时 fail-closed；扩缩容结果持久化到 CR spec；Redis 密码支持 SecretKeyRef；模块默认关闭并标注实验性
- 诊断积压探针改用 XPENDING 总数形式，消除 >1000 条时的静默截断
- 测试工具 flushdb 增加 `-Dstreammq.test.redis.flushAllowed=true` 本地模式守卫；Embedded Redis 更名为 ContainerizedRedisServer 并前置 Docker 可用性检查

#### 红队审查修复（本轮）

##### 投递可靠性 / 事务 / 调度

- **事务执行权锁补上 TTL**：`SETNX` 此前未设置过期时间，持有实例在提交临界区崩溃会导致事务永久卡死在
  COMMITTING、已提交业务消息永不投递；现默认 TTL 30s（`DEFAULT_TX_LOCK_TTL_MS` 真正生效），锁释放改为原子 compare-and-delete，
  不再误删接管者持有的锁
- 事务回滚路径补齐执行权锁（此前仅提交路径持锁），消除回滚 XDEL 与转投 XADD 的竞态
- 事务半消息缺失（HALF_MISSING）不再静默记为 COMMIT；降级为 UNKNOWN 走有界回查后按 ROLLBACK 安全终结并输出 ERROR
- 重试/DLQ payload Hash 键加入 `topic:group` 段：Redis Stream Entry ID 仅单流内唯一，
  旧键在多 Topic 并发重试时会跨 Topic 相互覆盖造成消息错投
- PelClaim 认领增加三重保护：空闲阈值默认 30s→60s（大于消费超时+宽限期）、目标级分布式互斥锁（消除多实例重复重投）、
  ORDERLY 目标认领前检查分片锁活性（正在处理的消息不再被抢走复制）
- 广播模式停止不再销毁消费者组：改为「心跳 + 僵尸组回收」模型——优雅停机不丢 PEL、重启从原位点继续（不再全量重放历史），
  崩溃实例的组由回收任务在心跳超时（10 分钟）后清理
- 自定义延时批次失败现在与分级路径一致地回补 ZSet 并复位批计数（此前整批消息因 ZREM 已生效而丢失）
- 延时 payload Hash 写入时附带 7 天 TTL；孤儿清理新增反向方向（无调度引用的 payload 主动删除），
  消除"调度器 ZREM 后崩溃 → 消息丢失 + 孤儿 Key 永久累积"
- 事务终态字段引入保留期维护（默认 7 天）+ 孤儿半消息清理，txstate Hash / half Stream 不再无限增长
- retry 流不存在时 readGroup 的 ClassCastException 现被识别为流缺失并自愈重建组（此前每轮拉取失败死循环）
- 广播消费者组标识改为容器级随机值：同一 JVM 内多个容器实例不再共享组名导致消息仅投递其一
- 修复 OpenTelemetry 自动装配两处启动期缺陷：`OpenTelemetry.noop()` 强转 `OpenTelemetrySdk` 必抛
  ClassCastException；自动装配类未使用 `@AutoConfiguration` 导致与用户同名 Bean 冲突
- 修复 AOP 代理场景下 @StreamMQDlqConsumer 元注解 Bean 在监听器注册时被误强转为
  @StreamMQConsumer 导致的 ClassCastException（DLQ 消费者此前无法经代理注册）
- 集成测试首跑暴露并修复：诊断报告断言未跟随 locale-neutral 文案、CoreRedisIntegrationIT 误用双参
  fromStreamFields、TracingSampleIT 未隔离 DemoRunner 启动事件

#### streammq-spring-boot-starter

- 管理 REST API 路由重构为「单操作 + 路径分发」模型，实际路由与 README 文档表完全一致；
  同时消除了多个无参 GET 操作在同一谓词上的歧义注册
- 新增独立 `streammq.admin.enabled` 开关，与 `streammq.health.enabled` 解耦

#### 其他

- `@StreamMQConsumer#consumeThreadMin/Max` 曾标记为 `@Deprecated`；第二轮审查中已恢复为真实生效的并发消费循环数（见上方"发布前红队审查第二轮修复"）
- FurySerializer 新增类注册白名单构造参数并在文档中给出反序列化安全建议；fury/protostuff 依赖改为 optional，
  不再强制传递给所有下游用户
- 异步发送改用专用虚拟线程执行器，不再占用 ForkJoinPool.commonPool
- GZIP 压缩遇非 Base64 body（PassThroughMessageConverter 组合）时跳过压缩而非抛异常中断发送
- DLQ 失败策略分发异常时保留 PEL 不再 drop+ACK（死信最后一副本不可丢）
- MAXLEN 裁剪后的 PEL 条目现在 ACK 移除并 WARN 提示，不再永生滞留积压统计
- RetryScheduler.stop() 关闭线程池（与其它调度器一致）；消费容器 stop 先取消拉取循环再注销组管理器、
  start 复位 pause 状态、动态注册幂等防重复消费循环、背压处理线程纳入生命周期管理
- 心跳写入与超时判定统一使用 Redis 服务器时钟（TIME），免疫跨主机时钟偏差；Rebalance 分配行随实例离场清理
- ConsumeContext#consumerName 返回真实 Redis 消费者名、shardId 按实际分片公式计算（此前返回虚构值）
- quickstart 示例补充 spring-boot-maven-plugin，支持 `java -jar` 直接运行
- 发布流水线增加测试门禁 job（verify 通过才允许 deploy）；GitHub Release 附带全部已发布模块构件

#### 发布前红队审查第二轮修复

##### P1 正确性 / 安全

- **DLQ 重试计数往返丢失修复**：`__dlqRetryCount` 以顶层 Entry 字段写入后经 converter decode→encode
  往返即丢失，策略层计数恒为 0 —— `LimitedRetryDlqFailureStrategy` 无限重试、`SecondaryDlqFailureStrategy`
  二级死信永不可达。现保留字段（`__` 前缀）解码时捕获进用户属性并随 props JSON 往返持久化；
  新增端到端回归测试 `DlqRetryRoundtripIT`
- **DLQ 重试调度作用域错配修复**：`scheduleDlqRetry` 此前把调度条目写入 `retryZSet(ns, dlqReg.topic, group)`，
  而扫描目标仅覆盖业务注册维度——条目落在无人扫描的 ZSet 上，DLQ 重试永不发生。
  现统一以 `{group}:{group}` 维度写入，且 DLQ 注册自动登记对应扫描目标
- **SQL92 用户属性过滤修复**：解码后全部属性落在用户属性表而 `PropertyExpression` 只读系统属性表，
  导致按文档示例使用 SQL92 的消费者静默零投递；属性查找现回退用户属性。新增回归测试 `Sql92UserPropertyIT`
- **JdkSerializer 反序列化加固**：内置 JEP 290 类名白名单过滤器（目标类型 + JDK 基础类型 + 数组展开 +
  深度/引用/字节上限），反序列化前拦截未知类；第三方类型经 `addAllowedClasses` 显式放行，
  另提供 `unrestricted()` 迁移逃生口（文档标注风险）
- **FurySerializer 默认翻转为类注册白名单（secure-by-default）**：未注册类直接拒绝；
  可信环境可显式 `new FurySerializer(false)` 关闭
- **Retry/Delay 转移崩溃窗口消除**：旧实现「ZREM 先行、XADD 后行」，进程在转移中途死亡即永久丢消息
  （孤儿 payload 还会被清理任务删除）。现改为 per-msgId 执行权 claim（SETNX+TTL，崩溃可接管）+
  「XADD + DEL payload + ZREM」单原子批，批失败整体不生效、entry 留存重试，任何时刻崩溃均不丢消息
- **顺序消费分片锁有界等待**：无限期 `lock.lock()` 在持有者挂死时造成分片永久停摆与线程堆积；
  现默认 5s 获取超时，超时转 RECONSUME_LATER 重投（顺序性仍由锁串行保证）

##### 功能落地 / 行为修正

- **`consumeThreadMin/Max` 从占位符变为真实并发旋钮**：此前适配层从未读取（Spring Cloud Stream 的
  `concurrency` 映射后被静默忽略）。现为 CONCURRENT 集群消费提交多条读循环（共享 consumer name，
  XREADGROUP 原子分配互不相交），index 0 独占 PEL 启动排空避免重复处理；取消语义按注册前缀完整回收。
  新增回归测试 `ConcurrentConsumeIT`；注解除去 @Deprecated 并更新文档
- 属性编码键冲突改为系统属性优先（业务同名用户属性不再静默篡改 SDK 内部元数据）
- Tag 选择器多标签 `&&` 表达式（一条消息仅携带一个 tag，语义永假）构造时 fail-fast 报错并给出替代建议
- SQL92 解析器：括号嵌套深度上限 64（超深嵌套以受控 IAE 失败而非 StackOverflowError 逃逸）、
  括号必须闭合（strict 模式契约兑现）、`build()` 由 fail-open 改为 fail-closed、
  数值比较优先 long 精确比较（64 位 ID 不再被 double 化失真）、NaN/Infinity 不匹配任何区间比较
- PassThroughMessageConverter 对 byte[] 以 Base64 无损编解码；其它非字符类型 fail-fast 抛出
  SerializationException（此前 toString() 静默写成对象地址串损毁数据）
- 消息 Entry 新增 `originTopic` 字段（Default 转换器）并在解码回填 topic：重试流/DLQ 流中的消息可溯源原始 Topic
- DEFER 与消费失败重试预算分离：DEFER 轮次不再递增 retryTimes、不触发 MAX_RETRY 进 DLQ（无上限由业务自控）
- 事件总线：close() 后 publish 不再向调用方抛 RejectedExecutionException；订阅者异常日志 DEBUG→WARN
- 发送路径命名校验补齐：topic 含 `:`/`*`/`{`/`}`/空白在 Producer 发送侧与事务半消息注册侧同样拒绝
  （Redis Cluster Hash Tag 定界符会导致 Key 家族强制同 slot 热点）
- BasicAuth 凭据比较改为 SHA-256 摘要常量时间比较，不再泄露密码长度

##### 测试 / 工程

- 新增回归测试：DlqRetryRoundtripIT（二级 DLQ 有限轮可达）、Sql92UserPropertyIT（用户属性匹配）、
  ConcurrentConsumeIT（4 循环 60 条消息不丢不重）、JdkSerializer 过滤器三例、SelectorParser 深嵌套/未闭合括号例、
  ManagementAuthenticatorDefaultTest（无 Bean 时装配 DenyAll 且拒绝访问）
- 消费吞吐基准方法学修正：持续灌数线程保证有货可读、补上字段转换与 XACK、消除空读 RTT 冒充吞吐；
  main() fork 参数与注解对齐消除结果来源歧义（README 相应章节已标注旧数字作废）
- streammq-test 对 streammq-redisson 的依赖改为 optional，不再强制传递适配层实现
- Kubernetes Operator 自动装配增加 fabric8 classpath 守卫（依赖缺失时优雅跳过而非 NoClassDefFoundError）
- ConsumerFilterResolver 出厂默认实现 `ReflectiveConsumerFilterResolver` 落地（消除零实现 SPI）
- CONTRIBUTING 默认值表修正（DenyAll/Noop/LogAndDrop/ConsistentHash）、@ExtendWith 示例更正、模块树补全

#### 结构重构（第二轮，行为等价）

- **DefaultStreamMQListenerContainer God class 拆分（1929 → ~1560 行）**：新增三个协作类——
  `RegistrationStore`（注册表与 per-consumer 策略缓存的唯一状态载体）、
  `MessageProcessor`（单条消息消费管线：过滤器/拦截器检查、DLQ/顺序/并发三类分发、
  超时取消与宽限期、指标记录）、`ContainerSupport`（共享小工具）；
  容器保留生命周期与读循环编排职责，类文档同步更新协作图
- **发送 API 收敛到 SendOptions**：Template/Service 每个模式仅保留规范形，
  删除 timeout/retry/callback 伸缩重载；六个 service 子接口合并为单一门面
  （601 行转发层 → 130 行）；topic 形态统一 MessageMetadataBuilder（内联超时重试）；
  `SendOptions` 补 defaults()/of()/equals/hashCode
- **ListenerConfig/ListenerRegistration 双建模合并**：Registration 成为唯一持有模型
  （吸收 consumerName/retryMode/converterInstance），ListenerConfig 降级为
  `from(reg, retryMode)` 单点派生视图；冗余 broadcast 标志删除；
  per-consumer 校验集中到注册构造器；容器 createConsumerFor 一行派生
- **parent↔BOM 版本属性 CI 守卫**：新增 guard job 校验两份 POM 共同声明的依赖版本一致、
  BOM streammq 版本与根 `<version>` 一致

#### 接口化与线程模型统一（第三轮重构）

- **容器协作组件全部接口化**：RegistrationStore / MessageProcessor / ContainerStateMachine /
  ConsumerTuning / PerConsumerSpiResolver / ConsumeLoopSupervisor / ListenerRegistrar /
  ConsumerGroupManagerFactory / SchedulerTargetBinder 均拆分为「接口 + Default 实现」，
  容器与组件之间只依赖接口——用户可对任一协作对象提供自定义实现
- **线程模型统一规范化**：
  - 移除全部 `Supplier<ExecutorService>` 包装，改为直接注入 `ExecutorService`
  - 移除库内散落的 `Thread.ofVirtual(...).name(...)` / `new Thread(r, name)` 手工建线方式
    （模板/事件总线/生产者默认统一虚拟线程池；调度器触发器为标准单线程
    `newSingleThreadScheduledExecutor`）
  - 执行器所有权规则：内部创建的池在 close/stop 时关闭；外部注入的池由提供方管理生命周期
  - `RedissonStreamProducer#setAsyncExecutor` / `DefaultStreamMessageTemplate#setAsyncSendExecutor` /
    `AsyncStreamMQEventBus(ExecutorService, boolean)` / `DefaultStreamMQListenerContainer#setConsumeExecutor`
    支持注入外部执行器（仅 INIT 状态可换）
- **Spring 装配**：新增 `streammqVirtualExecutor` Bean（`@ConditionalOnMissingBean(ExecutorService.class)`，
  用户注册任意 ExecutorService Bean 即全局覆盖）；容器 / 模板 / 事件总线自动装配均注入该池

## [0.1.0] - 2026-08-08

### Added

- **注解驱动消费** — `@StreamMQConsumer`, `@StreamMQDlqConsumer`, `@StreamMQTransactionConsumer`
- **StreamMessageTemplate 编程模型** — 同步、异步、单向、批量、事务五种发送方式
- **集群消费 + 广播消费** — 支持 `ConsumeMode.CLUSTERING` / `ConsumeMode.BROADCASTING`
- **顺序消费** — 基于 ShardingKey 的分片顺序消费，保证分区内严格有序
- **事务消息** — 半消息 + 本地事务 + 回查机制，保证最终一致性
- **延时消息** — 18 级固定延时（1s ~ 2h）+ 任意毫秒自定义延时
- **死信队列** — 消费重试耗尽后自动进入 DLQ，支持二级 DLQ 与自定义失败策略
- **消息过滤** — Tag 表达式 (`TagSelectorFilter`) + SQL92 表达式 (`SqlSelectorFilter`)
- **消息压缩** — GZIP 压缩编解码器，可配置压缩阈值 (`CompressionCodec` SPI)
- **背压控制** — InflightQueue 背压队列，防止消费过载
- **消费超时自动取消** — 可配置消费超时，超时自动中断并进入重试
- **Micrometer 指标** — 发送/消费/重试/DLQ 全链路指标，支持 Prometheus 暴露
- **链路追踪** — `TraceCollector` SPI，支持 traceId 透传与 MDC 日志
- **管理 REST API** — 消费组管理、Topic 查询、DLQ 操作、手动 ACK、触发重平衡
- **12 个 SPI 扩展点** — 序列化器、转换器、过滤器、拦截器、重试策略、重平衡策略、压缩编解码器、死信失败策略、管理鉴权器、链路追踪采集器
- **Spring Boot 3 自动装配** — `@EnableStreamMQ` 注解、ConfigurationProperties、Actuator 健康检查
- **BOM 模块** — `streammq-bom`，统一版本管理，可独立 import 到任意项目
- **测试工具包** — `streammq-test`，提供嵌入式 Redis、断言工具、Mock 工具、Testcontainers 集成
- **Spring Cloud Stream Binder** — `streammq-spring-cloud-stream-binder`，Spring Cloud Stream 集成
- **OpenTelemetry 链路追踪** — `streammq-tracing-opentelemetry`，分布式链路追踪集成
- **Kubernetes Operator** — `streammq-kubernetes`，CRD + Operator，弹性伸缩与配置热更新
- **消息拓扑可视化** — `streammq-diagnostics`，消息画像与流转拓扑

### Technical Stack

- Java 21, Spring Boot 3.3.5, Redisson 3.34.1
- Jackson 2.18.1, SLF4J 2.0.16, Micrometer 1.13.6
- JUnit 5.11.3, Mockito 5.14.2, Testcontainers 1.20.3
- Spotless + Google Java Format, Enforcer plugin, JaCoCo

### Documentation

- README with architecture overview, feature list, quick start guide
- V1.0 design documents: PRD, architecture, functional design, detailed design
- Design documents under `docs/` (PRD / architecture / functional / detailed)
- Configuration reference, deployment guide, FAQ

### Planned (V2.0, 规划中，尚未实现)

- Multi-backend abstraction (BackendProvider SPI) supporting Redis / Kafka / RabbitMQ / Pulsar
- Kafka backend implementation based on Kafka Client BackendProvider
- Cross-datacenter asynchronous replication (RPO ≤ 1s)
- Kafka wire protocol compatibility (native Kafka Client zero-code access)

> 注：以上 V2.0 规划项尚未实现，未包含在任何已发布版本中；详细规划见 README「路线图」章节。

[Unreleased]: https://github.com/HK-hub/StreamMQ/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/HK-hub/StreamMQ/releases/tag/v0.1.0
