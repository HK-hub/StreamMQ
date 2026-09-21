# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-09-10 — 首个 Maven Central 发布：持久化广播消费实例 + 安全默认与质量门禁

> **0.1.2 是 StreamMQ 第一个发布到 Maven Central 的版本**（0.1.0 / 0.1.1 均为内部迭代，从未对外发布，见下文。
> 版本号与 `pom.xml` / `streammq-bom` / 各模块一致，均为 `0.1.2`）。
> 本节同时包含发布前红队审查（第一轮 ~ 第七轮）的全部根因修复；审查依据 `docs/fullReview.md` 协议执行，
> 各轮结论与逐项处置见 [docs/REPORT.md](docs/REPORT.md)。R5 推送的真实 CI（run 35480290570）在
> `Verify (Integration)` 红：`ConsumerIT.ack_messagePelEmpty` 断言了异步 ack **未承诺**的同步语义——
> 已在 R6 按契约修复并新增流水线落地守卫（见下方"新增回归守卫"与 `docs/REPORT.md` §15.6）。**R7 终局门禁**
> `mvn clean verify -Djacoco.check.skip=false` 实测 **20/20 SUCCESS、1509 用例 0 失败/0 错误**
> （唯一跳过为 11 个 Cluster 用例：无 3 主集群时显式 skip 并打印启动指引），裁决 **GO 90/100**。
> R6 推送后真实 CI（run 35501791198，commit `271734c`）**全绿**：Guards / CVE gate / Formatting / Build /
> Test / Verify (Integration) / Staging smoke / Coverage report 全 success（OWASP 深扫无 NVD key 时按设计跳过）。

### Added

- **持久化广播消费实例身份（根因级修复：广播消费每次重启都是新编号）**：
  广播消费的每个实例使用一个独立 Redis 消费者组，组名由实例身份派生；旧实现每次重启都生成新身份，
  旧组成为僵尸组持续占用 Redis 内存，且重启期间产生的消息不会被补投。新增一整套机制从根本上解决：
  - `BroadcastInstanceRegistry` SPI（注册中心抽象）+ `RedisBroadcastInstanceRegistry`（单 Hash Key
    Lua 实现，Redis Cluster 安全）持久化每个实例的**租约**（`instanceId`/`host`/心跳时间）。
  - `BroadcastInstanceIdResolver` 把五级来源收敛为单一稳定身份：
    **显式配置 → 本地持久文件（对齐 RocketMQ `LocalFileOffsetStore`）→ Redis 注册中心回收同主机历史槽位 →
    Redis 注册中心分配 → 随机降级**。命中本地文件后仅做 1 次 claim 校验（裁决"身份被别的活实例占用"，
    防止两实例共用同一消费者名导致广播静默退化为集群消费）；注册中心不可达时跳过校验、直接信任本地文件
    （**Redis 停机期间身份不漂移、PEL 与消费位点保留**），覆盖物理机到 K8s 全部部署形态。
  - **同主机槽位回收 + 回收宽限期**（`streammq.consumer.broadcast-reclaim-grace`，默认 7 天）：重启后
    在宽限期内可回收同主机历史身份，**保住 PEL 与消费位点**，不重放也不丢失；超过宽限期才由清扫任务
    销毁消费者组释放内存。
  - 广播监听器心跳同时续租身份槽位；僵尸组回收与实例槽位清扫分工明确（前者 10 分钟、后者 7 天），互不破坏。
  - 配置项：`streammq.consumer.broadcast-instance-id`、`streammq.consumer.broadcast-instance-id-file`、
    `streammq.consumer.broadcast-lease-timeout`、`streammq.consumer.broadcast-reclaim-grace`。
- **内置高性能、安全序列化器：FlatBuffers 与 SBE（随 0.1.2 发布）**（与 Fury / Protostuff / JacksonJson 同属 SDK 内置实现，
  通过 `streammq.producer.serializer` 全限定类名选择，无需 SPI 注册）：
  - `FlatBuffersSerializer`：基于 FlatBuffers **FlexBuffers**（schema-less 动态格式）。逐字段零拷贝读取（直接基于
    ByteBuffer 偏移量寻址，不解析、不实例化任意类）、**免代码生成**、经反射处理任意 POJO；纯数据、
    **无反序列化代码执行面（安全）**。选用需添加 `com.google.flatbuffers:flatbuffers-java`（本项目固定 **24.3.25**，optional）。
  - `SbeSerializer`：基于 **SBE（Simple Binary Encoding，FIX 社区标准）**的信封模式——定长 8 字节消息头 +
    零解析拷贝；业务体以严格类型 Jackson 编码为 opaque 字节后，整体放入单一 `varData(payload)` 字段，
    `payloadLength()`/`getPayload()` 直接基于偏移量读取，**无 gadget RCE 面（安全）**。SBE 桩代码由
    `uk.co.real-logic:sbe-tool`（**1.18.0**，纯 Java 构建期生成，不进运行时）生成；运行时依赖
    `org.agrona:agrona`（**1.17.1**，optional）。
  - `streammq-benchmark` 新增 FlatBuffers / SBE 的序列化、反序列化与往返基准（吞吐 + 体积），与 Fury / Protostuff /
    JacksonJson / JDK 同台对比。
  - 安全与选型说明见 `SECURITY.md` 与 `docs/configuration-reference.md`。
- **文档**：新增序列化基准报告 `docs/benchmarks/serialization-2026-09-17.md`，并在中英文 README 的「性能基准测试」章节
  更新序列化吞吐表（新增 FlatBuffers / SBE / Protostuff 行与线长列）；`docs/configuration-reference.md` 补充
  `streammq.diagnostics.*` 模块前缀章节与延时 7 天上限说明。

### Fixed

- **顺序消费全局超时不生效**（发布前红队审查项）：注解 `orderlyConsumeTimeout` 的取值语义此前不可区分，
  导致全局 `defaultOrderlyConsumeTimeoutMillis` 的生效条件无法表达。现语义固定为
  **>0 = 覆盖全局；0 = 继承全局；<0 = 显式关闭**，且注解默认值即 `ANNOTATION_UNSET_LONG = -1`（显式关闭）。
  注意：因此**只修改全局键而不把注解显式写成 `0`，该全局键不生效**（`docs/configuration-reference.md` 已标注）。
- 广播组名在实例身份缺失时静默拼出字面量 `"null"`（真实缺陷）：`BroadcastGroupNaming` 的 `consumerName`/
  `effectiveGroup` 在 `instanceId` 为 `null`/空白时会生成 `{group}:{group}-null` 这样的组名，
  使所有未正确命名的广播实例塌缩进同一个 Redis 消费者组——广播语义静默退化为集群消费，且组名无法反解回身份、
  清扫任务永远回收不掉。现改为 **fail-fast**：`instanceId` 为空直接抛 `IllegalArgumentException`；
  监听器构造阶段也校验 `{group}-{instanceId}` 命名约定，启动期即失败而非把错误语义写进 Redis。
- 广播模式集成测试在真实 Redis 下确定性失败（非 flaky）的根因修复：`close()` 不再销毁组（保留 PEL）、
  广播组默认在 `NEWEST` 建组需先建组再发消息、组名带实例标识后注册表成员匹配规则同步更新。
- **DLQ 样例集成测试（`DlqSampleIT`）预存缺陷修复**：`@BeforeEach` 的 `cleanStreams()` 删除 topic 流会连带销毁 Redis 消费者组，
  监听器随后以 `NEWEST` 重建组、错过测试消息（`receivedMessages` 恒为 0）。现改为仅清理 retry/dlq 流，保留 topic 流与消费者组
  （组位点随消费推进、已 ACK 消息不重复投递，天然隔离）。
- **`CoreRedisIntegrationIT.consumer_throwException_failCount` 确定性失败的根因修复**（此前误判为偶发
  flaky）：`TestStreamMQListener.onMessage` 在持有锁的同步块内先 `countDown` 完成信号、再在锁外记录异常，
  测试线程 `waitForMessages` 仅等待 latch，存在"读到仍为空 exceptions"的竞态。现改为**先记录异常再发完成信号**
  （异常写入 happens-before countDown，经传递性保证测试读到），彻底消除竞态。此修复仅针对测试脚手架，
  不影响产品行为（产品侧异常路由依赖抛出的异常本身，与本测试内部计数无关）。
- **PEL 认领不再误伤活跃慢消费者**：并发消费的内联处理期间消费者无法自行心跳，"idle 超阈值"不等于"已死"。
  现以消费者组管理器**独立心跳线程**写入的 instances Hash 交叉判活（消费者名内嵌 instanceId，时间取 Redis
  服务器时钟），避免重复副作用与把已成功处理的消息误投入 DLQ。
- **顺序分片判活按注册项 Converter 的分片键字段名解析**（此前硬编码 `shardingKey`，切换 Compact/PassThrough
  Converter 后读到 null → 误判分片 0 → 活分片保护失效）。
- **按消费者 namespace 注册恢复目标**：此前 `@StreamMQConsumer#namespace` 覆盖全局值时，PEL/重试/DLQ 恢复
  会扫描错误的 Stream/Group 而静默失效。
- `TransactionScanner` 类缓存改为插入序 + 读写锁（消除访问序 `LinkedHashMap` 在读锁下结构变更导致的链表损坏）。
- `unregister` 释放 per-consumer handler（此前 `removeHandler` 无调用方，动态注销后无界泄漏）。
- `start()` 中途失败回滚 `STARTING → STOPPED`（此前容器卡死，且文档给出的补救路径不可达）。
- 5 个调度器 `start()`/`stop()` 同步化（消除 start 安装执行器与 stop 关闭执行器的竞态）。
- `TransactionCommitExecutor` 严格校验 Lua 返回值：非 `PUBLISHED`/`HALF_MISSING` 直接失败，不再乐观当作已发布。
- **`RBatch` 结构访问补全显式 codec（跨 codec 一致性回归修复）**：事务注册期的
  `RBatch.getScoredSortedSet(transactionCheckZSet)` 为链式调用、漏传 codec，导致 ZSet 成员按客户端全局 codec
  编码，而终态清理用 `StringCodec` 删除 —— 二者编码不一致使 ZREM 不匹配，**事务回查条目永久残留**。
  该缺陷由 `TransactionBinaryCodecIT`（Kryo5 客户端）捕获。另新增 `CodecExplicitnessTest` 架构守卫：
  静态扫描主源码，任何未显式指定 codec 的 Redis 结构访问直接失败，防止同类问题再次引入。
- **单条 ACK 改为有界异步流水线（性能 / 语义契约变更）**：`StreamMQListener#ack` 此前每条消息都**同步**等待一次
  Redis 往返（XACK），该阻塞 RTT 是消费吞吐的硬上限。现改为有界异步流水线（窗口 256）：窗口满时阻塞等待最老的
  ACK 完成以形成背压，停机时对在途 ACK 做有界排空。**注意语义变化**：`ack` 返回不再代表 Redis 端已确认；
  XACK 失败会记录 ERROR 且消息保留在 PEL 由认领调度器兜底重投（at-least-once 不变，消费端必须幂等）。
  需要"返回即已确认"时请用同步的 `ackBatch(List)`。Javadoc 已同步说明该契约。
- `FlatBuffersSerializer`：修复集合中 `null` 元素与 Map 中 `null` value 被静默丢弃的问题——改用 FlexBuffers
  `putNull()` 显式保留，避免反序列化后集合长度 / 键集不一致。
- **SBE / FlatBuffers 对畸形字节的边界校验与深度上限**：`FlatBuffersSerializer` 读取侧对 Blob/集合/映射的**声明长度**
  （单字段上限 = `min(streammq.producer.max-message-size, 64MB)`）与**嵌套深度**（64 层）做上限校验，
  畸形 / 深嵌套载荷抛 `SerializationException`，不再按声明长度盲目分配（旧实现可被 4 字节长度字段放大为 GB 级分配）；
  `SbeSerializer` 读取侧对信封头（`templateId`/`schemaId` 不匹配）、截断与声明长度（负数 / 超消息上限 / 超实际可读字节数）
  统一校验并抛 `SerializationException`，解析期运行时异常统一包装为 SPI 契约异常。
- **JDK 序列化器：含对象回引用（线格式 `TC_REFERENCE`）的合法载荷反序列化失败**：JEP 290 过滤器把 JDK 的
  「深度 / 引用计数」检查回调（`serialClass == null`、`arrayLength == -1`）误判为未知类并返回 `REJECTED`，
  合法载荷因此抛 `InvalidClassException: filter status: REJECTED`。现对 `serialClass == null` 返回 `UNDECIDED`，
  交由合并的深度 / 引用数 / 字节上限过滤器裁决——白名单边界不变，合法回引用载荷可正常反序列化。
- `streammq-benchmark`：补齐此前缺失的序列化依赖（`jackson-datatype-jsr310`、`protostuff-core`/`protostuff-runtime`，
  并为 `flatbuffers-java`/`agrona` 固定版本）。此前所有序列化基准在 `setup()` 阶段即因 `ClassNotFoundException` 失败
  （`streammq-redisson` 为 test scope 不传递其依赖，且 `protostuff`/`flatbuffers`/`agrona` 均为 optional 依赖）。

### Changed
- **依赖基线升级到安全版本线（R4-CVE）：Spring Boot 3.3.5 → 3.5.16（Spring Framework 6.2.x / Spring Data 3.5.x /
  Micrometer 1.15.x），Redisson 3.34.1 → 3.52.0，Spring Cloud Stream 4.1.3 → 4.3.3，Jackson 2.18.10 → 2.21.4，
  Netty 统一钉到 4.1.138.Final，AssertJ 3.27.7，commons-compress 1.27.1。目的：**发布构件带给使用方的依赖闭包
  不再包含任何 High/Critical 公告**（此前 13 个包的 High 公告只能靠 Boot 3.5 / Spring 6.2 线修复）。
  支持矩阵相应表述为 Spring Boot 3.3–3.5。升级过程修复了两处兼容问题（见下方 Fixed）。
- **CVE 门禁改为「发布闭包 + 严重度阈值」**：CI 每次 push/PR 生成发布构件的 CycloneDX SBOM（
  `bom-shipped.cdx.json`，从 4 个发布构件做依赖闭包裁剪，剔除 samples/未发布模块与 provided/optional 面），
  用 `osv-scanner`（OSV 库，无需密钥）扫描：任意 CVSS ≥ 7.0 公告阻断构建，<7.0 打印并随完整 JSON 报告上传为
  构建产物；OWASP/NVD 深扫保留为每周/按需增强（需 `NVD_API_KEY`）。门禁命令与阈值口径同步写入 SECURITY.md。

- **`@StreamMQConsumer` 新增 `consumeThreads` 并发旋钮（0.1.2 新增，默认 1）**：并发消费循环数的唯一推荐写法
  （仅 CONCURRENT 集群消费生效，取值夹取到 `[1, 64]`）。旧名 `consumeThreadMin` 标记 `@Deprecated` 并仅为源码兼容保留
  （`consumeThreads` 保持默认 1 且旧属性被显式设为非默认值时仍按旧属性生效）；`consumeThreadMax` 废弃且**不再影响并发数**
  （显式设置为非默认值时注册期输出 WARN，将于 0.2.0 移除）。历史缺陷：只配置 `consumeThreadMax` 的用户实际只得到 1 个消费循环且无任何提示。
- **依赖升级：Jackson 2.17.2 → 2.18.10**（修复 **GHSA-r7wm-3cxj-wff9** / **GHSA-72hv-8253-57qq** 两个 High CVE）。
  父 POM 中 `jackson-bom` 的 import **声明在 Spring Boot BOM 之前**（Maven「先声明者优先」），
  避免被 Boot 管理的 2.17.2 逐构件覆盖；`NOTICE` 同步更新。
- **异常口径 javadoc 澄清（0.1.2 起）**：方法参数 / 契约违反继续使用 JDK 标准异常
  （`IllegalArgumentException` / `IllegalStateException` / `NullPointerException`）；框架运行时错误（Broker 交互、
  序列化、事务、发送超时、消费中断）使用 `StreamMQException` 子类；**外部配置错误**使用 `StreamMQClientException`
  （首个真实使用点为 starter 的 `StreamMQProperties#validate()`）；`ConsumerInterruptedException` 由适配层在消费路径
  包装 `InterruptedException` 抛出，core 仅提供类型。
- **序列化器 null / 空输入语义统一**：6 个内置实现（Jackson / JDK / Fury / Protostuff / FlatBuffers / SBE）一致——
  `serialize(null)` 返回 `null`，`deserialize(null | 空数组)` 返回 `null`（不抛异常）；新增
  `SerializerNullSemanticsTest` 作为跨实现回归守卫。
- **文档**：`SECURITY.md` 改为英文为主（文末附中文摘要），修正 FlatBuffers / SBE 的版本身份为 **0.1.2**；
  `docs/configuration-reference.md` 新增 `streammq.diagnostics.*` 模块前缀与 `streammq.tracing.otel.*` 章节、
  延时 7 天上限说明；中英文 README 补最小依赖矩阵的 scope 事实、benchmark 复现命令改为 Linux/macOS 可执行写法。
- **默认序列化器回退为 `JacksonJsonSerializer`（安全默认，0.1.1 起改为 Fury 的回退）**：库的默认反序列化器
  不应把 RCE 面传播给所有下游应用。0.1.1 把默认设为 Fury 宽松模式（`requireClassRegistration=false`）——
  该模式允许把 Redis 中字节流反序列化为 classpath 上任意类，在共享/多租户 Redis 上是反序列化 RCE 攻击面。
  现默认 `JacksonJsonSerializer`（严格类型、无 gadget 面、Redis 中人类可读）。需要高吞吐的用户显式 opt-in 到
  `FurySerializer`（并建议开启类注册白名单）。
- **升级注意（数据兼容）**：默认序列化器切换后，**新写入消息的 body 为 Jackson JSON 格式，与存量 Fury 二进制消息不互通**——
  Fury 配置的生产者写入的消息，Jackson 配置的消费者无法反序列化（反之亦然，消费端会批量报反序列化失败）。
  升级时请先消费完（drain）存量消息再切换默认值，或显式保持 `streammq.producer.serializer` 与存量数据格式一致
  （存量数据为 Fury 时继续显式配置 `FurySerializer`，待存量清空后再切回默认）。
- **`fory-core`（原 `fury-core`，坐标 `org.apache.fory:fory-core`） / `protostuff-*` 改为 `optional` 依赖**：不再强制把 Guava / Protostuff 拖入每个下游应用
  classpath（Guava 是 Spring Boot 应用最常见的版本冲突源）。选用 Fory/Protostuff 时自行加入对应依赖；
  装配层对"配置了但 classpath 缺失"给出可操作的 `IllegalStateException` 而非含义不明的 `NoClassDefFoundError`。
- **默认并发消费超时改为 `0`（不启用每条消息的超时包装）**：超时保护此前对每条消息执行一次
  `executor.submit()` + `Future.get(timeout)` + 超时后 `join`，是每条消息的固定开销，而 99.99% 的消息毫秒级完成。
  关闭后卡死消息由 `PelClaimScheduler` 在空闲阈值（默认 60s）后认领重投兜底，at-least-once 语义不变，
  仅恢复延迟更长；需要更快恢复时由 `streammq.consumer.consume-timeout-millis` / `@StreamMQConsumer#consumeTimeout()` 显式开启。
- **JaCoCo 覆盖率门禁改造为按发布模块设卡**：分支覆盖门禁由 0.15 提升为 **0.40**（消息队列的 Bug 几乎全在分支上，
  15% 等于没有门禁）；旧"全局 LINE 0.50"阈值因各模块单测覆盖差异极大（脚手架模块仅 17%~36%）而形同虚设或误伤，
  现改为 `streammq-core` / `redisson` / `starter` / `binder` / `diagnostics` / `tracing` 六个发布模块各自贴合实际覆盖率的
  LINE+BRANCH 阈值，受 `jacoco.check.skip`（默认 true）控制，CI 用 `-Djacoco.check.skip=false` 启用。
- **共享执行器字段加 `volatile`**：`DefaultStreamMQListenerContainer.consumeExecutor` 与 `DelayMessageScheduler.scanExecutor`
  在不持锁的 `stop()` / `launchLoop()` 中被读取、在持锁的 `setConsumeExecutor` / `ensureScanExecutorAlive` 中被替换，
  非 `volatile` 会让另一线程（如 Spring 生命周期线程）读到过期引用。
- **`MessageSink.dispatch` 由 1ms `parkNanos` 自旋改为带超时的阻塞 `offer`**：队列满时不再空转 CPU，
  仍以 200ms 周期检查 `running` 以保证停机响应。
- `RedissonBroadcastGroupRegistry.DEFAULT_MAX_SWEEP` 可见性由 private 提升为 public，供装配层复用。
- 版本统一为 `0.1.2`（parent / BOM / 各模块 / 两份 README）；`release.yml` 移除对不存在的 `streammq-test-support`
  模块的引用。
- **发布面收缩（P2-1/P2-2）**：`streammq-kubernetes` 移出**发布清单**（被 `excludeArtifacts` 排除：仍在 reactor 内随默认构建编译，但不会上传 Central，仅以源码形式提供；见下方 Maven 条目）；`release.yml` 的 `excludeArtifacts` 扩展为 `streammq-tracing-opentelemetry` / `streammq-diagnostics` / `streammq-spring-cloud-stream-binder`，首发实际发布 **6 个构件**：`streammq-parent`（parent POM，供下游以 `<parent>` 继承，packaging=pom、无 jar/sources/javadoc 产物）/ `streammq-bom` / `streammq-core` / `streammq-redisson` / `streammq-spring-boot-starter` / `streammq-test`（`streammq-test` 不在 `excludeArtifacts` 中，`release.yml` 亦上传其 jar），把需要永久 API 兼容承诺的构件从 8 个收敛到 **4 个 jar 模块**（`parent` / `bom` 为 pom 打包，无 API 面）。
- **`ConsumeAction` 明确为值对象 + 可 switch（P3-1）**：保留逐消息 `defer` 延迟（框架 `handleDefer` 实际消费 `getDeferDelay()`，纯 `enum` 常量无法携带每实例状态，故不改为 `enum`），新增 `Type` 枚举供 `switch (action.type())` 使用，javadoc 说明。
- **`Message.equals/hashCode` 值对象语义修正（P3-2）**：messageId 为 null 的两个内容相同消息现在判定为相等（保持值对象契约），已分配 ID 与未分配 ID 的消息始终不等。
- **`SpiResolver` 错误信息增强（P3-6）**：实例化 SPI 失败时给出中文可操作提示（缺 public 无参构造 / 应走 Spring Bean 覆盖）。
- **移除空标记注解 `@EnableStreamMQ`（P3-8）**：该注解为空标记（不含 `@Import`、不触发任何装配），首发前清理，自动装配独立生效；更新 EN/ZH README 与 demo 指南（quickstart 不再需要启用注解）。
- **删除 3 个 `@Deprecated` 构造器（P2-3）**：`DefaultStreamMQListenerContainer` 原 5/6/7 参 @Deprecated 重载全部移除，替换为 2 个干净的便捷构造（5 参、7 参），二者均显式委派给全参 8 参构造（默认 `DlqConfig`/背压容量统一声明，消除多重载默认语义漂移）；2 处 6 参调用方（`DefaultStreamMQListenerContainerTest`、`CoreRedisIntegrationIT`）补齐 `DlqConfig` 参数。
- **消费者组心跳调度器共享（P3-4）**：`RedissonConsumerGroupManager` 由原"每消费者组一条单线程"改为跨所有组共享的有界 daemon 线程池，`unregister()` 不再关闭共享执行器，消除消费者组数量大时的线程膨胀。
- **JMH 配置加强（Sec9）**：序列化/模板两类纯 CPU benchmark 的 `fork` 提升至 3、`warmup`/`measurement` 加严；消费者 benchmark 保留 `fork=1`（本地 Windows Redis 抖动，已在注释中说明）。
  > 已被第六轮取代（见下方 R6/B2）：`@Fork(3)` + 长迭代结构性需要 ≈100 分钟，超出 CI 预算，现统一收敛为 `@Fork(1)` + 短迭代。
- **补充长跑稳定性 IT（P2-6）**：新增 `LongRunStabilityIT`（500 条消息、并发 3，验证不丢不重），与既有 `ConcurrentConsumeIT`/`HighConcurrencyStressIT` 共同覆盖并发不变量。
- **完整配置参考（D5）**：新增 `docs/configuration-reference.md`，枚举全部 `streammq.*` 配置项（根/生产者/消费者/组/DLQ/重试/延时/事务/健康/重平衡/追踪/管理）及默认值、安全项与 Actuator 暴露说明。
- **文档**：英文 README 补 `management.endpoints.web.exposure.include=streammq`（否则 `/actuator/streammq` 404，A1/D2）；EN/ZH 统一扩展点口径为"16 个（面向用户 + 内部装配），通过注解 Class 属性或 Spring Bean 覆盖，不使用 ServiceLoader"（D3）；明确背压默认关闭（`inflight-capacity: 0`，D7/P2-12）；移除无意义的纯内存 `messageCreateAndConsume` benchmark 数字（Sec9）；SECURITY.md 注明 `streammq-core` 运行时经反射加载 Spring（无编译期依赖，失败开放，P2-5）。
- ACK 热路径逐消息 `LOG.info` 降为 DEBUG（消除热路径对象分配与 appender 竞争）。
- **Maven（依赖管理语义变更）**：`streammq-bom` **不再导入 `spring-boot-dependencies`**。此前该导入会连带管理
  Spring / Jackson / SLF4J / Micrometer 等第三方版本，在使用方 import 顺序靠后时**静默覆盖使用方的 Boot 版本**
  （且与本 BOM 自身注释声明的"不管这些"自相矛盾）。现在 BOM 只管 StreamMQ 自身构件与 Redisson；**使用方需自行
  管理 Spring Boot 版本**（如 `spring-boot-starter-parent` 或自己的 `spring-boot-dependencies`）。StreamMQ 各模块的
  传递依赖版本由各自 POM 携带，已用隔离本地仓库的 staging smoke 实测：仅 import 本 BOM 即可正常解析与编译。
- **Maven**：BOM 与发布集对齐——移除 `streammq-diagnostics` / `streammq-tracing-opentelemetry` /
  `streammq-spring-cloud-stream-binder` 的管理项（它们被 release `excludeArtifacts` 永久排除，写进 BOM 会让
  使用方解析到不存在的构件）；`streammq-kubernetes` 重新纳入 reactor（口径统一：**在 reactor 内构建、但不发布**——
  仍被 `excludeArtifacts` 排除、Central 上不存在，属 source-only 模块；其健康注册回归用例在第四轮按测试类型
  由 `*IT` 改名为 `KubernetesHealthRegistrationTest`、随 surefire 执行，模块内已无 `*IT`）；样本模块显式声明
  未发布构件的版本。
- **API（`streammq-core` 客户端地址可信策略）**：删除静态全局 API
  `WebRequestAuthSupport.configure(...)` / `isTrustForwardedHeaders()` / `getTrustedProxyCidrs()` /
  `getClientAddressFromRequest()`；改为不可变值对象 `WebRequestAuthSupport.ClientAddressPolicy`
  （`DEFAULT` = 不信任 `X-Forwarded-For`）与 `getClientAddressFromRequest(ClientAddressPolicy)`。
  装配层把策略注册为 Bean（starter 由 `streammq.admin.trust-forwarded-headers` / `trusted-proxies` 构造），
  `RateLimitedAuthenticator` 新增接收策略的构造器，`streammq-diagnostics` 未装配时退化为安全默认。
  消除"同 JVM 多 Spring 上下文 last-writer-wins / 上下文重启后残留"的问题。
- **发布/CI 门禁**：release 通道启用 JaCoCo 覆盖率门禁（`-Djacoco.check.skip=false`）+ 集成测试执行量
  tripwire + 版本改写后重新 `verify`（保证"被测试的 = 被发布的"）；staging smoke 使用隔离本地仓库，并新增
  「未发布构件必须不可解析」的反向断言；OWASP 扫描加有限重试与每周计划触发。
- **发布通道门禁补齐（第六轮红队 F-1~F-11）**：`release.yml` 现自带 CI 的两道硬门禁——`guard`
  （parent↔BOM 属性同步 + 发布集一致性：modules ↔ excludeArtifacts ↔ BOM 三方等价）与 `sbom-scan`
  （CycloneDX 聚合 SBOM + `osv-scanner`，无密钥的 High/Critical CVE 硬门禁，含下载二进制的 SHA-256 校验），
  `publish` 的 `needs` 由 `[test]` 改为 `[test, guard, sbom-scan]`，两个新 job 与 `test` 并行；
  `workflow_dispatch` 发布路径新增断言：`git fetch --tags --force` 后要求 `v<version>` tag 已存在且
  `git rev-list -n1` == `HEAD`（此前 tag 由 Create Release 现场创建、所发布字节不绑定任何已校验 tag）；
  BOM 的 `streammq.version` 属性单独硬断言（此前只断言 BOM `<version>`，属性漏改会让 BOM 静默指向上一版
  二进制）；staging smoke 新增解析版本断言（所有 `io.github.streammq:*` 解析版本必须等于本次构建版本，
  封堵"回退 Central 解析旧构件"的假绿）；`autoPublish=false` 期间 GitHub Release 正文注入
  "Central Portal 部署处于 validated、需维护者人工 Publish" 的醒目提示；publish job 超时预算
  45 → 90 分钟（该 job 内要跑两次全量 reactor 构建）；OWASP 抑制清单改用
  `${maven.multiModuleProjectDirectory}/owasp-suppressions.xml`（相对路径在 `-pl` 扫描下永不解析）；
  release 资产清单与文档口径统一为 6 个 Central 构件（含 `streammq-parent`）。
- **文档**：Quick Start 的 Redis 配置由无绑定的 `redisson.singleServerConfig.*` 改为 `spring.data.redis.*`
  （示例与样本 IT 同步）；中英文 README 修复默认序列化器 / 默认值 / Fury 安全姿态等相互矛盾的表述；
  `docs/configuration-reference.md` 纠正 9 处错误默认值；模块表标注 0.1.x 仅源码提供的模块；修复死链。
- 新增 [docs/REPORT.md](docs/REPORT.md)：第二轮红队审查报告、逐项处置与发布门禁结论。

- **PEL 认领脚本在 `XADD` 失败时静默丢失消息（第四轮红队 P1）**：Redis Lua 无回滚语义——脚本先 `XACK`
  认领、再 `XADD` 副本；目标键类型冲突（WRONGTYPE）、`maxmemory` OOM 或 ACL 拒绝时 `XADD` 抛错，而先前的 `XACK`
  已生效：条目从 PEL 消失、副本未写入，业务与运维两侧都无信号。现改为 `pcall` 包裹 `XADD` 并返回失败标记，
  Java 侧用内存中仍持有的字段做补偿（直接重试 → 隔离区 Hash 落盘 + 隔离区 ZSet 登记，7 天 TTL，ERROR 日志给出 key），
  并在认领前对目标键做**类型自检**（不可写则整轮跳过认领，条目留在 PEL 等人工修复）。
- **PEL 扫描"头部饥饿"**：每轮都从 `XPENDING` 头部取固定窗口，头部被存活消费者长期占据时，其后方死亡实例的
  遗留条目永远不进入扫描窗口。现改为**游标分页**（本轮扫完从最后一条之后继续，到达 PEL 尾部回到起点），
  保证全量 PEL 逐轮被遍历。
- **顺序消费分片锁竞争被计入业务重试预算，未处理消息被 ACK 进 DLQ（R3-25 根因修复）**：竞争信号与 handler 失败
  共用返回值，多实例 rebalance/僵尸 handler 持锁时竞争会耗尽 `maxReconsumeTimes`，把**从未执行 handler** 的消息
  转投 DLQ。现新增 `OrderlyShardBusyException`（纯新增类型，零 SPI 破坏）：锁管理器在预算内多轮等待后仍拿不到锁
  即抛出；处理器单独捕获，返回 `RECONSUME_LATER` 且**不消耗预算、不写 retry ZSet、不进 DLQ、不 ACK**，消息留在
  PEL 由认领调度器兜底（限频 WARN 可观测）。
- **广播实例身份本地文件被同机多应用互相覆盖**：默认路径 `${user.home}/.streammq/instance-id` 是"每 OS 用户单例"，
  应用 B 读到应用 A 的身份、被注册中心拒绝后覆盖该文件，A 下次重启又读到 B 的身份——身份互踩、组名漂移、
  停机窗口的广播消息被静默跳过。现改为**按 namespace+group 分片**的默认文件（`instance-id-<ns>_<group>`），
  文件内按 `id pid timestamp` **多记录**存储：重启复用**已退出进程**的身份，绝不覆盖仍在运行进程的身份。
- **广播租约续期与拉取循环耦合**：续租此前只发生在 `doRead` 之后，单次 handler 超过租约超时（默认 20s）期间
  不续租，同主机第二进程即可抢占槽位 → 两个活跃进程共用同一广播组（广播静默退化为集群消费）。现为广播监听器
  提供**独立心跳线程**（5s 固定间隔，与消息处理时长无关，close 时取消）。
- **判活窗口与心跳间隔缺跨字段校验**：`pel-claim-min-idle-ms` 小于 `group.heartbeat-interval-ms` 时，活跃慢消费者
  的心跳会被判为过期 → 复制重投、重试耗尽后已成功处理的消息进 DLQ。现启动期校验
  `pel-claim-min-idle-ms >= 3 × heartbeat-interval-ms`，非法组合直接失败。
- **广播租约参数零校验**：`broadcast-lease-timeout <= 0` / `reclaim-grace < lease-timeout` 会让任意槽位"立即过期"，
  同机另一进程可立即回收身份。现构造期快速失败并给出具体参数。
- **事务状态缺失（MISSING）时 commit 请求被静默忽略**：`TransactionScanner` 在状态字段缺失时按"已终态"debug 返回，
  半消息既不投递也不清理。现显式 ERROR 告警并降级为 `UNKNOWN` 走有界回查（元数据确实丢失时以 ROLLBACK 明确终结）。
- **调度扫描每轮少扫一条**（`valueRange(..., batchSize - 1)`）：retry/delay 扫描窗口改为扫满 `batchSize`。
- **延时投递指标高估**：未真正投递（claim 未拿到 / payload 被隔离 / 原子批失败）也计入投递指标；
  现仅在原子批真正成功时计数，未投递路径降为 DEBUG 观测。
- **消费循环 supervisor 陈旧 Future 导致"静默不消费"**：循环自终结后残留的已完成 Future 会让新循环被
  `cancel(true)` 且无日志；现提交前先清理已完成 Future，取消分支输出 WARN。
- **重试策略返回 null 时的求值顺序错误**：`decision.type()` 在 null 判断之前求值导致 NPE、null 兜底成死代码；
  现先判 null 并按兜底策略处理，热路径 INFO 日志降为 DEBUG。
- **`MessageId` 的 `equals`/`hashCode` 与 `compareTo` 不一致**：`compareTo` 按 `(timestamp, sequence)` 数值比较，
  而 `equals` 按原始字符串比较（`"01-2"` 与 `of(1,2)` 比较相等但 `equals` 为 false）。现统一为数值语义，
  构造时规范化；`of()` 拒绝负值（避免生成无法回解析的 `-1--1`）。
- **`BatchMessage` 异常类型与 javadoc 不符**：空列表抛 `IllegalStateException` 而文档写 `IllegalArgumentException`；
  `add` 抛 NPE 而 `addAll` 抛 IAE。现统一为 `IllegalArgumentException`。
- 删除 `StreamMQKeys.transactionLock(...)` 死键布局（去锁化后全仓零引用）。
- **基准口径有效性判定（R4-38）**：`StreamConsumerBenchmark` 新增 `feederSanityThroughput` 探针
  （与补货线程同一条 `syncSend` 路径）；`main()` 结束后解析 JMH 结果并判定
  `feeder ≥ 3 × consume`，不满足时打印 `INVALID RUN` 并以非零码退出——把"补货端下界"与
  "消费能力"彻底区分开，杜绝把受补货约束的数字当作容量依据。
- **`streammq-test` 覆盖率门禁按门禁命令实测校准**（首版占位 0.30/0.20 → 实测 −3pt 的 0.78/0.39），
  与其他发布模块的设卡口径一致。

- **Redisson 3.5x 下监听器启动被 BUSYGROUP 退避阻塞（新基线上线时发现）**：Redisson 3.5x 对「组已存在」的
  `XGROUP CREATE` 会先做约 5s 退避重试才抛 BUSYGROUP（3.34 立即返回），使消费者组预先存在（重启、预建组、
  集成测试）时监听器启动被阻塞数秒、启动窗口内的消息可能漏读。现改为**先探测组是否存在**（`listGroups()`）
  再决定是否创建：命中即直接返回，语义不变且启动零延迟。
- **Spring Cloud Stream 4.3 的 Binder 单元测试适配**：`AbstractExtendedBindingProperties` 自 4.3 起通过
  `ConfigurableApplicationContext` 注入 `propertiesBinder`（ApplicationContextAware），纯单测环境需显式提供
  上下文，否则 `getExtended{Consumer,Producer}Properties` 抛 NPE（生产路径由 Spring 注入，不受影响）。

### Removed

- **`ConsumeAction.DEFER` 常量（改用 `ConsumeAction.defer(Duration)`）**：该常量历史上携带 `null` 延迟，
  业务返回它时框架在取延迟处 NPE 且被吞掉，消息既不 ACK 也不重投。现移除常量，延迟重投统一走
  `defer(Duration)`，并把"DEFER 动作必须携带正延迟"提升为**构造期不变量**（违反时直接抛
  `IllegalArgumentException`），从根上杜绝这类静默失效。
- **`StreamMQEventBus` SPI 及其实现（`AsyncStreamMQEventBus`）与自动装配 Bean**：该扩展点未被任何生产代码接线
  （全仓库零引用的死 API），首发前移除，避免形成永久的 API 兼容承诺。

### Security

- 默认反序列化器由 Fury 宽松模式回退为 `JacksonJsonSerializer`，消除库默认传播的反序列化 RCE 面
  （详见上方 Changed / 默认序列化器）。共享/多租户 Redis 下如需 Fury 吞吐，务必开启类注册白名单。
- `release.yml` 流水线修复：此前引用的 `streammq-test-support` 模块不存在，会导致发布 CI 失败。
- **Fory 升级至 1.7.3，消除 CVE-2026-50076（本项目受影响）**：序列化底层库由 `org.apache.fury:fury-core:0.9.0`
  迁移到 **Apache Fory（原 Apache Fury）`org.apache.fory:fory-core:1.7.3`**。1.1.0 之前的 fury-core / fory-core
  存在 CVE-2026-50076（CVSS 9.1：反序列化可绕过类注册校验触发 classpath 上的 resolve/readExternal 钩子），
  故本版本声明版本下限 1.1.0。Java 类名 `FurySerializer` 与 `name()="fury"` 保持不变（兼容既有配置写法），
  文档统一改称底层库为 Apache Fory 1.7.3。
- **宽松模式构造重新引入系统属性门禁**：`new FurySerializer(false)`（关闭类注册白名单）现在必须显式设置
  `-Dstreammq.security.allowUnrestrictedSerializer=true` 才能创建，否则抛 `SecurityException`（与
  `JdkSerializer.unrestricted()` 同语义）；即使设置了该属性，构造宽松实例时仍会输出 WARN 风险提醒。
  0.1.1 曾移除该门禁（改为仅告警），本次恢复为 fail-fast。
- **传输层 codec 显式化（消除 SDK 侧反序列化 gadget 面）**：SDK 自有 Redis 键（业务 Stream / DLQ /
  重试 / 延迟 / 事务半消息 / 注册表 / 广播租约）全部显式使用 `StringCodec`，不再继承下游
  `RedissonClient` 的全局 codec（Redisson 默认 `Kryo5Codec(registrationRequired=false)`，即未注册限制的
  Java 反序列化）。此前任何可写 Redis 的一方都能投放 gadget 载荷在消费端触发 RCE，且全局 codec 非字符串时
  会出现静默跨 codec 不兼容。
- **载荷类型护栏改为「形态归一化 + 危险命名空间拒绝」**：`PayloadTypeSafety` 先归一化 JVM 描述符形态
  （`[Ljava.lang.Runtime;` → `java.lang.Runtime`），拒绝非法类名 / `$Lambda` / `$Proxy` 形态，并扩充
  已知 gadget 命名空间黑名单（Commons-Collections / BeanUtils / fastjson / Xalan / SnakeYAML / Groovy /
  ROME / XStream / Hibernate / 脚本引擎等）。
- **`JdkSerializer`**：不再把「载荷派生的目标类型名」写入本次 `ObjectInputFilter` 允许集（消除"载荷自扩
  白名单"）；移除整包 `java.lang.` 放行，改为逐类放行语言标量 + 单独放行枚举，杜绝
  `java.lang.reflect.Proxy` / `java.lang.invoke.SerializedLambda` 等 gadget 使能类被放行。
- **BasicAuth 时序与卫生**：改为非短路比较（消除"用户名错误即跳过口令比较"的用户名有效性时序预言），构造期
  只保留密码 SHA-256 摘要，避免每请求把 `char[]` 复制成不可变 `String`。
- **毒丸日志脱敏**：DLQ 毒丸不再打印完整载荷与用户属性，改为字段名 + 数量。
- **Fury 白名单可用性**：新增 `streammq.producer.fury-registered-classes`，白名单模式下可直接声明业务消息体
  类型；未声明时启动日志给出可操作告警。文档 / Javadoc / SECURITY.md 统一为"默认强制类注册白名单；
  `new FurySerializer()` 即白名单，`new FurySerializer(false)` 才是宽松模式（受系统属性门禁保护）"。
- **Jackson 升级（2.17.2 → 2.21.4，经 2.18.10 中转）**：修复 **GHSA-r7wm-3cxj-wff9** 与 **GHSA-72hv-8253-57qq**
  两个 High CVE；`jackson-bom` 声明在 Spring Boot BOM 之前，确保不会被 Boot 管理的版本静默覆盖。
- **SBE / FlatBuffers 读取侧长度与深度上限**：FlatBuffers 对单字段声明长度的上限为
  `min(streammq.producer.max-message-size, 64MB)`，嵌套深度上限 64 层；SBE 校验信封头（`templateId`/`schemaId`）、
  截断与声明长度（负数 / 超消息上限 / 超实际可读字节数）。畸形载荷一律抛 `SerializationException`，
  **不按声明长度分配内存**（消除"4 字节长度字段放大为 GB 级分配"的内存放大面）。
- **`PayloadTypeSafety` 危险命名空间黑名单扩充**：新增 commons-configuration / jelly / fileupload / dbcp(2) / text、
  xpath / velocity / ignite / activemq / myfaces / struts / tomcat / log4j、fastjson2、Guava collect、mchange、Hikari、
  net.sf.json、ehcache、org.json、quartz、jboss、Javassist、Groovy、Rhino（`org.mozilla.javascript`）、
  `org.python`、logback 等条目（护栏仍是命名空间拒绝式黑名单，属纵深防御）。

### Fixed — 第五轮发布前红队审查（R5，发布候选）

> 依据 `docs/fullReview.md` 协议对全部 11 个模块、4 个 workflow、全部发布物料与全部测试做独立取证；
> 与发布通道**完全相同**的门禁命令 `mvn clean verify -Djacoco.check.skip=false` 实测。逐项证据见 [docs/REPORT.md](docs/REPORT.md)。

**数据面正确性（可能导致消息静默不投递/丢失）**

- **顺序消费失败被写进「无人消费」的重试 Stream**：ORDERLY 没有 retry 消费循环（重试在分片锁内原地进行），
  但过滤器求值异常、`processMessage` 的 `Throwable` 兜底、`DEFER` 三条路径仍会走到
  `handleReconsumeLater` → 重试 ZSet → `RetryScheduler` 转投进 retry Stream，而**没有任何循环读取该 Stream**，
  消息在流中被裁剪前静默沉没。现于 `DefaultRetryAndDlqHandler.handleAction` 统一收口：ORDERLY 的
  非成功动作一律 `routeToDlq`（`dlqReason=maxRetryOrderly`）并 ACK，DLQ 写入失败则保留 PEL。
- **运行期动态注册的消费者未绑定调度目标**（容器 RUNNING 后调用 `registerConsumer`）：只建读循环，
  未注册 retry / PEL 认领目标 —— 这些消费者的失败消息写入重试 ZSet 后无人扫描（payload 7 天过期即隔离丢失），
  崩溃遗留 PENDING 也无人恢复。现 `SchedulerTargetBinder` 新增 `bindTargets`/`unbindTargets`（单注册项），
  与启动期批量绑定共用同一套规则（`bindOneRetryTarget`/`bindOnePelClaimTarget`），运行期注册即时补绑。
- **调度目标只增不减**：`unregister` 不解除调度目标，反复动态注册/注销后调度器持续扫描已注销目标。
  现 `RetryScheduler.unregisterRetryTarget` / `PelClaimScheduler.unregisterTargets` 配合 `unbindTargets` 成对解除。
- **事务回查批量扫描恒少一条且 `batch-size=1` 时完全失效**：`TransactionScanner.scanTimeoutHalf` 使用
  `valueRange(..., 0, batchSize - 1)`，`batchSize=1` 时 `LIMIT 0` → 永不扫描 → 半消息永久悬挂。
  现与其他调度器统一为 `count = batchSize`。
- **「读取 Redis 服务器时钟」从未真正生效**：`PelClaimScheduler` 与 `RedissonConsumerGroupManager` 内联的 Lua
  脚本返回**标量**整数，却声明为 `ReturnType.MULTI`（期望数组回复），解码异常被 `catch` 吞掉后静默回退本机时钟
  —— 跨主机 NTP 偏差会误判实例存活、误踢消费者、复制重投。现抽出 `RedisServerClock`（`ReturnType.INTEGER`
  + 单一实现）并补 IT 守卫。
- **`MessageConverter` 返回不可变 Map 时重试/DLQ 路由失效**：`routeToDlq` / `handleDefer` / 二级 DLQ 直接在
  转换器返回值上 `put`，遇到 `Map.of(...)` 之类不可变实现抛 `UnsupportedOperationException` 并被吞成
  "DLQ routing failed" → 消息永久滞留 PEL。现全部改为先做可变拷贝。
- **广播实例身份文件并发写互相覆盖**：临时文件名固定为 `<name>.tmp`，同机多进程并发写同一身份文件会互相踩踏；
  read-modify-write 也无互斥。现临时文件名带 pid+UUID，并对「读-改-写」加 JVM 内互斥 + 跨进程文件锁。
- **监听器工厂 `createListener`/`close` 竞态导致监听器逃逸**：通过 `closed` 检查后、入队前若 `close()` 完成排空，
  新监听器永不关闭且其租约心跳持续续租 Redis 槽位。现以生命周期锁使「检查+入队」与「置位+排空」互斥，构建期间
  被关闭则自关闭并抛错。
- **广播僵尸组清扫恒少扫一条**：`valueRange(..., 0, maxSweep - 1)`，`maxSweep=1` 时永不回收。改为 `maxSweep`。
- **`checkerTimeoutMillis <= 0` 使事务回查线程无限等待**：等待实现为 `join(Duration)`，而 `join(0)` 语义是
  永久等待 —— 挂死的 `TransactionChecker` 会让扫描线程永久持有 `groupLock`，整个回查调度停摆。现 setter
  快速失败（`> 0`）并在使用点兜底夹取。

**安全与运维面**

- **诊断端点参数无校验**：`/streammq/diagnostics/**` 未对 `topic`/`group`/`messageId` 做名称校验，原始输入会
  被拼进 Redis Key 与鉴权资源串。现与 Actuator 端点统一走 `StringUtils.requireValidName`，非法输入返回 400。
- **诊断健康概览恒为 `UP`**：严重积压/慢消费期间仍报健康（典型「静默故障」）。现按积压严重度与慢消费者推导
  `UP` / `DEGRADED` / `DOWN`。
- **管理端点回吐 Redis 内部异常信息**：多个 catch 分支把 `ex.getMessage()`（含 Key 名、`NOGROUP`、连接/ACL
  错误）原样返回 HTTP 响应。现返回「操作名 + 异常类型 + 关联 ID」，细节只进日志。
- **运行期组配置上限过宽**：`inflightCapacity` 可设到 `Integer.MAX_VALUE`（背压队列 OOM 面），
  休眠/退避/宽限可设到 `Long.MAX_VALUE`（消费循环近乎静默停摆）。现收敛为 `[0, 100000]` 与 `[1, 300000]`。
- **`FailureRetryLimiter` key 空间可无界增长**：清理过期条目后在冷却窗口内持续以不同 target 失败时 Map 仍会
  增长，与其文档承诺相悖。现超限且清理无效时放弃记录（限流降级为放行，key 空间恒有界）。

**发布通道与物料**

- **GPG 非交互签名缺失导致首发被阻断**：`gpg` profile 未声明 `--pinentry-mode loopback`（gpg 2.1+ 在无 tty 的
  runner 上必须显式 loopback），且未显式传入口令。现两处 POM 补齐 `gpgArguments`，`release.yml` 以
  `-Dgpg.passphrase="$MAVEN_GPG_PASSPHRASE"` 显式传入。
- **发布资产清单与发布集不符**：`release.yml` 的 Release 资产仍列出 `tracing`/`diagnostics`/`binder` 三个
  Central 不可解析的模块，与同处注释及 `CONTRIBUTING.md` 矛盾。现资产收敛为 Central 可解析的发布集。
- **`versions:set` 可能漏改无父 POM 的 `streammq-bom`**：加 `-DprocessAllModules=true` 并新增断言步骤，
  把「BOM 版本未随发布升版」从静默漂移变为硬失败。
- **CVE 硬门禁的 `osv-scanner` 二进制无完整性校验**（下载后直接执行，且它决定「是否阻断发布」）。
  现固定官方 SHA-256 校验（`sha256sum -c`）。
- **CI 三个 job 缺 `timeout-minutes`**：guard / build / formatting 补齐，避免 runner 卡死无限等待。

**文档与元数据（发布物料一致性）**

- `NOTICE` 6 处第三方版本与 `pom.xml` 脱钩（Redisson / Jackson / Micrometer / AssertJ /
  Spring Cloud Stream / Spring Integration），已同步为实际基线。
- 两个 README：技术栈表 Redisson / Jackson 版本与徽章（Spring Boot / Redisson）修正；zh 基准环境表标注
  「历史测量环境」以消除与当前基线的表面矛盾。
- `SECURITY.md` 中文摘要与英文正文自相矛盾（Jackson 版本、CVE 门禁口径），已同步为「无密钥 SBOM+osv 硬门禁 +
  OWASP 为增强扫描」。
- `CONTRIBUTING.md`：tripwire 阈值更正为真实口径（分模块下限 + 全局 ≥ 230 + 跳过率 ≤ 20%）、
  `parent.pom.xml` → `pom.xml`、Release 资产说明与实现对齐。
- `docs/configuration-reference.md`：配置校验异常更正为 `StreamMQClientException`；补齐
  `orderly-consume-timeout-millis` 的三分支语义与「全局键只在注解显式写 0 时生效」；`trace.storage` 默认值口径；
  标注 `admin.startup-warn` 为不走宽松绑定的直读精确键。
- `streammq-spring-boot-starter/pom.xml` 的 `<description>` 移除 0.1.2 已删除的 `@EnableStreamMq` 注解。
- `StreamMQProperties` 的 `orderly-consume-timeout-millis` javadoc 与 `DefaultConsumerTuning` 行内注释更正为
  真实默认值 `-1`（显式关闭）。
- `Message` 的 `body` javadoc 更正：框架**有意**支持 null body（无载荷消息；内置序列化器统一
  `serialize(null) → null` / `deserialize(null|empty) → null`，对端可能发来无载荷消息）。真实边界是
  「**发送 API** 必填、**值对象**可为 null」，原文"必填"表述会误导维护者加上破坏性校验
  （实测：加校验会直接打破 `toStreamFieldsNullBody` 等既有契约）。

**新增回归守卫（失败即红）**

- `OrderlyFailureRoutingTest`：ORDERLY + `RECONSUME_LATER`/`DEFER` 必须走 DLQ 且**绝不**触碰重试调度。
- `DefaultSchedulerTargetBinderTest`：单注册项绑定产生的目标集合与批量绑定一致；注销成对解除；null 调度器安全跳过。
- `RedisServerClockIT`：真实 Redis 上必须取到服务器时间（锁定 `ReturnType` 与脚本返回值的一致性）。
- `StreamMQDiagnosticsEndpointTest`：非法 topic/group 返回 400 且不触达下游；健康状态按严重度推导。

### Fixed — 第六轮发布前红队审查（R6，发布候选）

> 依据 `docs/fullReview.md` 协议对消费数据面、调度器/事务状态机、重试/DLQ 契约、管理/安全面、Kubernetes 模块、
> 基准方法与发布通道做独立取证（含真实 3 主 Redis Cluster 实测与 JMH 重跑）；门禁命令
> `mvn clean verify -Djacoco.check.skip=false` 实测。逐项证据与裁决见 [docs/REPORT.md](docs/REPORT.md)。

**消费数据面（可能导致消息静默不投递/丢失）**

- **顺序消费失败路径的本地重投缺失（R1-1）**：分片锁竞争、ORDERLY `defer`、DLQ 转投失败三条路径此前仅
  "留在 PEL 等认领"，而 PEL 认领会跳过心跳新鲜的属主实例——属主存活期间消息**永不重投**（静默黑洞）。
  现新增 `OrderlyDeferredRetryQueue` + 重投执行器（由 primary 读循环驱动、不新增线程），三条路径登记后按
  延迟重投；容器停止时清空（未 ACK 消息仍留在 PEL 由认领兜底）。
- **DLQ 模式毒丸被裸 ACK（R1-3）**：目标流与当前消费流相同（写回会自复制成无限循环）时此前直接 ACK，
  原始字段永久丢失。现先隔离落盘（原始字段可恢复，TTL 与 PEL 认领补偿落盘一致）再 ACK。
- **暂停期心跳按 `pausedSleepMillis`（默认 100ms）节流（R1-5）**：暂停是"降载"语义，不应把心跳提升到
  100ms 一次。现按容器心跳间隔节流。
- **运行期配置改了不生效且用户无从发现（R1-6）**：消费循环构造时快照配置，`setter` 对已运行循环无效。
  现改为**每轮读取**即时生效；背压容量（构造参数、不可热改）由管理端点如实回显"是否已被运行中循环采用"。
- **同一 `(topic, group)` 运行期重复注册 = 新增而非替换（R1-7）**：旧循环继续消费、新注册从不生效。
  现登记"已接线"集合：重复注册先取消旧循环与 inflight 泵，再提交新注册。
- **`drainPendingOnce` 契约（R1-8）**：监听器未实现该能力（返回 `null`）时 WARN 一次并跳过，不 NPE、不中断。
- **顺序分片锁的有限租约（R1-9，§13 闭环项）**：新增 `streammq.consumer.orderly-shard-lock-lease-millis`
  （`0` = 看门狗续期 + 严格有序，默认；`> 0` = 有限租约，持有者卡死时到期让位、代价是极端情况下的瞬时乱序）。
- **超时包装路径丢失 MDC（R1-10）**：业务回调运行在执行器线程，现把读循环线程的 MDC 上下文显式传递
  （普通与 ORDERLY 超时包装两条路径）。

**调度器与事务状态机**

- **事务强制终结缺状态 CAS（R2-1）**：判定"卡死"与写终态之间并发实例可能已完成转投，无条件 `HSET` 会覆盖
  已提交状态。现脚本首步要求状态仍为 `COMMITTING` 才允许转投，否则一律不投递、不改写。
- **保留期清理吞吐低于写入速度（R2-2）**：固定 128/10 轮的节奏在数万事务/天的部署下使 `txstate` Hash 无界增长。
  现 HSCAN 分页游标化 + 单轮上限提升一个数量级 + 终态 `.done` 标记补齐（终态脚本执行后、收尾前崩溃的字段
  才能被回收）。
- **慢 checker 导致孤儿回查线程重复启动（R2-3）**：每轮为同一 `txId` 再起虚拟线程。现登记"存活租约"，
  线程结束自摘；诊断可见存活数。
- **调度时间基准混用本机时钟（R2-4）**：延时写入、到期判定、退避 score 统一为 **Redis 服务器时钟**
  （与调度扫描侧同一时间源，跨主机 NTP 偏差不再平移延时时长与重试节奏）；读取失败回退本机时钟并限频 WARN。
- **孤儿清理 N+1 往返（R2-5，§13 闭环项）**：逐条 `isExists` 改为服务端单条 Lua 批量判定（一次往返，语义不变）。
- **重试/DLQ 流被 `MAXLEN` 有损裁剪（R2-6）**：转投进重试/死信流的条目是消息的**唯一副本**
  （原 topic 条目已 XACK、payload 已在同一原子批删除），裁剪即静默丢失。现对这两类流不施加有损裁剪。

**重试/DLQ 与序列化契约**

- **空字符串 body 往返退化为 `null`（R3-1）**：`send(topic, "")` 现端到端还原为空串（`null ↔ null`、
  空串 ↔ 空串，序列化/转换/解码三处协同）。
- **事务发送状态语义定稿（R3-2）**：事务路径**仅** `COMMIT_MESSAGE` 为 `SEND_OK`；`ROLLBACK` / `UNKNOWN`
  一律 `SEND_FAILED` 并携带事务状态，失败路径消息 ID 使用可辨识的占位 `MessageId.pending()`。
- **延迟超过 payload TTL 时消息在到期前丢失载荷（R3-3）**：payload TTL 改为 `max(基础 TTL, 延迟 + 宽限)`，
  覆盖"延迟 + 宽限"不变式，避免长延迟重试到期时读不到 payload 只能进隔离区（事实丢失）。
- **`RetryPolicy.shouldStopRetry` 从未被接线（R3-4）**：策略说"停"但框架继续重试。现接线：返回 `true`
  直接进 DLQ（`reason=MAX_RETRY`），与 `nextRetryDelay` 返回 `null` 的既有停止信号并存。
- **DLQ 策略读到全局配置而非"按消费者合并后"的生效配置（R3-5）**：生效 `DlqConfig` 随决策上下文传递，
  策略判定与容器实际行为一致。
- **`secondary-dlq-enabled=false` 时仍写二级 DLQ（R3-6）**：现为权威门控——关闭时按 drop 处理（ACK），
  限频 WARN + DEBUG 留痕，开关不再形同虚设。
- **哨兵 topic 冲突（R3-7）**：DLQ 重试转投改为"哨兵 topic + `retryScope=dlq`"双重判定；调度器入口拒绝
  以保留前缀 `__` 开头的业务 target（纵深防御，注册可能来自第三方直接调用）。
- **畸形 topic 的异常契约（R3-8）**：Entry 字段中的 topic 违反命名校验时，`IllegalArgumentException`
  被包装为消费侧可辨识异常而非裸崩。

**广播身份（R6-B1）**

- **本地身份文件复用前不做占用校验**：同机重启/双实例共用消费者名时，命中本地文件会静默复用身份，
  广播消费退化为集群消费。现命中后仅做 1 次 claim 校验并区分三种结局：**确认**可复用；**明确拒绝**
  （被其它主机的活实例占用）绝不复用、转注册中心重分配；**不可达**则信任本地文件（Redis 停机期间
  身份不漂移、PEL 与消费位点保留）。

**Redis Cluster（§13 闭环项：由文档约束升级为运行期可观测事实）**

- **`RedisClusterCompatibility` 前置守卫**：生产者/消费者启动探测一次，命中 Cluster 输出**一次性可操作 WARN**；
  依赖跨 key 原子性的路径（延时入队/转投、重试与 DLQ 的调度/转投、事务 prepare/commit、跨流 PEL 认领）
  在客户端被显式配置为 Cluster 时**显式拒绝**（可操作 `StreamMQException`），把静默降级变成确定性失败。
- **真实 3 主 Cluster 实测**（`RedisClusterCompatibilityIT`，16384 slots 全覆盖 + 服务端 `CLUSTER SLOTS`
  权威映射）：单 key 路径（XADD / XREADGROUP / XACK）可用；多 key Lua 被服务端以 `CROSSSLOT` 拒绝且
  **零副作用**（fail-safe）；多 key `REDIS_WRITE_ATOMIC` 批同节点=MULTI/EXEC 拒绝、跨节点=按节点拆分提交
  （**静默失去原子性**，无异常）——两种失败形态均由用例锁定，两个 README 的部署声明同步为实测口径。
- **架构级静态守卫**（`CrossKeyAtomicityGuardTest`）：扫描源码，任何原子批调用点缺少配套
  `requireCrossKeyAtomicity` 前置拒绝即红（已用"删守卫 → 测试必红 → 原样恢复"验证）。

**管理/安全面（R6-S1~S9）**

- **启动提醒组件是死代码（S1）**：`@Component` 位于自动装配包、不在用户组件扫描范围，安全提醒从未输出。
  现改为自动装配内显式注册的 Bean。
- **管理端点门控挂在 `health.enabled` 上（S2）**：关闭健康指示器会连带关闭管理端点。现拆出独立的
  `StreamMQAdminAutoConfiguration`（健康/管理开关各自门控）。
- **多候选 Bean 导致 `getIfAvailable()` 抛错（S3）**：`ObjectProvider#getIfAvailable` 在多个候选时抛
  `NoUniqueBeanDefinitionException`。现 `StreamMQBeanResolution` 收敛解析语义（多 `CompressionCodec`
  Bean 支持按名称写入/解压）。
- **Fury 宽松模式门禁提示误导（S4）**：提示"缺 `fory-core` 依赖"而真实原因是宽松模式门禁未开启。
  现按真实原因给出可操作提示。
- **管理面列表接口无结果上界（S5）**：`?count=2000000000` 可让端点把整条 DLQ Stream 载入内存。现请求值
  统一夹取到上界（DLQ 列表等）。
- **`rebalance.virtual-nodes <= 0` 静默回退默认值（S6）**：用户配错无从发现。现显式校验/告警。
- **`batch-size` 超上限由"静默夹取"演进为"生效值回显 + 交叉校验（S7）**：启动日志打印**生效值**
  （含被夹取后的值），配置校验同时检查 `batch-size` 与 `max-batch-size-limit` 的关系。
- **运行期配置变更"假生效"（S9）**：管理端点回显 `effects`（key → `immediate` / `requires-restart`），
  按组分发（绝不调用容器级 `pause()` 连带暂停其它消费者组）。

**Kubernetes（K1~K10，§13 闭环项：模块深度审计）**

- K1：`CustomResource` 构造器的 `@Group/@Version` 契约断言；K2：`GracefulShutdownHandler` 改
  `SmartLifecycle` 接线，且不再无条件睡满 `graceful-shutdown-timeout-ms`（未就绪即结束）；K3：informer
  `inNamespace(ns)` 收敛 watch 范围；K4：状态回写**先比较再写**（避免自触发无限调和）；K5：`readyReplicas`
  按 CR 显式传入（消除实例级共享计数器串值）；K6：HPA 以真实积压探针（`BacklogProbe` 的 XLEN/XPENDING）
  刷新 lag 指标，缺失探针时跳过决策必伴随限频 WARN；K7：status 回写改 in-place patch 而非整对象 replace
  （陈旧快照不再覆盖用户并发修改的 spec）；K8：fabric8 Mock Server 回归覆盖回写与幂等；K9：扫描范围与
  watch 语义对齐、清理已消失 CR 的每-CR 状态（map 不再只增不减）；K10：ConfigMap 版本键用**实际**注入的
  ns/name（label 模式下同命名空间多匹配不再串版本）。新增 6 个测试类（HPA 扫描与副本持久化、ConfigMap watch
  范围、Cluster 模型与调和、属性装配），并修正一处遗留口径：本模块**不存在** `*IT`（健康注册用例在第四轮已由
  `KubernetesHealthRegistrationIT` 改名为 `KubernetesHealthRegistrationTest` 并由 surefire 执行），此前为
  `*IT` 声明的 failsafe 实际执行 0 个用例，本轮移除该无效果声明（全仓 14 个声明 failsafe 的模块与 14 个含
  `*IT` 的模块此后一一对应）。

**可观测性与资源面**

- 诊断健康概览增加 5 秒 TTL 缓存（`/health` 轮询从每次 2N 次 Redis 往返降为每窗口一次；测试可置 0 关闭）。
- 拓扑查询结果上界：`maxTraceQuerySize`（默认 500），超限按聚合顺序截断并计数 WARN，消除"一次请求把
  窗口内全部追踪记录聚合进堆内存"的可外部触发 OOM 面。
- OTel `Scope#close()` 仅允许在**创建线程**上关闭（注册表记录 owner），避免淘汰/清空发生在调度线程时
  静默破坏该线程的 current context（trace 链路错挂/丢失）。

**基准方法学（B1/B2/B4）**

- **B1 消费口径**：旧 harness"每条一次同步 XADD 持续补货"使消费数字被补货端封顶（实测仅其 0.66–0.78×）。
  现改为**预灌积压 + 低水位批量补货**，测量期间出现空读即判 `INVALID` 并非零退出；
  `ConsumeValidityReport` 按 payload 落盘口径证据（预灌/补货速率、批大小、空读次数）。
- **B2 时间预算**：`@Fork(3, warmups=2)` + 30 方法 × 双模式结构性需要 ≈100 分钟，远超 CI 预算。现注解收敛为
  `@Fork(1)` + 短迭代（全量 ≈17–21 分钟），`BenchmarkBudgetTest` 按"注解 × 参数组合 × 模式 × fork"静态
  校验 45 分钟上限（超预算即红）。
- **B4 参数真源**：`main()` 不再用 `OptionsBuilder` 覆盖 fork/预热/测量（消除"注解一套、main() 又一套"
  的双源），临时覆盖只走 JMH 命令行参数。

**新增回归守卫（失败即红）**

- `CrossKeyAtomicityGuardTest`：架构级扫描——原子批调用点缺前置拒绝即红。
- `RedisClusterCompatibilityIT`：真实 3 主 Cluster 上锁定单 key 可用、多 key Lua `CROSSSLOT` 零副作用、
  多 key 原子批"同节点拒绝 / 跨节点静默拆分"两种失败形态；守卫命中延时/事务全路径。
- `Round6SchedulerRedTeamIT`：事务状态机、保留期清理、重试流不裁剪（真实 Redis）。
- `OrderlyDeferredRetryQueueTest` / `OrderlyMessageIT`：R1-1/R1-2 延迟重投闭环（含真实 Redis 端到端）。
- `PoisonEntryHandlingIT`：DLQ 模式毒丸先隔离落盘再 ACK、合法消息继续消费。
- `RetrySchedulingAndDlqGatingTest` / `DlqFailureStrategyEffectiveConfigTest`：R3-4/5/6 重试与 DLQ 门控契约。
- `TransactionSendStatusTest`：事务发送状态语义（仅 COMMIT 为 `SEND_OK`）。
- `EmptyBodyRoundTripTest`：空串 body 端到端往返 + 畸形 topic 异常契约。
- `RetrySchedulerOrphanAndClockTest` / `RetrySchedulerSentinelDefenseTest`：R2-4/5 服务器时钟与哨兵冲突防御。
- `RedissonOrderlyShardLockManagerLeaseTest`：R1-9 租约语义（0 = 看门狗 + 严格有序）。
- `RedissonStreamProducerDelayClockTest` / `DelayMessageSchedulerTest`：延时写入/到期同一服务器时钟。
- `DefaultMessageProcessorShardBusyTest` / `InFlightCountTrackingTest`：R1-1/R1-2/R1-10 与 in-flight 计数。
- `StreamMQAdminAutoConfigurationTest` / `StreamMQAdminEndpointTest`：R6-S1/2/5/9 门控、上界与诚实回显。
- `StreamMQPropertiesValidateTest` / `StreamMQCompressionCodecAutoConfigurationTest` /
  `StreamMQListenerContainerWiringTest`：配置校验与装配接线（S3/S6/S7/S8）。
- `CloudK8sPropertiesWiringTest` + `autoscaler/`、`config/`、`operator/` 测试：K1~K10。
- `BenchmarkBudgetTest` / `ConsumeValidityReportTest`：B1/B2 基准方法学门禁。
- `ConsumerIT`（ack 契约，CI 实测驱动）：`ack_messagePelEmpty` 改为断言**停机排空**语义（`ack()` →
  `close()` → PEL 为空，与 `StreamMQListener#ack` 的"有界异步流水线"契约一致）；新增
  `ack_asyncPipelineLandsWithoutClose`——`ack()` 后不停机也必须在有界时间内把 XACK 落到 Redis
  （流水线断裂/许可泄漏即红）。

**基准与文档回填（2026-09-20 重跑产物）**

- **JMH 全量重跑并回填**：`streammq-benchmark/BENCHMARK_REPORT.md` §1–§3 的占位符已换成实测值——
  序列化 6 实现 × 双模式（Fory 序列化 4,323,664 ops/s，为 Jackson 的 ~10.3×）、发送 3 模式 × 3 负载
  （异步 100 并发 17,654 / 16,944 / 12,527 ops/s）、消费 2 负载（12,572 / 7,521 ops/s，含逐迭代原始值）。
  报告同时记录运行参数（独立 Redis 6380、`backlog=50000`、`feederThreads=2`）、硬件、实测时长（三组 14 分 25 秒），
  以及有效性证据（两档均 `avgBatchSize=100.0`、`starvedReads=0`、`valid=true`）。EN/ZH README 性能章节同步为同一组数字。
- **更正 2026-09-17 序列化快照的 JDK 结论**：该快照称"JDK 反序列化未被测量——过滤器拒绝基准载荷"，
  与当前 `JdkSerializer.installFilter`（目标类型随调用加入本次放行集）不符；已更正并指向 2026-09-20 实测
  （`jdkDeserialize` 116,017 ops/s、`jdkRoundTrip` 86,815 ops/s）。
- **消费口径提升的可信度补强**：同一基准在旧补货口径下仅 2,383 / 2,018 ops/s（补货端封顶），
  新口径 12,572 / 7,521 ops/s（5.3× / 3.7×），`supplyTight=false` 为机器可读的结论证据。

**R6 复核补充（第二遍独立审计，全部为存量缺陷的残余副本；详见 `docs/REPORT.md` 附录 A）**

> 以下条目含**可观察的行为变化**。0.1.2 为首个 Central 发布版本，均不构成兼容性负担。

- **发送重试的拦截器/指标语义收敛为终态**（行为变化）：重试循环中的**中间失败不再触发**
  `ProducerInterceptor.onException`、也不计失败指标；`afterSend` / `onException` 恰好在终态调用一次
  （成功 → `afterSend(success)`；不可重试 → `onException`；重试耗尽 → `afterSend(failedResult)`）。
  修复前，追踪侧会把"第 1 次失败、第 2 次成功"的生产者 Span 提前以 ERROR 结束（导出的链路永远是失败），
  指标会把一次逻辑发送记成 N 次失败。自定义拦截器若依赖"每次尝试都收到回调"，请改用日志/自有埋点。
- **`syncSendBatch(List, long)` 补上"同 Topic"校验**（行为变化）：javadoc 早已声明该前提与
  `IllegalArgumentException`，实现此前未校验（混合 topic 会静默逐条投递）。现按契约 fail-fast。
- **`ProducerConfig.namespace` 纳入校验**（行为变化）：与消费侧 `ListenerConfig` 同一入口
  （`StringUtils.requireValidNamespace`），非法字符不再静默拼进 Redis Key。
- **`DefaultListenerRegistration` 的数值参数校验统一为 fail-fast**（行为变化）：`shardCount` /
  `streamMaxLen` 不再静默夹取——`shardCount = -1` 此前会变成 0（零分片顺序消费者，不可用且无提示），
  现在直接抛 `IllegalArgumentException`。
- **`CompressionCodec` 约定并统一异常类型**：压缩/解压失败一律抛 `SerializationException`
  （Gzip/LZ4 此前抛其父类 `StreamMQException`，使消费侧按 `SerializationException` 识别"毒丸消息"的分支漏判，
  损坏载荷会被当业务异常反复重试而不是进 DLQ）。因是子类，对既有捕获方向后兼容。
- **`asyncSend(..., SendCallback)` 对 `callback` 做 fail-fast**：传 null 时不再把 NPE 吞成完成线程上的一行 WARN
  （调用方此前既拿不到结果也拿不到异常）。
- **管理端点 4 处错误响应脱敏**：`ackPending` / `triggerRebalance` / `createTopic` / `deleteTopic` 统一改走
  `describeFailure`（响应只含操作名 + 异常类型 + 关联 ID），不再回吐 Redis 版本、完整 Key 名与 `NOGROUP`/ACL 文本。
- **诊断/可观测性修正**：`/streammq/diagnostics/health` 整块 5s 缓存（此前每次 ≈2N 次 Redis 往返）；
  `/actuator/streammq` 的 overview 使用 3s 快照（直接访问 `/groups` 仍实时）；
  `SlowConsumeReport` 删除伪造的 `threadPoolActive`/`threadPoolMax`（实为消费者实例数）改为单一
  `consumerInstances`；`StreamMQTopologyService.getTopicTraces` 增加 500 条结果上限、
  链路耗时改为按时间戳极值（原先"首尾相减"依赖查询顺序，可算出 0 或负数）；
  OTel `Scope` 只在创建线程关闭（此前淘汰/清空可能跨线程关闭，破坏无关线程的 current context）。
- **PEL 认领的目的键自检下沉到 DLQ 分支**：DLQ 键被非 stream 占用时不再阻断整轮扫描（同流重投照常进行），
  需要写 DLQ 的条目跳过并留在 PEL，绝不丢消息。
- **kubernetes**：`envDrift` 判定改为「变量名 → 值」集合语义并在写入时合并（此前 `List.equals` 顺序敏感，
  webhook/sidecar 注入的变量会让调和永久判定漂移、每次 reconcile 都 patch 且抹掉注入变量）；
  `HpaAutoScaler` 补 `@ConditionalOnMissingBean`（防组件扫描场景下双实例双调度线程）。
- **文档/物料**：README 双语新增 `## Deployment` / `## 部署形态`（拓扑支持矩阵 + Cluster 快速失败行为 +
  实测证据链接，守卫异常文案引用的章节此前并不存在）；测试规模口径更新为实测（1482 = 1185 单元 + 297 集成，
  并写明 CI tripwire 下限）；`CONTRIBUTING` 覆盖率门禁补列 `streammq-test`、japicmp 说明改为"当前无内建排除"；
  `NOTICE` 版本对齐（Spring 6.2.x / Netty 4.1.138.Final）并补 `commons-compress`；演示脚本升级到 Spring Boot
  3.5.16、演示指南补 `mvn install` 前置与正确的管理端点 URL；样例 tracing 改用 `${opentelemetry.version}`；
  workflow 注释归属更正；**18 个文件的 `@since 1.1.0`** 校正为 0.1.2。

### Fixed — 第七轮发布前红队审查（R7，发布候选）

> 依据 `docs/fullReview.md` 协议对 11 个模块主源码、文档物料与门禁口径做独立取证（分域并行审计 + 主审逐项代码级核对）。
> 本轮以"上一轮已修过的每一类缺陷是否还有孪生副本"为起点，并把**文档承诺 vs 实现事实**作为独立维度系统扫描。
> 门禁命令 `mvn clean verify -Djacoco.check.skip=false` 实测。逐项证据与裁决见 [docs/REPORT.md](docs/REPORT.md)。

**数据面正确性**

- **顺序消费 PEL 认领的"超限转 DLQ"分支缺少分片锁存活保护（P1）**：同一次扫描中"未超限重投"分支用
  `isShardLockHeld` 保护了"心跳过期但分片看门狗锁仍被存活 handler 持有"的场景，而"超限转 DLQ"分支没有该保护。
  判活第一道依赖实例心跳，当心跳线程被 GC/线程池饱和拖过 `instance-timeout` 而业务线程仍持锁处理中时会误判死亡，
  **正在被合法处理的 ORDERLY 消息被提前复制进 DLQ**（重复投递 + 伪死信污染补偿逻辑）。
  现将该判定**前移到 `retryTimes` 分支之前**，统一保护两条分支。
- **重试/DLQ 重试 ZSet 的写侧使用本机时钟（P2）**：写侧（`scheduleRetry` / `scheduleDlqRetry`）用
  `System.currentTimeMillis()` 生成 score，而读侧（`RetryScheduler` 到期判定）用 **Redis 服务器时钟**比较 ——
  跨主机 NTP 偏差（可达数十秒）会把整条重试链的触发时刻平移，使退避节奏与业务预期不符。
  现抽出 `nowMillis()`（Redis 服务器时钟，失败回退本机并限频 WARN），读写两侧同源。
- **延时孤儿 payload 清理存在"快照-扫描"竞态可误删在线 payload（P2）**：清理侧先在 T0 物化"仍被引用"的
  msgId 快照，再在 T1 扫描 key 做差集；T0→T1 之间新入队的延时消息（payload 与 ZSet 条目在同一原子批中写入）
  不在快照里，会被判定为孤儿并删除 —— 到期转投时读不到 payload 只能进隔离区，**业务视角即静默丢失**。
  现把「是否仍被任一延时 ZSet 引用」的判定与删除下沉为服务端**单条 Lua**（`ZSCORE` 全部 ZSet，任一命中即保留，
  否则 `DEL`），判定与删除原子执行，与调用方快照时点解耦。
- **批量发送静默跳过 body 压缩（P2）**：`syncSendBatch` 未调用 `applyCompression`，而单条与异步路径都调用了
  —— `compress-threshold` 在批量投递时被静默忽略，同一消息的体积随发送方式而变。现已对齐。
- **DEFER 超长延迟的语义与 TTL 保证在 javadoc 中说明**（`handleDefer`）：DEFER **有意不夹取**到 7 天上限
  （业务显式表达"何时再处理"，节奏自控），正确性由 `payload TTL = max(基础 TTL, 延迟 + 宽限)` 保证。
  该口径原仅存在于注释中，现补入方法 javadoc 并保留既有回归用例。

**配置承诺 vs 实现事实（"声明了但从未生效"）**

- **`@StreamMQDlqConsumer` 的 7 个 DLQ 数值属性从未被任何生产代码读取（P2）**：DLQ 调优实际只取全局
  `streammq.dlq.*`，而 `DlqConfig` 的 javadoc 与官方样例却声明"注解优先级最高" —— 用户按文档设置
  `maxDlqRetryAttempts` / `secondaryDlqEnabled` 等后**静默无效**（可能导致死信被过早丢弃）。
  现按真实优先级接线：注解侧改用哨兵（`ANNOTATION_UNSET_*` / `SecondaryDlqMode.INHERIT` / 空串），装配期折算为
  新的 `DlqConfigOverride`（未声明字段为 `null`），运行期由 `applyTo(base)` 合并到全局配置上 ——
  「注解 &gt; 全局 &gt; 框架默认」首次成为可执行事实。新增 `DlqConfigOverrideTest`（8 例）。
- **`DefaultListenerRegistration.Builder.converterInstance(...)` 被静默丢弃（P2）**：公开 Builder 链上该方法只写
  Builder 字段，唯一构造器从未读取 → `getConverterInstance()` 恒为 `null`。内部路径经
  `setConverterInstance(...)` 回填，掩盖了该缺陷。现已回填并加注释。
- **`@StreamMQConsumer.retryStreamMaxLen()` 从未被读取，且与 R2-6 不变式冲突（P2）**：重试流条目是消息的
  **唯一副本**，施加 `MAXLEN` 有损裁剪即等于静默丢消息（R2-6 已因此禁用该裁剪并输出 WARN）。
  该注解属性因此**移除**（保留只会是"配了没用"或"配了就丢数据"）；`README.zh-CN.md` 同步并补说明：
  全局键 `streammq.retry.stream-max-len` 非 0 会在启动期显式 WARN 声明失效。
- **`RedissonClientMissingFailureAnalyzer` 恒不匹配（P2，死代码）**：实现写成
  `REDISSON_CLIENT_CLASS.equals(cause.getBeanType())`，而 `getBeanType()` 返回 `Class<?>` ——
  `String.equals(Class)` **恒为 false**，`analyze()` 永远返回 `null`。"把裸 `NoSuchBeanDefinitionException`
  换成含依赖声明与配置示例的可操作报告"这一 DX 特性**从未生效**。现按 `Class#getName()` 比较，
  新增 `RedissonClientMissingFailureAnalyzerTest`（3 例）。
- **诊断模块三个 analyzer 缺 `@ConditionalOnBean` → 文档承诺的"优雅降级"变成启动失败（P2）**：类级条件只检查
  **类路径**（`@ConditionalOnClass(StreamMQTraceService.class)` 恒真），而三个 `@Bean` 方法把
  `StreamMQTraceService` / `StreamMQListenerContainer` 作为**硬依赖**注入 —— 用户只配
  `streammq.diagnostics.enabled=true` 而未开追踪时，上下文以 `UnsatisfiedDependencyException` 启动失败。
  现补 `@ConditionalOnBean`，与文档"缺前置即不装配"一致。
- **诊断模块属性零校验（P2）**：`backlog-warning-threshold > backlog-critical-threshold` 会让
  `/streammq/diagnostics/health` **常态返回 DOWN**（看板长期误报）；`recent-window-ms = 0` 会让
  `produceRate/consumeRate` 变成 `Infinity` 并随响应体返回。现新增
  `StreamMQDiagnosticsProperties#validate()`（由 `InitializingBean` 启动期调用），
  新增 `StreamMQDiagnosticsPropertiesValidateTest`（8 例）。
- **`streammq.admin.startup-warn` 非法取值可阻断应用启动（P3）**：两处 `ApplicationReadyEvent` 监听器用
  `getProperty(key, Boolean.class)` 读取该键，对 YAML 中很自然的 `off` / `no` / `disable` 会抛
  `IllegalArgumentException` 并穿透 `SpringApplication.run` —— 一个纯日志开关能把启动搞挂。
  现抽出 `StartupWarnToggle`（容错解析 + 非法值限频 WARN + 按启用处理），新增 `StartupWarnToggleTest`（4 例）。
- **`StreamMQProperties#validate()` 覆盖缺口（P3）**：补齐 `retry.stream-max-len`（负值）、
  `dlq.alert-threshold`（&lt; 1）、`dlq.retry-backoff-multiplier`（&lt; 1.0 会使退避延迟坍缩）、
  `dlq.retry-max-delay-ms`（&le; 0 / 小于基础延迟）、`dlq.secondary-dlq-key-prefix`（会直接拼进 Redis Key）、
  `producer.group` / `transaction.default-group`（命名校验）、`trace.storage`（白名单 fail-fast +
  `trace.enabled=true` 但 storage≠redis 时 WARN）。
- **`ProducerConfig` 数值零校验（P3）**：`send-message-timeout` / `stream-max-len` / `compress-threshold` /
  `max-message-size` / `retry-times` 此前被静默接受，与消费侧 `ListenerConfig` 的 fail-fast 口径不一致
  （`maxMessageSize &lt; 0` 会拒绝所有消息、`sendMessageTimeout &lt;= 0` 让每次发送立即超时）。现按同口径校验。
- **`ListenerConfig` 漏校验 `streamMaxLen`（P3）**：同一"数值参数 fail-fast"策略在派生视图上出现漏点。已补齐。

**可观测性 / 健康面（消除"假健康"）**

- **Kubernetes HPA 只看 XPENDING 做积压判定（P2）**：消费者进程全挂时 `XPENDING ≈ 0`（没人读就没有未确认）
  而 `XLEN` 持续增长 —— HPA 会判定"无积压"**永不扩容**（恰是最需要扩容的场景）。现 `BacklogProbe.Result`
  增加 `consumerCount`，HPA 在"无活跃消费者"时以 `streamSize` 作为积压信号，"有活跃消费者"时仍以
  `pendingCount` 为准（避免未裁剪历史导致常年过度扩容）。新增两例守卫。
- **Kubernetes `status.message` 不在 CRD schema 中（P2）**：结构化 status schema 会**静默裁剪**未声明字段，
  因此 Operator 写入的 `message`（镜像缺失、Deployment 创建失败等全部可操作诊断信息）永远不可见。
  现补入 CRD schema；并把声明却从不写入的 `conditions` 字段从 CRD 与 Java 模型**移除**（宁缺勿假）。
- **Kubernetes 优雅关闭被 `@ConditionalOnClass(HealthIndicator.class)` 连带门控（P3）**：`actuator` 在本模块是
  `provided`，未引入 Actuator 的应用**不会**注册优雅关闭处理器（`pause → 等在途 → stop` 整条链路静默失效），
  K8s 滚动发布/驱逐时在途消息被中断。现拆出独立的 `GracefulShutdownConfiguration`（与健康探针解耦）。
- **`HpaAutoScaler` 同时是 `@Component` 与 `@Bean`（P3）**：组件扫描路径下会先注册一个**未经属性注入**的实例
  （全部参数回落硬编码默认值，用户配置静默失效），且不受 `enabled=false` 约束也会启动调度线程。
  现移除 `@Component`，自动装配成为唯一装配真源。
- **K8s 健康指标 / 就绪探针只看 `isRunning()`（P3）**：消费循环批量启动失败时仍报 UP / `ready=true`
  （"假就绪"会把流量导入一个不消费的 Pod）。现纳入消费循环健康并回传失败详情；Binder 健康指示器同修。
- **`getConsumeLoopFailures()` / `isConsumeLoopsHealthy()` 提升为容器接口的一部分（default 方法）**：
  此前只存在于 redisson 具体实现上，导致 starter / Binder / Kubernetes 三个健康面各写各的判据（或无法访问）。
- **`CloudK8sProperties` 零校验（P3）**：`hpa-sync-interval-seconds = 0` 会在
  `afterPropertiesSet` 抛**不带配置键信息**的异常（启动失败但无法定位），而 `reconcile-interval-seconds`
  非法值却被静默忽略 —— 同模块两种口径。现统一为 fail-fast（含跨字段
  `hpa-scale-down-threshold < hpa-scale-up-threshold`），并在 `config-refresh-enabled=true` 而
  `operator.enabled=false` 时输出可操作 WARN（此前"配了但什么都没发生"）。
- **健康检查详情泄漏 Redis 异常文本（P3）**：`Health.down(ex)` 与 `ex.getMessage()` 会把 Key 名 /
  `NOGROUP` / `NOPERM` / 连接串带入健康响应（`show-details=always` 下对外可见）。现详情只给
  「异常类型 + 关联 ID」，完整信息（含堆栈）只进日志。
- **管理端点 `createTopic` 仍在回吐 Redis 异常（P2，脱敏残留副本）**：A-1 统一了其余 4 处却漏了这一处。
- **管理端点 404 响应不再原样回显请求路径（P3）**：改为「段数 + 首段安全字符摘要」，完整路径进日志。
- **管理端点失败限流文案改为英文（P4）**：与其余全英文的机器可读响应体一致（`retryAfterMs` 已给出可操作值）。

**API 契约与文档一致性**

- **`@StreamMQConsumer#orderlyConsumeTimeout()` 的 javadoc 语义写反（P1）**：文档写"`0` = 显式关闭、
  `-1` = 跟随全局"，而代码是"`0` = 继承全局、`<0`（默认 `-1`）= 显式关闭"——与 CHANGELOG、
  `configuration-reference.md`、两份 README **全部相反**，且该 javadoc 随 `sources/javadoc.jar` 发布。
  现已按代码事实重写（英文 README 无对应表格，英文读者只能看 Javadoc，误配会让超时保护语义反向）。
- **`ListenerConfig` / `ListenerRegistration` / `DefaultListenerRegistration` 的"夹取"表述与实现相反（P3）**：
  R6 已把注册模型全部改为 fail-fast，但三处 javadoc 仍写"采取夹取策略"。现统一口径，并显式说明
  `consumeThreads` 是唯一例外（夹取到 [1, 64]）。
- **`ConsumerInterceptor` 声称支持"消息预处理（解密/解压）"（P3）**：`beforeConsume` 只返回 `boolean`，
  `Message` 是不可变值对象 —— 拦截器**没有**把改写后消息交回管线的途径，文档承诺无法实现。
  现改写为真实能力（追踪/审计/限流/只读判断）并指明正确扩展点（自定义 `MessageConverter`）。
- **`MessageConverter` 三参默认方法 javadoc 机制描述错误（P3）**：称"两参与三参互为委托，均未覆盖会互相递归
  → StackOverflowError"，实际是单向委托 + 三参抛 `UnsupportedOperationException`。已改正。
- **`Message` 系统属性插入顺序被静默丢弃（P3）**：`MessageBuilder` / `MessageMetadataBuilder` 都承诺保序，
  但 `Message` 构造器对系统属性用 `HashMap` 拷贝（只对用户属性保序），`addProperty` 同样改用 `HashMap`。
  现统一为 `LinkedHashMap`，新增 `MessagePropertyOrderTest`（4 例）。
- **`TransactionScanState.ofCode` 大小写敏感（P3）**：与同类"线上协议编码"枚举（`TraceStorageType` /
  `DlqReason`）的容错口径不一致，`PREPARE` 与 `prepare` 一个命中一个落到 `UNKNOWN`（走"状态缺失"分支）。
  现统一为 `equalsIgnoreCase`。
- **`BodyTypeResolver` 不识别 `DlqMessageConsumer`（P3）**：`AbstractDlqMessageConsumer<Order>` 的层次遍历
  解析不到目标类型，DLQ 条目缺少 `bodyType` 字段时回退为 `String` 而非声明的 `Order`。现补分支。
- **Binder 消费端 `partitioned=true` 被静默忽略（P3）**：生产端已 fail-fast，消费端不检查 —— 用户按
  Spring Cloud Stream 文档配 `instance-index` 后每个实例仍消费全量消息（分区语义无声消失）。现与生产端同口径。
- **`osv-scanner` SBOM 文件名文档漂移（P3）**：`SECURITY.md` 与 `ci.yml` 注释写 `bom-shipped.json`，
  实际产物是 `bom-shipped.cdx.json`。已统一。
- **`retryStreamMaxLen` 从 zh README 注解属性表中移除**并补"为什么没有该属性"的说明（见上）。

**构建 / 发布工程**

- **根 POM 的 `spring.version` 是死属性且取值与文档矛盾（P3）**：全仓零引用，却携带与
  `NOTICE` / `SECURITY.md`（Spring 6.2.x）不符的 `6.1.14`。已删除。
- **`streammq-test` 把 `streammq-redisson` 声明为 `optional` compile 但主源码零引用（P3）**：已核实
  `src/main` 无任何适配层引用（仅测试源码有），改为 `test` scope，消除"下游读 POM 误判需要适配层"。
- **`codeql.yml` 的 analyze job 缺 `timeout-minutes`（P3）**：与其余 workflow"每个 job 显式预算"口径不一致。已补。
- **`release.yml` 的 `test` job 继承 workflow 级 `contents: write`（P3）**：该 job 只构建/测试，写权限仅
  `publish` 的 Create Release 需要。已下发最小权限 `contents: read`。
- **`streammq-tracing-opentelemetry/pom.xml` 依赖块缩进破损（P3）**：已重新格式化。
- **`CONTRIBUTING` 的 SPI 清单缺 `ExpressionSelectorFilter`**（标题称 18 项却列 17 行）；**zh README
  文档导航缺 `docs/configuration-reference.md`**；**EN README 模块表缺 Binder 的"分区生产不支持"限定**。均已补齐。

**新增回归守卫（失败即红）**

- `DlqConfigOverrideTest`：注解 DLQ 数值属性的"覆盖 / 跟随全局 / 非法值拒绝"三类语义。
- `RedissonClientMissingFailureAnalyzerTest`：分析器必须命中 `RedissonClient`、对无关类型沉默、`beanType` 为 null 不崩。
- `StartupWarnToggleTest`：`off`/`no`/`disable` 等取值不得抛异常（否则阻断启动）。
- `StreamMQDiagnosticsPropertiesValidateTest`：阈值倒挂 / 零窗口 / 非正查询上限一律 fail-fast。
- `MessagePropertyOrderTest`：系统属性、用户属性、`addProperty`、`with*` 全部保序。
- `HpaAutoScalerScanTest`（新增 2 例）：无活跃消费者时以 XLEN 为积压信号；有活跃消费者时以 XPENDING 为准。
- `DefaultListenerRegistrarDlqAndBroadcastTest`（注解代理桩修正）：DLQ 数值属性返回**注解真实默认值**（哨兵），
  否则代理退化成非法取值会让覆盖构造期失败。

## [0.1.1] - 2026-08-29 — 内部迭代版本（未发布到 Maven Central）

### Changed

- **Fury 类注册白名单可配置化，默认放宽为开箱即用**（`streammq.producer.fury-require-class-registration`，默认 `false`）：
  - `FurySerializer` 无参构造默认翻转为宽松模式（`requireClassRegistration=false`），任意 POJO 开箱即用；
    `new FurySerializer(true)` / `new FurySerializer<>(Xxx.class)` 保持强制类注册白名单模式。
  - Spring Boot 自动装配按上述配置实例化默认序列化器；其余序列化器行为不变。
  - **移除 `new FurySerializer(false)` 的 SecurityException 门控**：宽松构造不再要求
    `-Dstreammq.security.allowUnrestrictedSerializer=true`，仅打印 WARN 风险提醒；该系统属性
    保留为"已确认风险"的静默通道（设置后抑制 WARN）。
  - 安全建议：共享/多租户 Redis 请配置 `fury-require-class-registration: true` 并预注册业务类型。

- **默认序列化器由 Jackson 切换为 Apache Fury**（`streammq.producer.serializer` 默认值 =
  `io.github.streammq.adapter.redisson.serializer.FurySerializer`，常量 `StreamMQConstants#DEFAULT_SERIALIZER`）。
  - `fury-core` 在 `streammq-redisson` 中由 `optional` 调整为普通依赖，保证默认装配开箱可用。
  - **数据兼容提示**：切换后新写入消息的 body 为 Fury 二进制格式，与既有 Jackson JSON 消息不互通。
    升级时请先消费完存量消息，或显式配置 `serializer: io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer` 保持原行为。
  - Fury 默认不强制类注册（宽松模式），自定义 body 类型开箱即用；生产环境可通过
    `fury-require-class-registration: true` 开启类注册白名单。

- **新消费者组起始消费位点可配置**（`streammq.consumer.consume-from-where`，默认 `CONSUME_FROM_LAST`）：
  仅首次创建消费者组时生效。默认从 Stream 末尾开始（只消费组创建后新写入的消息，向长期运行 Topic 追加组不会重放历史）；
  需重放历史时设为 `CONSUME_FROM_FIRST`。`@StreamMQConsumer#consumeFromWhere()` 可 per-consumer 覆盖。

### Fixed

- **广播消费者组名碰撞**（`instanceToken` 容器级唯一）：`resolveInstanceToken` 未显式配置时回退到
  **本地主机名**，而主机名是进程级值——同一 JVM 内多个容器实例（测试、多租户宿主）会解析到同一
  标识，导致广播组名（`group:consumerName`）完全相同、广播语义退化为集群（消息只投递给组内一个
  消费者、对端收不到消息）。现在主机名分支追加进程内容器序号：首个容器保持纯主机名（单容器生产
  常态，重启后组名不变、PEL 可恢复），同 JVM 内后续容器依次 `-2`、`-3`……保证容器级唯一。
- 全量复核发现的测试基建加固：
  - IT 基类 Redisson 客户端超时放宽（本地单实例 Redis 全量压测下偶发 3s 响应超时，属负载 flaky）
  - 全仓 spotless 格式统一（历史 CRLF 行尾等存量违规）
- **配置值三方统一（property → 默认值常量 → 运行实际值）**：
  - `@StreamMQConsumer` 的数值属性改用独立"未设置"哨兵（`-1`），消除"注解显式值恰等于常量默认值时被全局配置静默覆盖"的哨兵碰撞配置失效。
  - 修复 `streammq.retry.max-reconsume-times` 此前声明后从未被读取、重试预算只取注解值的配置失效，现已接入解析链。
  - `RedissonStreamListener` 批量校验改以可配置的 `max-batch-size-limit`（默认 1000）为准，避免用户调大上界后校验拒绝的自相矛盾。
  - 延时消息延时超过 7 天时发送侧快速失败（此前 payload TTL 会在投递前过期导致静默丢失）；payload 写入改用 Redis 事务原子提交。
  - `estimateFieldSize` 改为 UTF-8 字节精确估算，消除中文/emoji 高估、ASCII 低估导致的误判。
  - DLQ Stream 新增 `streammq.dlq.stream-max-len`（默认 0=不限制）上限配置。

### Fixed（发布前红队审查第三批 — 2026-09-03）

- **管理端点 CSRF 同源防护**：`/actuator/streammq/**` 的全部写/删操作（POST/DELETE）新增同源校验，
  跨站请求（携带与外域 `Host` 不一致的 `Origin`）直接返回 HTTP 403；不影响 curl / SDK 等合法非浏览器调用。
  新增 `WebRequestAuthSupport#isSameOriginRequest()`（fail-open：非 Web 环境或无 `Origin` 一律放行）。
  仍建议在反向代理层强制同源并启用 Spring Security。
- **删除单条 DLQ 消息需显式确认**：`StreamMQAdminEndpoint#deleteDlq(group, msgId, confirm)` 要求
  `confirm` 必须等于 `msgId`，缺失/不匹配时拒绝并给出明确提示，防止路径参数被误构造时直接删除排障数据
  （破坏性操作不可逆）。`StreamMQActuatorEndpoint` 的 DELETE 分发透传 `confirm` 参数。
- **`AllowAllAuthenticator` 启动强告警（P2-5）**：此前仅 `DenyAll` 触发告警，而零鉴权、可被任意调用方
  删除 Topic/DLQ/重投的 `AllowAll` 反而无提示。现由 `AdminEndpointExposureStartupWarner` 针对该最危险场景
  发出 `SECURITY ALERT`（可通过 `-Dstreammq.admin.startup-warn=false` 抑制）。`StreamMQActuatorEndpoint`
  新增 `allowAll` 标记，由 `StreamMQHealthAutoConfiguration` 在装配时注入。
- **`streammq-test` 集成测试从未执行（P2-2）**：本模块此前未声明 `maven-failsafe-plugin`，导致
  `CoreRedisIntegrationIT`（44 个针对真实 Redis 的核心集成测试）以 `*IT` 命名被 surefire 忽略、又无
  failsafe 接管，在本地与 CI 中均从未运行。现声明 failsafe，无 Redis 时经 `Assumptions` 优雅跳过。
- **CI 默认 `verify` 启用覆盖率门禁（P2-3）**：`ci.yml` 的集成测试步骤改为
  `mvn verify -Djacoco.check.skip=false -Dspotless.check.skip=true`，默认既验证 IT 执行数（tripwire
  防静默跳过），也兜住发布模块的覆盖红线；spotless 由专门 job / 本地 pre-commit 负责，避免重复卡构建。
- **容器 `stop` 执行器生命周期修正**：仅当执行器为容器自有（`ownsExecutor`）时才 `shutdown`，外部注入的
  共享执行器（如 Spring 的 `streammqExecutor`）在 `stop` 时**不再关闭**，避免误关中断事件总线 / 异步发送
  / 事务回查并拖慢停机；对应单元测试修正为先 `start` 离开 INIT 再 `stop`，消除假阳性，并新增
  `stopShutsDownInternalExecutor` 对照用例。
- **DLQ pending 重投原子化（防重复投递）**：DLQ 尾拷贝重投改为原子「`XACK` 旧条目 + `XADD` 副本」
  （`PelClaimScheduler#xaddAndAck` + `LUA_XADD_AND_ACK`），认领成功（返回 1）才写副本；旧条目已被其它
  实例/消费者先行认领时（`XACK` 返回 0）跳过，杜绝并发重复投递。

> **关于 `v0.1.0` 标签（发布前必读）**
>
> 仓库中曾存在指向 `f54b1fe`（2026-08-25）的 `v0.1.0` 标签，而其后有 11 个修复提交（含多项 P0/P1）
> 未被包含。由于 Maven Central 构件**不可变**，同一个版本号不能被重新发布为不同内容，
> 因此 **0.1.0 不再作为发布版本使用**；0.1.1 也仅为内部迭代，首个对外发布版本为 **0.1.2**。
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
> 0.1.1 与 0.1.0 均从未发布到 Maven Central（0.1.1 为内部迭代版本），因此不存在对外数据兼容义务。事务相关 Redis key
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
> - 正确性论证：事务状态机、执行权锁、回查计数等 Lua 脚本为**单 key 原子执行**（`KEYS[1]` 只含一个
>   key），不依赖跨 key 同 slot。**注意例外**：跨 key 的转投脚本必须多 key 原子执行——
>   `TransactionCommitExecutor` 的提交脚本（half 流 + 目标 topic 流 + txstate，3 个 key）与
>   `PelClaimScheduler#xaddAndAck` 在源/目标不同时（源流 + DLQ 流，2 个 key）。这两条路径在
>   **Redis Cluster 下会因跨 slot 报 `CROSSSLOT`**（因此 0.1.x 支持的部署形态为单实例与主从/Sentinel；
>   Cluster 支持列在后续版本议题中，README 的部署建议与此一致）。
> - 兼容义务：0.1.2 为首个公开发布版本，此前版本（0.1.1、0.1.0 标签及其前身）均未公开发布，**无数据兼容
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
- **CI 新增 `coverage` job（P3-13）**：仅针对已发布模块启用 JaCoCo 覆盖率门禁（当时的初版阈值
  LINE ≥ 30% / BRANCH ≥ 15%，防灾难性回退而非考核线；该阈值已在 0.1.2 发布前按实测重设为按模块设卡，
  见上方 0.1.2 章节）；提供 Redis service 运行 verify，让集成测试贡献覆盖率
  （redisson 实测：仅单测约 0.33 行覆盖，含 IT 约 0.64 行覆盖）。
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
  `RedisAvailability`，二者同处 `streammq-test` 模块，但 `RedisAvailability` 对 `streammq-core` /
  `slf4j-api` / `redisson` 的依赖此前被声明为 `<optional>`（不传递），又被 `excludeArtifacts`
  排除发布。结果是：外部用户引入 `streammq-test` 后会得到 `NoClassDefFoundError`，且**无法通过补依赖自救**。
  本轮修复：`streammq-test` 对 `streammq-core` / `slf4j-api` / `redisson` 改为可传递的普通 compile 依赖
  （测试框架与 `streammq-redisson` 仍保持 optional，交由使用方决定版本）。

### Fixed (P1)

- **消费者创建失败从此不再是静默故障**：消费循环在创建监听器失败时（Redis 认证失败、消费者组非法、
  配置错误）此前只打一条 ERROR 日志就退出——消费者在 `/actuator/streammq/groups` 仍然可见、
  健康检查仍然 UP。本轮新增 `LoopFailureReporter` 上报通道，容器登记失败原因并纳入：
  - `DefaultStreamMQListenerContainer#getConsumeLoopFailures()`
  - 健康检查（`HealthIndicator` 在存在启动失败时返回 DOWN，详情含 loopKey → 原因）
  - 管理端点总览 `status` 字段
  `start()` / `stop()` 会清空登记表，避免历史失败影响下一轮判定。
- **广播消费组累积可被观测**：新增 `RedissonBroadcastGroupRegistry#countBroadcastGroups()`、
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
- **`RedissonBroadcastGroupRegistry#countBroadcastGroups()`** 与管理端点总览的 `broadcastGroups` 字段。
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

- live-Redis 集成套件更名为 `CoreRedisIntegrationIT` 归入 failsafe；`RedisAvailability` 以 PING/+PONG 协议握手探测，位于 `streammq-test` 模块（零外部依赖，随 `streammq-test` 一同发布）
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

[0.1.2]: https://github.com/HK-hub/StreamMQ/releases/tag/v0.1.2
[0.1.1]: https://github.com/HK-hub/StreamMQ/releases/tag/v0.1.1
[0.1.0]: https://github.com/HK-hub/StreamMQ/releases/tag/v0.1.0
