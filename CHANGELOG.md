# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- 启动时管理端点暴露面 WARN：`AdminEndpointExposureStartupWarner` 在 `ApplicationReadyEvent` 阶段检测 `/actuator/streammq/**` 是否在主应用端口（不受 `management.endpoints.web.exposure.*` 治理），启用且未隔离时输出安全提醒；可通过 `-Dstreammq.admin.startup-warn=false` 关闭。
- Maven `maven-antrun-plugin` 在 `generate-test-resources` 阶段生成 `target/it-list.txt`：全项目 `*IT.java` 集成测试清单，作为 CI 工件 `integration-tests.txt` 上传，配套 verify tripwire 防止 Redis 静默失效。
- Spring Cloud Stream Binder 模块级 `package-info.java` 增加依赖与限制说明（Redisson 传递依赖、分区生产不支持、DenyAll 鉴权器默认）。
- `DefaultPerConsumerSpiResolver` 全局默认 `RebalanceStrategy` 回退路径：`streammq.rebalance.strategy` 配置（默认 `ConsistentHashRebalanceStrategy`）现真实生效——`@StreamMQConsumer` 注解未显式指定 `rebalanceStrategy` 时优先使用全局配置。
- 集成测试 `DefaultPerConsumerSpiResolverRebalanceTest`：覆盖三种回退路径（无全局 / 全局为 ConsistentHash / per-consumer 覆盖全局）。
- `TransactionLockManager` / `TransactionCommitExecutor` / `TransactionRetentionSweeper` / `TransactionMetricsRecorder` 四个事务协作类（拆分自 `TransactionScanner` god class），均可在隔离单元测试中独立验证。
- `CONTRIBUTING.md` 新增「Cutting a Release」章节：Central Portal 发布流程、首次人工 Publish 步骤、autoPublish 翻转 checklist、凭据配置、发布门禁。

### Changed

- 集成测试在无 Redis 环境统一自动跳过（含 Spring Boot 自动装配 IT），保证 `mvn verify` 在任意环境可复现
- 调度器（Retry/Delay/Transaction/PelClaim）SmartLifecycle 相位调整为先于消费容器启动、晚于其停止
- 事务消息：未注入 TransactionScanner 时快速失败（不再提供"先投递再回滚"的假事务回退路径）
- 诊断 REST 报告增加 locale-neutral `code` 字段，message 文本改为英文；移除伪造的线程池活跃度指标
- `OrderProducer`（streammq-sample-quickstart）从 308 行精简为 4 个核心方法：保留 `createOrder` / `createOrderWithBuilder` / `createOrderAsync` / `createOrdersBatch`；更复杂的 `oneway / callback / metadataBuilder / timeout-retry` 模式迁移至 `streammq-sample-interceptor` 与 `streammq-sample-delay`。
- 文档导航：`docs/02-architecture.md` / `03-functional-design.md` / `04-detailed-design.md` 移入 `docs/historical/`，README 文档导航表只保留 `docs/01-PRD.md` 与 Javadoc，提示历史设计稿仅供考古。
- README「环境要求」新增提示：`mvn verify` 需要本地 Redis（`localhost:6379`），无 Redis 时 IT 自动跳过，CI 通过 Docker service 提供。

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