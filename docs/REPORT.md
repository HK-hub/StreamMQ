# StreamMQ 发布前红队审查报告（第四轮 · REPORT）

> 审查依据：`docs/fullReview.md` 全文协议（31 节）+ 真实第三方开发者视角 + 发布门禁实测。
> 审查方式：红队式「先证明它不该发布」——静态审计（6 个并行审计员分域）+ 代码级取证 + 真实 Redis 集成测试 +
> 与发布通道**完全一致**的门禁命令实测。所有结论以代码/日志/实测输出为证据，不以文档自述为准。
> 基线：`c7baa29`（第三轮 END 的提交）。第三轮报告与逐项判定见 git 历史（本文件为第四轮报告）。
> **结论摘要：第三轮遗留的 4 项 P2 与 8 组 P3/P4 全部闭环；本轮新发现并修复 1 个 P0、9 个 P1、33 个 P2，以及全部 P3/P4 整备项。**

---

## 1. Executive Summary

第四轮审查没有停留在"复核上一轮结论"，而是**把项目当作第一次发布来否定**：对全部 11 个模块、
4 个 workflow、全部发布物料、全部序列化器（含 0.1.2 新增的 FlatBuffers / SBE）与全部测试做了独立取证。
结果是：上一轮 GO 的**残留项全部真实可修**，且**新发现了若干个上一轮没有暴露的缺陷**——其中最严重的三个：

1. **PEL 认领脚本会在 XADD 失败时静默丢消息（P1）**：脚本语义是"先 XACK 认领、再 XADD 副本"，而 Redis Lua
   无回滚语义。目标键被非 stream 占用（WRONGTYPE）、`maxmemory` OOM 或 ACL 拒绝 XADD 时，条目已从 PEL 消失、
   副本未写入，Java 侧只打 WARN——消息永久消失且无隔离、无指标、无 DLQ。已修复：脚本内 `pcall` + 失败标记，
   Java 侧用内存字段做补偿（直接重试 → 隔离区 Hash + ZSet 登记 + ERROR），并在认领前做**目的键类型自检**
   （不可写则整轮放弃认领，条目留在 PEL）。
2. **JDK 序列化器对合法载荷反序列化失败（P1，可用性缺陷）**：JEP 290 过滤器把 JDK 的「深度/引用」回调
   （`serialClass == null`）误判为未知类并 `REJECTED`；实测含对象回引用的载荷（如两个字段同值的 POJO、
   `ArrayList` 的 `Object[]` 后备数组）必然抛 `InvalidClassException`。已修复：`UNDECIDED` + 结构类型
   （`java.lang.Object`）放行，同时**保留**"未放行类被拒"的反向断言。
3. **发布通道的 CVE 门禁既不可执行也不可满足（P1 + P0）**：`release.yml` 的 publish job **没有 Redis service**，
   却执行带覆盖率门禁的 `mvn clean verify -Djacoco.check.skip=false`（redisson 阈值含 IT 标定）→ 必然
   BUILD FAILURE，deploy 永不执行；同时 CI 只设置 `env: NVD_API_KEY` 而**未传** `-DnvdApiKeyEnvironmentVariable`，
   插件永远不会读取该 key，匿名模式在 NVD 限流下必然失败——"配置 secrets 即可发布"的上一轮结论不成立。
   已修复：publish job 补齐 Redis service；CVE 门禁改为**无密钥可稳定执行**的 SBOM + OSV 扫描（硬门禁），
   OWASP/NVD 降为有密钥时的增强扫描并传入正确参数。

此外，本轮把上一轮标记为"残留/0.2.0"的 12 组问题**全部按根因修复**（不是文档化绕过）：
顺序消费锁竞争不再消耗业务重试预算（不再把未处理消息送进 DLQ）、广播身份文件按应用分片且不再互相覆盖、
租约心跳与拉取循环解耦、判活窗口与心跳间隔跨字段校验、事务 MISSING 状态显式降级、PEL 扫描游标分页
（消除头部饥饿）、调度扫描窗口扫满、投递指标不再高估、消费循环陈旧 Future 清理、`MessageId` 语义自洽、
`BatchMessage` 异常口径统一、Jackson 升级到修复版（2 个 High CVE）、`ConsumeAction.DEFER` 死常量与
`StreamMQEventBus` 死 SPI 移除、IDE 元数据/演示脚本/README 与代码事实对齐（含中文版三处语义反转）。

**最终裁决见 §6。**

---

## 2. 审查范围与方法

| 维度 | 覆盖 | 方法 |
|---|---|---|
| 架构/模块 | 11 个模块、20 个 reactor 项目、全部 pom 与依赖 scope | 依赖图、发布集推导、SPI 边界、循环依赖检查 |
| 核心代码 | core 103 + redisson 117 + starter 20 + 其余模块全部主源码 | 逐类审计 + 行级取证 + 编译告警（`-Werror -Xlint:unchecked`） |
| 序列化/安全 | 8 个内置序列化器（含新增 FlatBuffers/SBE）、PayloadTypeSafety、JdkSerializer JEP 290 过滤器 | 畸形字节实证、JDK 过滤器行为探针、CVE 区间比对（OSV `2.17.2 → 修复区间`） |
| 数据面语义 | PEL 认领、重试/DLQ、延时、事务半消息、顺序消费、广播、背压、ACK 管线 | 真实 Redis IT + 故障注入 + 不变量断言 |
| 生命周期/并发 | 容器 start/stop、调度器 start/stop、心跳、线程池、资源释放 | 竞态窗口分析 + 陈旧 Future 复现 + 命名线程核查 |
| 测试体系 | 1206 个用例（surefire + failsafe） | 零断言扫描、弱断言扫描、失败路径覆盖、夹具可达性 |
| DX/文档 | README 双语、配置参考、CONTRIBUTING、SECURITY、NOTICE、CHANGELOG、8 个样本 | 文档条目→代码事实逐条核对（含 IDE 元数据与 jar 内产物） |
| CI/发布 | 4 个 workflow 逐 job 逐 step、japicmp、JaCoCo、中央仓库物料、ISO 时间戳 | YAML/shell 解析实跑 + 参数拼接实测 + 门禁命令实测 |
| 性能 | JMH harness、基准报告口径、调度器吞吐、N+1 | harness 与容器路径对比、扫描窗口与 RTT 计数 |

**验证纪律（本轮与上一轮最大的不同）**：任何"通过/GO"结论必须由与发布通道**完全相同**的命令
`mvn clean verify -Djacoco.check.skip=false` 的原始输出支撑（普通 `mvn clean verify` 不启用覆盖率门禁）。

---

## 3. 本轮发现与处置（按严重级）

### P0（发布阻断）

| 编号 | 问题 | 证据 | 处置 |
|---|---|---|---|
| R4-01 | `release.yml` 的 publish job 无 Redis service，却执行含 IT 标定阈值的门禁 verify → 覆盖率必然跌破 → deploy 永不执行 | `release.yml` publish job（无 `services:`）vs `mvn clean verify -Djacoco.check.skip=false`；redisson 阈值 LINE 0.60 依赖 IT 贡献 | FIXED：publish job 补齐与 test job 一致的 `redis:7.2` service + 健康检查 |

### P1（发布前必修）

| 编号 | 问题 | 证据 | 处置 |
|---|---|---|---|
| R4-02 | PEL 认领脚本 `XACK → XADD` 在 XADD 失败时静默丢消息（无隔离/无指标/仅 WARN） | `PelClaimScheduler` 旧脚本 + WRONGTYPE/OOM/ACL 触发路径 | FIXED：`pcall` + `XADD_FAILED` 标记 + Java 补偿（重试 → 隔离区 Hash/ZSet + ERROR）+ 目的键类型自检；新增 `PelClaimCompensationIT`（先断不认领、再断恢复后可投递） |
| R4-03 | JDK 序列化器过滤 `serialClass == null` 一律 REJECTED → 任何含回引用/数组的合法载荷反序列化失败 | JDK 21 `ObjectInputStream.filterCheck(null, -1)` 实跑探针；`ArrayList` 的 `Object[]` 触发 `checkArray` REJECTED | FIXED：`UNDECIDED` + `java.lang.Object` 放行；`JdkSerializerTest` 新增 3 例（回引用 List、同值 POJO、未放行类仍被拒） |
| R4-04 | CVE 门禁永不可通过：`NVD_API_KEY` 只进 env，插件参数未传；且 OWASP 硬门禁在无 key 时必然 408/429 | dependency-check-maven 12.1.1 参数语义（`nvdApiKeyEnvironmentVariable` 无默认值）+ 工作流命令 | FIXED：新增无密钥硬门禁 `sbom-scan`（CycloneDX SBOM + osv-scanner v2，退出码判定）；OWASP 传入 `-DnvdApiKeyEnvironmentVariable`/`-DnvdValidForHours`/`-DdataDirectory` + 缓存 + 收敛到发布集，无 key 时按设计跳过 |
| R4-05 | 顺序消费锁竞争计入业务重试预算 → 从未执行 handler 的消息被 ACK 进 DLQ（R3-25 根因） | `RedissonOrderlyShardLockManager` 竞争返回 `RECONSUME_LATER`；`DefaultMessageProcessor` 预算循环 | FIXED：新增 `OrderlyShardBusyException`（零 SPI 破坏）+ 锁预算内多轮等待；处理器单独捕获：不计数、不写 retry ZSet、不 DLQ、不 ACK（留 PEL 由认领兜底），限频 WARN |
| R4-06 | 广播实例身份文件是"每 OS 用户单例"，同机多应用互相覆盖 → 身份抖动、组名漂移、停机期消息静默跳过 | `BroadcastInstanceIdResolver` 旧实现（默认 `~/.streammq/instance-id`、无条件覆盖） | FIXED：默认路径按 `namespace+group` 分片；文件内 `id pid timestamp` 多记录；重启复用**已退出进程**身份、绝不覆盖活进程身份 |
| R4-07 | 广播租约心跳只在拉取循环内刷新 → handler 超过租约超时（20s）期间被同机第二进程抢占，广播静默退化为集群 | `RedissonStreamListener` 心跳调用点（doRead 之后） | FIXED：独立心跳线程（5s 固定间隔，守护线程，close 取消） |
| R4-08 | IDE 元数据与代码事实相反：`serializer` 默认写 Fury、`fury-require-class-registration` 默认写 false、虚构 `RoundRobinRebalanceStrategy` | 解包 0.1.2 jar 内 `spring-configuration-metadata.json` | FIXED：默认值改为 Jackson/true，补全 8 个序列化器 hint，删除虚构类 |
| R4-09 | 版本身份四方矛盾（FlatBuffers/SBE 在 `[Unreleased]` vs SECURITY "0.1.3 起" vs README 已列为 0.1.2 内置） | CHANGELOG/SECURITY/README/pom 交叉比对 | FIXED：统一为 0.1.2；`[Unreleased]` 合并进 `[0.1.2]` |
| R4-10 | 被 zh README 作为"一键演示"的脚本必然失败（BOM 0.1.0 从未发布、`@EnableStreamMQ` 已删除、`singleServerConfig` 无绑定） | `docs/demo/quickstart-demo.sh` 三处 vs CHANGELOG/代码 | FIXED：脚本按当前 API 重写（0.1.2 BOM、`spring.data.redis.*`、无 `@EnableStreamMQ`） |

### P2（应修，全部闭环）

| 编号 | 问题 | 处置 |
|---|---|---|
| R4-11 | PEL 扫描"头部饥饿"：头部被存活消费者占据时其后条目永不被扫描 | 游标分页（本轮扫完从最后一条之后继续，到达尾部回起点） |
| R4-12 | 判活窗口与心跳间隔无跨字段校验（窗口 < 心跳间隔 ⇒ 活跃慢消费者被判死并复制重投） | 启动期校验 `pel-claim-min-idle-ms >= 3 × heartbeat-interval-ms` |
| R4-13 | 广播租约参数零校验（0/负值让槽位"立即过期"） | 构造期快速失败（`leaseTimeout > 0` 且 `grace >= leaseTimeout`） |
| R4-14 | 事务状态 MISSING 时 commit 请求被静默忽略（半消息不投递也不清理） | ERROR + 降级 UNKNOWN 走有界回查（元数据丢失时以 ROLLBACK 明确终结） |
| R4-15 | 调度扫描每轮少扫一条（`batchSize - 1`） | 改为扫满 `batchSize`（retry/delay 两处） |
| R4-16 | 延时投递指标高估（未真正投递也计数） | 仅原子批成功计指标；未投递路径降为 DEBUG |
| R4-17 | 消费循环陈旧 Future 让新循环被静默 cancel → "静默不消费" | 提交前清理已完成 Future；取消分支 WARN |
| R4-18 | DLQ 目标无实例心跳 → 活跃慢 DLQ 消费者被复制重投 | DLQ 注册同样创建组管理器（心跳行与消费者名 token 同源，判活可精确命中） |
| R4-19 | DLQ 失败策略返回 null 时的求值顺序错误（NPE 吞掉、兜底成死代码） | 先判 null 并按默认策略处理；热路径 INFO 降 DEBUG |
| R4-20 | Jackson 2.17.2 命中 2 个 High CVE（发布门禁将红） | 升级 **2.18.10**（`jackson-bom` 声明在 Boot BOM 之前，避免被 2.17.2 覆盖） |
| R4-21 | SBE 读取侧无边界校验（4 字节长度可放大为 2GB 分配、裸异常逃逸） | 信封头/截断/声明长度校验 + `SerializationException` 包装 |
| R4-22 | FlatBuffers 读取侧无界（Blob/集合声明长度、深嵌套爆栈、循环引用） | 单字段上限（min(消息上限, 64MB)）+ 深度上限 64 + `StackOverflowError` 包装 + 写侧环检测 |
| R4-23 | FlatBuffers 类型映射缺口（非 String 键静默变形、Set/Deque/SortedMap 字段失败） | 非 String 键写侧快速失败；读侧按声明类型构造；javadoc 矩阵同步 |
| R4-24 | 序列化器 null/空语义不一致（4 个返回 `byte[0]`、2 个返回 null/抛异常） | 统一为 `serialize(null) → null`、`deserialize(null\|empty) → null` + 表驱动回归测试 |
| R4-25 | `consumeThreadMin/Max` 语义与生态相反（只配 max = 单线程且无提示） | 新增 `consumeThreads` 为唯一推荐旋钮；min 废弃但兼容、max 废弃且显式设置时 WARN |
| R4-26 | `ConsumeAction.DEFER` 单例携带 null 延迟 → 返回它时静默失效 | 移除常量（首个公开发布前收敛）；DEFER 必须携带正延迟提升为构造期不变量 |
| R4-27 | `StreamMQEventBus` 是死 SPI（实现 + Bean 齐全但零调用、零订阅） | 移除接口/实现/Bean |
| R4-28 | `MessageId` 的 `equals/hashCode` 与 `compareTo` 不一致（`"01-2"` vs `of(1,2)`）；`of(-1,-1)` 生成不可回解析 ID | 统一数值语义 + 规范化 + 非负校验 |
| R4-29 | `BatchMessage` 异常类型与 javadoc 不符（ISE vs IAE；add NPE vs addAll IAE） | 统一 `IllegalArgumentException` |
| R4-30 | `MessageMetadataBuilder` null 值 NPE 后移、`Map.copyOf` 丢序；`Message` 延时无校验 | setter 即时判空、保序不可变视图、`Message` 构造统一校验（含 7 天上限） |
| R4-31 | namespace 在程序化路径不校验（可注入 `:` 破坏键结构） | `ListenerConfig`/`DefaultListenerRegistration` 统一走 `requireValidNamespace` |
| R4-32 | `JdkSerializer` 的"回引用误判"修复未覆盖 core javadoc 中的 Fury 表述与错误交叉引用 | core javadoc 统一为 "Apache Fory（原 Fury）" + 坐标/下限/门禁 |
| R4-33 | 9 处 javadoc 指向不存在的模块 `streammq-redisson-adapter` | 全量更正为 `streammq-redisson` |
| R4-34 | CI tripwire 对"单模块 IT 整体跳过"失明（redisson 111 个 IT 全跳过仍通过） | 改为按模块下限断言（redisson ≥100、starter ≥30、test ≥40、samples ≥16、全局 ≥230、跳过率 ≤20%） |
| R4-35 | japicmp 可静默跳过（`ignoreMissingOldVersion` 默认 true = 假绿）；`io.github.streammq.internal.*` 是死配置 | 显式 `ignoreMissingOldVersion=false` + 报告断言；删除死 exclude，兼容策略与代码事实对齐 |
| R4-36 | `coverage` job 与 verify job 重复执行门禁；staging-smoke 反向断言清单硬编码；`help:evaluate` 多模块输出未清洗 | coverage 改为复用 verify 数据仅出报告；staging 动态读 `excludeArtifacts`；`-N` + `tail -n1` |
| R4-37 | benchmark 工作流在干净 runner 上必然失败（未 install、无 `-am`） | 改为 `-pl streammq-benchmark -am install` |
| R4-38 | 消费基准 harness 绕过容器路径且受补货端约束（对消费侧回归不敏感） | 口径如实披露（下界 + 绕过容器 + 攒批 XACK）；新增 `feederSanityThroughput` 探针 + `main()` 有效性判定：feeder < 3 × consume 时打印 INVALID RUN 并以非零码退出 |
| R4-39 | 13 个零断言用例、样本 IT 固定命名空间/固定载荷（跨运行假红） | 13 处改为显式断言（部分叠加日志/行为证据）；8 个样本 IT 全部改为随机命名空间 + `@AfterAll` 清理 + 载荷唯一化 |
| R4-40 | CONTRIBUTING 示例/命令/覆盖率目标与代码事实不符（4 处编译错误示例、错误 FQCN、80%/90% 不实目标） | 按真实 API 重写；FQCN/阈值更正 |
| R4-41 | 文档事实错误：广播僵尸组清扫 TTL 参数、zh README 顺序消费超时语义三处反转、SPI 示例签名、死锚点、延时 7 天上限缺失、诊断模块配置缺失 | 全部按代码事实更正/补齐（EN/zh/config-reference 三处同步） |
| R4-42 | NOTICE 缺 4 个第三方依赖、Jackson 版本过时；README 技术栈表缺项 | 补齐（FlatBuffers/Agrona/SBE/fabric8）+ 版本同步 |
| R4-43 | `SECURITY.md` 全中文而 EN README 指向它 | 重写为英文为主（附中文摘要），技术口径与代码逐条核对；依赖基线与 CVE 门禁口径同步更新到 3.5.16 线 |

### 发布工程与依赖基线（第二轮迭代，2026-09-18 晚）

首次 push 后 CI 真实执行暴露了 CVE 门禁的三层问题，本轮全部闭环：

| 编号 | 问题 | 证据 | 处置 |
|---|---|---|---|
| R4-44 | 新建的 SBOM 步骤必然失败：`makeAggregateBom` 默认 `skipNotDeployed=true`，根工程（pom 打包）不参与 deploy → `target/bom.json` 根本不生成 | CI job "CVE gate (CycloneDX SBOM + osv-scanner)" 日志：`target/bom.json was not generated`；本地复现 + 插件字节码核对 | FIXED：显式 `-Dcyclonedx.skipNotDeployed=false`；本地实测生成 198 组件 SBOM |
| R4-45 | CVE 门禁口径不可操作：聚合 SBOM 含 samples/未发布模块（Tomcat 等无关依赖），且"任意公告即失败"会让门禁长期变红 | 本地 osv-scanner 实测：聚合扫描 24 个含公告包（13 个 High/Critical），其中多数属示例应用依赖面 | FIXED：门禁范围收敛为**发布构件依赖闭包**（`bom-shipped.cdx.json`，BFS 裁剪，剔除其它 streammq 模块与 provided/optional 面）；阻断阈值 CVSS ≥ 7.0（与 OWASP 时代 `failBuildOnCVSS=7` 对齐），<7.0 打印且随 JSON 报告上传为构建产物 |
| R4-46 | 依赖基线本身带着 13 个 High/Critical 公告：这些公告只能由 Boot 3.5 / Spring 6.2 / Spring Data 3.5 / Micrometer 1.15 线修复，旧基线（Boot 3.3.x + Redisson 3.34.1）无法修复 | OSV 公告区间逐一比对（spring-core/expression 修复于 6.2.x，spring-data-commons 3.5.12，micrometer 1.15.12，actuator starter 3.5.12，spring-boot 3.5.12，netty 4.1.136+） | FIXED：基线升级到 Spring Boot **3.5.16** + Redisson **3.52.0** + Spring Cloud Stream **4.3.3** + Jackson **2.21.4** + Netty **4.1.138.Final** + AssertJ 3.27.7 + commons-compress 1.27.1；升级后闭包扫描 0 个 ≥7.0 公告（余 5 个 Medium 观测项） |
| R4-47 | 升级暴露 Redisson 3.5x 行为变化：对"组已存在"的 `XGROUP CREATE` 会先做约 5s 退避重试才抛 BUSYGROUP（3.34 立即返回）→ 组预先存在时（重启/预建组/IT）监听器启动被阻塞数秒、启动窗口消息漏读 | 独立探针实测：3.34.1 二次调用立即返回；3.52.0 二次调用耗时 **4865ms**；streammq-test 的 `ConsumerTests` 由 6/6 绿变为 4 失败 | FIXED：`ensureGroup` 改为**先探测（`listGroups()`）再创建**——命中即返回，语义不变且启动零延迟；修复后 6/6 恢复绿 |
| R4-48 | 升级暴露 Spring Cloud Stream 4.3 兼容问题：`AbstractExtendedBindingProperties` 自 4.3 起通过 `ConfigurableApplicationContext` 注入 `propertiesBinder`，纯单测环境为 null → `StreamMQMessageBinderTest` 2 例 NPE | CI 与本地 failsafe 日志（`propertiesBinder is null`） | FIXED：测试装配提供最小 `AnnotationConfigApplicationContext` 并注入（生产路径由 Spring 注入，不受影响）；binder 13/13 恢复绿 |

**依赖基线与支持矩阵**：0.1.2 以 Spring Boot 3.5 构建，支持矩阵表述为 **Spring Boot 3.3–3.5**（README 双语已同步）；
`streammq-bom` 仍不 import `spring-boot-dependencies`（版本由使用方 BOM 决定）。

**CVE 门禁的本地可复现命令**（与 CI `sbom-scan` 一致）：

```text
mvn -DskipTests -Dcyclonedx.skip=false -Dcyclonedx.skipNotDeployed=false org.cyclonedx:cyclonedx-maven-plugin:makeAggregateBom
# 裁剪发布闭包 → target/bom-shipped.cdx.json（BFS 脚本与 CI 的 "Filter the SBOM..." 步骤逐字一致）
osv-scanner scan source -L target/bom-shipped.cdx.json --format json --output-file target/osv-report.json
# 阈值裁决：任一 CVSS ≥ 7.0 → 阻断（实测：0 个阻断 / 5 个 Medium 观测项）
```

### P3/P4（本轮一并闭环的整备项）

- **`RateLimitedAuthenticator`**：淘汰扫描加上界（防表满时 O(表大小)/请求的 CPU 放大）；鉴权耗时不再让时间基准偏早。
- **`TraceRecord`/`TraceCollector`**：record 的 `attributes` 补防御拷贝；`Slf4jTraceCollector` 用例补日志事件断言。
- **接口内 `Logger` 常量下移**；`AbstractDlqMessageConsumer` 空实现基类删除；`OrderlyShardLockManager` 泛型化。
- **`CodecExplicitnessTest` 守卫扩充**：getter 清单补全（含 `getAtomicLong` 等无 codec 形参的结构 getter 的说明口径），最小匹配数 60 → 75；全仓 0 违规维持。
- **`StreamMQKeys.transactionLock` 死键删除**；`StreamMQKeys.quarantinePayloadHash` 新增（认领补偿落盘）。
- **可复现构建**：`project.build.outputTimestamp`；CI 三个未 pin 插件（versions/help/dependency）固定版本。
- **workflow 卫生**：4 个 workflow 增加 `concurrency`；第三方 action 固定到 commit SHA；job 级 `timeout-minutes`；surefire/failsafe 增加 `forkedProcessTimeoutInSeconds`。
- **`.gitignore`** 补 `.claude/`、`redisson341/`；`.gitattributes` 维持 LF 归一化。
- **kubernetes 模块**：`*IT` 误命名改为 `*Test`（纯上下文用例，不依赖基础设施）。
- **样本/测试卫生**：`application-it.yml` 注释中的"默认 Fury"更正为 Jackson。

---

## 4. 未被本轮完全闭环的事项（诚实声明）

1. **真实 Maven Central 发布未执行**（无凭据）：`-Pgpg deploy` → Portal staging 的签名/校验流程、
   `autoPublish=false + waitUntil=validated` 的行为需一次 dry-run 才能定论。发布链路的所有**代码侧**前置
   （Central Portal 插件、BOM、sources/javadoc、GPG profile、staging 隔离 + 反向断言）已就绪。
2. **GitHub Actions runner 上的真实执行未发生**：4 个 workflow 的改动已通过 YAML 解析、
   `bash -n` 语法检查与关键脚本的参数拼接实测，但"远端真实跑通"要等一次 push。
3. **JMH 未重跑**：基准报告的消费/序列化数字口径已如实标注（下界、绕过容器、攒批 XACK），
   harness 已加"补货端吞吐 ≥ 消费测量 ×3"的有效性断言；容器路径端到端基准列为后续工作。
4. **Redis Cluster 未实测**：CROSSSLOT 为推断（已按 0.1.x 不支持 Cluster 明示）。
5. **`streammq-kubernetes` 的业务逻辑未深度审计**（模块不发布、无消费者；仅在 reactor 内编译与薄用例覆盖）。

---

## 5. 验证证据

### 5.1 门禁命令（与发布通道完全一致）

```text
mvn clean verify -Djacoco.check.skip=false
```

| 项 | 结果 |
|---|---|
| Reactor | **20/20 模块 SUCCESS**（含依赖基线升级与 CVE 门禁改造后的最终树） |
| 测试总数（surefire + failsafe） | **1206**（单元 958 / 集成 248） |
| 失败 / 错误 / 跳过 | **0 / 0 / 0** |
| JaCoCo 覆盖率门禁 | 发布模块全部达标，无 `Rule violated`（按「实测 −3pt」口径设卡） |
| `spotless:check` | 全模块通过 |
| enforcer | Java 21 / Maven 3.9 / dependencyConvergence / banDuplicatePomDependencyVersions 全绿 |

逐模块覆盖率（门禁命令实测）：

| 模块 | LINE 实测 | LINE 门槛 | BRANCH 实测 | BRANCH 门槛 |
|---|---:|---:|---:|---:|
| streammq-core | 0.502 | 0.48 | 0.449 | 0.44 |
| streammq-redisson（含真实 Redis IT） | 0.646 | 0.60 | 0.564 | 0.50 |
| streammq-spring-boot-starter | 0.603 | 0.55 | 0.457 | 0.41 |
| streammq-test（本轮新增门禁） | 0.815 | 0.78 | 0.424 | 0.39 |
| streammq-diagnostics（不发布） | 0.815 | 0.75 | 0.724 | 0.69 |
| streammq-tracing-opentelemetry（不发布） | 0.775 | 0.70 | 0.680 | 0.65 |
| streammq-spring-cloud-stream-binder（不发布） | 0.807 | 0.75 | 0.703 | 0.67 |

**证据文件**：`/tmp/streammq-r4/verify-gate6.log`（最终树上的门禁命令原始输出，20/20 SUCCESS、1206 测试）。
同一目录保留本轮迭代过程的门禁日志（gate1…gate5：分别暴露 core 断言口径、JdkSerializer 回引用、DeferActionIT
时序预算、streammq-test 空批异常口径等失败并据此修复），可逐轮对照。

### 5.2 本轮新增/强化的"失败即红"用例（节选）

| 用例 | 锁定的失败路径 |
|---|---|
| `PelClaimCompensationIT` | WRONGTYPE 目的键：认领不发生（不丢消息）→ 恢复后自动继续 |
| `JdkSerializerTest#roundTripListWithSharedReferences` | 回引用载荷不被 JEP 290 误杀（且未放行类仍被拒） |
| `RedissonStreamListenerAckFailureTest` | 异步 XACK 失败：不抛、许可释放、有界排空 |
| `TransactionScannerFailureInjectionTest` | COMMITTING/ROLLBACKING 预算耗尽强制终结 + `failureReason`；回查器异常/超时降级 |
| `OrderlyMessageIT#orderlyListener_shardLockBusy_*` | 锁竞争不消耗预算、不进 DLQ、消息留 PEL；业务失败路径仍按预算进 DLQ |
| `PoisonEntryHandlingIT` | 字段缺失毒丸不阻断读循环、合法消息继续消费、毒丸可隔离 |
| `DeferActionIT` | `defer(Duration)` 延迟生效、不消耗重试预算、期间不进 DLQ |
| `SerializerNullSemanticsTest` | 6 个序列化器 × null/空输入语义一致 |
| 8 个样本 IT | 随机命名空间 + 清理 + 载荷唯一化（跨运行不再假红） |

---

## 6. Final Verdict

### 6.1 发布阻断清单

| 级别 | 数量 | 状态 |
|---|---:|---|
| P0 | 1 | 全部闭环（R4-01） |
| P1 | 9 | 全部闭环（R4-02 … R4-10） |
| P2 | 33 | 全部闭环（R4-11 … R4-43） |
| P3/P4 | 上述整备项 | 全部闭环 |

### 6.2 维度评分

| 维度 | 分数 | 依据 |
|---|---:|---|
| 产品目标 | 9 | 问题定义与取舍诚实；"为何不用 X"有实质回答 |
| 功能完整度 | 9 | 重试/DLQ/延时/事务/顺序/广播/背压/可观测齐备；Cluster 与容器化基准列为后续 |
| 架构 | 9 | 依赖无环、SPI 缝真实、发布面收敛；God 容器等结构性整备已部分推进 |
| 模块设计 | 8 | 职责可解释；kubernetes 仍为不发布模块（已降噪） |
| 设计模式/原则 | 8 | 模式均有实际价值；重复脚本/调度模板已完成共享化收敛 |
| 实现质量 | 9 | 本轮修复 3 个静默数据语义缺陷（丢消息/误 DLQ/误判活） |
| API / SDK | 9 | Builder + 不可变值对象 + 类型化异常；死 API 已移除，兼容策略与门禁对齐 |
| Maven 工程 | 9 | 门禁命令与发布通道一致；CVE 硬门禁无需人工前置；japicmp 不再假绿 |
| 测试 | 9 | 1206 用例 + 真实 Redis IT + 故障注入 + 失败即红用例；零断言清零 |
| 并发 | 9 | 竞争信号与业务失败解耦、心跳解耦、陈旧 Future 清理、竞态守卫齐备 |
| 性能 | 8 | 热路径无阻塞往返；基准口径如实；容器路径基准为后续 |
| 安全 | 9 | 反序列化白名单/边界/门禁齐备，Jackson 升级修复 2 个 High CVE |
| 文档 | 9 | 双语 + 配置参考 + SECURITY 英文主体；事实性冲突已全量更正 |
| Developer Experience | 9 | Quick Start 可用、IDE 元数据正确、演示脚本可跑、错误信息可定位 |
| 开源准备度 | 9 | 许可证/治理/发布流程就绪；发布物料口径一致 |

```text
Overall = round(132 / 15 × 10) = 88 / 100
```

> 分数只用于排优先级；是否发布由 6.3 的门禁规则决定（无未决 P0/P1/P2）。

### 6.3 裁决

```text
Release Status: GO
Release Readiness Score: 88 / 100

Must Fix Before Release: 0 items
Should Fix:             0 items
Open P2:                0 items
Open P3/P4:             0 items（结构性整备见 §4 的后续工作，均不影响发布）
Release Prerequisite:   无（CVE 门禁为无密钥硬门禁：发布闭包 SBOM + osv-scanner，实测 0 个 >=7.0 公告；
                        有 NVD key 时额外获得每周 OWASP 深扫）
```

**与上一轮的差异必须被记录**：第三轮裁决 GO 但保留 4 项 P2 与 8 组 P3/P4（"0.2.0 / 1.0 前"）。
本轮把**全部残留项按根因修复**，并新增修复了上一轮未发现的 1 个 P0 与 9 个 P1（其中排序最前的是：PEL 丢消息 / JDK 序列化可用性 / 发布门禁不可执行）。
本报告中的每一条"已修复"都以工作区代码 + 门禁命令实测为证；§4 列出的 5 项为**外部环境依赖**的未执行验证，
不属于代码缺陷。
