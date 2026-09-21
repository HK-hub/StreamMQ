# StreamMQ 发布前红队审查报告

> **本文件包含三轮报告**：
> - **最新：第六轮（R6）** —— 见文末 [第六轮发布前红队审查报告（R6）](#第六轮发布前红队审查报告r6)。
>   未发现 P0；新修 5 个 P1（静默不投递 / 唯一副本丢失 / 状态覆盖 / 广播语义退化）、45 个 P2 与全部可执行 P3/P4；
>   第五轮 §13 的六项未闭环项闭环五项（真实 Central 发布以无签名 dry-run 演练替代）。
>   终局门禁 20/20 SUCCESS、1482 用例 0 失败/0 跳过（06:17），裁决 **GO 90/100**（§17）；并记录一次
>   **CI 真实 runner 复核**（R5 推送的 CI 红 → 根因是用例断言了异步 ack 未承诺的同步语义，见 §15.6）。
>   **另附 [附录 A](#附录-a--r6-复核补充第二遍独立审计)**：R6 主体闭环后的第二遍独立审计（4 个分域审计员 +
>   逐行核对冻结树）新修 8 项 P2 与全部可执行 P3/P4 残余副本，门禁复验 20/20 SUCCESS、1482 用例 0 失败/0 跳过，
>   主裁决不变。
> - 第五轮（R5）—— 门禁命令首跑即红，修复后复验 20/20 SUCCESS、1249 用例全绿。
> - 第四轮（下文）—— 历史记录，保留以供追溯。

# 第四轮报告（历史记录）

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
2. ~~**GitHub Actions runner 上的真实执行未发生**~~ → **已闭环（2026-09-18）**：提交 `9ef0877` 的 CI
   全绿（run [35328121221](https://github.com/HK-hub/StreamMQ/actions/runs/35328121221)）：
   Guards / CVE gate（SBOM + osv-scanner）/ Formatting / Build / Test / Verify（含全部真实 Redis IT）/
   Coverage report / Staging smoke 全部 success（OWASP 深扫按设计在无 NVD key 时跳过——默认 CVE 门禁
   为无需密钥的 sbom-scan）。过程中由 CI 实测驱动修复了 5 项（R4-44…R4-48）。
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

### 5.1b CI 真实执行证据（GitHub Actions）

| Job | 结果 |
|---|---|
| Guards (parent↔BOM sync + publish set consistency) | success（并实测捕获 `streammq-bom` 的 redisson 版本漂移 → 已修复） |
| CVE gate (CycloneDX SBOM + osv-scanner) | success（发布闭包：0 个 ≥7.0 公告） |
| Formatting / Build / Test | success |
| Verify (Integration, 真实 Redis) | success（首次运行即暴露并修复了 `BroadcastPauseHeartbeatIT` 的启动竞态） |
| Coverage report (published modules) | success |
| Staging smoke (consumer-side resolve) | success |
| OWASP deep scan | skipped（无 `NVD_API_KEY`；默认硬门禁为无需密钥的 sbom-scan） |

> 结论：**CI 通道在真实 runner 上全绿**，报告 §4 中"未在 runner 上执行"的保留项已闭环。

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

---

# 第五轮发布前红队审查报告（R5）

> 审查依据：`docs/fullReview.md` 全文协议（31 节）。
> 审查方式：**红队式「先证明它不该发布」**——静态审计（4 个并行审计员分域 + 主审逐项代码级取证）+
> 与发布通道**完全相同**的门禁命令实测（`mvn clean verify -Djacoco.check.skip=false`）+ 真实 Redis 集成测试。
> 基线：`058d217`（第四轮 END 的提交）。
>
> **结论摘要：本轮门禁命令首跑即红（`BroadcastPauseHeartbeatIT` 确定性失败），据此推翻第四轮"GO"的
> 隐含前提「门禁命令可绿」；修复该阻断项后又发现并修复 2 个 P1、14 个 P2 与全部可执行 P3/P4 整备项。
> 最终 20/20 模块 SUCCESS、1249 用例 0 失败 0 跳过、CI 同口径 tripwire 全部达标。**

---

## 1. Executive Summary

第四轮报告以「门禁命令实测全绿」作为 GO 的核心证据。本轮**第一步就复现了该前提不成立**：

```text
mvn clean verify -Djacoco.check.skip=false
→ [ERROR] Tests run: 1, Failures: 0, Errors: 1 -- in io.github.streammq.adapter.redisson.it.BroadcastPauseHeartbeatIT
→ BUILD FAILURE (streammq-redisson)  ← 发布通道 deploy 永不执行
```

根因不是"环境抖动"，而是**测试断言的写法与 Awaitility 的语义不匹配**（见 §4 R5-01）：条件内直接调用
`XINFO GROUPS`，在 stream 键尚未创建时抛 `ERR no such key`，而 Awaitility 对条件抛出的异常**默认立即上抛、
不重试**——于是"等组建好"这个断言在快机器上**确定性失败**。这类缺陷在 CI runner 上可能因时序不同而侥幸通过，
正是"本地/CI 双绿但发布通道红"的典型形态。

修复该阻断项后，本轮对 11 个模块、4 个 workflow、全部发布物料与全部测试做了独立取证，新发现并修复：

- **2 个 P1（数据面静默不投递）**：顺序消费失败被写进"无人消费"的重试 Stream；运行期动态注册的消费者
  未绑定调度目标（失败消息写入重试 ZSet 后无人扫描，payload 7 天过期即丢失）。
- **14 个 P2**：Redis 服务器时钟从未真正取到、事务回查批量恒少一条且 `batch-size=1` 时完全失效、
  `MessageConverter` 返回不可变 Map 时 DLQ/重试路由失效、广播身份文件并发写互相覆盖、
  监听器工厂 create/close 竞态、诊断端点无参数校验且健康状态恒 UP、管理端点回吐 Redis 内部异常、
  运行期组配置上限过宽、GPG 非交互签名缺失、`osv-scanner` 二进制无校验、发布资产清单与发布集不符、
  `versions:set` 可能漏改 BOM、CI 三处缺超时预算、以及一批发布物料版本漂移。
- **全部可执行 P3/P4 整备项**：调度目标只增不减、广播僵尸组清扫 `maxSweep-1`、
  `checkerTimeoutMillis<=0` 导致回查线程永久阻塞、`FailureRetryLimiter` key 空间可无界增长、
  注解 `consumeFromWhere` 无法表达"显式 LAST"、`Message` 构造器不校验必填 body 等。

**最终裁决见 §19。**

---

## 2. Project Understanding

| 维度 | 事实（以代码/物料为准） |
|---|---|
| 目标 | 把已有 Redis（Stream）变成消息总线，提供 RocketMQ 风格编程模型，避免另建 MQ 集群 |
| 用户 | 中小规模（< 1 亿/天）、已有 Redis、Spring Boot 3 的 Java 21 团队 |
| 核心能力 | 注解消费、Template/Service 双发送 API、事务半消息、延时、顺序、批量、DLQ（含二级）、Tag/SQL92 过滤、压缩、背压、可观测、管理 REST |
| 架构 | `core`（抽象/SPI，零 Spring）→ `redisson`（适配实现）→ `spring-boot-starter`（装配/端点）；另有 tracing / diagnostics / binder / kubernetes / test / samples / benchmark |
| 发布面 | 5 个构件（parent/bom/core/redisson/starter/test）；其余为 source-only，`excludeArtifacts` 明确排除 |
| 技术路线 | Redis Stream + Redisson；JDK 21（虚拟线程消费循环）；安全默认（Jackson 取代 Fury 作默认序列化器） |

**目标一致性：PASS。** 未发现目标漂移；README 的"为什么不用 X"（RTopic / 原生 XADD+XREADGROUP / RocketMQ / Kafka）
有实质回答，且"不推荐场景"（超大规模、严格 ACID、多机房、IoT）诚实划界。

---

## 3. Architecture Review

- **依赖方向：PASS。** `core` 不依赖 Spring（反射解耦 Web 上下文），`redisson` 依赖 `core`，`starter` 依赖二者；
  无循环依赖（enforcer `dependencyConvergence` + `banDuplicatePomDependencyVersions` 全绿）。
- **SPI 缝：PASS。** 18 个扩展点均有真实默认实现与解析路径（注解 `Class` 属性或 Spring Bean 覆盖），非 ServiceLoader。
- **生命周期：PASS（本轮补强后）。** 容器/调度器/监听器工厂的 start/stop/close 幂等且有竞态守卫；
  本轮补上监听器工厂的 create/close 互斥（R5-08）。
- **扩展机制：PASS。** `SchedulerTargetBinder` / `RegistrationStore` / `MessageProcessor` 等协作对象接口化，
  用户可替换；本轮把"批量绑定"与"单注册项绑定"收敛为同一规则源（R5-03）。

**结构性观察（非缺陷）**：`DefaultStreamMQListenerContainer` 仍是较大的编排类（约 1400 行），
但其职责已通过 `ListenerContainerMetadata` / `DefaultSchedulerTargetBinder` / `DefaultConsumeLoopSupervisor`
等协作对象外移，且所有懒构建点已统一为 double-checked locking。属可接受的"编排类"体量，不构成发布阻断。

---

## 4. Release Blockers（本轮发现，全部已闭环）

### P0（发布阻断）

| 编号 | 问题 | 证据 | 处置 |
|---|---|---|---|
| R5-01 | 门禁命令首跑即红：`BroadcastPauseHeartbeatIT` 在真实 Redis 上确定性失败（`XINFO GROUPS ... ERR no such key`），发布通道 `deploy` 永不执行 | `mvn clean verify -Djacoco.check.skip=false` 原始输出：`Tests run: 1, Errors: 1 <<< FAILURE!`，BUILD FAILURE `-rf :streammq-redisson` | FIXED：该断言用 Awaitility 等待"实例专属组建立"，但条件内直接 `XINFO GROUPS`；stream 键在建组完成前不存在时抛异常，而 Awaitility **默认不重试异常**。改为「先判键存在 + `ignoreExceptions()`」，使断言真正表达"等到组建好为止"。（注：生产路径 `groupAlreadyExists` 本身有 `catch` 兜底，非产品缺陷；但门禁红等价于不可发布。） |

### P1（发布前必修）

| 编号 | 问题 | 证据 | 处置 |
|---|---|---|---|
| R5-02 | 顺序消费失败被路由进**无人消费**的重试 Stream → 静默丢失 | `DefaultMessageProcessor:233-249`（过滤器异常→`handleAction(RECONSUME_LATER)`）、`:161-163`（`Throwable` 兜底→`handleFailure`→同样 `RECONSUME_LATER`）；而 `DefaultConsumeLoopSupervisor:51-58` 只为 AUTO_ACK 提交 retry 循环，ORDERLY 无循环读 retry Stream；`DefaultSchedulerTargetBinder` 却按 `(topic,group)` 注册了 retry 目标 → ZSet 被扫、消息被转投进无人读的 Stream | FIXED：在 `DefaultRetryAndDlqHandler.handleAction` 收口——ORDERLY（非 DLQ）的非成功动作一律 `routeToDlq(dlqReason=maxRetryOrderly)` 并 ACK；DLQ 写入失败则保留 PEL。新增 `OrderlyFailureRoutingTest`（含"绝不调用 `createBatch`"断言） |
| R5-03 | 运行期动态注册的消费者未绑定调度目标 → 失败消息永不重投 | `DefaultStreamMQListenerContainer.wireRegistrationIfRunning`（旧实现只建组管理器 + 提交读循环，无 `schedulerBinder` 调用）；调度目标仅在 `start()` 批量绑定一次 | FIXED：`SchedulerTargetBinder` 新增 `bindTargets`/`unbindTargets`（单注册项），批量与单注册项共用 `bindOneRetryTarget`/`bindOnePelClaimTarget` 单一规则源；容器保留 `RetryScheduler` 引用并在动态注册时补绑。新增 `DefaultSchedulerTargetBinderTest`（7 例，含 ORDERLY/AUTO_ACK/DLQ/广播四类映射与 null 调度器安全跳过） |

### P2（应修，全部闭环）

| 编号 | 问题 | 处置 |
|---|---|---|
| R5-04 | **「读取 Redis 服务器时钟」从未真正生效**：Lua 返回标量整数却声明 `ReturnType.MULTI`，解码异常被 `catch` 吞掉 → 始终静默回退本机时钟（跨主机 NTP 偏差会误判实例存活 / 误踢消费者 / 复制重投） | 抽出 `RedisServerClock`（`ReturnType.INTEGER` + 单一实现），`PelClaimScheduler` / `RedissonConsumerGroupManager` 共用；新增 `RedisServerClockIT`（真实 Redis 上必须取到时间，锁定返回类型与脚本语义一致） |
| R5-05 | 事务回查批量扫描恒少一条，`batch-size=1` 时 `LIMIT 0` → **回查彻底失效、半消息永久悬挂** | `TransactionScanner.scanTimeoutHalf` 改为 `count = batchSize`（与 Retry/Delay 调度器口径统一） |
| R5-06 | `MessageConverter` 返回不可变 Map（如 `Map.of`）时，`routeToDlq` / `handleDefer` / 二级 DLQ 直接 `put` → `UnsupportedOperationException` 被吞成 "DLQ routing failed"，消息永久滞留 PEL | 三处统一先做可变拷贝（`new LinkedHashMap<>(...)`）；由 `OrderlyFailureRoutingTest` 以 `Map.of` 载荷锁定 |
| R5-07 | 广播实例身份文件并发写互相覆盖：临时文件名固定 `<name>.tmp`（同机多进程互踩），且 read-modify-write 无互斥 | 临时文件名带 `pid-UUID`；`upsertLocalIdRecord` 加 JVM 内互斥 + 跨进程 `FileChannel.lock()`；`finally` 清理临时文件 |
| R5-08 | 监听器工厂 `createListener` 与 `close()` 竞态 → 新建监听器逃逸出排空队列、永不关闭且持续续租 Redis 槽位 | 引入生命周期锁，使「检查 closed + 入队」与「置位 + 排空」互斥；构建期间被关闭则自关闭并抛错 |
| R5-09 | 诊断端点（`/streammq/diagnostics/**`，挂在**主端口**）对 `topic`/`group`/`messageId` 零校验，原始输入被拼进 Redis Key 与鉴权资源串 | 与 Actuator 端点统一走 `StringUtils.requireValidName`；非法输入 → 400 且不触达下游（新增 7 例单测） |
| R5-10 | 诊断健康概览 `status` 恒为 `UP`：严重积压/慢消费期间仍报健康 | 按积压严重度与慢消费者推导 `UP`/`DEGRADED`/`DOWN`（新增常量与 4 例状态推导测试） |
| R5-11 | 管理端点把 Redis 内部异常信息（Key 名、`NOGROUP`、连接/ACL 文本）原样回吐 HTTP 响应 | 新增 `describeFailure(operation, ex)`：响应只给「操作名 + 异常类型 + 关联 ID」，完整信息（含堆栈）只进日志；7 处 catch 分支统一收敛 |
| R5-12 | 运行期组配置上限过宽：`inflightCapacity` 可设 `Integer.MAX_VALUE`（背压队列 OOM 面）、休眠/退避/宽限可设 `Long.MAX_VALUE`（消费循环近乎静默停摆） | 收敛为 `[0, 100000]` 与 `[1, 300000]`，并以具名常量 + javadoc 说明取值依据 |
| R5-13 | GPG 非交互签名缺失 → 首发在 `sign` 步骤中断：`gpg` profile 未声明 `--pinentry-mode loopback`（gpg 2.1+ 无 tty 时必须显式），也未显式传入口令 | 根 POM 与 BOM 的 `gpg` profile 补齐 `gpgArguments`；`release.yml` 以 `-Dgpg.passphrase="$MAVEN_GPG_PASSPHRASE"` 显式传入（不依赖"插件恰好读取同名环境变量"的隐式约定） |
| R5-14 | CVE 硬门禁的执行体 `osv-scanner` 下载后**无完整性校验**直接执行（它决定"是否阻断发布"，属供应链投毒面） | 固定官方 SHA-256（取自 GitHub Release 资产元数据 digest）并以 `sha256sum -c` 校验，失败即中断 |
| R5-15 | Release 资产清单仍列出 `tracing`/`diagnostics`/`binder` 三个 Central **不可解析**的模块，与同处注释及 `CONTRIBUTING.md` 直接矛盾 | 资产收敛为 Central 可解析的发布集（bom/core/redisson/starter/test），注释同步 |
| R5-16 | `versions:set` 可能漏改**无 `<parent>`** 的 `streammq-bom` → 发布后 `streammq-bom:<新版本>` 永不存在，使用方 import 失败 | 加 `-DprocessAllModules=true` 并新增断言步骤（BOM 版本 ≠ 目标版本 → 硬失败） |
| R5-17 | CI 的 guard / build / formatting 三个 job 缺 `timeout-minutes` | 分别补 15 / 30 / 20 分钟，与其余 job 口径一致 |

### P3/P4（本轮一并闭环）

- **调度目标只增不减**：`unregister` 新增 `schedulerBinder().unbindTargets(...)`，配合
  `RetryScheduler.unregisterRetryTarget` / `PelClaimScheduler.unregisterTargets`（DLQ 的 sentinel 维度亦成对解除）。
- **广播僵尸组清扫 `maxSweep - 1`**：`maxSweep=1` 时 `LIMIT 0` → 永不回收。改为 `maxSweep`。
- **`checkerTimeoutMillis <= 0` 使事务回查线程无限等待**：等待是 `join(Duration)`，`join(0)` 语义为永久等待 →
  挂死的 checker 永久持有 `groupLock`，回查调度停摆。setter 快速失败 + 使用点兜底夹取。
- **`FailureRetryLimiter` key 空间可无界增长**（冷却窗口内以大量不同 target 失败时清理无效）：
  清理后仍满则放弃记录——限流退化为放行，但 key 空间恒有界。
- **注解 `consumeFromWhere` 无法表达"显式 LAST"**：枚举属性无 `null` 哨兵，旧默认值取 `CONSUME_FROM_LAST`
  导致"未声明"与"显式 LAST"不可区分 → 全局设为 `FIRST` 时，想强制 `LAST` 的消费者被**静默忽略（语义反向）**。
  新增独立哨兵 `ANNOTATION_DEFAULT`（注解默认值），三分支语义可表达；补 `ConsumeFromWhereResolutionTest`。
  **0.1.2 尚未发布，此处属"发布前收敛"，不构成兼容性负担。**
- **`Message` 的 `body` 契约描述错误**：javadoc 声明 body「必填」，但框架**有意**支持 null body（无载荷消息；
  内置序列化器统一 `serialize(null) → null` / `deserialize(null|empty) → null`，对端可能发来无载荷消息）。
  真实边界是「**发送 API** 必填、**值对象**可为 null」。本轮曾尝试"收口到构造器"强制非空，门禁命令立即暴露
  `toStreamFieldsNullBody` 等既有契约被打破 —— 遂改为**改写 javadoc 明确边界**，而非加上破坏性校验
  （这正是"先实测、再下结论"的价值：单看代码会把它误判为缺陷）。
- **`DefaultListenerRegistration.consumerFilter` 暴露内部可变数组**：构造期改为 `clone()`，并覆盖 Lombok
  getter 返回副本（与 `shardLocks` 的防御性拷贝口径一致）。
- **发布物料版本漂移**（见 §13）：`NOTICE` 6 处、两个 README 的技术栈表与徽章、
  `SECURITY.md` 中文摘要、`CONTRIBUTING.md` 门禁阈值与文件名、`configuration-reference.md` 的异常类型/超时语义/
  默认值/精确键、starter POM 的 `<description>`（含已删除的 `@EnableStreamMq`）、根 POM 与 CHANGELOG 中
  Jackson 版本与"Boot 管理 2.17.2"的过期前提。

---

## 5. Top Problems（按严重度，前 10）

1. R5-01 门禁命令确定性失败（发布通道不可用）。
2. R5-02 ORDERLY 失败进无人消费的 retry Stream（数据面静默丢失）。
3. R5-03 动态注册消费者无调度目标（失败消息永不重投）。
4. R5-04 Redis 服务器时钟从未生效（跨主机判活失准）。
5. R5-05 事务回查 `LIMIT batchSize-1`（`batch-size=1` 时回查失效）。
6. R5-06 不可变 Map 载荷使 DLQ/重试路由失效（消息滞留 PEL）。
7. R5-13 GPG 非交互签名缺失（首发中断）。
8. R5-07/R5-08 广播身份文件互踩 + 监听器工厂竞态（身份漂移、槽位泄漏）。
9. R5-09/R5-10/R5-11 诊断与管理端点：无校验、假健康、内部信息泄漏。
10. R5-14 `osv-scanner` 无校验 + R5-15 资产清单错位 + R5-16 BOM 版本可能漏改（发布工程可信度）。

---

## 6. Concurrency Review

| 项 | 结论 |
|---|---|
| 线程安全 | PASS。值对象构造期防御性拷贝 + 不可变视图；协作对象状态以 `volatile` / 并发容器 / 显式锁保护 |
| 竞态 | **本轮修复 3 处**：监听器工厂 create/close（R5-08）、广播身份文件并发写（R5-07）、调度目标绑定/解绑的原子口径（R5-03） |
| 死锁 | PASS。事务回查由"分布式锁"改为 Lua CAS 抢占，无锁泄漏；`groupLock` 的等待已加有界超时（R5 补 `>0` 校验） |
| 线程池 | PASS。各调度器构造建池 / `stop()` 关闭 / 支持 restart 重建；RejectedExecution 有处理；容器执行器按 `ownsExecutor` 决定是否关闭 |
| 异步 | PASS。XACK 失败不抛、许可释放、有界排空 |

**已知设计权衡（非缺陷，如实声明）**：顺序消费分片锁采用 Redisson 看门狗续期（不设固定 lease），
以"严格有序"优先于"卡死 handler 自动让位"；卡死 handler 需进程重启解除。该权衡已在
`RedissonOrderlyShardLockManager` 与 `orderlyConsumeTimeout` javadoc 中声明，1.0 前保持不变。

---

## 7. Performance Review

- 热路径无阻塞往返；消费循环使用虚拟线程；选批/ACK 管线化；`MultiLock`/Lua 原子操作替代读改写。
- 本轮修复的 `LIMIT` 语义问题同时消除了"扫描窗口少一条"的隐性性能语义缺陷。
- 已知待办（不影响发布）：调度器孤儿清理仍有逐条 `isExists` 往返（N+1），已记录为后续优化项；
  容器路径端到端基准仍为后续工作，现有基准口径已在 README 如实标注（下界 + 绕过容器 + 攒批 XACK）。

---

## 8. Security Review

- **改进**：诊断端点参数校验（R5-09）、管理端点错误信息脱敏（R5-11）、运行期配置上限收敛（R5-12）、
  CVE 门禁执行体完整性校验（R5-14）。
- **既有 PASS**：默认 `DenyAllAuthenticator`；破坏性操作强制 `confirm`；XFF 默认不信任且需 `trusted-proxies` CIDR 才采信；
  鉴权失败限流按不可伪造的 `remoteAddr` 聚合且 fail-closed；反序列化默认 Jackson（无 gadget 面）、
  Fury 强制类注册白名单、JDK 过滤器 JEP 290；载荷派生类型经 `PayloadTypeSafety` 黑名单；凭据不落日志。

---

## 9. Test Review

- **失败即红守卫新增 4 个类 / 14 例**：`OrderlyFailureRoutingTest`(2)、`DefaultSchedulerTargetBinderTest`(7)、
  `RedisServerClockIT`(1)、`StreamMQDiagnosticsEndpointTest`(7)、`ConsumeFromWhereResolutionTest`(4)。
- 门禁命令实测：**1249 用例，0 失败 0 错误 0 跳过**；集成测试分模块 119/36/44/22、全局 **271**（CI tripwire 下限 100/30/40/16、230 全部达标，跳过率 0% ≤ 20%）。
- 已知覆盖缺口（不影响发布）：容器路径端到端基准、Redis Cluster 实测、`streammq-kubernetes` 业务逻辑深度审计。

---

## 10. Documentation Review

本轮修正的文档与代码不一致项（全部以代码事实为准）：

- README（EN/ZH）：技术栈表 Redisson / Jackson 版本、Spring Boot 与 Redisson 徽章；zh 基准环境表标注"历史测量环境"。
- `SECURITY.md`：中文摘要与英文正文的 Jackson 版本与 CVE 门禁口径统一。
- `CONTRIBUTING.md`：tripwire 阈值改为真实口径、`parent.pom.xml` → `pom.xml`、Release 资产说明。
- `docs/configuration-reference.md`：校验异常类型、`orderly-consume-timeout-millis` 三分支语义与生效条件、
  `trace.storage` 默认值口径、`admin.startup-warn` 为直读精确键。
- `NOTICE` / 根 POM / CHANGELOG：第三方版本与前提描述对齐实际基线。

---

## 11. Developer Experience Review

- **上手路径**：Quick Start 可编译、可运行；README 明确"必须自带 `redisson-spring-boot-starter`"、
  "`/actuator/streammq` 需显式 exposure"、"`streammq-diagnostics` 需额外坐标"等易踩坑点。
- **可诊断性**：健康检查 `DOWN` 携带 `listenerContainer.consumeLoopFailures`；消费循环失败与运行期持续失败
  均可观测；本轮又消除了两处"假健康"与"内部信息外泄"。
- **幂等与兼容**：0.1.2 为首个发布版本，本轮对注解默认值等公共语义的收敛**不构成兼容负担**。

---

## 12. Open Source Readiness Review

- 治理文件齐备（LICENSE / NOTICE / CONTRIBUTING / SECURITY / CODE_OF_CONDUCT / Issue & PR 模板）。
- 发布集守卫（Guard job）对 `parent ↔ BOM ↔ excludeArtifacts` 三方一致性做断言。
- 供应链：第三方 action 固定 commit SHA；`osv-scanner` 二进制固定 SHA-256（本轮）。
- 已知保留：真实 Maven Central 发布（`-Pgpg deploy`）与 Portal staging 校验尚未执行（需凭据）；
  本地已就绪全部代码侧前置，且本轮补齐了此前必然导致首发中断的 GPG 非交互配置。

---

## 13. Release Blockers（最终）

| 级别 | 数量 | 状态 |
|---|---:|---|
| P0 | 1（R5-01） | 全部闭环 |
| P1 | 2（R5-02、R5-03） | 全部闭环 |
| P2 | 14（R5-04 … R5-17） | 全部闭环 |
| P3/P4 | 上述整备项 + 发布物料一致性 | 全部闭环 |

**未闭环项（诚实声明，均非代码缺陷）**：

1. 真实 Maven Central 发布未执行（无凭据）——代码侧前置已全部就绪，且本轮修复了会导致首发中断的 GPG 配置缺陷。
2. JMH 未重跑（基线与口径已如实标注；harness 已加补货端有效性断言）。
3. Redis Cluster 未实测（0.1.x 明确不支持，CROSSSLOT 为推断）。
4. `streammq-kubernetes` 业务逻辑未深度审计（模块不发布、当前无消费者；本轮仅核查其线程/资源与校验缺口）。
5. 调度器孤儿清理的 N+1 往返（性能优化项，不影响正确性）。
6. 顺序消费分片锁的看门狗续期策略（严格有序 vs 卡死让位的显式设计权衡，已在 javadoc 声明）。

---

## 14. Verification Evidence

### 14.1 门禁命令（与发布通道完全一致）

```text
mvn clean verify -Djacoco.check.skip=false
```

| 项 | 结果 |
|---|---|
| Reactor | **20/20 模块 SUCCESS** |
| 测试总数（surefire + failsafe） | **1249** |
| 失败 / 错误 / 跳过 | **0 / 0 / 0** |
| JaCoCo 覆盖率门禁 | 7 个模块全部执行 `check`，无 `Rule violated` |
| Spotless | 全模块通过（本轮对全部改动文件执行 `spotless:apply` 后复验） |
| enforcer | Java 21 / Maven 3.9 / dependencyConvergence / banDuplicatePomDependencyVersions 全绿 |

集成测试分模块执行数（CI tripwire 口径）：

| 模块 | 实际执行 | 门禁下限 |
|---|---:|---:|
| streammq-redisson | 119 | 100 |
| streammq-spring-boot-starter | 36 | 30 |
| streammq-test | 44 | 40 |
| streammq-samples/* | 22 | 16 |
| **全局** | **271** | **230** |
| 跳过率 | 0% | ≤ 20% |

### 14.2 本轮新增"失败即红"用例（全绿）

| 用例 | 锁定的失败路径 |
|---|---|
| `BroadcastPauseHeartbeatIT` | 广播暂停期心跳保活 + 恢复不重放（并修掉其自身的断言竞态） |
| `OrderlyFailureRoutingTest` | ORDERLY 非成功动作必须进 DLQ 且**绝不**触碰重试调度 |
| `DefaultSchedulerTargetBinderTest` | 单注册项绑定与批量绑定目标集合一致；注销成对解除；null 调度器安全跳过 |
| `RedisServerClockIT` | 真实 Redis 上必须取到服务器时间（锁定 `ReturnType` 与脚本返回值一致） |
| `StreamMQDiagnosticsEndpointTest` | 非法参数 400 且不触达下游；健康状态按严重度推导 |
| `ConsumeFromWhereResolutionTest` | 「未声明跟随全局」与「显式覆盖全局」两条语义都可表达 |

---

## 15. Final Verdict

### 15.1 维度评分

| 维度 | 分数 | 依据 |
|---|---:|---|
| 产品目标 | 9 | 问题定义与取舍诚实；"为何不用 X"有实质回答与划界 |
| 功能完整度 | 9 | 重试/DLQ/延时/事务/顺序/广播/背压/可观测/管理齐备 |
| 架构 | 9 | 依赖无环、SPI 缝真实、发布面收敛；绑定规则收敛为单一来源 |
| 模块设计 | 9 | 职责可解释；本轮把调度目标绑定规则去重 |
| API / SDK | 9 | Builder + 不可变值对象 + 类型化异常；发布前收敛了注解哨兵语义 |
| 实现质量 | 9 | 本轮修复 2 个静默不投递、1 个失效的回查窗口、3 处并发/资源缺陷 |
| 测试 | 9 | 1249 用例 + 真实 Redis IT + 故障注入 + 失败即红守卫；跳过率 0 |
| 并发 | 9 | 竞态与生命周期守卫补齐；已知有序性权衡显式声明 |
| 性能 | 8 | 热路径无阻塞往返；容器路径基准与 N+1 清理列为后续 |
| 安全 | 9 | 默认拒绝 + 参数校验 + 错误信息脱敏 + 供应链校验齐备 |
| Maven 工程 | 9 | 门禁命令与发布通道一致；CVE 硬门禁无需密钥；GPG 非交互签名可用 |
| Developer Experience | 9 | Quick Start 可用、错误信息可定位、健康检查不假绿 |
| 文档 | 9 | 双语 + 配置参考 + SECURITY；本轮全量消除事实性冲突 |
| 可维护性 | 9 | 单一规则源、防御性拷贝、javadoc 与实现对齐 |
| 可扩展性 | 9 | 18 个扩展点 + 协作对象接口化，均可替换 |
| 开源准备度 | 9 | 治理/许可/发布流程/物料一致性就绪 |

```text
Overall = round(139 / 16 × 10) = 87 / 100
```

> 分数用于排优先级；是否发布由 15.2 的门禁规则决定（无未决 P0/P1/P2）。

### 15.2 裁决

```text
Release Status: GO
Release Readiness Score: 87 / 100

Must Fix Before Release: 0 items
Should Fix:             0 items
Open P0/P1/P2:          0 / 0 / 0 items
Open P3/P4:             0 items（§13 的未闭环项均为外部环境依赖或显式设计权衡，非代码缺陷）

门禁证据: mvn clean verify -Djacoco.check.skip=false → 20/20 SUCCESS，1249 用例，0 失败/跳过
发布前置: 无（CVE 硬门禁为无密钥的发布闭包 SBOM + osv-scanner 扫描；
          GPG 非交互签名配置已就绪；Release 资产与 Central 发布集一致）
```

**与上一轮的差异必须被记录**：第四轮裁决 GO，但其核心证据"门禁命令全绿"在本轮**首跑即被推翻**
（R5-01，确定性失败而非 flaky）。本轮实际是从"门禁红"开始，把阻断项与随后发现的 2 个 P1、14 个 P2
全部按根因修复，最终以门禁命令全绿 + CI 同口径 tripwire 达标作为裁决依据。这再次印证
`fullReview.md` 的核心原则：**不因"上一轮已通过"而默认当前仍然成立**。

---

# 第六轮发布前红队审查报告（R6）

> 审查依据：`docs/fullReview.md` 全文协议（31 节）。
> 审查方式：红队式「先证明它不该发布」——静态审计（分域并行审计员 + 主审逐项代码级取证）+
> **真实 3 主 Redis Cluster 实测** + **JMH 全量重跑** + 与发布通道**完全相同**的门禁命令实测。
> 基线：`b5e6486`（第五轮闭环的提交）。

**结论摘要：本轮未发现 P0。新发现并修复 5 个 P1、45 个 P2（含发布通道门禁 11 项）与全部可执行 P3/P4；
第五轮 §13 的六项未闭环项闭环五项（JMH 重跑 / Redis Cluster 实测 / kubernetes 深度审计 / 孤儿清理 N+1 /
顺序锁租约语义），仅"真实 Maven Central 发布"因无私钥口令以**无签名 dry-run 演练**替代。
门禁命令终局全绿（详见 §15）。**

---

## 1. Executive Summary

第五轮的审查重心是"自证门禁可绿"与 AUTO_ACK 数据面。本轮换一个起点：**假设第五轮修复过的每一类缺陷，
都在其它路径上存在同构副本**；并把第五轮未触及的三个域（Redis Cluster 语义、kubernetes 模块、
基准方法学）与发布通道本身纳入取证范围。事实证明这个假设成立——本轮最重的发现都不是"新功能没做"，
而是同一类缺陷的孪生副本：

- R5-02 修复了 AUTO_ACK 的失败重投（旧实现把消息写进**无人消费**的重试 Stream），但**顺序消费（ORDERLY）
  的失败路径**仍有三条分支只"留在 PEL 等认领"——而 PEL 认领会跳过心跳新鲜的属主实例，属主存活期间
  消息**永不重投**（R1-1，静默黑洞）。
- R5 修复了 DLQ 路由遇不可变 Map 的 `UnsupportedOperationException`，但**重试/DLQ 流自身**仍被
  `MAXLEN` 有损裁剪——而转投进这两类流的条目是消息的**唯一副本**（原条目已 XACK、payload 已在同一
  原子批删除），裁剪即静默丢失（R2-6）。
- 事务回查在 R5 修好了批量窗口，但"判定卡死 → 写终态"之间**没有状态 CAS**：并发实例可能已完成转投，
  无条件 `HSET` 会覆盖已提交状态（R2-1）。
- 广播身份在 R5 修好了"按容器唯一"，但**复用本地身份文件前不做占用校验**：同机重启 / 双实例共用
  消费者名时静默复用身份，广播消费退化为集群消费（R6-B1）。

三个此前未审计的域各有一批结构性发现：**Redis Cluster** 的多 key 原子路径在 Cluster 客户端下**静默失效**
（按节点拆分提交、无异常）——本轮把"不支持"从文档声明升级为**运行期前置拒绝 + 真实 3 主集群实测**；
**kubernetes** 模块的状态回写会**自触发无限调和**、副本计数是实例级共享值（串值）、HPA 指标不反映真实积压；
**基准方法学**的消费口径被补货端封顶（真实吞吐被低估至 0.66–0.78×），且 `@Fork(3)` + 长迭代结构性需要
≈100 分钟、远超 CI 预算。

发布通道侧，本轮补齐了 11 项门禁（F 族）：release 通道此前**完全依赖可选的 NVD 深扫**（无密钥即
`exit 0`），等于"无密钥时发布通道没有任何 CVE 门禁"；`workflow_dispatch` 发布路径**完全不校验 tag**，
所发布字节绑定不到任何已校验的 commit；BOM 的 `streammq.version` 属性漏改会让 BOM 静默指向上一版；
staging smoke 会因"回退 Central 解析旧构件"而**假绿**。

**门禁复验**：终局门禁命令 `mvn clean verify -Djacoco.check.skip=false` 全绿（20/20 模块 SUCCESS，
0 失败 0 跳过）。过程记录如实保留：本轮修复引入的 tracing javadoc 折行超限（spotless 违规）曾使
**门禁首跑即红**——这不是产品缺陷，但同样等价于"不可发布"，修正后复验全绿（详见 §15.2）。

---

## 2. Project Understanding

| 维度 | 事实（以代码/物料为准） |
|---|---|
| 目标 | 把已有 Redis（Stream）变成消息总线，提供 RocketMQ 风格编程模型，避免另建 MQ 集群 |
| 用户 | 中小规模（< 1 亿/天）、已有 Redis、Spring Boot 3、Java 21 的团队 |
| 核心能力 | 注解消费、Template/Service 双发送 API、事务半消息、延时、顺序、批量、DLQ（含二级）、Tag/SQL92 过滤、压缩、背压、可观测、管理 REST |
| 架构 | `core`（抽象/SPI，零 Spring）→ `redisson`（适配实现）→ `spring-boot-starter`（装配/端点）；另有 tracing / diagnostics / binder / kubernetes / test / samples / benchmark |
| 发布面 | 6 个 Central 构件（parent/bom/core/redisson/starter/test）；其余为 source-only，`excludeArtifacts` 明确排除 |
| 技术路线 | Redis Stream + Redisson；JDK 21（虚拟线程消费循环）；安全默认（Fury 类注册可配置、默认宽松） |

**目标一致性：PASS。** 本轮未发现目标漂移。与 R5 相比的两处**语义收敛**（均在 0.1.2 未发布前，
不构成兼容性负担）：事务发送状态定稿为"仅 `COMMIT_MESSAGE` 为 `SEND_OK`"（R3-2）；
`RetryPolicy.shouldStopRetry` 接线为"返回 `true` → 直接进 DLQ"（R3-4）。两者都写回 javadoc 与
`docs/configuration-reference.md`，文档与实现同源。

---

## 3. Architecture Review

- **依赖方向：PASS。** `core` 不依赖 Spring，`redisson` 依赖 `core`，`starter` 依赖二者；enforcer
  `dependencyConvergence` + `banDuplicatePomDependencyVersions` 全绿。
- **SPI 缝：PASS（本轮补强）。** 18 个扩展点均有真实默认实现与解析路径。本轮修好两处"缝在但语义漏"的
  缺陷：`MessageConverter` 返回值契约（R5-06 的不可变 Map 已修，本轮确认三处拷贝口径一致）、
  `RetryPolicy.shouldStopRetry` 此前**从未被接线**（R3-4）。
- **生命周期：PASS。** 容器 / 调度器 / 监听器工厂的 start/stop/close 幂等；本轮补上"重复注册 = 替换"
  与"运行期配置即时生效"（R1-6 / R1-7）。
- **扩展机制：PASS。** 本轮新增的 `OrderlyDeferredRetryQueue` 由 primary 读循环驱动，**不新增线程**，
  容量满时显式拒绝并 ERROR 留痕（消息仍在 PEL），不使用无界队列。

**结构性观察（非缺陷）**：`DefaultStreamMQListenerContainer` 继续作为编排类存在（≈1500 行），
但其新增职责（延迟重投队列、运行期配置读取、重复注册替换）均以协作对象或单一方法落点实现，
未把逻辑回灌进主循环。

---

## 4. Release Blockers（本轮发现，全部已闭环）

> 编号沿用分域审计清单（R1 = 消费数据面，R2 = 调度器/事务，R3 = 重试/DLQ/序列化，S = 管理与安全，
> K = kubernetes，B = 基准，F = 发布通道）。个别编号在同一处改动中被合并处置，故不单独列行。

### P1（发布前必修，全部闭环）

| 编号 | 问题 | 证据 | 处置 |
|---|---|---|---|
| R1-1（含 R1-2） | **顺序消费失败路径的本地重投缺失**：分片锁竞争、ORDERLY `defer`、DLQ 转投失败三条分支只"留在 PEL 等认领"，而 PEL 认领会跳过心跳新鲜的属主实例——属主存活期间消息**永不重投**（静默黑洞） | `DefaultMessageProcessor` 三条分支的返回值路径；`PelClaimScheduler` 的存活实例跳过规则 | 新增 `OrderlyDeferredRetryQueue` + 重投执行器（primary 读循环驱动、不新增线程）：三条路径登记后按延迟重投；队列满显式 ERROR 拒绝（消息留 PEL）；容器停止时清空（未 ACK 仍由认领兜底）。`OrderlyDeferredRetryQueueTest` / `OrderlyMessageIT` |
| R1-3 | **DLQ 模式毒丸被裸 ACK**：目标流与当前消费流相同时（写回会自复制成无限循环）旧实现直接 ACK，原始字段永久丢失 | `DefaultMessageProcessor` 的"同名流"分支 | 改为**先隔离落盘再 ACK**：原始字段写入隔离 Hash + ZSet 登记（TTL 与 PEL 认领补偿落盘一致，7 天可恢复），再 ACK。`PoisonEntryHandlingIT` |
| R2-1 | **事务强制终结缺状态 CAS**：判定"卡死"与写终态之间，并发实例可能已完成转投，无条件 `HSET` 会覆盖**已提交**状态 | `TransactionScanner` 强制终结路径 | 脚本首步要求状态仍为 `COMMITTING` 才允许转投，否则**不投递、不改写**（幂等让位）。`Round6SchedulerRedTeamIT` |
| R2-6 | **重试/DLQ 流被 `MAXLEN` 有损裁剪**：转投进这两类流的条目是消息的**唯一副本**（原 topic 条目已 XACK、payload 已在同一原子批删除），裁剪即静默丢失 | 流创建/写入处的 `MAXLEN` 参数 | 对重试流与 DLQ 流**不施加有损裁剪**；无界增长风险改由告警与保留期清理承担（用户可配 `streammq.dlq.stream-max-len` 显式启用）。`Round6SchedulerRedTeamIT` |
| R6-B1 | **广播身份本地文件复用前不校验占用**：同机重启 / 双实例共用消费者名时，命中本地文件即静默复用身份，广播消费退化为集群消费（对端实例收不到消息） | `BroadcastInstanceIdResolver` 的本地文件读取分支 | 命中后仅做 **1 次 claim 校验**并区分三种结局：**确认**可复用；**明确拒绝**（被其它主机活实例占用）绝不复用、转注册中心重分配；**不可达**则信任本地文件（Redis 停机期间身份不漂移、PEL 与位点保留）。`BroadcastInstanceIdResolverTest` 断言更新为新契约 |

### P2（应修，全部闭环）

| 域 | 项数 | 明细 |
|---|---:|---|
| 消费数据面 | 3 | R1-5 暂停期心跳按 `pausedSleepMillis`（默认 100ms）节流——暂停是"降载"语义，不应把心跳提升到 100ms 一次；R1-6 消费循环构造时**快照**配置，运行期 `setter` 对已运行循环无效；R1-7 同一 `(topic, group)` 重复注册变成"新增"而非"替换"（旧循环继续消费、新注册从不生效） |
| 调度器与事务 | 3 | R2-2 保留期清理节奏固定（128/10 轮），数万事务/天下 `txstate` Hash 无界增长；R2-3 慢 checker 使同一 `txId` 每轮重复启动孤儿回查线程；R2-4 调度时间基准（延时写入、到期判定、退避 score）混用**本机时钟**，跨主机 NTP 偏差平移延时时长与重试节奏 |
| 重试 / DLQ / 序列化 | 7 | R3-1 空串 body 往返退化为 `null`；R3-2 事务发送状态语义未定稿；R3-3 延迟超 payload TTL 时载荷先过期（长延迟重试到期读不到 payload）；R3-4 `RetryPolicy.shouldStopRetry` 从未被接线；R3-5 DLQ 策略读到全局配置而非"按消费者合并后"的生效配置；R3-6 `secondary-dlq-enabled=false` 时仍写二级 DLQ；R3-7 哨兵 topic 与业务 topic 冲突 |
| Redis Cluster | 1 | 无前置拒绝：多 key 原子路径在 Cluster 客户端下**静默失去原子性**（按节点拆分提交、无异常、无日志） |
| 管理与安全面 | 9 | S1 启动安全提醒组件是死代码（`@Component` 不在用户扫描范围）；S2 管理端点门控挂在 `health.enabled` 上；S3 多候选 Bean 使 `getIfAvailable()` 抛 `NoUniqueBeanDefinitionException`；S4 Fury 宽松模式门禁提示误导；S5 列表接口无结果上界（`?count=2000000000` 可整条流载入内存）；S6 `virtual-nodes<=0` 静默回退默认值；S7 `batch-size` 超上限由"静默夹取"演进为生效值回显 + 交叉校验；S8 顺序锁租约属性只到文档未接线；S9 运行期配置变更"假生效"（端点不区分 `immediate` / `requires-restart`） |
| 可观测性 | 3 | O1 诊断健康概览无缓存（`/health` 轮询每次 2N 次 Redis 往返）；O2 拓扑查询无结果上界（一次请求可把窗口内全部追踪记录聚合进堆内存）；O3 OTel `Scope#close()` 可在非创建线程关闭，静默破坏该线程的 current context |
| Kubernetes | 5 | K2 优雅关闭无条件睡满预算；K4 状态回写自触发无限调和；K5 `readyReplicas` 为实例级共享计数器（多 CR 串值）；K6 HPA lag 指标不反映真实积压；K7 整对象 `replace` 覆盖用户并发修改的 spec |
| 基准方法学 | 3 | B1 消费口径被补货端封顶（实测仅其 0.66–0.78×）；B2 `@Fork(3)` + 长迭代 + 双模式结构性需要 ≈100 分钟，远超 CI 预算；B4 `main()` 用 `OptionsBuilder` 覆盖注解参数（"注解一套、main() 又一套"的双真源） |
| 发布通道 | 11 | F-1 … F-11：见 §14 |

### P3/P4（本轮一并闭环）

- **R1-8 `drainPendingOnce` 契约**：监听器未实现该能力（返回 `null`）时 WARN 一次并跳过，不 NPE、不中断消费。
- **R1-10 超时包装路径丢失 MDC**：业务回调运行在执行器线程，现把读循环线程的 MDC 上下文显式传递
  （普通与 ORDERLY 超时包装两条路径）。
- **R2-5 孤儿清理 N+1 往返**：逐条 `isExists` 改为服务端单条 Lua 批量判定（一次往返，语义不变）——
  第五轮 §13 遗留的"性能优化项"由此闭环。
- **R3-8 畸形 topic 的异常契约**：Entry 字段中的 topic 违反命名校验时，`IllegalArgumentException`
  被包装为消费侧可辨识异常而非裸崩。
- **K1 / K3 / K8 / K9 / K10**：`CustomResource` 构造器的 `@Group/@Version` 契约断言；informer
  `inNamespace(ns)` 收敛 watch 范围；fabric8 Mock Server 回归覆盖状态回写与幂等；扫描范围与 watch 语义
  对齐并清理已消失 CR 的每-CR 状态（map 不再只增不减）；ConfigMap 版本键用**实际**注入的 ns/name。
- **发布物料一致性**：`docs/benchmarks/serialization-2026-09-17.md` 的 JDK 结论更正（见 §13）、
  CHANGELOG 中"fork 提升至 3"与最终 `@Fork(1)` 的自相矛盾说明、双语 README 与基准报告同步为实测数字。

---

## 5. Top Problems（按严重度，前 10）

1. **R1-1 顺序消费失败路径永不重投**（P1）——属主存活即静默黑洞，三条分支同源。
2. **R2-6 重试/DLQ 流被有损裁剪**（P1）——流内条目是唯一副本，裁剪即永久丢失。
3. **R2-1 事务强制终结覆盖已提交状态**（P1）——并发下状态被降级，可能触发重复投递。
4. **R6-B1 广播身份静默复用**（P1）——广播语义静默退化为集群，对端收不到消息。
5. **R1-3 DLQ 毒丸裸 ACK**（P1）——原始字段永久丢失（现改为先隔离落盘）。
6. **Cluster 多 key 原子路径静默失效**（P2）——无异常、无日志，用户以为有原子性。
7. **B1 消费基准被补货端封顶**（P2）——对外公布的数字系统性低估。
8. **K4 状态回写自触发无限调和**（P2）——Operator 空转、API Server 压力。
9. **F-2 发布通道无密钥时没有任何 CVE 门禁**（P2）——供应链门禁形同虚设。
10. **O2 拓扑查询无上界**（P2）——可被外部请求触发 OOM。

---

## 6. 消费数据面（R1 族）

除 §4 列出的 R1-1 / R1-3 / R1-5 / R1-6 / R1-7 / R1-8 / R1-10 外：

- **R1-9（§13 闭环项）顺序分片锁的租约语义**：旧实现只有"看门狗续期"一种模式——持有者卡死时锁永不过期，
  该分片永久停摆。现新增 `streammq.consumer.orderly-shard-lock-lease-millis`：`0`（默认）= 看门狗续期 +
  严格有序；`> 0` = 有限租约，持有者卡死时到期让位，代价是极端情况下的瞬时乱序。负值启动失败、
  `0 < v < 5000` 启动 WARN（正常慢 handler 可能被判为卡死）。**这是显式权衡而非缺陷修复**，权衡说明写在
  `StreamMQProperties` javadoc 与 `docs/configuration-reference.md`。`RedissonOrderlyShardLockManagerLeaseTest`。
- **in-flight 计数**：`InflightSink` 的计数在异常路径下与真实在途数偏离，`InFlightCountTrackingTest` 锁定修正。
- **失败即红守卫**：`DefaultMessageProcessorShardBusyTest`、`OrderlyDeferredRetryQueueTest`、
  `PoisonEntryHandlingIT`、`OrderlyMessageIT`（真实 Redis 端到端）。

---

## 7. 调度器与事务状态机（R2 族）

- **R2-2 保留期清理吞吐**：HSCAN 分页游标化 + 单轮上限提升一个数量级 + 终态 `.done` 标记补齐
  （终态脚本执行后、收尾前崩溃的字段才可能被回收）。
- **R2-3 孤儿回查线程**：登记"存活租约"，线程结束自摘；诊断可见存活数，不再为同一 `txId` 重复起线程。
- **R2-4 统一时间源**：延时写入、到期判定、退避 score 全部改用 **Redis 服务器时钟**（与调度扫描侧同源）；
  读取失败回退本机时钟并**限频 WARN**。`RedissonStreamProducerDelayClockTest` / `DelayMessageSchedulerTest` /
  `RetrySchedulerOrphanAndClockTest`。
- **R2-5 N+1**：见 §4 P3/P4。
- **失败即红守卫**：`Round6SchedulerRedTeamIT`（真实 Redis 上覆盖事务状态机、保留期清理、重试流不裁剪）。

---

## 8. 重试 / DLQ 与序列化契约（R3 族）

- **R3-1 空串 body**：`send(topic, "")` 端到端还原为空串（`null ↔ null`、空串 ↔ 空串；序列化 / 转换 /
  解码三处协同）。`EmptyBodyRoundTripTest`。
- **R3-2 事务发送状态**：事务路径**仅** `COMMIT_MESSAGE` 为 `SEND_OK`；`ROLLBACK` / `UNKNOWN` 一律
  `SEND_FAILED` 并携带事务状态；失败路径消息 ID 使用可辨识占位 `MessageId.pending()`。`TransactionSendStatusTest`。
- **R3-3 payload TTL**：TTL 改为 `max(基础 TTL, 延迟 + 宽限)`（宽限 1 小时），覆盖"延迟 + 宽限"不变式，
  避免长延迟重试到期时读不到 payload 只能进隔离区。
- **R3-4 `shouldStopRetry` 接线**：策略说"停"而框架继续重试的静默不生效已修复；返回 `true` 直接进 DLQ
  （`reason=MAX_RETRY`），与 `nextRetryDelay` 返回 `null` 的既有停止信号并存。
- **R3-5 生效配置**：生效 `DlqConfig` 随决策上下文传递，策略判定与容器实际行为一致。
  `DlqFailureStrategyEffectiveConfigTest`。
- **R3-6 二级 DLQ 门控**：`secondary-dlq-enabled=false` 成为权威门控——关闭时按 drop 处理（ACK），
  限频 WARN + DEBUG 留痕。
- **R3-7 哨兵 topic 防御**：DLQ 重试转投改为"哨兵 topic + `retryScope=dlq`"双重判定；调度器入口
  拒绝以保留前缀 `__` 开头的业务 target（纵深防御，注册可能来自第三方直接调用）。
  `RetrySchedulerSentinelDefenseTest`。

---

## 9. 广播身份（R6-B1）

除 §4 的 P1 修复外，本轮把三种结局写进 javadoc 与测试，并明确"Redis 停机期间身份不漂移"是有意选择
（可用性优先于一致性，代价是停机期间同机新实例可能读到旧身份——由 claim 校验在 Redis 恢复后纠正）。

---

## 10. 管理与安全面（S 族）

- **S1 死代码**：`AdminEndpointExposureStartupWarner` / `AuthenticatorStartupLogger` 不再依赖组件扫描，
  改为自动装配内**显式注册的 Bean**（否则 `SECURITY ALERT` 永不输出）。`StreamMQAdminAutoConfigurationTest`。
- **S2 门控解耦**：拆出独立的 `StreamMQAdminAutoConfiguration`——`streammq.health.enabled` 只门控健康指示器，
  关闭它不再连带关闭管理端点。
- **S3 Bean 解析**：`StreamMQBeanResolution` 收敛解析语义；多 `CompressionCodec` Bean 支持按名称
  写入 / 解压。`StreamMQCompressionCodecAutoConfigurationTest`。
- **S4 提示诚实**：Fury 宽松模式的门禁提示改为按**真实原因**给出可操作说明。
- **S5 上界**：DLQ 列表等请求值统一夹取到上界，杜绝"一次请求把整条流载入内存"。
- **S6 / S7 配置校验**：`rebalance.virtual-nodes <= 0` 显式校验 / 告警；`batch-size` 启动日志打印**生效值**
  （含被夹取后的值）并交叉校验 `batch-size` 与 `max-batch-size-limit`。`StreamMQPropertiesValidateTest`。
- **S8 属性接线**：顺序锁租约属性从 starter 属性接到容器（此前只存在于文档）。
  `StreamMQListenerContainerWiringTest`。
- **S9 诚实回显**：`updateGroupConfig` 响应体包含 `effects`（key → `immediate` / `requires-restart`），
  按组分发（绝不调用容器级 `pause()` 连带暂停其它消费者组）。`StreamMQAdminEndpointTest`。

---

## 11. Kubernetes（K1~K10）

模块**不发布**（source-only），但已深度审计并修复：K2 `GracefulShutdownHandler` 改 `SmartLifecycle` 接线
且不再无条件睡满 `graceful-shutdown-timeout-ms`；K4 状态回写**先比较再写**（消除自触发无限调和）；
K5 `readyReplicas` 按 CR 显式传入（消除实例级共享计数器串值）；K6 HPA 以真实积压探针
（`BacklogProbe` 的 XLEN / XPENDING）刷新 lag 指标，缺失探针时跳过决策必伴随限频 WARN；K7 status 回写改
in-place patch 而非整对象 replace。新增 6 个测试类（HPA 扫描与副本持久化、ConfigMap watch 范围、Cluster 模型
与调和、属性装配）。**口径修正**：该模块不存在 `*IT`——健康注册用例在第四轮已由
`KubernetesHealthRegistrationIT` 改名为 `KubernetesHealthRegistrationTest`（纯 `WebApplicationContextRunner`
上下文测试）并由 surefire 执行；此前为该 `*IT` 声明的 failsafe 此后一直执行 0 个用例，本轮随本次修正移除
（模块内无 IT 即不声明 failsafe；全仓 14 个声明 failsafe 的模块与 14 个含 `*IT` 的模块一一对应）。

---

## 12. Redis Cluster：从文档声明到运行期事实

- **前置守卫** `RedisClusterCompatibility`：生产者 / 消费者启动探测一次，命中 Cluster 输出一次性
  可操作 WARN；依赖跨 key 原子性的路径（延时入队 / 转投、重试与 DLQ 的调度 / 转投、事务 prepare / commit、
  跨流 PEL 认领）在客户端被显式配置为 Cluster 时**显式拒绝**（可操作 `StreamMQException`）。
  调用点共 8 处（producer / delay / retry / pel-claim / transaction ×2 / handler ×2）。
- **真实 3 主 Cluster 实测**（`RedisClusterCompatibilityIT`，16384 slots 全覆盖 + 服务端 `CLUSTER SLOTS`
  权威映射）：
  - 单 key 路径（XADD / XREADGROUP / XACK）**可用**；
  - 多 key Lua 被服务端以 `CROSSSLOT` 拒绝且**零副作用**（fail-safe）；
  - 多 key `REDIS_WRITE_ATOMIC` 批：同节点 = MULTI/EXEC 拒绝；跨节点 = **按节点拆分提交（静默失去原子性，
    无异常）**——两种失败形态均由用例锁定。
- **架构级静态守卫**（`CrossKeyAtomicityGuardTest`）：扫描源码，任何原子批调用点缺少配套
  `requireCrossKeyAtomicity` 前置拒绝即红（已用"删守卫 → 测试必红 → 原样恢复"验证守卫有效）。
- 两个 README 的部署声明同步为**实测口径**（不再依赖推断的 CROSSSLOT 结论）。

---

## 13. 基准方法学与全量重跑（B 族）

- **B1 消费口径**：旧 harness"每条一次同步 XADD 持续补货"使消费数字被补货端封顶（实测仅其 0.66–0.78×）。
  现改为**预灌积压 + 低水位批量补货**；测量期间出现空读即判 `INVALID` 并非零退出。
  同一基准 2,383 / 2,018 → **12,572 / 7,521 ops/s（5.3× / 3.7×）**，`supplyTight=false` 为机器可读证据。
- **B2 时间预算**：注解收敛为 `@Fork(1)` + 短迭代（全量 ≈17–21 分钟），`BenchmarkBudgetTest` 按
  "注解 × 参数组合 × 模式 × fork"静态校验 45 分钟上限（超预算即红）。
- **B4 参数真源**：`main()` 不再用 `OptionsBuilder` 覆盖 fork / 预热 / 测量，临时覆盖只走 JMH 命令行参数。
- **全量重跑并回填**（2026-09-20）：序列化 6 实现 × 双模式、发送 3 模式 × 3 负载、消费 2 负载全部为实测值；
  报告记录运行参数（独立 Redis 6380、`backlog=50000`、`feederThreads=2`）、硬件与实测时长（三组 14 分 25 秒），
  以及有效性证据（两档均 `avgBatchSize=100.0`、`starvedReads=0`、`valid=true`）。
  双语 README 与 `streammq-benchmark/BENCHMARK_REPORT.md` 同步为同一组数字。
- **更正过期结论**：`docs/benchmarks/serialization-2026-09-17.md` 称"JDK 反序列化未被测量——过滤器拒绝
  基准载荷"，与当前 `JdkSerializer.installFilter`（目标类型随调用加入本次放行集）不符；已更正并指向
  2026-09-20 实测（`jdkDeserialize` 116,017 ops/s、`jdkRoundTrip` 86,815 ops/s）。

---

## 14. 发布通道与物料（F 族，11 项）

| 编号 | 问题 | 处置（`.github/workflows/release.yml`） |
|---|---|---|
| F-1 | `workflow_dispatch` 发布路径**完全没有 tag 校验**：tag 由"Create GitHub Release"现场创建，上传到 Central 的字节绑定不到任何已校验的 commit | 构建前硬断言：`git fetch --tags --force` 后 `v<version>` 必须已存在且 `rev-list -n1` == `HEAD`；test job 中重复一份用于 fail-fast（避免 45 分钟全量 verify 后才失败） |
| F-2 | release 通道此前**完全依赖可选的 NVD 深扫**（无密钥即 `exit 0`），等于无 CVE 门禁 | 自带 `guard` + `sbom-scan` 两个 job（命令 / 版本 / SHA 与 ci.yml 完全一致，与 test 并行），`publish.needs` 由 `[test]` 改为 `[test, guard, sbom-scan]` |
| F-3 | BOM 的 `streammq.version` 属性漏改 → BOM 静默指向上一版二进制（旧版本在 Central 存在，smoke 也照样绿） | `versions:set-property` 之后硬断言：按 BOM 自身求值该属性必须 == 目标版本 |
| F-4 | staging smoke 隔离不彻底（compile 未离线、无版本断言）→ 从第二次发布起，BOM 漂移致依赖回退 Central 旧物也"解析成功"，**假绿** | 列出实际解析到的 `io.github.streammq:*`，硬断言版本全部 == 本次构建版本（比 `-o` 离线更直接命中回归语义） |
| F-5 | `publish` job 超时预算 45 分钟，但该 job 要跑**两次**全量 reactor 构建 | 提升到 90 分钟（并保留 F-11 的显式预算） |
| F-6 | 聚合 SBOM 可能是"空壳"（根工程不参与 deploy，`skipNotDeployed` 默认跳过 → SBOM 根本不生成），使 CVE 门禁静默失效 | 显式 `skipNotDeployed=false` + SBOM 内容 sanity check（4 个发布构件必须在场、第三方依赖必须存在、组件总数下限兜底） |
| F-7 | `autoPublish=false` 期间 `mvn deploy` 只推进到 Central Portal 的 **validated** 状态，Release 页面若宣称"可用"会误导用户 | Release 正文注入醒目提示（"awaiting manual Publish"）+ 人工 Publish 步骤说明 |
| F-8 | osv-scanner 二进制是"是否阻断发布"的判定执行体，下载后未校验即执行属供应链投毒面 | 下载后 `sha256sum -c` 校验（digest 取自官方 Release 资产元数据），并做可执行冒烟；校验失败立即中断 |
| F-9 | SBOM 扫描范围过宽（含 samples 的 Web 容器等无关依赖），门禁口径与"交付给使用方的依赖闭包"不一致 | 从 4 个发布构件出发 BFS 过滤出**发布闭包**（跳过其它 streammq 模块与 optional/excluded 组件），只扫该闭包 |
| F-10 | `guard` job 的 parent↔BOM 一致性此前未在发布通道内执行 | 发布前强制核对：双方共同声明的依赖版本一致、BOM 的 streammq 版本 == 根 `<version>`，且三方清单（`<modules>` ↔ `excludeArtifacts` ↔ BOM 管理集合）严格等价 |
| F-11 | CI / benchmark 的多个 job 缺 `timeout-minutes`（runner 卡死即无限等待） | 全部 job 显式预算（ci 的 guard/build/formatting/test/verify/coverage/smoke、benchmark 三段 JMH 分别 20/12/10 分钟）；release 的 test / publish 同步补齐 |

**其它发布物料**：第三方 action 全部固定到 commit SHA；`dist` 资产清单与 Central 发布集（6 构件）
一致；`versions:set -DprocessAllModules=true` 且 BOM 单独断言（R5-16 延续）。

---

## 15. Verification Evidence

### 15.1 门禁命令与结果（与发布通道完全一致）

```text
mvn -B clean verify -Djacoco.check.skip=false      # = .github/workflows/release.yml 的 test job 口径
```

| 项 | 结果 |
|---|---|
| 命令 | `mvn -B clean verify -Djacoco.check.skip=false` |
| 模块 | **20 / 20 SUCCESS**（parent / bom / core / redisson / test / starter / tracing / diagnostics / binder / kubernetes / samples×8 / benchmark） |
| 用时 | **06:17 min**，Finished at `2026-09-20T17:12:07+08:00` |
| 用例 | **1482**（surefire 1185 / failsafe 297），Failures 0 / Errors 0 / **Skipped 0** |
| 覆盖率门禁 | `jacoco:0.8.12:check` 实跑（`-Djacoco.check.skip=false`，未跳过） |
| 格式/规约 | `spotless:2.43.0:check` 全模块通过；enforcer `RequireJavaVersion` / `RequireMavenVersion` / `dependencyConvergence` 通过 |

逐模块用例（取自各模块 surefire / failsafe 报告实数）：

| 模块 | surefire | failsafe | 合计 |
|---|---:|---:|---:|
| streammq-core | 283 | 0 | 283 |
| streammq-redisson | 596 | 142 | 738 |
| streammq-test | 50 | 44 | 94 |
| streammq-spring-boot-starter | 83 | 36 | 119 |
| streammq-tracing-opentelemetry | 33 | 17 | 50 |
| streammq-diagnostics | 53 | 21 | 74 |
| streammq-spring-cloud-stream-binder | 13 | 12 | 25 |
| streammq-kubernetes | 61 | **0** | 61 |
| 8 个 samples | 0 | 25 | 25 |
| streammq-benchmark | 13 | 0 | 13 |
| **合计** | **1185** | **297** | **1482** |

kubernetes 的 `failsafe-reports` 目录**不存在**：该模块无 `*IT`，本轮移除其无效果 failsafe 声明后，
用例数不变（61 由 surefire 执行，含 `KubernetesHealthRegistrationTest` 4 例）——与"无 IT 即不声明 failsafe"
的口径一致。

### 15.2 红 → 绿过程记录（如实保留）

1. **R6 中期 `gate4` 红**：`BroadcastInstanceIdResolverTest.localFileReusedAcrossResolveCalls` 断言的是
   **R6-B1 修复前的旧语义**（"每次重新生成身份"）。按新契约（Redis 不可达时**信任本地文件**、绝不静默
   换身份）改写断言后转绿。判据：旧断言若继续通过，说明 R6-B1 未生效。
2. **`gate-r6-final2` 红（spotless）**：tracing / diagnostics 在 15:29 的绿跑之后又有改动且未格式化，
   javadoc 折行超限导致 `BUILD FAILURE`（03:58 min）。`spotless:apply` 收敛 + 全仓 `spotless:check` 绿后
   重跑。**这不是产品缺陷，但同样等价于"不可发布"**——门禁纪律不区分红的原因。
3. **真实 3 主 Cluster 上三次转红**（`clusterit2/3/4`）：
   - `delayedProduceFailsSafelyOnCluster` 断言与真实键布局不符（**测试自假设错误**，非产品缺陷）；
   - `claimScriptShape_sameStreamVsCrossStream` 报 `Wrong number of args calling Redis command from script`
     ——该跨流脚本形状从未被产品调用（**测试形状错误**）；
   - `delayedMessagePathWorksOnCluster` 触发 `StreamMQBroker storeDelayPayloadAtomically failed`
     ——这条**是真实产品问题**：Cluster 客户端把多 key 原子批按节点拆分、无异常返回，延时路径会**静默
     部分写**。据此实现了 `RedisClusterCompatibility.requireCrossKeyAtomicity(...)` 前置 fail-fast
     （8 个调用点：producer / delay / retry / pel-claim / transaction ×2 / handler ×2）。
   最终 `clusterit5`：`RedisClusterCompatibilityIT` **11 用例全绿**，且"检测 Cluster → 可操作 WARN →
   `StreamMQException`"的行为被断言锁定。
4. **kubernetes pom 修正后重跑**：20/20 SUCCESS / 05:52 / 1481 用例全绿（§15.1 的上一版证据）。
5. **CI 真实 runner 复核**（详见 §15.6）：R5 推送的 CI 在 `Verify (Integration)` 红
   （`ConsumerIT.ack_messagePelEmpty` 断言了异步 ack **未承诺**的同步语义）→ 按契约修正用例并新增
   "流水线真实落地"守卫 → 当前树门禁 20/20 SUCCESS / 06:17 / **1482** 用例全绿（§15.1）。

**口径纪律**：本报告所有时长/数量均取自日志文件本身（出生/结束时间与报告实数），不引用记忆或估算值。

### 15.3 失败即红守卫（R6 新增 30 个测试文件，按缺陷族分组）

| 缺陷族 | 守卫（新文件） | 红的前提 |
|---|---|---|
| R1 消费数据面 | `OrderlyDeferredRetryQueueTest`、`InFlightCountTrackingTest`、`ConsumerMdcTraceTest`、`DefaultListenerRegistrarDlqAndBroadcastTest` | 顺序消费失败分支回到"留在 PEL 等人认领"、在飞计数/心跳失配 |
| R2 调度/事务/重试 | `RetrySchedulingAndDlqGatingTest`、`RetrySchedulerOrphanAndClockTest`、`RetrySchedulerSentinelDefenseTest`、`TransactionSendStatusTest`、`Round6SchedulerRedTeamIT`（真 Redis） | 重试/DLQ 流被 `MAXLEN` 有损裁剪（唯一副本丢失）、终态无 CAS 覆盖、孤儿哨兵失效 |
| R3 序列化 | `MessageSerializerContractTest`、`EmptyBodyRoundTripTest` | 各序列化器 null / 空体语义漂移 |
| R6-B1 广播身份 | `BroadcastInstanceIdResolverTest`（改写） | 复用本地身份文件前不做占用校验 → 广播退化为集群消费 |
| Redis Cluster | `RedisClusterCompatibilityTest`、`CrossKeyAtomicityGuardTest`、`RedisClusterCompatibilityIT`（真集群） | 多 key 原子路径静默拆分提交 |
| 顺序锁 | `RedissonOrderlyShardLockManagerLeaseTest` | 有限租约续期/让位语义回归 |
| 管理/装配面（S 族） | `StreamMQAdminAutoConfigurationTest`、`StreamMQListenerContainerWiringTest`、`StreamMQCompressionCodecAutoConfigurationTest`、`DlqFailureStrategyEffectiveConfigTest` | 端点鉴权/装配缺失、配置"假生效" |
| 延时/时钟 | `RedissonStreamProducerDelayClockTest` | 延时投递与 Redis 服务端时钟错位 |
| 可观测 | `StreamMQTracingAutoConfigurationTest`（改写） | OTel `Scope#close()` 跨线程关闭 |
| K8s（K1~K10） | `StreamMQClusterModelTest`、`StreamMQClusterControllerReconcileTest`、`HpaAutoScalerScanTest`、`HpaReplicasPersistTest`、`ConfigMapWatcherScopeTest`、`CloudK8sPropertiesWiringTest` | 自触发无限调和、实例级共享副本计数、watch 范围漂移 |
| 基准（B 族） | `BenchmarkBudgetTest`、`ConsumeValidityReportTest` | 注解固化参数超 CI 预算、补货端封顶被当成消费吞吐 |
| 样本 | `DlqDemoRunnerIT` | DLQ 演示路径与主代码脱节 |

### 15.4 真实环境实测

- **真实 3 主 Redis Cluster**（本地 7000/7001/7002 三主集群，`cluster/` 工作区）：
  `RedisClusterCompatibilityIT` 11 用例全绿（`clusterit5.log`）；单 key 路径（produce / 基本消费 ACK）
  可用，**所有跨 key 原子路径前置拒绝**并给出可操作 WARN——"0.1.x 不支持 Cluster"由文档声明升级为
  **运行期事实 + 实测证据**。
- **JMH 全量重跑**（2026-09-20，独立 Redis 6380，`backlog=50000`、`feederThreads=2`）：序列化 6 实现 ×
  双模式、发送 3 模式 × 3 负载、消费 2 负载全部为实测值，三组共 14 分 25 秒；有效性证据
  `avgBatchSize=100.0`、`starvedReads=0`、`valid=true`；数字回填双语 README、
  `streammq-benchmark/BENCHMARK_REPORT.md` 与 `docs/benchmarks/*`。
- **本地 Redis 6379 上的 296 个 IT 在门禁内真实执行**（0 跳过）：消费/重试/DLQ/延时/事务/顺序/广播/PEL
  认领的地面真值覆盖。

### 15.5 发布通道演练（无签名 Central dry-run）

```text
mvn -B clean deploy -DskipTests -DskipPublishing=true     # 无 -Pgpg，绝不接触口令
```

| 项 | 结果 |
|---|---|
| 结果 | **BUILD SUCCESS**，01:27 min，20/20 模块 |
| 发布集 | 日志 15 条 `Skipping Central Release Publishing for artifact '<x>' at user's request`，覆盖 6 构件：`streammq-parent` / `streammq-bom` / `streammq-core` / `streammq-redisson` / `streammq-spring-boot-starter` / `streammq-test` |
| 排除集 | 14 个 `excludeArtifacts` 模块**未产生任何上传候选**（运行期确证排除清单） |
| 物料 | 8 个 jar 模块产出 sources / javadoc jar |

**前置告警（诚实记录）**：`central-publishing-maven-plugin` 在检查 `skipPublishing` **之前**先解析凭据——
仅加 `-DskipPublishing=true` 会以 `Unable to get publisher server properties for server id: central` 失败
（`dryrun-central.log`）。演练因此改用临时工作区的 `settings-dryrun.xml`（`central` 为占位凭据，
**绝不写入用户真实的 `~/.m2/settings.xml`**），复跑即 `dryrun-central2.log` 的成功记录。真实发布需要
有效的 `central` 凭据 + 发布者持有的 GPG 口令（口令从未被猜测或写入日志）。

### 15.6 CI 真实 runner 复核（R5 推送的 CI 红 → 根因 → 修复）

**事实**：R5 推送（`b5e6486`）触发的 CI run **35480290570** 在 `Verify (Integration)` job 红，
其余 job（Guards / CVE gate / Build / Formatting / Test / Staging smoke）全绿：

```text
[ERROR] Tests run: 14, Failures: 1, Errors: 0, Skipped: 0 -- in io.github.streammq.adapter.redisson.it.ConsumerIT
[ERROR] io.github.streammq.adapter.redisson.it.ConsumerIT.ack_messagePelEmpty -- <<< FAILURE!
        Expecting empty but was: [PendingEntry{id=1789866312693-0, consumerName='consumer-1',
        idleTime=1, lastTimeDelivered=1}]
```

**根因（不是 flaky，是契约不一致）**：`StreamMQListener#ack` 的公开契约**明文**写着"有界异步流水线：
方法返回**不代表** XACK 已在 Redis 端完成；需要'返回即已确认'语义时用 `ackBatch(List)` 同步版"。
而 `ack_messagePelEmpty` 用例在 `ack()` 返回后**立即**断言 `listPending` 为空——断言了接口从未承诺的
语义；本地跑通常赢下这个竞态，CI runner 上输掉。**同类扫描**：全仓仅此一处该形态（`RedisClusterCompatibilityIT`
用原生同步 `stream.ack`，`ackedMessage_notRedelivered` 走 `neverDelivered` 读语义不受影响）。

**修复（按契约，而非放宽断言）**：

- `ack_messagePelEmpty` 改为断言**文档化的停机排空语义**：`ack()` → `close()`（有界排空在途 ACK）→
  断言 PEL 为空。
- 新增 `ack_asyncPipelineLandsWithoutClose`（失败即红）：`ack()` 后**不停机**，用 Awaitility 在 5 秒内
  等到 PEL 清空——异步流水线断裂/许可泄漏即红（这条守卫在修复前不存在）。

**复验**：`ConsumerIT` 连续 3 次独立运行 15/15 绿（4.4s / 4.4s / 4.4s，含新增用例）；全量门禁
20/20 SUCCESS / 06:17 / 1482 用例 0 失败（§15.1）。

**推送后 CI 复核（真实 runner，run 35501791198，commit `271734c`）：全绿** ——
Guards 5s / CVE gate 42s / Formatting 25s / Build 40s / Test 2m23s / **Verify (Integration) 5m50s**
（即 R5 推送时红的那个 job）/ Staging smoke 2m0s / Coverage report 55s 全 success；
OWASP 深扫按设计在无 NVD key 时跳过。

---

## 16. 未闭环项（诚实声明，均非代码缺陷）

1. **真实 Maven Central 发布未执行**：无私钥口令。以无签名 dry-run 演练替代（§15.5），发布通道的
   tag 校验 / guard / sbom-scan 门禁已在 `release.yml` 内固化并与 CI 同口径。
2. **CI benchmark workflow 的三段 JMH job 未在真实 runner 复跑**：本地全量重跑已回填（§15.4）；
   预算上限由 `BenchmarkBudgetTest` 静态校验，口径有效性由运行时 `valid` 判定。
3. **Redis Cluster 的跨 key 原子性本版本不支持**（划界而非缺陷）：前置拒绝 + fail-fast + 文档 + 真集群
   实测三重锁定；支持需 hash-tag 键设计，列为 0.2.0 范围。
4. **kubernetes 真集群 e2e 未验证**：无可用集群；现由 fabric8 Mock Server 回归（回写/幂等）与上下文
   装配测试覆盖，K1~K10 的逻辑面已审计闭环。
5. **顺序消费分片锁在有限租约极端时点的重排序可能**：显式设计权衡（卡死让位优先于严格有序），已在
   javadoc 声明并被 `RedissonOrderlyShardLockManagerLeaseTest` 锁定语义。

---

## 17. Final Verdict

### 17.1 维度评分

| 维度 | 分数 | 依据 |
|---|---:|---|
| 产品目标 | 9 | 定位诚实；"不支持 Cluster"由声明升级为运行期前置拒绝 + 实测 |
| 功能完整度 | 9 | 重试/DLQ/延时/事务/顺序/广播/背压/可观测/管理齐备；本轮补齐数据面孪生副本 |
| 架构 | 9 | 依赖无环、SPI 缝真实；跨 key 原子性收敛为单一前置校验点（8 调用点） |
| 模块设计 | 9 | 职责可解释；kubernetes 模块完成 K1~K10 深度审计 |
| API / SDK | 9 | Builder + 不可变值对象 + 类型化异常；无静默降级（宁可 fail-fast） |
| 实现质量 | 9 | 本轮修 5 个 P1（静默不投递 ×2 / 唯一副本丢失 / 状态覆盖 / 广播语义退化）+ 45 个 P2 |
| 测试 | 9 | 1482 用例、0 跳过；真 3 主 Cluster IT + 失败即红守卫 30 个新文件；ack 流水线契约守卫（CI 实测驱动） |
| 并发 | 9 | 广播身份占用校验、在飞计数、租约心跳独立；已知有序性权衡显式声明 |
| 性能 | 9 | JMH 全量重跑并回填实测（消费口径 5.3× / 3.7× 修正）；孤儿清理 N+1 闭环 |
| 安全 | 9 | 默认拒绝 + 参数夹取 + 错误脱敏 + 供应链校验（osv-scanner SHA 校验）齐备 |
| Maven 工程 | 9 | 门禁 = 发布通道口径；三方清单等价性硬断言；failsafe 声明与 `*IT` 一一对应 |
| Developer Experience | 9 | Quick Start 可用、错误可定位、健康检查不假绿、配置变更回显 effects |
| 文档 | 9 | 双语 + 配置参考 + SECURITY；本轮消除全部事实性冲突（含 kubernetes `*IT` 口径） |
| 可维护性 | 9 | 单一规则源、防御性拷贝、javadoc 与实现对齐 |
| 可扩展性 | 9 | 扩展点接口化，可替换 |
| 开源准备度 | 9 | 治理/许可/发布流程/物料一致性就绪；发布通道 11 项门禁闭环 |

```text
Overall = round(144 / 16 × 10) = 90 / 100
```

> 分数用于排优先级；是否发布由 17.2 的门禁规则决定（无未决 P0/P1/P2）。

### 17.2 裁决

```text
Release Status: GO
Release Readiness Score: 90 / 100

Must Fix Before Release: 0 items
Should Fix:             0 items
Open P0/P1/P2:          0 / 0 / 0 items
Open P3/P4:             0 items（§16 的未闭环项均为外部环境依赖或显式设计权衡，非代码缺陷）

门禁证据: mvn clean verify -Djacoco.check.skip=false → 20/20 SUCCESS，1482 用例，0 失败/0 跳过（06:17）
CI 复核: R5 推送的 CI 红（ConsumerIT 断言了异步 ack 未承诺的同步语义）已按契约修复，
         并新增"流水线落地"守卫随本轮推送复跑（§15.6）
真实环境: 3 主 Redis Cluster IT 11/11 绿；JMH 全量重跑并回填实测值
发布演练: mvn clean deploy -DskipPublishing=true（无签名）→ 20/20 SUCCESS，发布集 6 构件、
          排除集 14 模块无上传候选
发布前置: 无（CVE 硬门禁无需密钥；GPG 非交互配置就绪；Release 资产与 Central 发布集一致）
```

**与上一轮的差异必须被记录**：第五轮裁决 GO（87/100）后，本轮的起点是**假设上一轮修复过的每一类缺陷
都存在同构副本**——该假设成立：新发现 5 个 P1 全部是 R5 已修缺陷的孪生路径（顺序消费失败分支的
"永不重投"、重试/DLQ 流的唯一副本被裁剪、事务终态无 CAS、广播身份复用未校验），外加一个此前
从未审计的域（Redis Cluster 多 key 原子路径**静默拆分提交**）。同时，门禁"红→绿"的记录被完整保留
（§15.2）：本轮自身引入的格式化违规、测试旧语义断言、Cluster 上三条失败路径，全部先红后绿。
90/100 相对 87/100 的提升只来自**实测替代推断**（Cluster 实测、JMH 重跑、性能维度 8→9）与
**发布通道门禁补齐**；缺陷密度本身不构成加分项。该结果再次印证 `fullReview.md` 的核心原则：
**不因"上一轮已通过"而默认当前仍然成立**。

---

# 附录 A — R6 复核补充（第二遍独立审计）

> 触发原因：`fullReview.md` 的核心纪律是"不因上一轮已通过而默认当前成立"。R6 主体闭环后，主审对上表
> 冻结的工作树做**第二遍独立取证**：4 个分域审计员（core / redisson / 发布工程与文档 / 周边模块）各自
> 重新审读主源码与物料，主审再对每条候选逐行核对磁盘现状（终端直读，不使用任何缓存视图），
> 并以与发布通道**完全相同**的门禁命令实测收尾。
> 本附录只登记**该遍真实发现且已修复**的项；R6 主体已覆盖者不重复计列。

## A.1 发现与处置

| 编号 | 级别 | 问题（证据） | 处置 |
|---|---|---|---|
| A-1 | P2 | 管理端点 4 个操作（`ackPending` / `triggerRebalance` / `createTopic` / `deleteTopic`）的 catch 分支仍 `result.put("error", ex.getMessage())` —— 原样回吐 Redis 版本、完整 Key 名、`NOGROUP`/ACL/连接文本。R5 已为其余操作引入 `describeFailure` 脱敏，这 4 处漏改，属**同一缺陷的残留副本** | 4 处统一改走 `describeFailure(op, ex)`：响应只含「操作名 + 异常类型 + 关联 ID」，完整信息（含堆栈）只进日志 |
| A-2 | P2 | diagnostics `/health` 每次调用遍历全部消费者并逐个查追踪（≈2N 次 Redis 往返）；它是监控/看板的高频轮询入口，N 稍大即打满 Redis | 健康概览整块 **5s TTL 缓存**（`timestamp` 每次仍取实时值）；新增可测试用 `setHealthCacheTtlMillis(0)` 关闭缓存 |
| A-3 | P2 | `/actuator/streammq` 的 overview 每次 `listGroups()` = 2N 次 Redis 往返且**无缓存**，而同端点的 health 已有 5s 缓存 —— 同一端点内两套口径 | overview 走 **3s 快照缓存**；直接访问 `/actuator/streammq/groups` 仍取实时数据（避免"刚 rebalance 却看到旧列表"） |
| A-4 | P2 | `SlowConsumeReport.threadPoolActive` / `threadPoolMax` 填的都是**消费者实例数**（空报告填 `0` / `max(instances,1)`），字段语义与取值不符，运维会误读为"线程池 100% 打满" | 删除两个伪造字段，改为单一 `consumerInstances`（可观测事实）；宁缺勿假 |
| A-5 | P2 | `BoundedSpanRegistry` 在替换/淘汰/清空时跨线程关闭 OTel `Scope`。`Scope#close()` 回退的是**当前线程**的上下文栈 —— 在错误线程上关闭会静默破坏该线程的 current context（无关请求的链路错挂/丢失） | `Entry` 记录 owner 线程；**仅创建线程**关闭 Scope，非 owner 只结束 Span。`BoundedSpanRegistry` 与 consumer interceptor 的 javadoc 同步更正（此前 doc 声称"会被安全忽略"，与事实不符） |
| A-6 | P2 | ①`StreamMQTopologyService.getTopicTraces` 是公开查询却**无结果上界**（时间窗由调用方给定 ⇒ 一次请求可把整窗追踪记录聚合进堆）；②`computeTotalDuration` 用 `events.get(0)`/`get(size-1)` 首尾相减，而事件顺序来自查询顺序、**不保证按时间**（可算出 0 甚至负数） | ①结果上限 500，超限 WARN 并计数（不静默丢）；②耗时改为取时间戳**极值**，与列表顺序解耦 |
| A-7 | P2 | `PelClaimScheduler` 把"目的键可写性自检"放在扫描入口：DLQ 键被非 stream 占用时**整轮停摆**，连不写 DLQ 的"同流重投"也被一起阻断（保守过头 ⇒ 可用性下降） | 自检下沉到**确实写 DLQ 的分支**：该条跳过（消息留 PEL，绝不丢），同批次的正常重投不受影响（TOPIC / RETRY 两处） |
| A-8 | P2 | `DefaultStreamMessageTemplate` 对**可重试的中间失败**也调 `notifyException` 并计一次失败指标 ⇒ ①追踪侧把"第 1 次失败、第 2 次成功"的生产者 Span 提前以 ERROR 结束（成功那次再也找不到配对条目），导出的链路**永远是失败**；②指标把一次逻辑发送记成 N 次失败 | afterSend/onException 收敛为**终态回调**：成功 → `afterSend(success)`；不可重试 → `notifyException`；重试耗尽 → `afterSend(failedResult)`；中间失败只记日志。恰好一次 |
| A-9 | P3 | `StreamMessageProducer.syncSendBatch(List,long)` 默认实现 javadoc 承诺"消息必须同 Topic"并声明 `@throws IllegalArgumentException ... Topic 不一致`，实现**未做任何校验** ⇒ 混合 topic 批次被静默逐条投递到不同 topic | 补 fail-fast 校验（异常信息含冲突的两个 topic） |
| A-10 | P3 | `ProducerConfig.namespace` **无校验**（消费侧 `ListenerConfig` 早已 fail-fast）⇒ 同一份 namespace 配置在两侧行为不一致；含 `:` / `{}` 的值会被直接拼进所有 Redis Key | 新增显式全参构造器并走 `StringUtils.requireValidNamespace`（与消费侧同一入口） |
| A-11 | P3 | `DefaultListenerRegistration` 对 `shardCount` / `streamMaxLen` **静默夹取**、其余数值字段抛错；`shardCount = -1` 被夹成 `0` = 零分片顺序消费者（不可用却无任何提示） | 两处改为 `requireMin` fail-fast；类 javadoc 由"夹取策略"改写为"全部数值参数一律 fail-fast"，消除与实现相反的描述 |
| A-12 | P3 | `CompressionCodec` 未约定异常类型，Gzip/LZ4 解压失败抛父类 `StreamMQException` ⇒ 消费侧按 `SerializationException` 识别"毒丸消息"的分支**漏判**，损坏载荷会被当业务异常反复重试而不是进 DLQ | 接口 javadoc 定契约（失败抛 `SerializationException`）；4 处抛点统一（`SerializationException extends StreamMQException`，对既有调用方向后兼容） |
| A-13 | P3 | `asyncSend(..., SendCallback)` 未校验 `callback` ⇒ 传 null 时 NPE 在完成线程被 `catch(Throwable)` 吞成一行 WARN，调用方**既拿不到结果也拿不到异常** | `StreamMessageTemplate` / `StreamMessageService` 入口 `requireNonNull(callback, "callback")` |
| A-14 | P3 | k8s `StreamMQClusterController.envDrift` 用 `List.equals`（**顺序敏感**）：webhook/sidecar 注入或重排 env 会让漂移判定**永久为真** ⇒ 每次 reconcile 都 patch（无意义热循环），且 `withEnv(desired)` 会把注入的变量整段抹掉 | 漂移判定改为「变量名 → 值」集合语义（容忍额外变量）；写入改为**合并**（同名覆盖、缺失追加、保留额外变量） |
| A-15 | P3 | `HpaAutoScaler` 自带 `@Component`，`CloudK8sAutoConfiguration` 又以朴素 `@Bean` 暴露 ⇒ 组件扫描场景下容器里两个实例、**两个调度线程**（与 `HpaMetricsProvider` 的消歧口径也不一致） | 补 `@ConditionalOnMissingBean(HpaAutoScaler.class)` |
| A-16 | P3 | 运行期守卫的异常/WARN 文案写"See README 'Deployment'"，但 README **没有 Deployment 章节**（错误信息指向不存在的锚点，属可定位性缺陷） | README 双语新增 `## Deployment` / `## 部署形态`：拓扑支持矩阵 + fail-fast 行为 + 实测证据链接，并加入目录 |
| A-17 | P3 | `sbe-tool` 以 **optional 依赖**声明，注释却称"仅构建期、不进运行时"（自相矛盾）。实测**移除后 `generate-sbe` 立即失败** ——`exec-maven-plugin` 默认 `classpathScope=runtime`，从模块依赖类路径解析 `SbeTool`；`provided` 同样不在 runtime 类路径，也会失败 | 保留 optional 依赖（已保证不传递给使用方），把说明改准确并写明"为何不能改成 provided/不能删" |
| A-18 | P4 | 物料/文档一致性：`CONTRIBUTING` 的覆盖率门禁清单**漏列已纳入门禁的 `streammq-test`**；`CONTRIBUTING` 仍称 japicmp 已配置 `io.github.streammq.internal.*` 排除（该死配置已删）；`NOTICE` 的 Spring Framework（6.1.14）与 Netty（`4.1.x`）版本过期且缺 `commons-compress`；演示脚本钉 `spring-boot-starter-parent 3.3.5`（项目自己判定为不安全、已被依赖升级淘汰的基线）；演示指南给出不存在的管理端点 URL 且缺 `mvn install` 前置；样例 tracing 硬编码 OTel 版本；两处 workflow 注释把 `excludeArtifacts` 归给 `maven-deploy-plugin`；**18 个文件的 `@since 1.1.0`** 与项目版本 0.1.2 矛盾 | 逐项更正（证据：这些改动随 R6 代码提交 `5c4aa05` 落库，本附录的文档口径改动随后一并提交）：门禁清单补齐阈值、japicmp 说明改为"需自行新增排除"、NOTICE 版本对齐并补条目、演示脚本升到 3.5.16、指南补 `mvn install` 并改正确端点、样例改用 `${opentelemetry.version}`、workflow 注释归属更正、`@since` 全量对齐 0.1.2 |
| A-19 | P4 | 弱测试：`ScheduleQuarantineTest.healthyPayloadNotQuarantined` 用 `catch (RuntimeException ignored)` 吞掉异常后才断言 `never()` ⇒ 断言**恒成立**（被测路径根本没跑） | 补**路径可达性断言**（payload 必须被真实读取），使 `never()` 具备鉴别力 |
| A-20 | P4 | 弱测试：`BroadcastInstanceIdResolverTest.localFileReusedAcrossResolveCalls` 断言恒真——两次 `resolve()` 之间注册中心返回的 id **没有变化**，无法区分"复用本地身份"与"重新向注册中心申请" | 强化为"两次调用之间改变注册中心会返回的 id"，并让 `FakeRegistry` 忠实实现 `preferredId` 语义（真实注册中心行为）。**该用例因此暴露了真实契约**：本地身份复用前必须做一次 claim 校验（与 R6-B1 同一处置），用例名与注释同步更正为"复用本地身份（注册中心仅做归属确认）" |

## A.2 与 Cluster 工作的交叉复核

R6 主体新增的 `RedisClusterCompatibility`（拓扑诊断 + 跨 key 原子性前置守卫）由本遍独立复核，结论 **保留**：

- **方向正确且与定型声明一致**：它不是"支持 Cluster"，而是把 CHANGELOG 既有的"0.1.x 不支持"从**文档约定**升级为
  **运行期可观测事实 + 确定性拒绝**，正是 A 遍最担心的"静默降级"的正解。
- **非死代码**：`warnIfCluster` / `requireCrossKeyAtomicity` 在**9 处生产调用点**被使用（容器、生产者、
  Retry/DLQ、延时、PEL 认领、事务提交与回扫）。
- **探针可行性已独立实测**：本机以裸 RESP 直接 `EVAL "return redis.call('CLUSTER','KEYSLOT',...)"`，
  单机 Redis 返回 `This instance has cluster support disabled`（证明脚本**确实执行**，非"命令被脚本禁用"），
  与实现的"探针失败 ⇒ 判定非 Cluster"分支一致。
- **IT 证据可复现**：`RedisClusterCompatibilityIT` 在**本机真实 3 主集群**上 11/11 绿（`CLUSTER INFO`：
  `cluster_state:ok` / `16384` slots / `cluster_size:3`）；集群不存在时整类**显式 skip 并打印启动指引**，
  不产生"全绿但没测"的假象。
- 本遍唯一的补充是 A-16（守卫文案引用的 README 章节此前不存在）。

## A.3 门禁复验（本附录所属树）

```text
mvn clean verify -Djacoco.check.skip=false
```

| 项 | 结果 |
|---|---|
| Reactor | **20/20 模块 SUCCESS** |
| 用例 | **1482**（单元 1185 + 集成 297），**0 失败 / 0 错误 / 0 跳过** |
| 集成测试分模块 | redisson **142**（含真实 3 主 Cluster IT 11/11）、starter 36、test 44、samples 25、diagnostics 21、tracing 17、binder 12、kubernetes 0 |
| JaCoCo 覆盖率门禁 | 7 个模块全部执行 `check`，无 `Rule violated` |
| Spotless / enforcer | 全模块通过；Java 21 / Maven 3.9 / 依赖收敛全绿 |
| 耗时 | 06:34 min（本附录代码定稿树）；07:11 min（附录文档定稿后的最终树复跑，结论同上：20/20 SUCCESS、1482 用例 0 失败/0 跳过、Cluster IT 11/11） |

**过程中真实红过一次（诚实记录）**：本遍首次复跑门禁时 `streammq-core` 的 fork JVM 以
`EXCEPTION_ACCESS_VIOLATION (0xc0000005)` 在 `ClassLoader.defineClass1` 崩溃（JaCoCo + Mockito 双 agent 挂载下），
属**环境级 JVM 崩溃而非断言失败**（无 `Rule violated`、无测试失败行，`hs_err_pid*.log` 指向 `jvm.dll`）。
清理残留进程与 `surefire-reports` 后复跑即全绿，且此后连续复现稳定——记录在案，以免被误读为"从未出过问题"。

## A.4 裁决不变

本附录无 P0/P1，A-1…A-8 为 8 项 P2（全部闭环），其余为 P3/P4 整备项（全部闭环）。
主裁决与 §17.2 一致：**GO，无未决 P0/P1/P2**；评分与 §17.1 相同（第二遍发现的是存量缺陷的残余副本，
不改变维度评分，如实登记为工作量与证据补强）。

## A.5 复核后**有意不改动**的事项（与"未修复"区分）

以下条目经取证后判定为**合理设计或已由文档固化**，显式记录以免被读成"漏改"：

1. **binder 健康指示器不加 `@ConditionalOnBean(StreamMQListenerContainer.class)`**：容器 Bean 缺失时，
   `StreamMQMessageBinder` 自身也会在启动期失败（绑定器根本无法工作）。此时**启动即失败**比"指示器静默消失"
   更符合 fail-fast 原则，且能避免另一类假绿（健康面少了组件却看起来正常）。
2. **`MessageBuilder.from(m).build()` 无法往返 `body == null` 的消息**：`Message`（值对象）**允许** null body
   ——消费端确实可能收到无载荷消息，内置序列化器统一 `serialize(null) → null`；而 `MessageBuilder` 是**发送侧**
   构造器，"发送一条无 payload 消息"几乎总是漏设 body 的笔误（历史上正是它让占位消息污染过业务流）。
   因此保留 `build()` 的 fail-fast，并把边界写进 javadoc 与异常信息（明确"转发原消息而不要重建"）。
3. **`TestStreamMQListener.awaitMessages` 的"累计计数 + 存量补偿"语义**：这是刻意的（保证"先发后等"与
   "先等后发"都正确）。未改成"自本次调用起的新增数"，而是把该语义与两个使用注意（阶段间需 `reset()`；
   它只保证"至少 N 条"，不是"恰好 N 条"）写进 javadoc。
4. **Redis Cluster 的键设计不做 hash tag 改造**：改动会把整个命名空间钉到单一 slot（用户可控的 topic/group
   会被迫同槽形成热点），代价高于收益；0.1.x 的正解是"运行期前置拒绝 + 文档 + 实测"（§A.2），
   hash-tag 设计列入后续版本议题。
5. **顺序消费分片锁的有限租约时点重排序**：显式设计权衡（卡死让位优先于严格有序），已在 javadoc 声明并由
   `RedissonOrderlyShardLockManagerLeaseTest` 锁定语义（与 §16.5 同一结论）。
