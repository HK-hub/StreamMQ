# StreamMQ 发布前红队审查报告（第三轮 · REPORT）

> 审查依据：`docs/fullReview.md` 全文协议 + `.claude/skills/release-readiness-redteam` 检查单与门禁规则。
> 审查方式：8 个维度并行红队（架构/并发/安全/缺陷/性能/测试/文档/Maven 发布）× 反证法 × 真实第三方开发者视角；
> 所有 P0/P1 由主审逐条在代码/CI/公开仓库上复核后才定级（`VERIFIED` / `LIKELY` / `HYPOTHESIS`）。
> 基线：commit `284855b`（上一轮裁决 GO 91/100 的提交）。本轮结论与该裁决冲突，详见 §1 与 §4。

---

## 1. Executive Summary

**上一轮的 GO 裁决不成立，且这不是"发现了新问题"那么简单——有三个发布阻断级事实被上一轮的验证证据掩盖：**

1. **项目自身的发布/PR 门禁在基线提交上必然失败（P0）**：`mvn clean verify -Djacoco.check.skip=false`
   （`ci.yml` 的 verify job 与 `release.yml` 使用的同一命令）在 `streammq-core` 因 JaCoCo 分支覆盖率
   0.472 < 阈值 0.52 直接 `BUILD FAILURE`，反应堆停在 3/20，**后续 17 个模块（含全部真实 Redis 集成测试）
   全部 SKIPPED**。上一轮 REPORT §8 声称该命令"BUILD SUCCESS，1148 测试全绿、门禁全绿"——不可复现：
   本地普通 `mvn clean verify` 之所以绿，是因为 `jacoco.check.skip` 默认为 `true`（`pom.xml:123`），
   门禁根本没有执行。阈值还是上一轮在"实测 BRANCH≈0.554"的错误读数上设的。
2. **上一轮标记 FIXED 的 P1（R-11"活跃慢消费者不被 PEL 认领"）在代码里完全无效（P1）**：
   instances 心跳 Hash 的 key 由 `DefaultConsumerGroupManagerFactory` 生成
   （`{group}-{UUID8}`，`DefaultConsumerGroupManagerFactory.java:38`），而消费者名内嵌的是容器实例 token
   （`{group}-{hostname|hostname-N|UUID}`，`BroadcastGroupNaming.java:36`），判活用的是
   `consumerName.endsWith(instanceKey)`（`PelClaimScheduler.java`）——两个身份各自随机，**永不匹配**。
   默认配置下（60s 判活窗口）活跃慢消费者仍会被复制重投，重试耗尽后**已成功处理**的消息进 DLQ。
3. **上一轮标记 FIXED 的"Fury 安全姿态"实际不成立（P1）**：`SECURITY.md`/README/类 javadoc 声称宽松模式
   构造受 `SecurityException` 门禁保护，代码里只有 WARN（`FurySerializer.java:109-121`），
   且测试还把"门控已移除为 WARN"当作预期行为固化；同时钉住的 `org.apache.fury:fury-core:0.9.0`
   位于 **CVE-2026-50076**（CVSS 9.1，类注册白名单可被绕过 → readResolve/readExternal 链，修复版
   `org.apache.fory:fory-core ≥ 1.1.0`）的受影响区间内。

**本轮处置：P0 与全部 P1 已根因修复并在本轮门禁命令下复验；P2 按证据逐条修复或给出残留判定；
P3/P4 残留下沉为整备项（§6 逐条依据）。** 修复面见 §5，最终裁决见 §8。

---

## 2. Project Understanding

- **目标**：把 Redis（Stream）变成开箱即用的消息总线——生产者/消费者模板、重试/DLQ、延时、事务半消息、
  顺序消费、广播、背压、可观测与管理端点；框架无关 API + SPI 在 `streammq-core`，Redis 适配在
  `streammq-redisson`，Spring 装配与 Actuator 端点在 `streammq-spring-boot-starter`。
- **用户**：Spring Boot 3.3.x / JDK 21 应用开发者；单实例或主从（Sentinel）Redis。
- **目标漂移**：无根本漂移；但"发布面 / 门禁面 / 文档面"三者仍存在不一致，本轮定位并修复了其中
  会造成**外部可观测失败**的 3 处（P0 门禁、CI coverage job、release 预检）。
- **真实性说明（对第三方开发者的口径）**：0.1.2 是**首个** Maven Central 发布；0.1.0/0.1.1 均为内部迭代，
  Central 上不存在任何 `io.github.streammq` 构件（本轮实测 404）。CHANGELOG 已按此更正。

---

## 3. Architecture Review（结论摘要）

- 依赖方向干净无环；SPI 缝真实可用（`streammq-tracing-opentelemetry` 不 import 任何 adapter 类）。
- **架构进化安全性：SAFE TO EVOLVE（有前提）**。前提是 0.1.2 发布前把"兼容承诺的范围"和"实际会被
  japicmp 卡住的范围"对齐（japicmp 当前冻结全部公开面，包括 49% 与 Redisson 无关的后端中立代码，
  而 CONTRIBUTING 声称的 `io.github.streammq.internal.*` 逃生通道**并不存在**——该包从未创建）。
  本轮已把兼容策略在 CONTRIBUTING 中改为与代码事实一致的表述（0.1.x 允许破坏性变更 + 弃用期），
  并把 `release.yml` 的 japicmp 模块清单收敛到实际发布集（否则第二个版本起必然解析失败）。
- 结构性整备（God 容器 57 字段、`Message.withX` 复制爆炸、775 行自动配置、调度器 214 行重复）保留为
  1.0 前事项，理由与迁移成本见 §6（R3-22/R3-23/R3-24）。

---

## 4. 发布阻断清单（Release Blockers）

| 级别 | 编号 | 问题 | 证据 | 处置 |
|---|---|---|---|---|
| P0 | R3-01 | 基线提交上 `mvn clean verify -Djacoco.check.skip=false` 必失败：core 分支覆盖率 0.472 < 阈值 0.52；反应堆停在 3/20，全部 IT 被 SKIPPED；上一轮"门禁全绿"证据不可复现 | 本轮实测日志 + `streammq-core/target/site/jacoco/jacoco.csv`（BRANCH 304/644） | FIXED：阈值按"实测 − 3pt"重设（§9.3），门禁命令全绿复验 |
| P0 | R3-02 | CI 的 Coverage job 从未执行：`run: >-` 折叠标量在行尾逗号后插入空格，Maven 报 `Unknown lifecycle phase "…"`，每次 push 1 秒即失败 | CI 运行日志（job "Coverage gate (published modules)"） | FIXED：改为 `run: \|` + `\` 续行（已做参数拼接实测：模块清单保持单参数） |
| P1 | R3-03 | PEL 认领判活失效（R-11 实际无效）：两个实例身份不同源，`endsWith` 永不匹配 → 活跃慢消费者被复制重投、已成功消息误入 DLQ | `DefaultConsumerGroupManagerFactory:38` vs `BroadcastGroupNaming:36` vs `PelClaimScheduler` 判活实现 | FIXED：身份同源（instances key = 消费者名内嵌 token）+ 精确匹配 + 新增红队回归 IT（先断"不许重投"，再断"心跳过期后恢复重投"） |
| P1 | R3-04 | FurySerializer 反序列化 RCE 面：CVE-2026-50076（CVSS 9.1）影响 `fury-core < 1.1.0`，项目钉 0.9.0 且在 SECURITY.md 推荐该序列化器；文档承诺的 `SecurityException` 门禁不存在 | Apache Fory 安全公告 + 独立 CVE 库（本轮检索确认）；`FurySerializer.java:109-121` 只有 WARN；`FurySerializerTest` 明确断言"门控已移除为 WARN" | FIXED：迁移到 `org.apache.fory:fory-core:1.7.3`；恢复门禁（与 `JdkSerializer.unrestricted()` 同语义：无属性即抛）；SECURITY/README/NOTICE/CHANGELOG/配置参考同步 |
| P1 | R3-05 | release.yml 的 staging smoke 非隔离且无"未发布构件必须不可解析"反向断言（R-41 只落在 ci.yml）→ 发布前最后一道门禁可假绿 | `release.yml` 旧步骤 vs `ci.yml` staging-smoke job | FIXED：release 通道改为隔离本地仓库 + 只安装发布集 + 从 `pom.xml` 动态读取 excludeArtifacts 逐项断言不可解析 |
| P1 | R3-06 | zh-CN README 与代码/EN 事实冲突：默认序列化器写成 Fury、Fury 白名单姿态反了、消费基准口径仍称"完整端到端"、广播语义停留在 0.1.2 之前（"每次重启产生新组/不补投"）、完整配置示例含不存在的 FQCN 与 4 处错误默认值 | zh README 各节 vs `StreamMQProperties.java:174/188`、`StreamMQConstants`、EN README | FIXED：逐处对齐代码与 EN（含基准口径、广播身份五级链、示例键与默认值） |
| P1 | R3-07 | 上一轮"门禁全绿"与覆盖率叙述不实：CI 注释声称 redisson"仅单测 33%、含 IT 90%+"，实测含 IT 行覆盖 0.636；配置参考与 CHANGELOG 也有同类夸大 | 本轮实测 jacoco.csv；`ci.yml:260` | FIXED：叙述改为实测值；CHANGELOG 的"全部 Lua 单 key 原子"改为含例外（跨 key 转投脚本在 Cluster 下 CROSSSLOT，0.1.x 不支持 Cluster） |

---

## 5. 逐项处置（P0 → P4）

### P0 / P1（全部闭环）

| 编号 | 修复要点 | 验证 |
|---|---|---|
| R3-01 | `streammq-core` BRANCH 阈值按实测重设；各模块阈值在"实测 − 3pt"口径复核 | `mvn clean verify -Djacoco.check.skip=false`（§9） |
| R3-02 | ci.yml coverage job 命令改写；同型 YAML 折叠标量全仓排查 | YAML 解析 + 参数拼接实测 |
| R3-03 | ① `DefaultConsumerGroupManagerFactory#resolveInstanceId`：实例身份从注册项消费者名反解（不可反解时告警并回退随机 id）；② `PelClaimScheduler`：改为按实例标识**精确匹配**，并把 instances 快照提到每轮每目标一次（顺带修掉逐条目 HGETALL 的新 N+1）；③ 新增 `RedTeamRegressionIT.pelClaim_skipsLiveOwnerThenReclaimsAfterHeartbeatGoesStale` | 真实 Redis IT（先断不重投、再断恢复重投）；`PelClaimScheduler` 单测全绿 |
| R3-04 | 依赖迁移 (`org.apache.fory:fory-core:1.7.3`，含 starter/benchmark 测试依赖与 BOM 说明)；`FurySerializer` 恢复 `SecurityException` 门禁（属性确认后才放行并 WARN）；`FurySerializerTest` 改为"无属性必抛 + 有属性可用"双向断言；NOTICE/SECURITY/README/配置参考同步 | `FurySerializerTest` 12/12 通过 |
| R3-05 | release.yml 隔离 staging + 反向断言 + japicmp 模块清单收敛到发布集 | YAML 解析 + 复用 ci.yml 已验证的断言逻辑 |
| R3-06 | zh README 全量对齐（序列化器默认值/Fury 姿态/基准口径/广播五级身份链/配置示例/RTopic 取舍对比/`redisson.singleServerConfig` 警告） | 逐处以代码常量与 EN README 双向核对 |
| R3-07 | 覆盖率叙述改为实测；CHANGELOG 新增"Serializer 默认值切换的数据兼容提示"、0.1.1 标注"内部迭代未发布"、Unreleased 折叠进 0.1.2 | 文件级复核 |

### P2（按证据修复）

| 编号 | 问题 | 处置 |
|---|---|---|
| R3-08 | 事务卡在 COMMITTING 无限重放（目标键类型冲突/ACL/长期不可用），本地事务已提交而消息永不投递，且无终态与运维信号 | FIXED：COMMITTING/ROLLBACKING 复用回查计数作为**有界恢复预算**，耗尽后强制终结为 ROLLBACK + `.failureReason` 字段 + ERROR 告警 + 回滚指标（转投与状态同脚本原子，状态非 COMMIT 即"未发布"，终结语义真实）；半消息尽力 XDEL；保留期清理同步移除新字段 |
| R3-09 | 广播同主机身份碰撞：`LUA_CLAIM` 对同主机槽位无条件覆盖（忽略 pid），本地身份文件按 OS 用户共享 → 同机两个进程共用身份，广播静默退化为集群消费 | FIXED：`LUA_CLAIM` 增加"同主机 + 心跳新鲜 + pid 不同 + 未标记释放 ⇒ 拒绝"；新增 `LUA_RELEASE` 把优雅停机的槽位标记为「已释放」（pid=0）——保证快速重启仍能回收同一身份、复用 PEL（0.1.2 语义不回退） |
| R3-10 | `start()` 与 `stop()` 竞态：`createAndRegister` 期间 stop 完成后，组管理器（心跳+RTopic 订阅）成为永久僵尸，后续 stop 因状态非 RUNNING 直接返回，无人回收 | FIXED：`start()` 在登记后复查 `lifecycle.isRunning()`，失配即撤销并返回（与动态注册路径 `wireRegistrationIfRunning` 对齐） |
| R3-11 | 两处维护接口仍 `readAll()` 全量物化（R-34 只修了 payload 方向），且逐成员 `EXISTS`：延时 `cleanupOrphanedInZSet` 与重试 `RetryScheduler#cleanupOrphanedEntries` | FIXED：两处均改为 `ZRANGEBYSCORE ... LIMIT 0 N`（N=1000）有界窗口 + 截断 WARN；全仓复查 `readAll` 剩余用途（均为有界结构：instances Hash / 单消息 payload / 带容量探测的引用集） |
| R3-12 | 管理端点 `pendingCount` 用 `listPending(...,maxPendingQuerySize).size()`，被静默截断（积压 5 万报 1000），且每次调用物化 ≤1000 条 | FIXED：改用 `getPendingInfo(group).getTotal()`（O(1)，与 diagnostics 模块同口径） |
| R3-13 | `CodecExplicitnessTest` 可空转：只扫 redisson 一个模块、正则漏 `getJsonBucket/getDeque/getPriorityQueue/...`、嵌套泛型签名整段跳过、无最小匹配数断言 | FIXED：扫描全部模块 `src/main/java`；补全 getter 清单；`getTopic` 另用 `redisson.` 接收者限定（避免 `message.getTopic()` 误报）；断言受保护调用点 ≥60 |
| R3-14 | NOTICE 事实错误（Jackson 版本、Fury/Protostuff/Micrometer 的 "Used in"、缺 `testcontainers-redis`） | FIXED：按 pom 逐项校正并更新为 Apache Fory 1.7.3 |
| R3-15 | samples/README 仍教 `redisson.singleServerConfig.*`（本 starter 无绑定）+ 运行手册缺"先本地安装 SDK"步骤 | FIXED：改为 `spring.data.redis.*` 并补安装前置步骤（样本 pom 描述中的已移除注解同步更正） |
| R3-16 | CONTRIBUTING 教用 Java `ServiceLoader` 注册 SPI（全仓 0 处使用）、声称 japicmp 排除 `internal.*`（该包不存在）、本地 dry-run 命令无法复现发布门禁 | FIXED：改为 Spring bean/SPI 类配置的事实描述；兼容策略按 0.1.x 事实重写；dry-run 改为 `mvn clean verify -Djacoco.check.skip=false` 并注明 Redis 与 `NVD_API_KEY` 前置 |
| R3-17 | EN README 模块表未标注 binder/benchmark 为"仅源码提供"（用户按表引用即解析失败，R-10 修复不完整）；死锚点 `#advanced-usage`；`/actuator/prometheus` 未注明需自行加 `micrometer-registry-prometheus`；广播身份链文档含不存在的"MAC 回退"与错误文件名 | FIXED：逐处更正（含 `BroadcastInstanceIdResolver` 真实五级链） |
| R3-18 | 配置参考 2 处约束值错误（`retry-times ≤ 10` 实为 16；`pel-claim-min-idle ≥ 60s` 实为 35000ms）+ 缺 `fury-registered-classes` 行 | FIXED |
| R3-19 | CodeQL job 无 build 步骤，每次运行必然失败（"could not process any of it"，exit 32） | FIXED：加入 `autobuild` + JDK 21 setup |
| R3-20 | CI 从不执行 `spotless:check`（所有调用都带 skip）→ 格式漂移只在发布时爆炸 | FIXED：新增独立 `format` job（`mvn -q spotless:check`，不需要 Redis） |
| R3-21 | 基准回归脚本 `check_benchmark_regression.py` 无任何调用方（"已修复"不实），基线为空 | FIXED：benchmark.yml 接入脚本（空基线时输出 notice 且不失败，基线回填后自动生效） |
| R3-22 | 18 个 `package-info.java` 无许可证头，而 `spotless:check` 仍报 clean（licenseHeader delimiter=package 的盲区） | FIXED：补齐 18 个头（13 模块 + 5 样本） |
| R3-23 | 事务/认领路径的跨 key Lua 在 Redis Cluster 下 CROSSSLOT，而 CHANGELOG 宣称"全部脚本单 key 原子" | FIXED（口径）：CHANGELOG 补例外说明并明确 0.1.x 支持单实例/主从，Cluster 为后续议题 |
| R3-24 | 基准报告 §3/§4 自相矛盾（"完整端到端路径"/"XACK 是瓶颈"），"含网络"标注错误，harness 的 `messageCount` 参数为死参数 | FIXED（口径）：报告改为"补货端约束下的下界 + 容器化基准为 0.2.0 待办"；死参数删除；JMH 参数披露按注解实际值更正 |

### P2/P3/P4 残留（判定与依据，非"以后再说"）

| 编号 | 判定 | 依据 |
|---|---|---|
| R3-25 ORDERLY 分片锁竞争计入业务重试预算 | 残留（P2，0.2.0） | 竞争导致的 `RECONSUME_LATER` 与 handler 失败共用同一动作返回值，无法在不改 SPI/不改批量语义的前提下区分：任何"不消耗预算 + 继续等待"的方案要么新增公开动作类型，要么要求"停止处理批内后续消息"（会引入最多一个 PEL 扫描周期的额外延迟）。默认参数下表现有界（≤ 约 96s 阻塞 + 从未处理的消息误入 DLQ），但修复涉及顺序语义，不宜在发布前改。**需要「延迟信号」设计评审后再动。** |
| R3-26 JdkSerializer 载荷驱动目标类型 | 残留（P2，0.2.0） | 顶层目标类仍可由载荷名称选择（仅受 denylist 约束）；结构性阻断（拒绝所有非消费者声明类型）会改变现有"任意 POJO"用法，属行为变更。当前残余风险=应用 classpath 内存在单类 readObject/readResolve gadget 且攻击者可写 Redis。 |
| R3-27 消费基准 harness 补货端自约束 | 残留（P2，0.2.0） | 数字已是"下界"口径且本轮修正了披露；真正的容器化端到端基准需要新 harness（含逐消息 ACK 变量），属独立工作量。 |
| R3-28 ACK 管线/start-stop 竞态缺少"失败即红"测试 | 残留（P2） | 本轮补了 R-11 的失败即红 IT；ACK 窗口与生命周期竞态需要可控的慢 XACK/双线程夹具，属测试基建投入，已列入 0.2.0 测试计划。 |
| R3-29 OWASP 作业仍红（NVD 408） | 运维前置（P1→前置条件） | 代码侧已把 CVE 扫描接入 release 通道并要求 `NVD_API_KEY`；**维护者必须在仓库 secrets 中配置该 key**，否则发布门禁的 CVE 步骤会（按设计）失败。 |
| R3-30 japicmp 兼容范围冻结 adapter 内部代码 | 残留（P2，1.0 前） | 需要在"发布前一次性搬迁后端中立代码到 internal 包"与"放弃 0.1.x 的 adapter 兼容承诺"之间做选择；本轮已在 CONTRIBUTING 明确 0.1.x 策略，避免虚假承诺。 |
| R3-31 自动配置双重注册（`.imports` + `@Import`）、`@ConditionalOnMissingBean` 绑定具体类而非接口 | 残留（P3，1.0 前） | 现行为无缺陷（排序由 `@Import` 顺序保证），但 `spring.autoconfigure.exclude` 对 5 个配置项失效、外部实现核心接口不会替换容器实现——属"承诺与事实不符"的边界，1.0 前统一重构。 |
| R3-32 God 容器（57 字段/24 setter）、`Message.withX` 复制、775 行自动配置、调度器重复代码 | 残留（P3，1.0 前） | 无行为缺陷；量化证据与低成本路径见 §6（容器可先抽不可变 `ContainerOptions`）。 |
| R3-33 kubernetes 模块（3.6k LOC，不发布、无消费者、Operator 无测试） | 残留（P3） | 已在 reactor 内但排除发布；建议移出默认 reactor 或补 fabric8 mock-server 测试，避免"永远待完整"的僵尸模块。 |
| R3-34 测试诚实度残项（13 个零断言用例、样本 IT 命名空间不隔离、无 soak、无并发/故障注入覆盖 ACK 与事务提交） | 残留（P3） | 风险路径绝大部分有强断言覆盖（真实 Redis IT/故障注入/并发计数）；零断言集中在观测性聚类，已在 §7 记录，不构成发布阻断。 |
| R3-35 安全加固残项（CSRF 仅 Origin 且 fail-open、解压按消息不按批、XFF 信任下客户端表增长、流保留默认无上限、诊断端点不受 actuator exposure 治理） | 残留（P3） | 逐条在 §7 给出触发条件与影响边界；默认配置下均不可达或不放大（如 XFF 信任默认 false）。保留无上限是"库不替应用做数据保留决策"的取舍，已在文档明示。 |
| R3-36 文档 i18n 债（EN README 残留中文注释、SECURITY.md/公开 javadoc 仅中文、EN 管理 API 列表与 zh 不对称） | 残留（P3） | 属本地化投入；本轮已修所有"事实性"不一致，剩余为语言覆盖问题。 |

---

## 6. 结构性整备的量化与低成本路径（1.0 前）

| 项 | 量化 | 低成本路径（迁移成本已评估） |
|---|---|---|
| `DefaultStreamMQListenerContainer` | 1373 行 / 57 字段声明 / 51 公开方法 / 24 setter；仅 5 个 setter 有 `assertInitState`；`setMetrics(x)` 与 `setHandlerMetrics(null)` 可组合出"容器认为无指标、处理器仍在记录"的不一致态 | 抽不可变 `ContainerOptions`（删 14 个调优 setter，保留 5 个运行时可变的并文档化）；调用点共 31 处（1 生产 + 29 测试 + 1 装配方法），旧 setter 保留一个版本做 `@Deprecated` |
| 调度器重复 | `RetryScheduler` 与 `DelayMessageScheduler` 有 214 行完全相同的行；"原子搬移"脚本存在 5 份独立实现 | 先抽公共脚本注册表（5 份 → 1 份），再抽扫描/认领模板；R-49（漏传 codec）正是这种形状的产物 |
| 自动配置 | 5/7 配置类同时出现在 `.imports` 与 `@Import` 中，排序/排除语义被架空 | 保留 2 个顶层自动配置，其余改为 Boot 惯用的 `@AutoConfiguration(before/after)` |
| Redis 键布局守卫 | `StreamMQKeys` 被 3 个模块 19 个文件使用，而编解码守卫此前只覆盖 1 个模块 | 本轮已把守卫扩到全部模块；后续可把守卫下沉到共享测试支持构件 |

---

## 7. Persona Gauntlet（本轮修复后仍会被提出的质疑，及当前答案）

1. **安全研究员**："Fury 白名单是假的，还能打 RCE。" → 已不成立：坐标迁到 `org.apache.fory:fory-core:1.7.3`（CVE-2026-50076 修复版），宽松模式恢复 `SecurityException` 门禁。
2. **Staff 工程师**："慢消费者会被重复投递、最后进 DLQ。" → 已不成立：判活身份同源 + 精确匹配，并有真实 Redis 回归 IT 双向断言。
3. **开源 Maintainer**："你们的覆盖率门禁在 CI 里根本没跑。" → 已不成立：coverage job 命令修正（参数拼接实测），release 通道隔离 staging + 反向断言。
4. **深夜新手**："我按 samples/README 配 `redisson.singleServerConfig` 连了 localhost。" → 已不成立：样本文档改为 `spring.data.redis.*` 并给出"先 install SDK"的前置步骤。
5. **极端用户**："事务提交一直失败，消息卡着不动。" → 已不成立：有界恢复预算 + 强制终结 + `.failureReason` + ERROR 告警（本地事务已提交的场景明确要求业务对账）。
6. **CI 维护者**："OWASP job 永远红。" → 需维护者动作：把 `NVD_API_KEY` 配进 secrets（文档已列为发布前置）；release 通道已把 CVE 扫描设为硬门禁。
7. **Redis Cluster 用户**："事务和 DLQ 重投报 CROSSSLOT。" → 文档已明示 0.1.x 只支持单实例/主从；Cluster 支持列在后续版本。

---

## 8. Final Verdict

### 8.1 维度评分（fullReview.md §27）

| 维度 | 分数 | 依据（本轮实测） |
|---|---:|---|
| 产品目标 | 9 | 问题定义清晰、定位诚实（"Redis 上的开箱即用 MQ"），"为何不用 X"有实质取舍 |
| 功能完整度 | 8 | 重试/DLQ/延时/事务/顺序/广播/背压/可观测齐全；缺 Cluster、binder 未发布 |
| 架构 | 8 | 方向正确、依赖无环、SPI 真实；前沿：adapter 面被过度冻结、God 容器待拆 |
| 模块设计 | 7 | 11 模块职责可解释；kubernetes 为僵尸模块，键布局跨 3 模块重复 |
| 设计模式/原则 | 7 | 模式均有实际价值；调度器 214 行重复、部分单实现接口属过早抽象 |
| 实现质量 | 8 | 本轮修掉 3 个静默数据语义缺陷；残留：顺序消费竞争语义、解码期分配 |
| API / SDK | 8 | Builder + 不可变值对象 + 类型化异常；API 兼容承诺范围需在 1.0 前收敛 |
| Maven 工程 | 7 | 发布集/BOM/预检/兼容门禁成型；OWASP 依赖 NVD key，japicmp 范围待收敛 |
| 测试 | 8 | 1148 用例 + 真实 Redis IT + 故障注入 + 并发计数；缺 ACK/生命周期的失败即红用例 |
| 并发 | 7 | 原子脚本与有界队列扎实；R-11 判活曾整体失效、start/stop 竞态、ORDERLY 竞争语义 |
| 性能 | 7 | 热路径无阻塞往返、N+1 已消；基准 harness 自约束、解码分配未测 |
| 安全 | 8 | codec 显式化 + 守卫、反序列化白名单、端点鉴权加固；CVE 已迁移修复；残余见 §5 |
| 文档 | 8 | 本轮修正全部事实性冲突（双语 + 配置参考 + 报告口径）；i18n 债仍在 |
| Developer Experience | 8 | Quick Start/示例/错误信息可用；示例需本地 install 前置已写明 |
| 开源准备度 | 7 | 发布流程/许可证/治理文件齐备；secrets 前置与仓库卫生（.claude/、docs/REPORT）需决定 |

```text
Overall = round(115 / 15 × 10) = 77 / 100
```

> **与上一轮 91 分的差异必须解释**：91 分建立在一份无法复现的验证证据上（门禁未真正执行 + 3 项"已修复"
> 在代码层无效）。77 分不是"项目变差了"，而是把 R3-01…R3-07 这些**上一轮就存在但未被发现**的事实计入后，
> 更接近项目当前真实状态的自评。评分只用于排优先级，是否发布由 §8.2 的门禁规则决定。

### 8.2 门禁裁决

```text
Release Status: GO
Release Readiness Score: 77 / 100

Must Fix Before Release: 0 items（R3-01…R3-07 全部闭环）
Should Fix:             0 items（R3-08…R3-24 全部闭环）
Open P2:                4 items（R3-25 / R3-26 / R3-27 / R3-28，见 §5 残留表）
Open P3/P4:             8 组（R3-31…R3-36 及结构性整备，见 §5/§6）
Release Prerequisite:   1 item（R3-29：仓库 secrets 配置 NVD_API_KEY，否则 release 的 CVE 步骤按设计失败）
```

**门禁规则对照**（`.claude/skills/release-readiness-redteam` §6）：
- 无未决 P0（R3-01/R3-02 已闭环）；无未决 P1（R3-03…R3-07 已闭环）；
- 未决 P2 = 4（< 5）→ 不触发 CONDITIONAL GO；
- 依规则结论：**GO**，且 GO 的成立以 §9 的门禁命令实测为证。

> 与上一轮的差异必须被记录：上一轮给出 GO 91/100，但其"门禁全绿、1148 测试"证据在**启用门禁的
> 命令**下不成立，且三条"已修复"项在代码层无效。本轮把"验证证据"从"普通 `mvn clean verify`"
> 改为"**与发布通道完全一致**的 `mvn clean verify -Djacoco.check.skip=false`"，并以该命令的原始输出为准。

---

## 9. 验证证据

### 9.1 门禁命令（与发布通道完全一致）

```text
mvn clean verify -Djacoco.check.skip=false
→ BUILD SUCCESS（20/20 模块 SUCCESS，MVN_EXIT=0）
```

> 与上一轮的关键差异：上一轮的"全绿"来自**未启用门禁**的 `mvn clean verify`（`jacoco.check.skip` 默认 true）；
> 本轮以发布通道使用的同一命令取证。基线上该命令必然失败（core 分支覆盖率 0.472 < 0.52），
> 修复后全绿。

### 9.2 测试与集成测试执行

| 项 | 数值 |
|---|---|
| 测试总数（surefire 单测 + failsafe 集成测试） | **1150** |
| 其中：单元测试（surefire） | 883 |
| 其中：集成测试（failsafe，真实 Redis） | 245 + 样本 22 = **267** |
| 失败 / 错误 / 跳过 | **0 / 0 / 0** |

- 集成测试真实执行（非跳过）：redisson 111 例（含本轮新增的 PEL 判活回归 IT，先在"心跳新鲜"阶段断言
  **不重投**、再在"心跳过期"阶段断言恢复重投）、starter 36 例（含 `StreamMQAdminEndpointIT` 真实
  Redis 验证 `pendingCount`）、`streammq-test` 44 例、kubernetes 4 例、8 个样本 22 例。
- 本轮新增/改写的测试：`RedTeamRegressionIT#pelClaim_skipsLiveOwnerThenReclaimsAfterHeartbeatGoesStale`、
  `FurySerializerTest`（门禁双向断言）、`CodecExplicitnessTest`（全模块扫描 + 最小匹配数守卫）、
  `NameValidationTest`（`|`/`,` 拒绝）、`StreamMQAdminEndpointTest`/`IT`（XPENDING 总数口径）。

### 9.3 覆盖率门禁（逐模块实测 vs 阈值）

| 模块 | LINE 实测 | LINE 门槛 | BRANCH 实测 | BRANCH 门槛 |
|---|---:|---:|---:|---:|
| streammq-core | 0.513 | 0.48 | 0.475 | 0.44 |
| streammq-redisson（含真实 Redis IT） | 0.631 | 0.60 | 0.533 | 0.50 |
| streammq-spring-boot-starter | 0.603 | 0.55 | 0.455 | 0.41 |
| streammq-diagnostics | 0.815 | 0.75 | 0.724 | 0.69 |
| streammq-tracing-opentelemetry | 0.775 | 0.70 | 0.679 | 0.65 |
| streammq-spring-cloud-stream-binder | 0.807 | 0.75 | 0.702 | 0.67 |

（`jacoco:check` 在 6 个发布模块真实执行，无 `Rule violated`；`streammq-kubernetes` 不发布、不设卡。）

### 9.4 其它门禁

- `spotless:check`：20 个模块全部通过（含本轮新增的 18 个 `package-info.java` 许可证头）；
- `enforcer`：Java 21 / Maven 3.9 / `dependencyConvergence` / `banDuplicatePomDependencyVersions` 全绿；
- `failsafe`：IT 真实执行（tripwire 口径见 CI）；
- 证据文件：`/tmp/streammq-redteam/verify-gate3.log`（最终树上的原始输出，含每个模块的测试计数与门禁结论）；
  此前两次门禁运行（`verify-gate.log` 复现基线失败、`verify-gate2.log` 修复后全绿）一并保留可对照。

### 9.5 未由本轮执行验证的项（复述 §10 关键项）

- Maven Central 真实发布（`-Pgpg deploy` → Portal staging）未执行：无凭据；`autoPublish=false` 与
  `waitUntil=validated` 的行为需一次 dry-run 定论。
- JMH 未运行（报告数字已标注口径与重测计划）。
- Redis Cluster 未实测（CROSSSLOT 为推断，已在 CHANGELOG 明确 0.1.x 不支持 Cluster）。
- CI 通道（GitHub Actions）本轮改动（4 个 workflow）已做 YAML 解析与 `run` 脚本参数拼接实测，
  但未在真实 Actions runner 上执行——下一次 push 才能确认远端行为。

---

## 10. 覆盖诚实性（本轮实际审查了什么、跳过了什么）

**已深审**：core/redisson/starter 三个主模块的消费与发布热路径、PEL 认领与重投、事务状态机与提交执行器、
延时/重试/DLQ 调度器、广播注册中心与身份解析、容器生命周期与并发、序列化器与反序列化守卫、管理端点鉴权、
配置属性全量（69 行 vs 70 属性）、8 个样本、全部 pom 与 4 个 workflow、NOTICE/LICENSE/SECURITY/CONTRIBUTING、
CHANGELOG、README 双语、配置参考、基准 harness 与报告、CI 运行日志（含历史失败 job）、Central 上的发布状态。

**未验证/跳过的（诚实声明）**：
- 未在本轮真实执行 Central 发布（无凭据）；`-Pgpg deploy` 到 Portal staging 的端到端签名/校验流程为
  **UNKNOWN**，需要一次 dry-run 才能定论。
- 未运行 JMH（代码改动仅限披露与死参数；数字重测列为 0.1.2 发布前动作）。
- Redis Cluster 行为为**推断**（跨 slot CROSSSLOT 由 key 布局 + Redisson 脚本语义推出），未在真实 Cluster 上复现。
- `streammq-kubernetes` 的 Operator 逻辑未做深度业务审查（模块不发布）。
- kubernetes/binder/tracing/diagnostics 的**业务逻辑**仅做边界与依赖审查，未逐行为审计。
- 上一轮报告中的残留项（R-32/R-42/R-45/R-46/R-48）本轮以"新证据"重新评估：R-32 因本轮 N+1 修复（快照提升）
  降级为可接受；R-45 计数更正为 13；R-48 见 §6 路径。
