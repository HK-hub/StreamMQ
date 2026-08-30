# StreamMQ 发布质量修复计划

> **状态：✅ 全部完成并验证（2026-08-29）**
>
> - 23 项计划任务 + 4 项验证期新发现问题全部关闭，工作已随提交 `7e696da` 推送至 `origin/master`。
> - 验证环境：本地 Redis `127.0.0.1:6379`（db0，无密码）；`mvn clean verify` **21 模块 BUILD SUCCESS**。
> - 测试：**835 单元测试 + 202 集成测试 = 1021**，0 失败 / 0 错误 / **0 跳过**（0 跳过即证明所有
>   Redis IT 真实执行，未像此前无 Redis 时那样静默跳过）。
>
> ⚠️ 本计划完成 ≠ 可以发布。下方「后续待办」与「发布前决策（含 1 项阻断项）」仍需处理。

## Overview

修复 `docs/fullReview.md` 审查中发现的发布阻断与高风险问题，完善事务一致性、Diagnostics 自动装配、批量发送、序列化、安全配置、Maven 发布流程、测试和用户文档。所有行为修改遵循先写回归测试、再实现修复、最后运行模块与全量验证。

除原定范围外，全量验证期又发现并修复了 4 项问题（见 Phase 4），其中「事务消息在非 StringCodec 默认编码下只报成功、永不发布」是新的 P0。

## Architecture Decisions

- 事务状态机采用单向状态转移，并以 Redis 原子脚本保护中间态，禁止 COMMITTING/ROLLBACKING 互相覆盖。
- 事务相关 Redis key 使用统一 hash tag，确保 Redis Cluster 下的原子批操作落在同一 slot。
- 没有 `TransactionScanner` 时**快速失败**（抛 `TransactionException`），不降级为"先投递再回滚"的
  假事务路径；`UNKNOWN` 一律等待回查，绝不静默当作 COMMIT。
- Diagnostics 的积压探针改为可选依赖并提供 no-op 回退，避免自动装配因可选基础设施缺失而失败。
- 批量发送只对确定未落库的异常重试；超时和 Broker 异常按 at-least-once 语义直接失败。
- 发布工作流只接受经过校验的语义化 tag，**且 tag 必须指向本次检出的提交**；BOM 只暴露实际发布的 artifact。
- 消费循环启动失败必须进入健康检查与管理端点，不允许"注册成功但静默不消费"。
- **（验证期新增）事务链路上所有与 Lua 脚本交互的数据结构（txstate Hash、回查计数 Hash、事务执行权
  锁与延时/重试转移 claim 的 `RBucket`）一律显式 `StringCodec.INSTANCE`**，不依赖客户端默认 codec。
  依据：Lua 脚本只认识明文，而 `RMap`/`RBucket` 默认使用客户端 codec（redisson-spring-boot-starter
  默认为 Kryo 类二进制编码，带前缀），两侧编码不一致会造成"静默成功但实际未生效"的 P0 缺陷。

## Task List

### Phase 1: Release blockers

- [x] 修复事务状态机 CAS 竞态、半消息注册窗口、UNKNOWN/null 处理（null 归一化为 UNKNOWN）。
- [x] 修复批量发送重试语义；明确事务无 scanner 时快速失败（删除死代码 `executeInTransactionInline`）。
- [x] 修复 Diagnostics BacklogProbe 自动装配并补集成测试。
- [x] 修复 Redis Cluster 事务 key hash tag 并补验证。
- [x] 修复 Spotless/Javadoc 发布门禁。
- [x] **`streammq-test` 发布构件依赖不可解析**：`streammq-test-support` 纳入发布，运行时必需的
      依赖（test-support / core / slf4j / redisson）改为可传递。
- [x] **`v0.1.0` 标签污染**：标签早于 11 个修复提交且已推送；首个公开版本改为 **0.1.1**，
      `release.yml` 增加"tag 必须等于 HEAD"门禁。

### Phase 2: API、安全与资源

- [x] 暴露 Fury 类型注册 API，补默认 POJO 测试。
- [x] 限制 checker 超时并发，处理 Template 异步执行器生命周期。
- [x] 处理 streammq-test 依赖泄漏、BOM 未发布 artifact、重复模块名和 SCM tag。
- [x] 强化 release workflow 版本校验（+ OWASP 扫描 + IT tripwire 纳入发布门禁）。
- [x] 移除公开 API 中拼写错误的 `LocalTransactionState.UNKNOW`（首个公开版本前移除；
      此前"兼容 0.0.x 用户"的理由不成立——`git tag -l` 仅有 `v0.1.0`）。
- [x] 消费循环启动失败上报通道（`LoopFailureReporter`）+ 健康检查 DOWN + 管理端点可见。
- [x] `setConsumeExecutor` 关闭内部执行器，消除注入路径上的执行器泄漏。
- [x] BOM 收敛：不再覆盖 Jackson / SLF4J / Micrometer / Spring Cloud / OTel 等第三方版本。
- [x] 鉴权器长度预言机修复（先 SHA-256 再常量时间比较）+ 安全边界文档。

### Phase 3: Documentation and DX

- [x] 更新 README/README.en/样例，明确事务安全边界、配置、发布和故障排查。
- [x] 新增「广播消费的运维注意事项」与「消费者不消费时的排查路径」章节（中英同步）。
- [x] 更新 CHANGELOG（建立 0.1.1 条目，删除虚构的 0.0.x 兼容/迁移说明）。

### Phase 4: 全量验证期新发现（红队复查）

- [x] **（P0）事务消息在非 StringCodec 默认编码下「只报成功、永不发布」**：`casState` 等 Lua 用明文
      读写 txstate，而 Hash 由客户端默认 codec 写入，字段编码不一致导致 `HGET` 永久 miss，
      `markCommit`/`markRollback` 静默返回，目标 Stream 从不写入。统一为 `StringCodec` 修复，
      并新增 `TransactionBinaryCodecIT`（以 Kryo 默认 codec 运行）作为回归防护——
      此前 `AbstractRedisIT` 一律显式 `StringCodec`，恰好掩盖了该缺陷。
- [x] `setConsumeExecutor` 执行器传播回归：`MessageProcessor` 支持动态替换执行器，不再沿用启动期快照。
- [x] `StreamMQTracingIT` 因共享 Topic 串扰而 flaky：断言改为按内容匹配。
- [x] 安全令牌认证（`SecureCredentialMatcher` / `TokenAuthenticator`）与
      Redisson 缺失 `FailureAnalyzer` 补齐（含 `META-INF/spring.factories` 注册）。

### Checkpoint: Core

- [x] 单元测试通过（**835** 个，全绿）。
- [x] 事务和 Diagnostics 相关集成测试通过（本地 Redis 启动后验证：`TransactionSampleIT`、
      `TransactionBinaryCodecIT`、`StreamMQDiagnosticsIT` 均执行且全绿）。

### Checkpoint: Release

- [x] `mvn clean verify` 在有 Redis 环境下通过（本地 6379，21 模块 BUILD SUCCESS，1021 测试）。
- [x] Spotless、Javadoc、依赖收敛（enforcer）和发布工作流校验通过。

### 验证记录（2026-08-29）

| 轮次 | 结果 | 说明 |
| --- | --- | --- |
| 第 1 轮 | ❌ FAILURE | `streammq-sample-orderly` 期望 3 条、实收 4 条 |
| 第 2 轮 | ✅ SUCCESS | 清空残留数据后重跑，21 模块全绿 |

第 1 轮失败处置：

- 根因：上一次被中断的构建在**固定 topic** 上遗留了未确认消息（pel 重投特征：`reconsumeTimes=1`、
  `bornTimestamp` 早于本轮 34 分钟），本轮消费者从 0-0 消费时把它当成第一条。
  样例 IT 使用固定 topic + 固定消费组且无数据隔离（不同于 redisson 模块 IT 的随机 namespace），
  因此本地重复运行存在复现风险。
- 处置：一次性清空 `streammq:*` 共 **65 个残留 key**（模拟 CI 的全新 Redis）后重跑即全绿。
- 影响评估：非产品缺陷；CI 使用全新 Redis service 不受影响。

## 后续待办（已知、已排期）

> 以下条目已于 2026-08-29 逐条复核，行数为实测值。

- God class 二次拆分（**实测行数，原记录已过期**）：
  `TransactionScanner`(1082) / `DefaultStreamMQListenerContainer`(992) /
  `DefaultStreamMessageTemplate`(857，原记录误写为 `DefaultStreamMQTemplate`(906)，类名与行数均已更正) /
  `RedissonStreamListener`(793) / `PelClaimScheduler`(686)。
- `AbstractMessageConverter`(531) 比具体实现 `DefaultMessageConverter`(407) 更大——抽象边界错位
  （行数已复核无误）。
- **基准占位回填（原记录不全）**：README 中 `consumeThroughput` 与 `messageCreateAndConsume`
  **两行**均为占位，需在有 Redis 环境实测回填（中英文 README 同步）。
- **事务 key hash tag 变更缺迁移/升级说明**：风险表曾承诺"提供兼容读取/迁移说明"，
  但 `docs/`、README、CHANGELOG 中均未落地（见风险表）。
- **BOM 与发布清单不一致（新发现）**：`streammq-kubernetes` 仍在 BOM 中（L94），却被
  `excludeArtifacts` 排除发布（pom.xml L558）。这与架构决策"BOM 只暴露实际发布的 artifact"矛盾，
  会让使用方在依赖该模块时解析到中央仓库中并不存在的版本。需在发布前二选一：
  从 BOM 移除，或将其纳入发布。
- 缺少并发压力测试与故障注入测试（Redis 超时 / 断连 / kill -9 恢复）——全仓无相关测试。
- `-Werror` + `-Xlint:unchecked,deprecation`（pom.xml L340-341）使 JDK 升级脆弱。
- 管理端点缺少失败重试限流（starter 主代码中无相关实现）。

## Risks and Mitigations（附复核状态）

| Risk | Impact | Mitigation | 状态 |
| --- | --- | --- | --- |
| 移除 `UNKNOW` 是破坏性变更 | High | 在**首个公开版本**之前移除；新增回归测试断言该常量不得重新引入 | ✅ 已落地（`EnumsTest` 守卫 + CHANGELOG 记录） |
| `executeInTransaction` 不再降级 | Medium | 事务语义下静默降级比快速失败危险；错误信息直接给出启用 Scanner 的指引 | ✅ 已落地 |
| Redis Cluster key 变更影响已有数据 | High | 原定"提供兼容读取/迁移说明，保留 namespace 配置" | ⚠️ **未落地** — docs/README/CHANGELOG 均无迁移说明；已转为后续待办。0.1.x 为首个公开版本、无外部存量数据，实际影响低 |
| 测试依赖 scope 调整破坏测试工具 | Medium | 全量单测通过；`streammq-test` 需在真实引入场景验证 | ✅ 已验证 — `streammq-test` 在全量 verify 中作为真实依赖被 `CoreRedisIntegrationIT` 使用，`test-support` 已在 pom 中显式标注"不可排除" |
| 发布工作流校验过严 | Medium | 明确的 `vMAJOR.MINOR.PATCH` 校验 + 手动 dry-run 说明；tag≠HEAD 时给出修复命令 | ✅ 已落地（release.yml L103-113） |
| BOM 不再管理 Jackson 等版本 | Medium | 全量构建含 enforcer `dependencyConvergence` 通过 | ✅ 已验证 |
| 样例 IT 无数据隔离（本地重复运行） | Medium | CI 用全新 Redis；本地残留需先清理 `streammq:*` | ⚠️ 已知 — 见验证记录；建议长期为样例 IT 增加 namespace 隔离或启动前清理 |

## 发布前决策（原 Open Questions，已转为可执行判定）

1. **`v0.1.0` 是否已在 Central Portal 完成 publish？（🚧 阻断项，必须先确认）**
   - 判定方式：访问 `https://repo1.maven.org/maven2/io/github/streammq/streammq-core/0.1.0/`。
     - 返回 200 → 0.1.0 已发布。因 Maven Central 构件不可变，下一版本**必须改为 0.2.0**。
     - 返回 404 → 维持 **0.1.1**（groupId `io.github.streammq`，当前 pom 版本 0.1.1）。
   - 无论结论如何，均需先清理陈旧标签（CHANGELOG 已给出命令）：
     `git tag -d v0.1.0 && git push origin :refs/tags/v0.1.0`
2. **是否需要事务 key 迁移脚本？**
   - 结论建议：**不需要**。0.1.x 为首个公开版本，无外部存量数据。
   - 但仍建议补一句文档声明（归入后续待办）：事务相关 key 结构自 0.1.1 定型，此前未公开发布的
     版本无兼容义务。
3. **`streammq-kubernetes` 是否长期保留在发布清单外？**
   - 现状：模块在 reactor 中构建、BOM 中有条目，但被 `excludeArtifacts` 排除发布（实验性预览，
     无模块依赖、核心能力默认 no-op）。
   - 建议：维持排除，并同步把 BOM 中的该项移除，以消除"BOM 管理未发布 artifact"的不一致
     （见后续待办）；待 Operator 功能完整后单独评估是否纳入。
