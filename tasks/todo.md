# StreamMQ 修复任务清单

- [x] 事务状态机和半消息注册竞态
- [x] 批量重试与事务降级语义
- [x] Diagnostics 自动装配
- [x] Redis Cluster hash tag
- [x] Spotless/Javadoc 门禁
- [x] Fury 注册 API与超时 checker 并发
- [x] Maven 依赖/BOM/SCM/release workflow
- [x] 文档、CHANGELOG、配置和日志
- [x] 全量测试与最终审查（真实 Redis 下 `mvn clean verify` 21 模块 BUILD SUCCESS，总计 1021 测试 0 失败 0 跳过）

补充（本会话新增，已合入提交 7e696da）：
- [x] 事务跨 codec P0 修复（非 StringCodec 下事务消息只报成功、永不发布）+ `TransactionBinaryCodecIT` 回归
- [x] `setConsumeExecutor` 执行器传播回归 + 单测
- [x] 安全令牌认证（SecureCredentialMatcher/TokenAuthenticator）与 Redisson 缺失 FailureAnalyzer
- [x] 事务示例 2/2 通过、提交并推送

发布前红队审查（第二批）修复（2026-08-31，工作区待提交）：
- [x] P0-1 BOM 发布插件显式声明（BOM 纳入发布清单，消除使用方 import 失败）
- [x] P1-1 Topic 注册表（createTopic 不再写 `__placeholder` 占位消息；listTopics 合并注册表与消费者）
- [x] P1-3 `/actuator/streammq/stats` 从死端点变为真实统计（RuntimeStatsRegistry + 消费/重试/DLQ 上报）
- [x] P1-4 `updateGroupConfig` 逐 key 运行时真实生效（paused/resume/inflightCapacity 等），不支持 key 显式拒绝
- [x] P1-6 消费循环运行期持续失败健康上报（10 次阈值 → DOWN；成功拉取复位 → UP）+ processor Throwable 兜底
- [x] P1-8 InflightSink 泵异常显式路由 handleFailure（消息出队后不再静默停滞）
- [x] P2 DELETE topics 要求 confirm={topic} 显式确认（防误删，缺失/不匹配 400）
- [x] P2-8 staging-smoke 发布预检 job（使用方视角 import BOM 编译验证）
- [x] P2-13 japicmp API 兼容性门禁（release.yml 探测上一版本，破坏性变更阻断发布）
- [x] P3-13 JaCoCo 覆盖率门禁（CI coverage job，LINE ≥30% / BRANCH ≥15%，防灾难性回退）+ `@{jacoco.argLine}` 空默认值修复
- [x] XFF 可信代理安全模型（`streammq.admin.trust-forwarded-headers` 默认 false + `trusted-proxies` CIDR 白名单）
- [x] 管理端点暴露面说明修正（WebEndpoint 受 exposure.* 治理）、默认 SPI 实现去 `@Component`
- [x] 新增测试：WebRequestAuthSupportTest / RuntimeStatsRegistryTest / ConsumeLoopTaskTest /
      StreamMQAdminEndpointTest / HardeningTest confirm 用例 / MessageSinkTest P1-8 回归
- [x] P1-9 并发消费启动排空"偷取"在途消息致重复投递（hookDrainOwnPending 并发度门控：
      并发度 >1 跳过启动排空，遗留未 ACK 由 PelClaimScheduler 认领）—— 全量复核发现并修复
- [x] P1-10 广播消费者组名碰撞：instanceToken 自动推导回退到进程级主机名，同 JVM 多容器
      组名相同致广播退化为集群（消息只投给其一）。主机名分支追加进程内容器序号（首容器
      纯主机名保持重启稳定性，后续 -2、-3…），并加容器级唯一性回归单测
- [x] 全量 `mvn clean verify` 复核并提交（含 CHANGELOG 与任务文档更新）
      分段复核全部通过：全仓 install ✓；redisson 单测 419 + IT ✓；core/starter/tracing/
      diagnostics/kubernetes/binder/test ✓；samples 8 模块 ✓；benchmark（JMH 无单测）✓；
      全仓 spotless 合规 ✓
      说明：本地连续运行 DelaySampleIT 偶发"残留消息交叉污染"（上次运行失败中断后遗留
      延迟消息被下次消费），干净 Redis 下全绿，CI 全新实例无此问题；IT 基类已放宽客户端
      超时降低全量压测负载 flaky
