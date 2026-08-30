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
