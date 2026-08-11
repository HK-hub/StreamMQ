# StreamMQ 发布前全面审计（交叉验证版）

## 项目背景
StreamMQ 是基于 Redis Stream + Redisson 的开箱即用 Redis 消息队列 SDK。
- 技术栈：Java 21 / Spring Boot 3.3.5 / Redisson 3.34.1 / Maven 多模块（17 个模块）
- 版本：0.1.0（首个公开发布版本）
- 许可证：MIT
- 模块：bom / core / redisson / spring-boot-starter / tracing-opentelemetry / diagnostics / kubernetes / spring-cloud-stream-binder / test / samples(quickstart, transaction, orderly, delay, dlq, interceptor)
- 发布渠道：GitHub（开源仓库）/ Product Hunt / X / YouTube（演示）
- 目标用户：Java/Spring 后端开发者

## 审计目标
公开发布前全面自检，确保开发者首次 clone、构建、运行、体验时不踩坑。覆盖功能、特性、开发者体验、bug、性能、安全、文档、发布物等维度。所有发现须有可验证证据（file:line 或命令输出），不得臆测。

## 审计维度与检查项

### A. 构建与 CI（BUILD）
- A1 `mvn clean compile` 全模块零错误
- A2 `mvn test` 单元测试全绿（记录通过/跳过数）
- A3 `mvn verify` 集成测试在有 Redis 时全绿
- A4 `mvn enforcer:enforce` 依赖收敛/版本要求通过
- A5 `mvn spotless:check` 代码格式通过
- A6 是否有 CI（.github/workflows）且可触发
- A7 source/javadoc jar 可生成
- A8 SNAPSHOT 版本号是否适合公开版（应转正式版）

### B. 功能完整性（FUNC）--README 承诺 vs 实际实现
逐项核对文档承诺与代码实现是否一致：
B2 普通(sync/async) / B3 顺序 / B4 延时(level+自定义) / B5 事务 / B6 DLQ+二级DLQ / B7 重试 / B8 Tag+SQL92过滤 / B9 广播 / B10 压缩(GZIP) / B11 追踪(OTel+核心trace) / B12 诊断(画像/慢消费/积压/DLQ报告) / B13 K8s(健康/HPA/优雅停机/CRD) / B14 Spring Cloud Stream Binder / B15 每个 sample 能独立运行

### C. API 设计与稳定性（API）
C1 公开 API Javadoc 完整 / C2 SemVer 与 0.1.0 API 表面稳定性 / C3 无废弃/未完成 API 暴露 / C4 参数校验(null/非法值) / C5 异常层次清晰(StreamMQException 体系) / C6 Builder/Option 模式一致性

### D. Bug 与正确性（BUG）
D1 并发安全/竞态 / D2 资源泄漏(连接/线程/锁) / D3 边界(空/超大/超时) / D4 重试与死信路径 / D5 事务回查/超时 / D6 消费超时与 ACK 语义 / D7 TODO/FIXME/硬编码 / D8 自动配置顺序与 @ConditionalOnBean 时序可靠性

### E. 性能（PERF）
E1 是否有 JMH 基准 / E2 吞吐与延迟基线 / E3 大消息/批量表现 / E4 连接池配置合理性 / E5 序列化方案与默认选择 / E6 性能反模式(同步阻塞/不必要拷贝)

### F. 安全（SEC）
F1 依赖 CVE 扫描 / F2 Redis 认证授权支持 / F3 管理操作鉴权(ManagementAuthenticator) / F4 日志不泄露敏感信息 / F5 反序列化安全 / F6 输入校验(topic/group 名注入)

### G. 开发者体验（DX）
G1 quickstart 能否 5 分钟跑通 / G2 配置项自描述(configuration-metadata) / G3 错误信息可操作 / G4 启动失败诊断提示清晰 / G5 默认配置安全合理 / G6 README->首次运行最短路径

### H. 文档（DOC）
H1 README 与代码一致 / H2 快速开始可复现 / H3 配置项文档完整 / H4 架构文档与实现一致 / H5 Javadoc 覆盖率 / H6 CHANGELOG / H7 CONTRIBUTING/CODE_OF_CONDUCT

### I. 打包与发布（PKG）
I1 Maven 坐标/artifactId 命名一致 / I2 BOM 覆盖所有公开模块 / I3 source/javadoc jar / I4 Maven Central+GPG 准备 / I5 LICENSE 存在 / I6 NOTICE/第三方声明 / I7 依赖 scope 正确(provided/test)

### J. 观测性（OBS）
J1 Micrometer 指标覆盖 / J2 健康检查 / J3 链路追踪 / J4 日志级别与内容 / J5 诊断端点可用

### K. 兼容性（COMP）
K1 Java 21 要求明确 / K2 Spring Boot 兼容范围 / K3 Redis 版本(Stream ≥5.0) / K4 Redisson 版本 / K5 OS/容器

### L. 仓库卫生与发布物（REPO）
L1 .gitignore 完整 / L2 无敏感信息/临时文件提交 / L3 提交历史清晰 / L4 Issue/PR 模板 / L5 GitHub Release 说明 / L6 演示物料(截图/GIF/demo 脚本 for YouTube/PH) / L7 社交文案(X tagline/PH 介绍)

## 输出格式（用于多 agent 交叉验证）

### 1. 发现清单
| ID | 维度 | 严重度 | 位置(file:line) | 证据 | 建议 |
|----|------|--------|------------------|------|------|
| BUILD-01 | BUILD | Blocker | pom.xml:11 | 版本 0.1.0 | 转 0.1.0 正式版 |

严重度：
- **Blocker** 阻塞发布，必须修
- **High** 严重影响首次体验，发布前应修
- **Medium** 质量/体验受损，建议修
- **Low** 改进项
- **Info** 提示

### 2. 发布就绪评分
每维度 0–10 分 + 加权总分。

### 3. 发布结论
GO / GO-WITH-FIXES / NO-GO + 必修 Blocker 列表 + 发布后跟进项。

### 4. 文档承诺差异
README/文档承诺但未实现/未验证的功能清单。

## 约束
- 每条发现须有可验证证据（file:line 或命令输出），区分"已确认"与"推测"
- 每个维度给出明确"通过/不通过"判定
- 不臆测，无证据不下结论
