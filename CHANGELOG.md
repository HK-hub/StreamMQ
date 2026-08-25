# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- 集成测试在无 Redis 环境统一自动跳过（含 Spring Boot 自动装配 IT），保证 `mvn verify` 在任意环境可复现
- 调度器（Retry/Delay/Transaction/PelClaim）SmartLifecycle 相位调整为先于消费容器启动、晚于其停止
- 事务消息：未注入 TransactionScanner 时快速失败（不再提供"先投递再回滚"的假事务回退路径）
- 诊断 REST 报告增加 locale-neutral `code` 字段，message 文本改为英文；移除伪造的线程池活跃度指标

### Removed

- 移除未生效的配置项：`streammq.event.*`、`streammq.thread-name-prefix`、`streammq.tracing.collector`、`streammq.tracing.trace-topic`（自定义 TraceCollector 请直接声明 Spring Bean）
- 移除 Kubernetes 模块中无控制器的 StreamMQTopic / StreamMQConsumerGroup CRD 与模型
- 移除不可拉取的默认镜像名；`spec.image` 现为必填

### Fixed

#### streammq-redisson（投递可靠性）

- 并发消费组新增 PEL 启动排空 + PelClaim 认领覆盖，修复实例崩溃后消息永久滞留 PEL 的问题
- 毒丸消息逐条隔离进入 DLQ，不再拖垮整批已投递消息
- 延时消息改为「先写 payload 后写调度」+ 批量失败回补 ZSet，消除两处崩溃丢消息窗口
- 事务消息引入执行权锁（SETNX+TTL）串行化发布临界区，消除 COMMITTING 状态双实例重复发布
- PelClaim DLQ 分支调整为「先写 DLQ 后 ACK」，消除崩溃丢失窗口
- 同步发送仅在"确定未送达"的异常上重试；超时后已确认成功的结果直接返回，避免模板重试导致重复消息
- 重试/DLQ 调度改为单原子批次写入并附带 payload TTL；二级 DLQ 路由失败时保留 PEL 不再静默丢弃
- Rebalance 信号量初始化修复（此前许可从未初始化、注册期误占用）
- 广播模式消费者组改用稳定实例标识并在停止时销毁，修复组无限累积与全量重放
- 延时消息补齐 maxMessageSize 校验

#### streammq-spring-boot-starter

- 修复 Micrometer 指标自动装配失效（未注册为顶层 AutoConfiguration 导致排序失序）
- AOP 代理消费者的注解解析改用 target class，修复代理 Bean 无法注册消费的问题
- `@StreamMQDlqConsumer` 支持 `${}` 占位符解析
- 新增独立 `streammq.admin.enabled` 开关，与 `streammq.health.enabled` 解耦

#### streammq-tracing-opentelemetry

- 修复异步发送场景 Producer Span 泄漏（跨线程 ThreadLocal 配对失效），改为有界消息级注册表
- 实现 OTLP gRPC 导出器：配置 `otlp-endpoint` 即构建真实 SDK 导出链路（此前仅 no-op 且静默忽略端点配置）
- 消费 Span 增加 makeCurrent 作用域，业务侧 `Span.current()` 可正确挂接

#### 其他模块

- Kubernetes 控制器 phase 由 Deployment 就绪副本推导（对齐 CRD enum）；HPA 无指标时 fail-closed；扩缩容结果持久化到 CR spec；Redis 密码支持 SecretKeyRef；模块默认关闭并标注实验性
- 诊断积压探针改用 XPENDING 总数形式，消除 >1000 条时的静默截断
- 测试工具 flushdb 增加 `-Dstreammq.test.redis.flushAllowed=true` 本地模式守卫；Embedded Redis 更名为 ContainerizedRedisServer 并前置 Docker 可用性检查

## [0.1.0]

### Planned (V2.0, 规划中，尚未实现)

- Multi-backend abstraction (BackendProvider SPI) supporting Redis / Kafka / RabbitMQ / Pulsar
- Kafka backend implementation based on Kafka Client BackendProvider
- Cross-datacenter asynchronous replication (RPO ≤ 1s)
- Kafka wire protocol compatibility (native Kafka Client zero-code access)

> 注：以上 V2.0 规划项尚未实现，未包含在任何已发布版本中；详细规划见 README「路线图」章节。

### Changed

- N/A

### Deprecated

- N/A

### Removed

- N/A

### Fixed

- N/A

### Security

- N/A

---

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

[Unreleased]: https://github.com/HK-hub/StreamMQ/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/HK-hub/StreamMQ/releases/tag/v0.1.0