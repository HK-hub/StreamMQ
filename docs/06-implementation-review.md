# StreamMQ 实现审查报告

> 审查日期：2026-07-01
> 审查范围：4 份设计文档（PRD/架构/功能设计/详细设计）+ 78 个 Java 源文件（core 49 + redisson-adapter 18 + spring-boot-starter 11）
> 审查维度：文档间一致性 / 架构实现一致性 / API 契约一致性 / 代码质量

---

## 一、审查总览

| 维度 | 评分 | Critical | Major | Minor | 遗漏项 |
|------|------|----------|-------|-------|--------|
| 文档间一致性 | 5/10 | 21 | 16 | 10 | 20 |
| 架构→实现一致性 | 5/10 | 4 | 10 | 8 | 20 |
| API 契约一致性 | 4/10 | 10 | 12 | 12 | 10 |
| 代码质量 | 6/10 | 6 | 10 | 10 | - |
| **综合** | **5/10** | **41** | **48** | **40** | **50** |

### 总体结论

代码骨架（发送/消费/重试/延时/事务回查调度器/Spring Boot 装配）已基本完成，但存在 **4 类阻断性风险**：

1. **消息可靠性**：调度器先 ZREM 再处理导致消息丢失；事务消息流程断裂（半消息未接入）；批量发送占位 ID 破坏幂等
2. **功能正确性**：容器 start() 竞态导致消费循环可能不启动；asyncSend 拦截器顺序错误；CGLIB 代理 Bean 注解无法识别
3. **API 契约**：MessageBuilder with* 方法缺失（文档示例不可编译）；@EnableStreamMq 缺 @Import；@StreamMqOrderlyListener 缺属性
4. **架构缺失**：ConsumerGroupManager/Rebalance 完全缺失；顺序消费未实现 RLock；可观测性（指标/MDC）未落地

---

## 二、Critical 问题清单（按修复优先级）

### P0 - 立即修复（阻断功能正确性）

#### CR-001 容器 start() 竞态导致消费循环可能不启动
- **来源**：r4-C1
- **文件**：`DefaultStreamMqListenerContainer.java:186-194, 253-258`
- **问题**：`start()` 先调用 `doStartListeners()`（提交虚拟线程任务），后设置 `state=RUNNING`。虚拟线程可能在 state 仍为 STARTING 时执行 `while (state.get()==RUNNING)`，循环不进入，**消息完全不消费**。
- **修复**：在 `doStartListeners()` **之前**设置 `state.set(RUNNING)`。

#### CR-002 asyncSend 拦截器顺序错误
- **来源**：r4-C2, r2-M8
- **文件**：`DefaultStreamMqTemplate.java:147-156`
- **问题**：`asyncSend(Message)` 在 `whenComplete`（发送完成后）才调用 `applyInterceptorsBefore`，且忽略返回值。before 拦截器无法中止发送。
- **修复**：在 `producer.asyncSend(message)` 之前调用 `applyInterceptorsBefore`，被中止时返回 failedFuture。

#### CR-003 延时调度器先 ZREM 再处理，处理失败消息丢失
- **来源**：r4-C3
- **文件**：`DelayMessageScheduler.java:142-185`
- **问题**：先 `zset.remove(msgId)` 后读取 payload + XADD。任何中间步骤失败，msgId 已从 ZSet 移除，消息永久丢失。
- **修复**：catch 块中将 msgId 重新写回 ZSet（score=当前时间，立即重试）。

#### CR-004 重试调度器同 CR-003 模式的消息丢失风险
- **来源**：r4-C4
- **文件**：`RetryScheduler.java:172-178, 181-231`
- **问题**：与 CR-003 完全相同的先 ZREM 再处理模式。
- **修复**：同 CR-003，catch 块中回补 msgId 到 ZSet。

#### CR-005 RetryPolicy 返回 null 时直接 ACK 丢消息
- **来源**：r4-C6
- **文件**：`DefaultStreamMqListenerContainer.java:390-397`
- **问题**：`retryPolicy.nextRetryDelay()` 返回 null 时直接 ACK 丢弃消息，不进 DLQ。
- **修复**：null delay 时路由到 DLQ（写入 DLQ Stream + ACK）而非直接丢弃。

#### CR-006 批量发送占位 MessageId，破坏幂等
- **来源**：r4-C5
- **文件**：`RedissonStreamProducer.java:191-200`
- **问题**：`syncSendBatch` 中所有消息使用相同的占位 ID `now + "-0"`，业务基于 messageId 幂等失效。
- **修复**：收集 `addAsync` 返回的 `RFuture<StreamMessageId>`，execute 后获取真实 ID；无法获取时至少为每条消息生成唯一 ID。

#### CR-007 @EnableStreamMq 缺失 @Import，无法触发自动装配
- **来源**：r3-M11
- **文件**：`EnableStreamMq.java`
- **问题**：文档声明 `@Import(StreamMqAutoConfiguration.class)`，代码缺失。用户使用 `@EnableStreamMq` 无法触发自动装配。
- **修复**：补充 `@Import(StreamMqAutoConfiguration.class)` 元注解。

#### CR-008 MessageBuilder 缺失 with* 前缀方法，文档示例不可编译
- **来源**：r3-C1
- **文件**：`MessageBuilder.java`
- **问题**：文档所有示例使用 `MessageBuilder.withTopic(...).withTag(...).build()`，代码实例方法无 with 前缀。
- **修复**：为每个实例方法添加 with* 别名（委托到现有方法）。

#### CR-009 @StreamMqOrderlyListener 缺失 selectorExpression 与 serializer 属性
- **来源**：r3-C4
- **文件**：`StreamMqOrderlyListener.java`
- **问题**：与 @StreamMqListener 能力不对等，无法做 tag 过滤和序列化器覆盖。
- **修复**：补充 `selectorExpression` 和 `serializer` 属性。

#### CR-010 事务消息 executeInTransaction 未接入半消息流程
- **来源**：r2-C2, r4-M7
- **文件**：`DefaultStreamMqTemplate.java:217-276`
- **问题**：直接 syncSend 到目标 Stream，未调用 `TransactionScanner.registerHalfMessage`。ROLLBACK 时消息已入业务 Stream 无法回收，回查机制永不触发。
- **修复**：接入半消息流程（发送半消息 → 写 txstate/txcheck → 执行本地事务 → markCommit/markRollback）。

### P1 - 高优先级（影响健壮性）

#### CR-011 CGLIB 代理 Bean 注解无法识别
- **来源**：r4-M4
- **文件**：`StreamMqListenerRegistrar.java:94, 129, 171`
- **问题**：`bean.getClass().getAnnotation()` 无法穿透 CGLIB 代理。带 `@Transactional` 的 Listener 不会被注册。
- **修复**：使用 `AnnotationUtils.findAnnotation(bean.getClass(), ...)`。

#### CR-012 StreamMqBrokerException errorCode 类型与构造器不一致
- **来源**：r3-C5
- **文件**：`StreamMqBrokerException.java`
- **问题**：文档 `int errorCode`，代码 `String errorCode`；构造器参数顺序颠倒。
- **修复**：以代码 String 为准（适配 Redis 错误码），修订文档。

#### CR-013 ConsumerContext.messageTrack() 返回类型不一致
- **来源**：r3-C6
- **文件**：`ConsumerContext.java:74`
- **问题**：文档 `String`，代码 `Map<String,String>`。
- **修复**：以代码 Map 为准，修订文档。

#### CR-014 consumeTimeout 类型与单位不一致
- **来源**：r3-C3
- **文件**：`StreamMqListener.java:102`、`StreamMqOrderlyListener.java`
- **问题**：文档 `long 30000L`（ms），代码 `int 15`（分钟）。
- **修复**：统一为 `long` + 毫秒，与配置项一致。

#### CR-015 调度器 stop→start 不支持重启
- **来源**：r4-M3
- **文件**：`RetryScheduler.java`、`DelayMessageScheduler.java`、`TransactionScanner.java`
- **问题**：`stop()` 中 `scanExecutor.shutdown()` 后无法再 schedule。
- **修复**：stop() 中取消 scheduled future 而非 shutdown executor，或在 start() 中重建 executor。

---

## 三、Major 问题清单（按类别归并）

### 3.1 代码质量类

| ID | 问题 | 文件 | 修复建议 |
|----|------|------|----------|
| MJ-01 | InterruptedException 被吞没 | RedissonStreamConsumer.java:99-104, 181-196 | catch 块中恢复中断状态 |
| MJ-02 | ensureGroup 双重检查竞态 | RedissonStreamConsumer.java:217-241 | 改用 synchronized 或 CountDownLatch |
| MJ-03 | consumeLoop 中 createConsumerFor 在 try 块外 | DefaultStreamMqListenerContainer.java:261-295 | 移入 try 块，catch 中记录 ERROR |
| MJ-04 | syncSend timeoutMillis 未真正传递给 Redisson | RedissonStreamProducer.java:94-125 | 改用 asyncSend + get(timeout) |
| MJ-05 | SchedulerLifecycle 部分启动失败仍设 running=true | StreamMqSchedulerLifecycle.java:43-56 | 记录失败数，全部失败时不设 running |
| MJ-06 | handleReconsumeLater 三步非原子 | DefaultStreamMqListenerContainer.java:379-428 | 文档说明 at-least-once 限制 |
| MJ-07 | registerHalfMessage 三步非原子 | TransactionScanner.java:158-187 | 调整顺序：先状态后半消息 |
| MJ-08 | setProducerInterceptors 非原子 | DefaultStreamMqTemplate.java:295-302 | synchronized 块内操作 |

### 3.2 API 契约类

| ID | 问题 | 文件 | 修复建议 |
|----|------|------|----------|
| MJ-09 | @StreamMqTransactionListener.checkTimeout 类型/单位 | StreamMqTransactionListener.java:50 | 统一 long + ms |
| MJ-10 | serializer 属性类型弱化为 Class<?> | StreamMqListener.java:117, StreamMqProducer.java:60 | 恢复 `Class<? extends MessageSerializer<?>>` |
| MJ-11 | acknowledge() AUTO 模式行为不一致 | ConsumerContext.java:92-96 | 统一为抛 IllegalStateException |
| MJ-12 | RetryPolicy reconsumeTimes 语义 off-by-one | RetryPolicy.java:27 | 统一为"首次为 0" |
| MJ-13 | 异常构造器签名不一致（3个异常） | ProducerTimeoutException/TransactionException/ConsumerInterruptedException | 以代码为准，文档补字段 |
| MJ-14 | RebalanceStrategy.assign 签名全变 | RebalanceStrategy.java:30 | 以代码 Integer/String 为准 |
| MJ-15 | BatchMessage 缺配置字段与 FailStrategy 枚举 | BatchMessage.java | 配置下沉到 ProducerProperties，文档说明 |
| MJ-16 | @StreamMqListener @Target 范围收窄 | StreamMqListener.java:36 | 以代码 TYPE-only 为准 |

### 3.3 架构实现类

| ID | 问题 | 修复建议 |
|----|------|----------|
| MJ-17 | ContainerState 缺 ERROR 状态 | 补充 ERROR 状态或文档删除 |
| MJ-18 | 调度线程池模型不一致（3独立池 vs 1共享池） | 更新文档反映分散池设计 |
| MJ-19 | DelayLevel 枚举级别不一致（18级 vs 9级） | 以代码 18 级为准，文档修正 |
| MJ-20 | 重试 payload 复用 delayPayloadHash Key | 新增 retryPayloadHash Key |
| MJ-21 | 未实现背压 inflightQueue | 文档标注 v1.0 不实现 |
| MJ-22 | 未实现 Micrometer 指标埋点 | 文档标注 v1.0 不实现 |
| MJ-23 | 未实现 MDC 日志追踪 | 文档标注 v1.0 不实现 |
| MJ-24 | asyncSend 拦截器调用顺序（同 CR-002） | 见 CR-002 |
| MJ-25 | ListenerRegistration 字段与设计不符 | 更新文档类图 |
| MJ-26 | DefaultStreamMqTemplate 缺少设计字段 | 更新文档 |

---

## 四、架构缺失项（文档已设计但代码未实现）

| # | 设计点 | 优先级 | 处理建议 |
|---|--------|--------|----------|
| 1 | ConsumerGroupManager（Rebalance/心跳/RSemaphore/RTopic） | P2 | 文档"非目标"标注 v1.0 不支持 |
| 2 | 顺序消费 shard 级 RLock | P1 | 文档标注 v1.0 不支持或实现 |
| 3 | RetryScheduler Lua 原子转移 | P2 | 更新决策 D5 反映 Java 端实现 |
| 4 | 背压 inflightQueue | P3 | 文档标注 v1.0 不实现 |
| 5 | Micrometer 指标埋点（12+ 指标） | P2 | 文档标注 v1.0 不实现 |
| 6 | MDC 日志追踪 | P2 | 文档标注 v1.0 不实现 |
| 7 | ManagementAuthenticator/DenyAllAuthenticator | P3 | 文档标注 v1.0 不实现 |
| 8 | ListenerContainer ERROR 状态 + reset() | P3 | 文档删除或代码补充 |
| 9 | 消费超时 cancel | P2 | 文档标注 v1.0 不实现 |
| 10 | delayDeliveredCounter/retryTransferLock Key 使用 | P3 | 删除未用 Key 或实现调用方 |

---

## 五、文档不一致核心项（需文档修订）

### 5.1 需统一为代码实现的文档项

| # | 文档描述 | 代码实现 | 修订方向 |
|---|----------|----------|----------|
| 1 | 包名 `io.streammq.*` | `io.github.streammq.*` | 文档统一为 `io.github.streammq.*` |
| 2 | Listener 接口 ConsumeContext | ConsumerContext | 文档统一为 ConsumerContext |
| 3 | TransactionChecker 三种签名 | `TransactionChecker<T>` 泛型 + 2 参数 | 文档统一为代码版本 |
| 4 | executeInTransaction 签名 | `executeInTransaction(Message, TransactionCallback)` | 文档统一 |
| 5 | LocalTransactionState vs TransactionStatus | LocalTransactionState | 文档统一 |
| 6 | Action.DEFER | 代码无 DEFER | 文档删除 DEFER |
| 7 | SendStatus.FAIL | SEND_FAILED | 文档统一 |
| 8 | AckMode vs AcknowledgeMode | AcknowledgeMode | 文档统一 |
| 9 | DelayLevel 9 级 | 18 级 | 文档统一为 18 级 |
| 10 | Redis Key 三套格式 | `streammq:{ns}:{type}:{...}` | 文档统一 |
| 11 | SPI 默认实现类名冲突 | 以代码为准 | 文档统一 |
| 12 | StreamMqProducerFactory 签名 | `createProducer(Properties)` | 文档统一 |
| 13 | @EnableStreamMq.redissonRef | 代码无 | 文档删除或代码补充 |
| 14 | MessageConverter byte[] 契约 | `Map<String,String>` Stream Fields | 文档统一为代码版本 |
| 15 | MessageId 类型 String | MessageId 类 | 文档统一 |

### 5.2 配置项默认值冲突

| 配置项 | PRD/02 | 03 | 04 | 代码 | 统一值 |
|--------|--------|-----|-----|------|--------|
| transaction.check-interval | 60s | 10s | 60s | 60s | **60s** |
| transaction.transaction-timeout | 6s | 60s | - | - | **60s** |
| batch.fail-strategy | PARTIAL_SUCCESS | ALL_OR_NOTHING | PARTIAL_SUCCESS | - | **ALL_OR_NOTHING** |
| consume-timeout | - | 30s | 60s | 15分钟 | **30s** |
| block-millis | 5s | - | 2s | - | **2s** |
| producer.retry-times | 2 | 2 | 3 | - | **2** |
| compress-threshold | 4096 | 0 | - | - | **0** |
| RetryPolicy 默认 | ExponentialBackoff | FixedArray | - | FixedArray | **FixedArray** |

---

## 六、修复计划

### 阶段一：P0 代码修复（本轮执行）

| ID | 修复内容 | 文件 |
|----|----------|------|
| F-01 | 修复 start() 竞态 | DefaultStreamMqListenerContainer.java |
| F-02 | 修复 asyncSend 拦截器顺序 | DefaultStreamMqTemplate.java |
| F-03 | 修复延时调度器消息丢失（catch 回补 ZSet） | DelayMessageScheduler.java |
| F-04 | 修复重试调度器消息丢失（catch 回补 ZSet） | RetryScheduler.java |
| F-05 | 修复 RetryPolicy null delay 丢消息（路由 DLQ） | DefaultStreamMqListenerContainer.java |
| F-06 | 修复批量发送占位 ID | RedissonStreamProducer.java |
| F-07 | 补充 @EnableStreamMq @Import | EnableStreamMq.java |
| F-08 | 补充 MessageBuilder with* 别名方法 | MessageBuilder.java |
| F-09 | 补充 @StreamMqOrderlyListener 缺失属性 | StreamMqOrderlyListener.java |
| F-10 | 修复 CGLIB 代理注解识别 | StreamMqListenerRegistrar.java |

### 阶段二：P1 代码修复（本轮执行）

| ID | 修复内容 | 文件 |
|----|----------|------|
| F-11 | 修复 InterruptedException 吞没 | RedissonStreamConsumer.java |
| F-12 | 修复 consumeLoop createConsumerFor 位置 | DefaultStreamMqListenerContainer.java |
| F-13 | 修复 SchedulerLifecycle 部分启动失败 | StreamMqSchedulerLifecycle.java |
| F-14 | 修复 registerHalfMessage 顺序 | TransactionScanner.java |
| F-15 | 修复 consumeTimeout 类型/单位 | StreamMqListener.java, StreamMqOrderlyListener.java |
| F-16 | 修复 checkTimeout 类型/单位 | StreamMqTransactionListener.java |
| F-17 | 恢复 serializer 属性类型约束 | StreamMqListener.java, StreamMqProducer.java |
| F-18 | 修复 acknowledge() AUTO 模式行为 | ConsumerContext.java |
| F-19 | 调度器支持重启 | RetryScheduler/DelayMessageScheduler/TransactionScanner |

### 阶段三：文档修订（后续迭代）

- 统一 4 份文档的术语表、API 签名、配置项、Redis Key 格式
- 标注 v1.0 非目标项（Rebalance/顺序消费 RLock/指标/MDC）
- 修订所有与代码不一致的示例代码

---

## 七、积极发现

1. **模块分层清晰**：core/adapter/starter 三层分离，SPI 抽象合理
2. **虚拟线程使用**：消费端使用 `newVirtualThreadPerTaskExecutor`，适合 I/O 密集场景
3. **SLF4J 日志规范**：全代码库统一使用占位符，异常日志正确传递 stack trace
4. **Objects.requireNonNull 广泛使用**：构造器 fail-fast
5. **ExecutorService 关闭模式标准**：shutdown + awaitTermination + shutdownNow
6. **StreamMqKeys 集中管理**：Redis Key 拼接逻辑统一，namespace 处理一致
7. **Spring Boot 条件装配规范**：@ConditionalOnXxx 三件套使用正确
8. **SmartLifecycle 相位设计**：调度器先于 Listener 容器启动，停止反向
9. **守护线程设置**：调度线程设为 daemon，不阻塞 JVM 退出
10. **try-with-resources**：JdkSerializer 正确管理流资源

---

## 八、结论

当前代码实现覆盖了设计的核心骨架，但存在 **10 项 P0 级代码缺陷**（消息丢失/功能错误/契约不兼容）和 **9 项 P1 级健壮性问题**，需在进入生产测试前修复。架构层面的缺失（Rebalance/顺序消费 RLock/可观测性）建议在文档中明确标注为 v1.0 非目标，后续版本迭代实现。

文档与代码之间存在大量签名级不一致，建议以代码实现为事实基准（多数变更为增强），反向修订 4 份设计文档，建立"代码即文档"的同步机制。
