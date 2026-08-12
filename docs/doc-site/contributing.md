# 贡献指南

> 欢迎为 StreamMQ 贡献代码！本文档对标顶级开源社区（如 Spring、Redisson、Kafka）的贡献流程，旨在帮助贡献者快速上手并保持代码质量的一致性。
> StreamMQ 是一个以 MIT 协议开源的项目，任何形式的贡献（代码、文档、Issue、建议）都受欢迎。

---

## 目录

- [行为准则](#行为准则)
- [如何贡献](#如何贡献)
- [开发环境搭建](#开发环境搭建)
- [Fork 与分支工作流](#fork-与分支工作流)
- [代码风格规范](#代码风格规范)
- [Conventional Commits 规范](#conventional-commits-规范)
- [测试要求](#测试要求)
- [PR 审查流程](#pr-审查流程)
- [CI/CD 流水线](#cicd-流水线)
- [模块开发规范](#模块开发规范)
- [文档规范](#文档规范)
- [异常处理规范](#异常处理规范)
- [日志规范](#日志规范)
- [发布流程](#发布流程)
- [社区渠道](#社区渠道)
- [贡献者致谢](#贡献者致谢)

---

## 行为准则

StreamMQ 遵循开源社区通用的行为准则，所有参与者需同意：

- **友善包容**：尊重不同背景、经验、观点的贡献者
- **专业客观**：技术讨论对事不对人，避免人身攻击
- **协作共赢**：主动帮助新人，乐于分享知识
- **拒绝骚扰**：零容忍任何形式的骚扰、歧视或贬低行为
- **聚焦技术**：避免政治、宗教等与技术无关的话题

违反准则的行为请通过 GitHub 私信联系维护者，维护者保留处置违规行为的权利（包括但不限于警告、隐藏评论、封禁）。

完整行为准则参考 [Contributor Covenant 2.1](https://www.contributor-covenant.org/version/2/1/code_of_conduct/)。

---

## 如何贡献

StreamMQ 欢迎以下形式的贡献：

### 1. 提交 Issue

通过 [GitHub Issues](https://github.com/streammq/streammq/issues) 提交：

| 类型         | 模板                  | 必要信息                                            |
| ------------ | --------------------- | --------------------------------------------------- |
| Bug 报告     | `bug_report.md`       | 复现步骤、预期行为、实际行为、版本、日志            |
| 功能请求     | `feature_request.md`  | 使用场景、期望行为、替代方案、影响范围              |
| 改进建议     | -                     | 改进方向、收益评估、潜在风险                        |
| 文档问题     | -                     | 文档链接、问题描述、建议修改                        |

**Issue 标题建议格式：**
- `[Bug] 消费者并发场景下偶发 NPE`
- `[Feature] 支持 LZ4 压缩算法`
- `[Improvement] 优化 Producer 拦截器链性能`

### 2. 参与讨论

在 [GitHub Discussions](https://github.com/streammq/streammq/discussions) 中：
- 分享使用经验
- 讨论架构设计
- 提出疑问或建议
- 帮助其他用户解决问题

### 3. 提交 Pull Request

欢迎提交 PR 修复 Bug 或实现新功能。请务必：
- 关联相关 Issue
- 遵循代码风格规范
- 提供充分的测试用例
- 更新相关文档

### 4. 改进文档

文档与代码同等重要。如发现文档错误、过时或不清晰，欢迎提交 PR 改进。

---

## 开发环境搭建

### 基础环境

| 工具       | 版本     | 用途                          |
| ---------- | -------- | ----------------------------- |
| JDK        | 21+      | 编译与运行                    |
| Maven      | 3.9+     | 构建工具                      |
| Docker     | 24.0+    | 运行集成测试的嵌入式 Redis    |
| Git        | 2.30+    | 版本控制                      |
| IDE        | IntelliJ IDEA 2024.1+ 或 VS Code | 开发环境 |

### IDE 配置（IntelliJ IDEA）

1. **安装插件**
   - Lombok Plugin
   - Google Java Format（可选，已通过 Spotless 集成）
   - Checkstyle（可选）

2. **配置 JDK 21**
   - `File → Project Structure → Project SDK` 选择 JDK 21
   - `File → Project Structure → Project Language Level` 选择 `21`

3. **导入项目**
   - `File → Open` 选择 StreamMQ 根目录
   - 等待 Maven 自动导入依赖

4. **启用注解处理**
   - `Settings → Build, Execution, Deployment → Compiler → Annotation Processors`
   - 勾选 `Enable annotation processing`

5. **配置 Spotless 自动格式化**
   - 在 IDE 中安装 `File Watchers` 插件
   - 添加 Watcher 在保存时执行 `mvn spotless:apply`

### Redis 测试环境

StreamMQ 的集成测试依赖 Testcontainers 自动启动 Redis 容器，无需手动准备。但本地开发调试时建议运行一个独立的 Redis 实例：

```bash
# 通过 Docker 启动本地 Redis
docker run -d --name streammq-redis -p 6379:6379 \
  redis:7.2-alpine \
  redis-server --appendonly yes
```

### 克隆与编译

```bash
# Fork 仓库后克隆到本地
git clone https://github.com/<your-username>/streammq.git
cd streammq

# 添加上游仓库
git remote add upstream https://github.com/streammq/streammq.git

# 编译项目
mvn clean compile

# 运行所有测试（需要 Docker 运行）
mvn clean test
```

### 验证环境

```bash
# 验证 Java 版本
java -version
# 输出应包含 "version \"21"

# 验证 Maven 版本
mvn -v
# 输出应包含 Apache Maven 3.9+

# 验证 Docker
docker info
# 输出 Docker 服务信息

# 验证项目编译
mvn clean package -DskipTests
# 输出 BUILD SUCCESS
```

---

## Fork 与分支工作流

StreamMQ 采用 Fork + Branch 工作流，与主流开源项目一致。

### 1. Fork 仓库

在 GitHub 上 Fork [streammq/streammq](https://github.com/streammq/streammq) 到你的个人账户。

### 2. 克隆到本地

```bash
git clone https://github.com/<your-username>/streammq.git
cd streammq

# 添加上游仓库
git remote add upstream https://github.com/streammq/streammq.git

# 禁用直接推送到 upstream
git remote set-url --push upstream no_push
```

### 3. 同步上游变更

每次开始新工作前，先同步上游最新代码：

```bash
git fetch upstream
git checkout main
git rebase upstream/main
```

### 4. 创建特性分支

**分支命名规范：**

| 类型      | 命名格式                | 示例                                   |
| --------- | ----------------------- | -------------------------------------- |
| 新功能    | `feature/<描述>`        | `feature/support-lz4-compression`      |
| Bug 修复  | `fix/<描述>`            | `fix/consumer-npe-on-shutdown`         |
| 文档      | `docs/<描述>`           | `docs/update-deploy-guide`             |
| 重构      | `refactor/<描述>`       | `refactor/simplify-retry-policy`       |
| 测试      | `test/<描述>`           | `test/add-transaction-it`              |
| 性能优化  | `perf/<描述>`           | `perf/optimize-batch-send`             |

```bash
# 创建并切换到新分支
git checkout -b feature/support-lz4-compression

# 开发过程中定期提交
git add <files>
git commit -m "feat: add LZ4 compression codec"
```

### 5. 开发与测试

```bash
# 开发过程中持续编译
mvn -B -DskipTests compile

# 提交前运行代码格式化
mvn spotless:apply

# 运行相关模块测试
mvn test -pl streammq-core
mvn test -pl streammq-redisson

# 运行全部测试
mvn clean verify
```

### 6. 推送与发起 PR

```bash
# 推送到你的 Fork
git push origin feature/support-lz4-compression

# 在 GitHub 上发起 Pull Request
# 目标分支：streammq/streammq:main
```

**PR 标题格式遵循 Conventional Commits：**

```
feat: add LZ4 compression codec support
```

**PR 描述模板：**

```markdown
## 变更说明
<!-- 简要描述本次变更的目的与内容 -->

## 关联 Issue
Closes #123

## 变更类型
- [ ] 新功能（feature）
- [ ] Bug 修复（fix）
- [ ] 重构（refactor）
- [ ] 文档（docs）
- [ ] 测试（test）
- [ ] 性能优化（perf）
- [ ] 构建/CI（ci/chore）

## 测试
- [ ] 已添加/更新单元测试
- [ ] 已添加/更新集成测试
- [ ] 本地 `mvn clean verify` 通过

## 检查清单
- [ ] 代码符合 [Google Java Style](https://google.github.io/styleguide/javaguide.html)
- [ ] 提交信息遵循 [Conventional Commits](https://www.conventionalcommits.org/)
- [ ] 公共 API 已添加 Javadoc
- [ ] 无新增编译警告
- [ ] 已更新相关文档
```

### 7. 处理 Review 反馈

```bash
# 在同一分支上提交修改
git add <files>
git commit -m "fix: address review feedback"

# 推送（不要 force push，除非维护者要求）
git push origin feature/support-lz4-compression
```

### 8. 合并与清理

PR 合并后：

```bash
# 切回 main 并同步
git checkout main
git fetch upstream
git rebase upstream/main
git push origin main

# 删除本地与远程分支
git branch -d feature/support-lz4-compression
git push origin --delete feature/support-lz4-compression
```

---

## 代码风格规范

StreamMQ 严格遵循 [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html)，并通过 Spotless 插件自动强制执行。

### 1. 通用规则

| 规则                    | 说明                                                  |
| ----------------------- | ----------------------------------------------------- |
| 缩进                    | 4 个空格，禁止 Tab                                    |
| 行宽                    | 120 字符                                              |
| 文件编码                | UTF-8                                                 |
| 换行符                  | Unix（LF）                                            |
| 文件末尾                | 保留一个空行                                          |
| Import                  | 按 `java.*` / `javax.*` / `org.*` / `com.*` 分组，禁止通配符 import |
| 命名                    | 类名 PascalCase，方法/变量 camelCase，常量 UPPER_SNAKE_CASE |

### 2. JDK 21 特性使用

鼓励使用 JDK 21 新特性，但需注意可读性：

| 特性                | 使用建议                                              |
| ------------------- | ----------------------------------------------------- |
| `record`            | 不可变 DTO 推荐                                       |
| `sealed`            | 有限的子类型层次结构                                  |
| Pattern Matching    | 简化 instanceof、switch 表达式                        |
| Virtual Threads     | IO 密集型并发场景                                     |
| Text Blocks         | 多行字符串                                            |
| Switch Expressions  | 简化多分支逻辑                                        |

### 3. Lombok 使用规范

StreamMQ 使用 Lombok 简化样板代码，使用规范：

| 注解                 | 推荐场景                            | 备注                                  |
| -------------------- | ----------------------------------- | ------------------------------------- |
| `@Getter` / `@Setter`| POJO / DTO                          | 避免 `@Data`，按需选择                |
| `@Builder`           | 复杂对象构建                        | 配合 `@Builder.Default`               |
| `@RequiredArgsConstructor` | 依赖注入（final 字段）       | Spring Bean 推荐                      |
| `@Slf4j`             | 日志                                | 统一使用 SLF4J                        |
| `@NonNull`           | 参数 / 返回值非空标注               | 配合 Objects.requireNonNull           |
| `@Value`             | 不可变 DTO                          | 等价于 `@Getter` + `@AllArgsConstructor` + `final` |

**避免使用：**
- `@Data`：会生成 `equals/hashCode`，可能引起性能问题或循环引用
- `@ToString`：可能泄漏敏感信息，需谨慎
- `@EqualsAndHashCode`：需明确调用 super 字段

### 4. Null 检查规范

**统一使用 JDK 与 Apache Commons 工具类，禁止 `null == x` 这种倒置写法。**

```java
// 推荐
import java.util.Objects;

if (Objects.isNull(value)) { ... }
if (Objects.nonNull(value)) { ... }

// 推荐（字符串）
import org.apache.commons.lang3.StringUtils;

if (StringUtils.isEmpty(str)) { ... }
if (StringUtils.isNotEmpty(str)) { ... }
if (StringUtils.isBlank(str)) { ... }
if (StringUtils.isNotBlank(str)) { ... }

// 推荐（集合）
import org.apache.commons.collections4.CollectionUtils;

if (CollectionUtils.isEmpty(list)) { ... }
if (CollectionUtils.isNotEmpty(list)) { ... }

// 不推荐
if (value == null) { ... }             // 倒置写法被允许但不统一
if (str == null || str.isEmpty()) { ... }  // 重复造轮子
if (list == null || list.isEmpty()) { ... }
```

StreamMQ 内部提供了 `io.github.streammq.core.util.StringUtils` 与 `io.github.streammq.core.util.CollectionUtils` 工具类，优先使用项目内工具类。

### 5. 代码示例

**良好的代码示例：**

```java
@Slf4j
@Builder
@RequiredArgsConstructor
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {

    private final OrderService orderService;

    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        if (Objects.isNull(message) || StringUtils.isEmpty(message.getBody())) {
            log.warn("Received empty message, skip");
            return ConsumeAction.SUCCESS;
        }

        try {
            orderService.process(message.getBody());
            log.debug("Order processed: keys={}", message.getKeys());
            return ConsumeAction.SUCCESS;
        } catch (BusinessException e) {
            log.error("Failed to process order: keys={}", message.getKeys(), e);
            return ConsumeAction.RECONSUME_LATER;
        }
    }
}
```

### 6. Spotless 自动格式化

提交前必须运行：

```bash
# 自动格式化代码
mvn spotless:apply

# 检查代码格式（CI 会执行）
mvn spotless:check
```

---

## Conventional Commits 规范

StreamMQ 严格遵循 [Conventional Commits 1.0.0](https://www.conventionalcommits.org/) 规范，提交信息格式如下：

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### Type 清单

| Type       | 用途                                  | 示例                                              |
| ---------- | ------------------------------------- | ------------------------------------------------- |
| `feat`     | 新功能                                | `feat: add LZ4 compression codec`                 |
| `fix`      | Bug 修复                              | `fix: resolve NPE on consumer shutdown`           |
| `docs`     | 文档变更                              | `docs: update deployment guide`                   |
| `style`    | 代码风格变更（不影响功能）            | `style: reformat MessageBuilder`                  |
| `refactor` | 重构（既不是新功能也不是 Bug 修复）   | `refactor: simplify retry policy`                 |
| `perf`     | 性能优化                              | `perf: optimize batch send pipeline`              |
| `test`     | 测试相关                              | `test: add integration test for transaction`      |
| `build`    | 构建系统或依赖变更                    | `build: bump Spring Boot to 3.3.5`                |
| `ci`       | CI 配置变更                           | `ci: add JDK 21 to GitHub Actions matrix`         |
| `chore`    | 杂项（不修改源码也不更新文档）        | `chore: update .gitignore`                        |
| `revert`   | 回滚之前的提交                        | `revert: feat: add LZ4 compression codec`         |

### Scope 清单

为提升提交信息的可读性，建议添加 scope：

| Scope            | 涵盖模块/领域                          |
| ---------------- | -------------------------------------- |
| `core`           | streammq-core                          |
| `redisson`       | streammq-redisson                      |
| `starter`        | streammq-spring-boot-starter           |
| `bom`            | streammq-bom                           |
| `test`           | streammq-test                          |
| `samples`        | streammq-samples                       |
| `consumer`       | 消费者相关                             |
| `producer`       | 生产者相关                             |
| `transaction`    | 事务消息                               |
| `dlq`            | 死信队列                               |
| `trace`          | 链路追踪                               |
| `metrics`        | 指标监控                               |
| `security`       | 安全相关                               |
| `deps`           | 依赖升级                               |

### 提交示例

```bash
# 新功能
git commit -m "feat(redisson): support Redis Cluster mode"

# Bug 修复
git commit -m "fix(consumer): resolve message loss on rebalance

The rebalance strategy incorrectly cleared pending entries when
a new consumer joined the group. This commit ensures PEL entries
are preserved during rebalance.

Closes #123"

# 文档
git commit -m "docs: add K8s deployment guide"

# 重构
git commit -m "refactor(core): extract message builder to dedicated class"

# 性能优化
git commit -m "perf(producer): use pipeline for batch send"

# 测试
git commit -m "test(transaction): add integration test for checkback"

# 依赖升级
git commit -m "build(deps): bump Redisson to 3.34.1"

# Breaking Change（重大变更）
git commit -m "feat(core)!: change MessageBuilder API to fluent

BREAKING CHANGE: MessageBuilder now uses fluent API, existing
code needs to be updated.

Migration guide:
- Replace `new MessageBuilder<>()` with `MessageBuilder.withTopic()`
- Replace `builder.setBody()` with `builder.body()`"
```

### 提交粒度

- **小步提交**：每个 commit 应聚焦单一变更，便于 review 与 revert
- **可独立编译**：每个 commit 都应保证 `mvn compile` 通过
- **测试通过**：每个 commit 都应保证相关测试通过
- **不混合**：避免在同一个 commit 中混合多个无关变更

---

## 测试要求

StreamMQ 当前包含 965 个测试用例，所有 PR 必须满足测试要求。

### 1. 测试分类

| 测试类型     | 命名规则         | 位置                  | 框架                                | 运行命令                          |
| ------------ | ---------------- | --------------------- | ----------------------------------- | --------------------------------- |
| 单元测试     | `*Test.java`     | `src/test/java`       | JUnit 5 + Mockito + AssertJ         | `mvn test`                        |
| 集成测试     | `*IT.java`       | `src/test/java`       | JUnit 5 + Testcontainers + Awaitility | `mvn verify`                    |

### 2. 单元测试规范

**目录结构：** 与源码包结构对应

```
streammq-core/src/
├── main/java/io/github/streammq/core/message/MessageBuilder.java
└── test/java/io/github/streammq/core/message/MessageBuilderTest.java
```

**命名规范：**
- 测试类：`<被测类>Test`，如 `MessageBuilderTest`
- 测试方法：`should_<期望行为>_when_<前置条件>`，或简洁的 `<场景>_<期望>`

**示例：**

```java
@DisplayName("MessageBuilder 单元测试")
class MessageBuilderTest {

    @Test
    @DisplayName("构建包含所有字段的消息")
    void shouldBuildCompleteMessage_whenAllFieldsProvided() {
        Message<String> message = MessageBuilder.<String>withTopic("order-topic")
                .tag("created")
                .keys("order-123")
                .shardingKey("user-456")
                .body("{\"orderId\":123}")
                .delayLevel(DelayLevel.MINUTE_1)
                .withUserProperty("traceId", "t-001")
                .build();

        assertThat(message)
            .isNotNull()
            .satisfies(msg -> {
                assertThat(msg.getTopic()).isEqualTo("order-topic");
                assertThat(msg.getTag()).isEqualTo("created");
                assertThat(msg.getKeys()).isEqualTo("order-123");
                assertThat(msg.getShardingKey()).isEqualTo("user-456");
                assertThat(msg.getBody()).isEqualTo("{\"orderId\":123}");
                assertThat(msg.getDelayLevel()).isEqualTo(DelayLevel.MINUTE_1);
                assertThat(msg.getUserProperty("traceId")).isEqualTo("t-001");
            });
    }

    @Test
    @DisplayName("未指定 topic 时应抛出异常")
    void shouldThrowException_whenTopicIsNull() {
        assertThatThrownBy(() -> MessageBuilder.<String>withTopic(null).body("test").build())
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("topic");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "  ", "\t"})
    @DisplayName("空白 topic 应抛出异常")
    void shouldThrowException_whenTopicIsBlank(String invalidTopic) {
        assertThatThrownBy(() -> MessageBuilder.<String>withTopic(invalidTopic).body("test").build())
            .isInstanceOf(IllegalArgumentException.class);
    }
}
```

### 3. 集成测试规范

集成测试需启动真实的 Redis 实例（通过 Testcontainers），用于验证与 Redis 的交互。

**示例：**

```java
@Testcontainers
@DisplayName("StreamMessageTemplate 集成测试")
class StreamMessageTemplateIT {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7.2-alpine")
            .withExposedPorts(6379);

    private StreamMessageTemplate template;

    @BeforeEach
    void setUp() {
        String address = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
        // 初始化 template ...
    }

    @Test
    @DisplayName("同步发送消息应成功")
    void shouldSendSuccessfully_whenSyncSend() {
        Message<String> message = MessageBuilder.<String>withTopic("test-topic")
                .body("hello")
                .build();

        SendResult result = template.syncSend(message);

        assertThat(result.getStatus()).isEqualTo(SendStatus.SUCCESS);
        assertThat(result.getMessageId()).isNotNull();
    }

    @Test
    @DisplayName("异步发送应返回 CompletableFuture")
    void shouldReturnFuture_whenAsyncSend() throws Exception {
        Message<String> message = MessageBuilder.<String>withTopic("test-topic")
                .body("hello")
                .build();

        CompletableFuture<SendResult> future = template.asyncSend(message);

        SendResult result = future.get(3, TimeUnit.SECONDS);
        assertThat(result.getStatus()).isEqualTo(SendStatus.SUCCESS);
    }
}
```

### 4. 测试覆盖率要求

StreamMQ 通过 JaCoCo 强制执行测试覆盖率要求：

| 模块                          | 行覆盖率要求 | 分支覆盖率要求 |
| ----------------------------- | ------------ | -------------- |
| `streammq-core`               | ≥ 80%        | ≥ 70%          |
| `streammq-redisson`           | ≥ 70%        | ≥ 60%          |
| `streammq-spring-boot-starter`| ≥ 70%        | ≥ 60%          |
| `streammq-test`               | ≥ 70%        | ≥ 60%          |

**查看覆盖率报告：**

```bash
mvn clean verify
# 报告位于各模块 target/site/jacoco/index.html
```

### 5. 运行测试

```bash
# 运行所有测试（需要 Docker）
mvn clean test

# 运行所有测试 + 集成测试 + 覆盖率报告
mvn clean verify

# 仅运行指定模块测试
mvn test -pl streammq-core
mvn test -pl streammq-redisson

# 运行单个测试类
mvn test -Dtest=MessageTest
mvn test -pl streammq-core -Dtest=MessageBuilderTest

# 运行单个测试方法
mvn test -Dtest=MessageTest#shouldBuildCompleteMessage

# 运行特定模式的测试（如所有集成测试）
mvn test -Dtest=*IT

# 跳过测试快速编译
mvn clean package -DskipTests

# 仅运行单元测试，跳过集成测试
mvn test -DskipITs
```

### 6. 测试编写原则

- **AAA 模式**：Arrange（准备）、Act（执行）、Assert（断言）三段分明
- **单一职责**：每个测试方法只验证一个行为
- **独立性**：测试之间不应有依赖关系
- **可读性**：使用 `@DisplayName` 描述测试意图
- **断言充分**：使用 AssertJ 的链式断言，避免 JUnit 原生断言
- **避免 Thread.sleep**：使用 Awaitility 等待异步结果
- **Mock 适度**：仅 Mock 外部依赖，避免过度 Mock 导致测试脆弱

---

## PR 审查流程

### 1. 审查流程

```
提交 PR → 自动 CI 检查 → 维护者初审 → 详细 Review → 修改 → 合并
```

### 2. CI 自动检查

PR 提交后会自动触发 CI，包含：

| 检查项                | 命令                                  | 失败处理       |
| --------------------- | ------------------------------------- | -------------- |
| 代码格式化检查        | `mvn spotless:check`                  | 运行 `spotless:apply` |
| 编译检查              | `mvn compile -Werror`                 | 修复编译错误   |
| 依赖收敛检查          | `mvn enforcer:enforce`                | 排除冲突依赖   |
| 单元测试              | `mvn test`                            | 修复失败的测试 |
| 集成测试              | `mvn verify`                          | 修复失败的测试 |
| 测试覆盖率检查        | JaCoCo 阈值检查                       | 补充测试用例   |

**所有 CI 检查必须通过才会进入人工 Review。**

### 3. Review 标准

维护者会从以下维度审查 PR：

#### 功能正确性
- 是否解决了 Issue 描述的问题
- 边界条件是否处理（null、空集合、超长字符串等）
- 异常路径是否覆盖
- 并发场景是否安全

#### 代码质量
- 是否符合 Google Java Style
- 命名是否清晰表达意图
- 是否存在重复代码
- 是否过度设计

#### 测试覆盖
- 是否包含充分的单元测试
- 是否包含必要的集成测试
- 测试是否稳定（不依赖时序、随机数等）
- 测试覆盖率是否达标

#### 性能影响
- 是否引入性能回退
- 是否避免不必要的对象创建
- 是否合理使用缓存
- 是否避免阻塞操作

#### 兼容性
- 是否破坏向后兼容
- 是否影响现有功能
- 配置项变更是否有兼容方案

#### 文档完整性
- 公共 API 是否有 Javadoc
- 是否更新了相关文档
- 是否在 README / 配置参考中说明新特性

### 4. Review 礼仪

- 维护者会在 5 个工作日内响应 PR
- Review 反馈对事不对人，请勿理解为个人批评
- 如有分歧，可在 PR 中讨论或转为 Discussions
- 重大设计变更建议先在 Discussions 达成共识

### 5. 合并策略

| 策略              | 适用场景                                |
| ----------------- | --------------------------------------- |
| Squash and Merge  | **默认**，单 commit 合并，保持历史清晰 |
| Rebase and Merge  | 多个有意义的 commit，保留细分历史       |
| Merge Commit      | 仅用于特殊场景（如长期特性分支合并）    |

---

## CI/CD 流水线

StreamMQ 通过 GitHub Actions 实现 CI/CD，配置位于 `.github/workflows/`。

### 1. CI 流水线（PR 触发）

每个 PR 提交或更新时自动触发：

```yaml
name: CI

on:
  pull_request:
    branches: [main, develop]
  push:
    branches: [main, develop]

jobs:
  build-and-test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        java: [21]
    steps:
      - uses: actions/checkout@v4
      - name: Setup JDK ${{ matrix.java }}
        uses: actions/setup-java@v4
        with:
          java-version: ${{ matrix.java }}
          distribution: temurin
          cache: maven
      - name: Setup Docker
        uses: docker/setup-buildx-action@v3
      - name: Code Format Check
        run: mvn -B spotless:check
      - name: Compile
        run: mvn -B -DskipTests compile
      - name: Dependency Convergence
        run: mvn -B enforcer:enforce
      - name: Unit Tests
        run: mvn -B test
      - name: Integration Tests
        run: mvn -B verify -DskipUTs
      - name: Coverage Report
        run: mvn -B jacoco:report
      - name: Upload Coverage
        uses: codecov/codecov-action@v4
```

### 2. CD 流水线（Release 触发）

发布版本时触发：

```yaml
name: Release

on:
  push:
    tags:
      - 'v*'

jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Setup JDK 21
        uses: actions/setup-java@v4
        with:
          java-version: 21
          distribution: temurin
          cache: maven
          gpg-private-key: ${{ secrets.GPG_PRIVATE_KEY }}
          gpg-passphrase: ${{ secrets.GPG_PASSPHRASE }}
      - name: Deploy to Maven Central
        run: mvn -B clean deploy -Prelease
        env:
          OSSRH_USERNAME: ${{ secrets.OSSRH_USERNAME }}
          OSSRH_PASSWORD: ${{ secrets.OSSRH_PASSWORD }}
      - name: Create GitHub Release
        uses: softprops/action-gh-release@v2
        with:
          generate_release_notes: true
```

### 3. CI 检查项

| 阶段                  | 工具                       | 失败处理       |
| --------------------- | -------------------------- | -------------- |
| 代码格式化            | Spotless                   | 自动修复后重提 |
| 编译                  | maven-compiler-plugin      | 修复编译错误   |
| 依赖收敛              | maven-enforcer-plugin      | 排除冲突       |
| 单元测试              | maven-surefire-plugin      | 修复失败用例   |
| 集成测试              | maven-failsafe-plugin      | 修复失败用例   |
| 测试覆盖率            | JaCoCo                     | 补充测试       |
| 代码静态分析          | SpotBugs（可选）           | 修复告警       |

---

## 模块开发规范

StreamMQ 当前包含 11 个模块，新增模块需遵循以下规范。

### 1. 现有模块清单

| 模块                              | 用途                                | Maven artifactId                |
| --------------------------------- | ----------------------------------- | ------------------------------- |
| `streammq-bom`                    | BOM，统一版本管理                   | `streammq-bom`                  |
| `streammq-core`                   | 核心库，定义 API 接口与默认实现      | `streammq-core`                 |
| `streammq-redisson`               | Redisson 适配器，基于 Redis Stream  | `streammq-redisson`             |
| `streammq-spring-boot-starter`    | Spring Boot 3 Starter，自动装配    | `streammq-spring-boot-starter`  |
| `streammq-test`                   | 测试工具，提供嵌入式测试支持        | `streammq-test`                 |
| `streammq-samples`                | 示例模块集合                        | `streammq-samples`              |

### 2. 命名规范

**模块命名规则：**

| 类型      | 命名规则                    | 示例                            |
| --------- | --------------------------- | ------------------------------- |
| BOM       | `streammq-bom`              | `streammq-bom`                  |
| 核心      | `streammq-core`             | `streammq-core`                 |
| 适配器    | `streammq-<backend>`        | `streammq-redisson`             |
| Starter   | `streammq-<framework>-starter` | `streammq-spring-boot-starter` |
| 测试工具  | `streammq-test`             | `streammq-test`                 |
| 示例      | `streammq-sample-<场景>`    | `streammq-sample-quickstart`    |

**包命名规则：**

```
io.github.streammq
├── core                    # 核心库
│   ├── annotation          # 注解
│   ├── compression         # 压缩
│   ├── consumer            # 消费者
│   ├── converter           # 消息转换
│   ├── enums               # 枚举
│   ├── exception           # 异常
│   ├── filter              # 过滤器
│   ├── interceptor         # 拦截器
│   ├── listener            # 监听器
│   ├── message             # 消息
│   ├── metrics             # 指标
│   ├── policy              # 策略
│   ├── producer            # 生产者
│   ├── scheduler           # 调度器
│   ├── serializer          # 序列化
│   ├── service             # 服务
│   ├── template            # 模板
│   ├── trace               # 链路追踪
│   ├── transaction         # 事务
│   └── util                # 工具
├── adapter.redisson        # Redisson 适配器
├── spring.boot             # Spring Boot Starter
└── test                    # 测试工具
```

### 3. 现有示例模块

`streammq-samples` 下包含以下示例：

| 示例模块                       | 演示内容                          |
| ------------------------------ | --------------------------------- |
| `streammq-sample-quickstart`   | 快速入门示例                      |
| `streammq-sample-transaction`  | 事务消息示例                      |
| `streammq-sample-delay`        | 延时消息示例                      |
| `streammq-sample-orderly`      | 顺序消息示例                      |
| `streammq-sample-dlq`          | 死信队列示例                      |
| `streammq-sample-interceptor`  | 拦截器示例                        |

### 4. 新增模块流程

1. 在根 `pom.xml` 的 `<modules>` 中添加新模块
2. 创建模块目录与 `pom.xml`，继承 `streammq-parent`
3. 在 `streammq-bom` 的 `<dependencyManagement>` 中注册（如对外暴露）
4. 编写模块代码与测试
5. 更新本文档的模块清单
6. 添加模块说明到根 `README.md`

### 5. SPI 扩展点

StreamMQ 提供 12 个 SPI 扩展点，新增 SPI 需：

| 步骤                | 说明                                                   |
| ------------------- | ------------------------------------------------------ |
| 定义接口            | 在 `streammq-core` 中定义 SPI 接口                     |
| 提供默认实现        | 提供合理的默认实现                                     |
| SPI 注册            | 通过 `META-INF/services/` 或 `@AutoService` 注解注册   |
| 文档更新            | 在 `features.md` 中说明 SPI 用法                       |
| 测试覆盖            | 提供 SPI 实现的单元测试与集成测试                      |

**现有 SPI 清单：**

| SPI 接口                  | 作用                       |
| ------------------------- | -------------------------- |
| `MessageSerializer`       | 消息序列化/反序列化        |
| `MessageConverter`        | 消息体与业务对象转换       |
| `ProducerFilter`          | 生产者过滤器               |
| `ConsumerFilter`          | 消费者过滤器               |
| `ProducerInterceptor`     | 生产者拦截器               |
| `ConsumerInterceptor`     | 消费者拦截器               |
| `RetryPolicy`             | 重试策略                   |
| `RebalanceStrategy`       | 消费者重平衡策略           |
| `TraceCollector`          | Trace 上下文采集           |
| `CompressionCodec`        | 压缩编解码器               |
| `ManagementAuthenticator` | 管理接口鉴权               |
| `DlqFailureStrategy`      | DLQ 失败策略               |

---

## 文档规范

### 1. Javadoc 规范

所有 `public` 与 `protected` 的类、方法、字段必须有 Javadoc。

**类级别：**

```java
/**
 * 消息构建器，用于构建 {@link Message} 对象。
 *
 * <p>使用示例：</p>
 * <pre>{@code
 * Message<String> message = MessageBuilder.<String>withTopic("order-topic")
 *         .tag("created")
 *         .body("content")
 *         .build();
 * }</pre>
 *
 * @param <T> 消息体类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 * @see Message
 */
public class MessageBuilder<T> {
    // ...
}
```

**方法级别：**

```java
/**
 * 同步发送消息。
 *
 * <p>此方法会阻塞直到收到 Redis 响应或超时。</p>
 *
 * @param message 消息对象，不能为 null
 * @return 发送结果
 * @throws StreamMQClientException 当消息参数非法时
 * @throws StreamMQBrokerException 当发送失败且重试耗尽时
 * @throws ProducerTimeoutException 当发送超时时
 * @since 0.1.0
 * @see #asyncSend(Message)
 */
public <T> SendResult syncSend(Message<T> message) {
    // ...
}
```

**Javadoc 要求：**
- 第一行简述功能（以 `.` 结尾）
- 详细说明使用 `<p>` 分段
- 代码示例使用 `<pre>{@code ... }</pre>`
- 参数说明使用 `@param`，包含含义与约束
- 异常说明使用 `@throws`，包含触发条件
- 使用 `@see` 关联相关类/方法
- 使用 `@since` 标注引入版本

### 2. Markdown 文档规范

**文件位置：** 所有 Markdown 文档位于 `docs/doc-site/`。

**格式规范：**
- 标题层级：`#` 文档标题，`##` 章节，`###` 子章节，最多 4 级
- 代码块：标注语言（` ```java`, ` ```yaml`, ` ```bash`）
- 表格：使用标准 Markdown 表格，对齐列
- 链接：相对路径用于站内跳转，绝对路径用于外部链接
- 中文与英文/数字之间留一个空格（如 "使用 JDK 21"）

**文档更新时机：**
- 新增功能：更新 `features.md` 与 `api.md`
- 配置变更：更新 `configuration.md`
- 行为变更：更新 `faq.md`
- 部署变更：更新 `deploy.md`

---

## 异常处理规范

### 1. 自定义异常体系

StreamMQ 已定义完整的异常体系，新增异常需继承现有基类：

```
StreamMQException (基类)
├── StreamMQClientException    # 客户端异常（参数错误、配置错误）
├── StreamMQBrokerException    # Broker 异常（Redis 操作失败）
├── SerializationException     # 序列化异常
├── ProducerTimeoutException   # 生产者超时
├── TransactionException       # 事务异常
└── ConsumerInterruptedException # 消费者中断异常
```

**新增异常示例：**

```java
/**
 * 当压缩算法不支持时抛出。
 *
 * @since 0.1.0
 */
public class UnsupportedCompressionException extends StreamMQClientException {

    public UnsupportedCompressionException(String algorithm) {
        super("Unsupported compression algorithm: " + algorithm);
    }

    public UnsupportedCompressionException(String algorithm, Throwable cause) {
        super("Unsupported compression algorithm: " + algorithm, cause);
    }
}
```

### 2. 异常处理原则

#### 禁止吞掉异常

```java
// 错误：吞掉异常
try {
    riskyOperation();
} catch (Exception e) {
    // 什么也不做
}

// 错误：仅打印异常，不处理
try {
    riskyOperation();
} catch (Exception e) {
    e.printStackTrace();
}

// 正确：记录日志 + 适当处理
try {
    riskyOperation();
} catch (BusinessException e) {
    log.warn("Business operation failed, will retry", e);
    return ConsumeAction.RECONSUME_LATER;
}
```

#### 区分可恢复与不可恢复异常

```java
try {
    process(message);
} catch (BusinessException e) {
    // 可恢复异常：重试
    log.warn("Process failed, will retry: keys={}", message.getKeys(), e);
    return ConsumeAction.RECONSUME_LATER;
} catch (SerializationException e) {
    // 不可恢复异常：直接进入 DLQ，避免无意义重试
    log.error("Serialization failed, skip to DLQ: keys={}", message.getKeys(), e);
    return ConsumeAction.SUCCESS;  // ACK 后由 DLQ 处理
}
```

#### 异常信息要求

- 包含足够的上下文（messageId、keys、topic 等）
- 避免敏感信息（密码、token）
- 使用英文消息（便于日志检索）
- 保留异常链（`cause`）

```java
// 推荐
throw new StreamMQBrokerException(
    String.format("Failed to send message: topic=%s, keys=%s, messageId=%s",
        message.getTopic(), message.getKeys(), message.getMessageId()),
    cause);

// 不推荐
throw new RuntimeException("send failed", cause);
```

### 3. 异常与重试

StreamMQ 的重试机制基于 `ConsumeAction`：
- `SUCCESS`：消费成功，不重试
- `RECONSUME_LATER`：消费失败，进入重试
- `defer(Duration)`：延迟重试

抛出异常等效于 `RECONSUME_LATER`，但建议显式返回以便控制重试时机。

---

## 日志规范

### 1. 日志框架

StreamMQ 统一使用 SLF4J + Logback（由 Spring Boot 默认提供）。

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 或使用 Lombok
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrderConsumer {
    // 直接使用 log 字段
}
```

### 2. 日志级别

| 级别    | 使用场景                                          | 示例                                    |
| ------- | ------------------------------------------------- | --------------------------------------- |
| ERROR   | 系统错误、不可恢复异常、影响业务功能              | Redis 连接失败、序列化异常              |
| WARN    | 可恢复异常、潜在问题、降级行为                    | 重试、消费超时、配置项缺失使用默认值    |
| INFO    | 关键业务节点、状态变更、启动/停止                 | 应用启动、消费者注册、定时任务执行      |
| DEBUG   | 调试信息、详细执行流程                            | 消息内容、参数值、中间状态              |
| TRACE   | 极细粒度的调试信息                                | 单条消息的处理细节                      |

**生产环境推荐级别：** INFO
**开发环境推荐级别：** DEBUG

### 3. 日志格式要求

#### 占位符

```java
// 推荐：使用占位符
log.info("Send message: topic={}, keys={}, messageId={}",
    message.getTopic(), message.getKeys(), message.getMessageId());

// 不推荐：字符串拼接（性能差）
log.info("Send message: topic=" + message.getTopic());

// 不推荐：在 DEBUG 日志中调用昂贵方法
log.debug("Message body: " + expensiveSerialize(message));
// 应改为
if (log.isDebugEnabled()) {
    log.debug("Message body: {}", expensiveSerialize(message));
}
```

#### 异常日志

```java
// 推荐：异常作为最后一个参数，不放在占位符中
try {
    riskyOperation();
} catch (Exception e) {
    log.error("Operation failed: keys={}", message.getKeys(), e);
}

// 不推荐：异常放在占位符中
log.error("Operation failed: keys={}, error={}", message.getKeys(), e.getMessage());
// 不推荐：异常丢失堆栈
log.error("Operation failed: " + e.getMessage());
```

### 4. 关键路径日志

以下关键路径必须有日志：

| 路径                | 级别 | 必要字段                                          |
| ------------------- | ---- | ------------------------------------------------- |
| 应用启动            | INFO | 版本、namespace、配置摘要                        |
| 应用停止            | INFO | 优雅停机开始、消费者停止、连接关闭                |
| 消费者注册          | INFO | topic、consumerGroup、consumeMode                |
| 消息发送            | DEBUG| topic、keys、messageId、耗时                     |
| 消息发送失败        | WARN | topic、keys、失败原因                            |
| 消费开始            | DEBUG| topic、consumerGroup、messageId                  |
| 消费成功            | DEBUG| topic、messageId、耗时                           |
| 消费失败            | WARN | topic、messageId、reconsumeTimes、异常           |
| 进入 DLQ            | ERROR| topic、consumerGroup、messageId、reconsumeTimes  |
| 事务回查            | INFO | transactionId、回查次数                          |
| 重平衡              | INFO | consumerGroup、变更前后实例数                    |
| Redis 连接异常      | ERROR| host、port、异常                                 |

### 5. MDC 结构化日志

StreamMQ 自动在 MDC 中注入以下字段：

| MDC Key          | 说明                |
| ---------------- | ------------------- |
| `traceId`        | 链路追踪 ID         |
| `topic`          | 当前消息 topic      |
| `consumerGroup`  | 当前消费组          |
| `messageId`      | 当前消息 ID         |

**Logback 配置示例：**

```xml
<configuration>
    <property name="LOG_PATTERN"
              value="%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] [%X{traceId}] [%X{topic}] [%X{consumerGroup}] %-5level %logger{36} - %msg%n"/>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>${LOG_PATTERN}</pattern>
        </encoder>
    </appender>
    <root level="INFO">
        <appender-ref ref="STDOUT"/>
    </root>
</configuration>
```

### 6. 日志脱敏

敏感信息（密码、token、身份证号等）必须脱敏：

```java
// 推荐
log.info("Redis connection: host={}, port={}, password={}",
    host, port, maskPassword(password));

// 不推荐
log.info("Redis connection: password={}", password);
```

---

## 发布流程

### 1. 版本号规范

StreamMQ 遵循 [Semantic Versioning 2.0.0](https://semver.org/)：

```
MAJOR.MINOR.PATCH
```

| 版本部分 | 何时升级                                 | 示例                          |
| -------- | ---------------------------------------- | ----------------------------- |
| MAJOR    | 不兼容的 API 变更                        | 0.1.0 → 1.0.0                 |
| MINOR    | 向下兼容的功能新增                       | 0.1.0 → 0.2.0                 |
| PATCH    | 向下兼容的 Bug 修复                      | 0.1.0 → 0.1.1                 |

**预发布版本：** 使用 `-SNAPSHOT` 后缀，如 `0.1.0`（当前版本）。

### 2. 发布步骤

维护者执行的发布流程：

```bash
# 1. 更新版本号（移除 -SNAPSHOT）
mvn versions:set -DnewVersion=0.1.0 -DgenerateBackupPoms=false

# 2. 提交版本变更
git add -A
git commit -m "chore: release 0.1.0"

# 3. 创建 tag
git tag -a v0.1.0 -m "Release 0.1.0"

# 4. 推送 tag（触发 CD 流水线）
git push origin v0.1.0

# 5. 等待 CD 流水线完成（部署到 Maven Central）

# 6. 升级到下一个 SNAPSHOT 版本
mvn versions:set -DnewVersion=0.2.0-SNAPSHOT -DgenerateBackupPoms=false
git add -A
git commit -m "chore: prepare for next development iteration"
git push origin main
```

### 3. 发布检查清单

发布前需确认：

- [ ] 所有 CI 检查通过
- [ ] 测试覆盖率达标
- [ ] CHANGELOG 已更新
- [ ] 版本号已更新（pom.xml、README.md）
- [ ] 文档已同步
- [ ] Maven Central 凭证有效
- [ ] GPG 签名密钥未过期

### 4. 发布渠道

| 渠道          | 地址                                                    |
| ------------- | ------------------------------------------------------- |
| Maven Central | `io.github.streammq:streammq-*`                         |
| GitHub Release| [github.com/streammq/streammq/releases](https://github.com/streammq/streammq/releases) |
| 文档站        | [streammq.github.io/streammq](https://streammq.github.io/streammq) |

---

## 社区渠道

| 渠道              | 用途                                  | 链接                                                                  |
| ----------------- | ------------------------------------- | --------------------------------------------------------------------- |
| GitHub Issues     | Bug 报告、功能请求                    | [github.com/streammq/streammq/issues](https://github.com/streammq/streammq/issues) |
| GitHub Discussions| 通用讨论、问答、分享                  | [github.com/streammq/streammq/discussions](https://github.com/streammq/streammq/discussions) |
| GitHub Pull Requests | 代码贡献                          | [github.com/streammq/streammq/pulls](https://github.com/streammq/streammq/pulls) |
| 邮件列表          | 重要公告、release 通知                | 通过 GitHub Discussions 维护                                          |

### 沟通礼仪

- **先搜索后提问**：提问前先搜索 Issue 与 Discussions，避免重复
- **清晰描述**：提供足够的环境信息与复现步骤
- **耐心等待**：维护者利用业余时间维护项目，请耐心等待回复
- **友善互助**：尊重每位参与者，乐于帮助新人

### 安全漏洞报告

如发现安全漏洞：
- **不要**在公开 Issue 中提交
- 通过 GitHub Security Advisory 私下报告
- 或邮件联系维护者
- 维护者会在 72 小时内响应

---

## 贡献者致谢

StreamMQ 感谢每一位贡献者的付出。

### 贡献者展示

- 所有贡献者将列在 [GitHub Contributors](https://github.com/streammq/streammq/graphs/contributors) 页面
- 重大贡献者将在 `README.md` 的致谢章节列出
- 每个 Release Notes 会鸣谢本次版本的贡献者

### 贡献类型

我们认可所有形式的贡献：

| 贡献类型       | 说明                                       |
| -------------- | ------------------------------------------ |
| 代码贡献       | 提交 PR 修复 Bug 或实现新功能              |
| 文档贡献       | 改进文档、翻译、示例                       |
| 测试贡献       | 编写测试用例、提升覆盖率                   |
| Issue 贡献     | 报告 Bug、提出功能建议                     |
| Review 贡献    | 参与代码审查、提供反馈                     |
| 推广贡献       | 撰写博客、演讲、分享使用经验               |

### 成为 Maintainer

长期活跃的贡献者有机会被邀请成为 Maintainer，享有的权限与责任：

- 合并 PR 的权限
- 参与版本发布决策
- 参与项目路线图规划
- 维护项目代码质量

**成为 Maintainer 的条件：**
- 持续贡献 6 个月以上
- 提交并被合并 10+ 个有意义的 PR
- 熟悉项目架构与代码规范
- 获得现有 Maintainer 团队的推荐

---

## 常见问题

### Q: 我可以为 StreamMQ 添加新功能吗？

**A:** 非常欢迎！建议先在 Discussions 中发起讨论，说明功能用途与设计思路，避免重复劳动或与项目方向冲突。

### Q: 我的 PR 多久会被 Review？

**A:** 维护者会在 5 个工作日内响应。如超过 1 周未响应，可在 PR 中 `@` 相关维护者提醒。

### Q: 我可以提交 Breaking Change 吗？

**A:** 可以，但需：
1. 先在 Discussions 中讨论必要性
2. 在 PR 中详细说明迁移路径
3. 提供兼容方案（如 `@Deprecated`）
4. 等待 MAJOR 版本发布

### Q: 如何成为 StreamMQ 的 Committer？

**A:** 持续贡献代码 6 个月以上，提交 10+ 有意义的 PR，并获得 Maintainer 团队推荐。

### Q: 我可以基于 StreamMQ 开发商业产品吗？

**A:** 可以。StreamMQ 基于 MIT 协议开源，允许商业使用、修改、分发，只需保留版权与许可声明。

---

## 参考资源

- [StreamMQ GitHub](https://github.com/streammq/streammq)
- [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html)
- [Conventional Commits](https://www.conventionalcommits.org/)
- [Semantic Versioning](https://semver.org/)
- [Contributor Covenant](https://www.contributor-covenant.org/)
- [JUnit 5 文档](https://junit.org/junit5/docs/current/user-guide/)
- [AssertJ 文档](https://assertj.github.io/doc/)
- [Testcontainers 文档](https://java.testcontainers.org/)
- [Lombok 文档](https://projectlombok.org/features/all)

---

*感谢你为 StreamMQ 贡献力量！每一行代码、每一个 Issue、每一处文档改进都让项目更好。*

*StreamMQ · 让 Redis 成为你的消息总线。*
