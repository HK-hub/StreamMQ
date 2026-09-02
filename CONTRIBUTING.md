# Contributing to StreamMQ

First off, thank you for considering contributing to StreamMQ! It's people like you that make StreamMQ such a great message middleware SDK.

## Table of Contents

- [Development Setup](#development-setup)
- [Git Workflow](#git-workflow)
- [DCO（开发者原产地证明）](#dco开发者原产地证明)
- [Code Style](#code-style)
- [Testing Requirements](#testing-requirements)
- [Module Architecture](#module-architecture)
- [SPI Extension Point Guide](#spi-extension-point-guide)
- [Pull Request Process](#pull-request-process)
- [Reporting Bugs](#reporting-bugs)
- [Suggesting Enhancements](#suggesting-enhancements)

## Development Setup

### Prerequisites

| Tool | Minimum Version | Recommended Version |
|------|----------------|--------------------|
| JDK | 21 | 21+ |
| Maven | 3.9 | 3.9+ |
| Redis | 7.2 | 7.2+ |
| Git | 2.30+ | Latest stable |

### Quick Setup

```bash
# 1. Fork the repository on GitHub
#    https://github.com/HK-hub/StreamMQ/fork

# 2. Clone your fork
git clone https://github.com/<your-username>/streammq.git
cd streammq

# 3. Add upstream remote
git remote add upstream https://github.com/HK-hub/StreamMQ.git

# 4. Build the project
mvn clean compile

# 5. Run tests
mvn test

# 6. Start Redis for integration tests
docker run -d --name streammq-redis -p 6379:6379 redis:7.2

# 7. Run integration tests
mvn verify   # 集成测试在检测到本地 Redis (localhost:6379) 时自动运行，否则跳过
```

### IDE Configuration

StreamMQ uses:
- **Lombok** — Enable annotation processing in your IDE
- **Google Java Format** — Code formatting via Spotless plugin
- **JDK 21** — Ensure your IDE uses JDK 21 for the project

#### IntelliJ IDEA

1. Install the Lombok plugin
2. Enable annotation processing: `Settings → Build → Compiler → Annotation Processors`
3. Set Project SDK to JDK 21
4. Install the Google Java Format plugin (optional, Spotless will enforce formatting)

#### VS Code

1. Install "Extension Pack for Java"
2. Install "Lombok Annotations Support"
3. Set `java.configuration.runtimes` to JDK 21

## Git Workflow

### Branch Naming Convention

| Prefix | Description | Example |
|--------|-------------|---------|
| `feat/` | New feature | `feat/transaction-message` |
| `fix/` | Bug fix | `fix/dlq-retry-exhausted` |
| `refactor/` | Code refactoring | `refactor/spi-resolver` |
| `docs/` | Documentation changes | `docs/configuration-guide` |
| `test/` | Test additions | `test/consumer-filter` |
| `chore/` | Maintenance tasks | `chore/upgrade-deps` |

### Commit Message Convention

We follow [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:** `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`, `perf`, `build`, `ci`

**Examples:**
```
feat(core): add SQL92 message filter support

- Add SqlSelectorFilter implementation
- Integrate with ConsumerFilterChain
- Add unit tests for parsing and evaluation

Closes #123
```

```
fix(diagnostics): resolve NPE in MessageProfileService

The profile service could throw NPE when processing null
message metadata. Added null-safety checks.

Fixes #456
```

### Workflow Steps

```bash
# 1. Sync with upstream
git fetch upstream
git checkout main
git pull upstream main

# 2. Create feature branch
git checkout -b feat/my-feature

# 3. Make changes, write tests
mvn clean test

# 4. Ensure code style compliance
mvn spotless:apply
mvn spotless:check

# 5. Ensure all checks pass
mvn verify

# 6. Commit with DCO sign-off（见下一节）
git commit -s -m "feat: add your feature"

# 7. Push and create PR
git push origin feat/my-feature
```

## DCO（开发者原产地证明）

本项目采用 [DCO](https://developercertificate.org/)（Developer Certificate of Origin，开发者原产地证明）。**提交 PR 即表示您声明：该贡献由您本人创作或有权提交，并以 MIT 协议随本项目入库。**

所有提交必须携带 `Signed-off-by` 尾注：

```bash
git commit -s -m "feat(core): add SQL92 message filter support"
```

`-s` 会基于当前 Git 配置的 `user.name` / `user.email` 自动附加如下尾注：

```
Signed-off-by: 张三 <zhangsan@example.com>
```

常见问题处理：

```bash
# 忘记签名时，为最近一次提交补签
git commit --amend -s --no-edit

# 为分支上的全部历史提交批量补签（以 upstream/main 为基线）
git rebase upstream/main --exec 'git commit --amend -s --no-edit'
```

注意：`Signed-off-by` 中的姓名与邮箱必须与提交作者信息一致，否则 PR 校验无法通过。

## Code Style

### Formatting

StreamMQ uses the [Spotless](https://github.com/diffplug/spotless) Maven plugin with **Google Java Format** for code formatting.

```bash
# Check formatting
mvn spotless:check

# Apply formatting automatically
mvn spotless:apply
```

### Java Style Guidelines

1. **Indentation**: 2 spaces (Google Java Format standard)
2. **Line length**: 100 characters
3. **Naming**:
   - Classes: UpperCamelCase (`StreamMessageTemplate`)
   - Methods/fields: lowerCamelCase (`syncSend`)
   - Constants: UPPER_SNAKE_CASE (`MAX_RECONSUME_TIMES`)
   - Packages: lowercase (`io.github.streammq.core.consumer`)
4. **Javadoc**: All public API must have Javadoc
5. **Exceptions**: Use specific exception types (`StreamMQClientException`, `StreamMQBrokerException`)
6. **Null safety**: Use `@Nullable` / `@NonNull` annotations from JSR 305
7. **SPI**: Implementations must be registered via Spring `@Component` or Java `ServiceLoader`

### Code Quality Tools

| Tool | Command | Purpose |
|------|---------|---------|
| Enforcer | `mvn enforcer:enforce` | Version constraints, duplicate deps |
| Spotless | `mvn spotless:check` | Code formatting |
| Compiler | `-Xlint:unchecked,deprecation -Werror` | Warnings as errors |

## Testing Requirements

### Test Structure

StreamMQ has a layered testing approach:

| Test Type | Location | Command | Description |
|-----------|----------|---------|-------------|
| Unit tests | `src/test/java` | `mvn test` | Isolated tests per module |
| Integration tests | `src/test/java` (IT suffix) | `mvn verify   # 集成测试在检测到本地 Redis (localhost:6379) 时自动运行，否则跳过` | Redis-backed tests |
| Testcontainers | Test utilities | Auto-configured | Redis container for IT |

### Writing Tests

1. **Test class naming**: `<ClassUnderTest>Test` (unit) / `<ClassUnderTest>IT` (integration)
2. **Test method naming**: `should_<behavior>_when_<condition>` or `<method>_<scenario>_<expected>`
3. **Coverage targets**: Core modules ≥ 80%, SPI implementations ≥ 90%
4. **Assertions**: Use AssertJ (`assertThat(...).isEqualTo(...)`)
5. **Mocks**: Use Mockito with `@ExtendWith(MockitoExtension.class)`

#### Unit Test Example

```java
@ExtendWith(MockitoExtension.class)
class StreamMessageTemplateTest {

    @Mock
    private StreamMessageProducer producer;

    @Test
    void should_send_message_when_producer_returns_success() {
        Message<String> message = MessageBuilder.<String>withTopic("test-topic")
                .body("hello")
                .build();
        SendResult expected = SendResult.success("msg-001", 0L);

        when(producer.send(any())).thenReturn(expected);

        SendResult result = template.syncSend(message);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getMessageId()).isEqualTo("msg-001");
    }
}
```

#### Integration Test Example

```java
// 集成测试继承 StreamMQTestBase（无 Redis 时通过 Assumptions 自动跳过），
// 或使用 @EnabledIf("io.github.streammq.core.util.RedisAvailability#localhostAvailable")
class StreamMessageServiceIT extends StreamMQTestBase {

    @Test
    void should_consume_message_when_sent_by_producer() {
        Message<String> message = MessageBuilder.<String>withTopic("it-topic")
                .tag("test")
                .body("integration-test")
                .build();

        template.syncSend(message);

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(consumer.getReceivedMessages()).hasSize(1);
                });
    }
}
```

### Running the Full Test Suite

```bash
# Unit tests only (fast)
mvn test

# Unit + integration tests (requires Redis)
mvn verify   # 集成测试在检测到本地 Redis (localhost:6379) 时自动运行，否则跳过

# With coverage report
mvn verify jacoco:report
```

## Module Architecture

StreamMQ is organized as a multi-module Maven project:

```
streammq-parent (POM)
├── streammq-bom              # BOM for dependency version management
├── streammq-core             # Core abstractions (messages, annotations, SPI, API)
├── streammq-redisson         # Redisson adapter (Redis Stream implementation)
├── streammq-spring-boot-starter  # Spring Boot 3 auto-configuration
├── streammq-diagnostics      # Diagnostics & monitoring endpoints
├── streammq-kubernetes       # Kubernetes integration (HPA, health, config)
├── streammq-tracing-opentelemetry  # OpenTelemetry tracing integration
├── streammq-spring-cloud-stream-binder  # Spring Cloud Stream Binder
├── streammq-test             # Test utilities (EmbeddedRedis, assertions, mocks)
├── streammq-benchmark        # JMH benchmarks (not published to Maven Central)
└── streammq-samples          # Sample projects (quickstart, transaction, delay, etc.)
```

### Module Dependency Graph

```
streammq-parent
    ├── streammq-bom (independent, import in any project)
    ├── streammq-core (no Spring dependency, pure Java)
    │   └── streammq-redisson (implements core SPI via Redisson)
    │       └── streammq-spring-boot-starter (auto-configuration)
    ├── streammq-core
    │   └── streammq-diagnostics (extends core)
    ├── streammq-core
    │   └── streammq-tracing-opentelemetry (extends core)
    ├── streammq-spring-boot-starter
    │   └── streammq-kubernetes (K8s-aware features)
    ├── streammq-spring-boot-starter
    │   └── streammq-spring-cloud-stream-binder (Spring Cloud Stream)
    └── streammq-test (test utilities, depends on core)
```

### Key Abstractions

| Module | Key Class | Purpose |
|--------|-----------|---------|
| core | `StreamMessageTemplate` | Unified send entry point |
| core | `StreamMessageConcurrentlyConsumer` | Concurrent consumer interface (annotation-driven) |
| core | `StreamMessageOrderlyConsumer` | Orderly consumer interface (sharding + distributed lock) |
| core | `MessageSerializer` | SPI: serialize/deserialize |
| core | `ConsumerFilter` | SPI: message filtering |
| core | `ProducerInterceptor` / `ConsumerInterceptor` | SPI: interceptors |
| redisson | (implements core SPI via Redisson) | Redis Stream operations |
| spring-boot-starter | `StreamMQAutoConfiguration` | Auto-config entry point |
| diagnostics | `StreamMQDiagnosticsEndpoint` | Actuator endpoint |

## SPI Extension Point Guide

StreamMQ provides 12 SPI extension points. All SPI interfaces are in `streammq-core` and can be implemented as Spring beans or registered via Java `ServiceLoader`.

### List of SPI Interfaces

| SPI Interface | Module | Purpose |
|--------------|--------|---------|
| `MessageSerializer` | core | Serialize/deserialize messages |
| `MessageConverter` | core | Convert between message body and domain objects |
| `ProducerFilter` | core | Filter messages before sending |
| `ConsumerFilter` | core | Filter messages before consumption |
| `ProducerInterceptor` | core | Intercept send operations |
| `ConsumerInterceptor` | core | Intercept consume operations |
| `RetryPolicy` | core | Control retry behavior |
| `RebalanceStrategy` | core | Consumer rebalance strategy |
| `CompressionCodec` | core | Compress/decompress message bodies |
| `TraceCollector` | core | Collect trace context for distributed tracing |
| `ManagementAuthenticator` | core | Authenticate management API requests |
| `DlqFailureStrategy` | core | Handle DLQ consumption failures |

### Implementing an SPI

```java
package com.example;

import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.exception.SerializationException;
import org.springframework.stereotype.Component;

@Component
public class CustomJsonSerializer implements MessageSerializer {

    @Override
    public byte[] serialize(Object obj) throws SerializationException {
        try {
            return objectMapper.writeValueAsBytes(obj);
        } catch (Exception e) {
            throw new SerializationException("Failed to serialize", e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> type) throws SerializationException {
        try {
            return objectMapper.readValue(bytes, type);
        } catch (Exception e) {
            throw new SerializationException("Failed to deserialize", e);
        }
    }

    @Override
    public String name() {
        return "custom-json";
    }
}
```

### Using an SPI Implementation

```java
@Component
@StreamMQConsumer(
    topic = "order-topic",
    consumerGroup = "order-group",
    serializer = CustomJsonSerializer.class,
    consumerFilter = {CustomTagFilter.class}
)
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {
    // ...
}
```

### Default Implementations

| SPI Interface | Default |
|--------------|---------|
| `MessageSerializer` | `FurySerializer` |
| `MessageConverter` | `DefaultMessageConverter` |
| `RetryPolicy` | `FixedArrayRetryPolicy` |
| `RebalanceStrategy` | `ConsistentHashRebalanceStrategy`（配置默认）/ `AverageRebalanceStrategy`（API 默认） |
| `CompressionCodec` | `GzipCompressionCodec` |
| `TraceCollector` | `NoopTraceCollector`（`streammq.tracing.enabled=true` 时为 `Slf4jTraceCollector`） |
| `ManagementAuthenticator` | `DenyAllAuthenticator`（fail-closed，需显式注册鉴权 Bean 开放） |
| `DlqFailureStrategy` | `LogAndDropDlqFailureStrategy` |

### Fury serializer registration

`FurySerializer` is the **default serializer** (`streammq.producer.serializer`). By default it
does **not** enforce class registration (unrestricted mode, `requireClassRegistration=false`):
any POJO works out of the box, but bytes stored in Redis can be deserialized to arbitrary classes
on the classpath. For shared/multi-tenant Redis, enable the class registration whitelist via
`streammq.producer.fury-require-class-registration: true` (Spring Boot) or `new FurySerializer(true)`
(Java API), then register application payloads before the first send/receive. Prefer constructor
registration in Spring configuration so startup fails early for a missing type:

```java
@Bean
MessageSerializer<OrderCreated> orderSerializer() {
    return new FurySerializer<>(OrderCreated.class, OrderUpdated.class);
}
```

For dynamic setup, call `register(Class<?>)` or `registerAll(Class<?>...)` once during
initialization. Do not register classes based on untrusted input. Constructing an unrestricted
serializer (`new FurySerializer()` / `new FurySerializer(false)`) logs a WARN; after confirming
Redis is fully trusted and isolated, set `-Dstreammq.security.allowUnrestrictedSerializer=true`
to suppress the warning.

## Pull Request Process

1. **Ensure the PR description clearly describes the problem and solution.** Include the relevant issue number if applicable.

2. **Build and test locally before submitting.** All PRs must pass the CI pipeline (build, test, verify).

3. **Follow the PR template.** The template includes sections for:
   - What type of change is this?
   - What is the current behavior?
   - What is the new behavior?
   - Does this introduce a breaking change?
   - Checklist

4. **Code review requirements:**
   - At least one approval from a maintainer
   - All CI checks must pass
   - No merge conflicts

5. **PR size:** Keep PRs focused. Large PRs (>500 lines changed) should be broken into smaller, logical chunks when possible.

## Cutting a Release

StreamMQ 通过 Maven Central Portal (`org.sonatype.central:central-publishing-maven-plugin`) 发布。发布流程如下：

1. **更新版本号** — 升级根 `pom.xml` 与 `streammq-bom/pom.xml` 中的 `<version>` 与 `<streammq.version>`，保持一致（CI `guard` job 会校验）。
2. **更新 CHANGELOG** — 将 `[Unreleased]` 段合并入新版本，附日期。
3. **本地 dry-run** — `mvn clean verify -DskipITs=true -Dspotless.check.skip=true -Dowasp.skip=true`；`mvn verify` 需本地 Redis。
4. **打 tag** — `git tag -s v0.x.y -m "Release v0.x.y"`（签名 tag 满足 GPG 要求）。
5. **触发 `release.yml`** — `workflow_dispatch` 或推送 tag；`test` job 会运行 `mvn clean verify` 兜底，`publish` job 会上传至 Central Portal。
6. **人工确认发布** — `parent.pom.xml` 中 `<autoPublish>false</autoPublish>`，首个版本需在 [Central Portal](https://central.sonatype.com/) 人工点击 "Publish"。
7. **首次发布后** — 将 `<autoPublish>` 翻转为 `true`，提交 PR 并在本节追加 changelog 行；后续发布由 CI 自动完成。
8. **创建 GitHub Release** — `release.yml` 会基于 tag 自动创建 Release 并附带全部已发布构件（jar + sources + javadoc）。

### 发布门禁

发布 job (`release.yml#publish`) 依赖 `test` job（`mvn clean verify`）通过——任何单测/集成测试/Spotless/JaCoCo 失败都会阻塞发布。 `verify` job 的集成测试 tripwire 要求实际执行 IT ≥ 80 且跳过率 ≤ 50%，防止 Redis 静默失效导致"假绿色"。

### 凭据配置

CI 通过 GitHub Secrets 注入：

- `CENTRAL_USERNAME` / `CENTRAL_TOKEN` — Central Portal 凭据
- `GPG_PRIVATE_KEY` / `GPG_PASSPHRASE` — 签名密钥

本地发布需在 `~/.m2/settings.xml` 中以 `server-id=central` 配置相同凭据。

## Reporting Bugs

When submitting a bug report, please use the [bug report template](.github/ISSUE_TEMPLATE/bug_report.md) and include:

- **Java version** and **StreamMQ version**
- **Steps to reproduce** the issue
- **Expected behavior** vs **Actual behavior**
- **Minimal reproducible code** or test case
- **Stack trace** if an exception is thrown

## Suggesting Enhancements

When submitting a feature request, please use the [feature request template](.github/ISSUE_TEMPLATE/feature_request.md) and include:

- **Problem description** — What problem does this solve?
- **Proposed solution** — How should it work?
- **Alternative approaches** — What have you considered?
- **Code examples** — Show how the feature would be used

## Getting Help

- **GitHub Discussions**: Ask questions in [Discussions](https://github.com/HK-hub/StreamMQ/discussions)
- **Documentation**: Read the design docs in [docs/](docs/) and the [README](README.md)
- **Design docs**: Check [docs/](docs/) for architecture and design documents

## Code of Conduct

This project and everyone participating in it is governed by the [StreamMQ Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to the project maintainers.

## Thank You

Your contributions to StreamMQ are greatly appreciated! Every contribution, no matter how small, helps make StreamMQ better for everyone.
