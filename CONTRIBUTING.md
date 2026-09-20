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
7. **SPI**: Implementations are resolved as Spring beans or referenced via the annotation's `Class` attribute — Java `ServiceLoader` is **not** used

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
3. **Coverage targets**: the gate is enforced per module by `jacoco-maven-plugin` (`check` bound to `verify`, enabled with `-Djacoco.check.skip=false`). Thresholds are *measured coverage minus ~3 points* (see the comments in each module's `pom.xml`):
   - `streammq-core`: LINE ≥ 0.48, BRANCH ≥ 0.44 (unit tests only — core has no IT)
   - `streammq-redisson`: LINE ≥ 0.60, BRANCH ≥ 0.50 (measured **with the real-Redis ITs** running; without Redis the gate is not meaningful)
   - `streammq-spring-boot-starter`: LINE ≥ 0.55, BRANCH ≥ 0.41 (auto-configuration has many branches by nature)
   - `streammq-test`: LINE ≥ 0.78, BRANCH ≥ 0.39 (test utilities — the gate **does** apply to this published module)
   - Modules that do not declare the plugin (e.g. `streammq-samples/*`, `streammq-benchmark`) are outside the gate
4. **Assertions**: Use AssertJ (`assertThat(...).isEqualTo(...)`)
5. **Mocks**: Use Mockito with `@ExtendWith(MockitoExtension.class)`

#### Unit Test Example

```java
@ExtendWith(MockitoExtension.class)
class StreamMessageServiceTest {

    @Mock
    private StreamMessageTemplate template;

    private StreamMessageService service;

    @BeforeEach
    void setUp() {
        service = new DefaultStreamMessageService(template);
    }

    @Test
    void should_return_send_result_when_template_sync_send_succeeds() {
        // MessageId is a value object — build it with the real Stream entry id, there is no
        // SendResult.success(...) factory method.
        SendResult expected =
                new SendResult(new MessageId("1700000000000-0"), "test-topic", null, 0L);
        when(template.syncSend(any(Message.class), any(SendOptions.class))).thenReturn(expected);

        SendResult result = service.send("test-topic", "hello");

        assertThat(result.isSuccess()).isTrue();
        // getMessageId() returns MessageId (not String) — compare via toString() or equals().
        assertThat(result.getMessageId().toString()).isEqualTo("1700000000000-0");
        verify(template).syncSend(any(Message.class), any(SendOptions.class));
    }
}
```

Notes on the real API used above (all verified against the sources):

- `SendResult` has **no** `success(...)` factory — use the public constructor
  `SendResult(MessageId messageId, String topic, String tag, long bornTimestamp)`.
- `MessageId` is constructed with the Redis Stream entry id: `new MessageId("1700000000000-0")`.
- The send entry point on `StreamMessageProducer` is `syncSend(Message<?>)` (plus
  `asyncSend` / `syncSendBatch` / `sendOneway`); `StreamMessageTemplate` exposes
  `syncSend(Message<T>, SendOptions)` plus a `syncSend(Message<T>)` default method.

#### Integration Test Example

```java
// 集成测试继承 StreamMQTestBase（无 Redis 时通过 Assumptions 自动跳过），
// 或使用 @EnabledIf("io.github.streammq.test.util.RedisAvailability#localhostAvailable")
class StreamMessageServiceIT extends StreamMQTestBase {

    // 示意：template 为应用中已装配的发送模板（StreamMessageTemplate），
    // consumer 为被测消费者 Bean，其 getReceivedMessages() 是消费者自己实现的「已收到消息」记录方法。
    // StreamMQTestBase 本身只提供 redisServer / redissonClient / clearRedisData() 等测试基础设施。

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

StreamMQ ships **18 extension points** in total — user-facing SPI interfaces plus internal assembly points (the authoritative table is the [README's Extension Points section](README.md#extension-points)). All extension points are resolved as Spring beans or via the annotation's `Class` attribute — **not** via Java `ServiceLoader`.

### List of SPI Interfaces (most commonly implemented)

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
| `CompressionCodecRegistry` | core | Register/look up codecs by name (consumer side decompression) |
| `TraceCollector` | core | Collect trace context for distributed tracing |
| `ManagementAuthenticator` | core | Authenticate management API requests |
| `DlqFailureStrategy` | core | Handle DLQ consumption failures |
| `BroadcastInstanceRegistry` | core | Stable broadcast instance identity (lease/reclaim/sweep, 0.1.2) |
| `ConsumerFilterResolver` | core | Resolve per-consumer filters |
| `OrderlyShardLockManager` | core | Shard distributed lock for ordered consumption |
| `ConsumerGroupManager` | core | Consumer-group instance management |

### Implementing an SPI

```java
package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.serializer.MessageSerializer;
import org.springframework.stereotype.Component;

@Component
public class CustomJsonSerializer<T> implements MessageSerializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(T object, Class<T> type) throws SerializationException {
        try {
            return objectMapper.writeValueAsBytes(object);
        } catch (Exception e) {
            throw new SerializationException("Failed to serialize " + type.getName(), e);
        }
    }

    @Override
    public <R> R deserialize(byte[] bytes, Class<R> type) throws SerializationException {
        try {
            return objectMapper.readValue(bytes, type);
        } catch (Exception e) {
            throw new SerializationException("Failed to deserialize " + type.getName(), e);
        }
    }

    // name() has a default implementation (simple class name); override it to pick a custom name.
    @Override
    public String name() {
        return "custom-json";
    }
}
```

> The SPI signature is exactly `byte[] serialize(T object, Class<T> type)` and
> `<R> R deserialize(byte[] bytes, Class<R> type)` on
> `io.github.streammq.core.serializer.MessageSerializer<T>`. `SerializationException`
> (`io.github.streammq.core.exception.SerializationException`) extends `StreamMQException` →
> `RuntimeException`, so it is **unchecked** — the `throws` clause is documentation; keep it to
> mirror the interface.

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
| `MessageSerializer` | `JacksonJsonSerializer`（0.1.2 起的默认，严格类型/无 gadget 面）；`FurySerializer` 为 opt-in 高吞吐实现 |
| `MessageConverter` | `DefaultMessageConverter` |
| `RetryPolicy` | `FixedArrayRetryPolicy` |
| `RebalanceStrategy` | `ConsistentHashRebalanceStrategy`（配置默认）/ `AverageRebalanceStrategy`（API 默认） |
| `CompressionCodec` | `GzipCompressionCodec` |
| `TraceCollector` | `NoopTraceCollector`（`streammq.tracing.enabled=true` 时为 `Slf4jTraceCollector`） |
| `ManagementAuthenticator` | `DenyAllAuthenticator`（fail-closed，需显式注册鉴权 Bean 开放） |
| `DlqFailureStrategy` | `LogAndDropDlqFailureStrategy` |

### Fory (Fury) serializer registration

`FurySerializer` is the **opt-in high-throughput serializer** (the default is
`JacksonJsonSerializer`). Its underlying library is **Apache Fory (formerly Apache Fury) 1.7.3**,
Maven coordinates `org.apache.fory:fory-core` (**>= 1.1.0** — earlier fury-core/fory-core versions
are affected by CVE-2026-50076). It is an `optional` dependency of `streammq-redisson`: add it
explicitly when you opt in. The Java class name `FurySerializer` and `name()="fury"` are stable for
configuration compatibility.

It **enforces the class-registration whitelist by default** (`requireClassRegistration=true`):
only registered types can be deserialized; unregistered POJOs are rejected. Register application
payloads before the first send/receive. Prefer constructor registration in Spring configuration so
startup fails early for a missing type:

```java
@Bean
MessageSerializer<OrderCreated> orderSerializer() {
    return new FurySerializer<>(OrderCreated.class, OrderUpdated.class);
}
```

For dynamic setup, call `register(Class<?>)` or `registerAll(Class<?>...)` once during
initialization; in Spring Boot you can instead declare `streammq.producer.fury-registered-classes`.
Do not register classes based on untrusted input. Unrestricted mode
(`new FurySerializer(false)`) is gated: it throws `SecurityException` unless
`-Dstreammq.security.allowUnrestrictedSerializer=true` is set, and it still logs a WARN when the
property allows construction — never enable it for shared/multi-tenant Redis.

## API 兼容性策略（japicmp）

发布通道内置 japicmp 门禁（见 `.github/workflows/release.yml`）：探测 Maven Central 上的上一个发布版本，
与当前构建产物做二进制/源码兼容性对比，发现破坏性变更即阻断发布。

- **0.1.x 处于 pre-1.0（功能预览）阶段**：公开 API 仍可能随社区反馈演进。破坏性变更需在 CHANGELOG 中
  给出说明与升级指引，并优先走一个版本的**弃用期**：在当前版本标记 `@Deprecated` 并在 CHANGELOG 中说明，
  下个 minor/major 版本再移除；不要在同一次发布里既弃用又移除；
- **japicmp 门禁从第二个 Central 发布版本起生效**：0.1.2 是首个 Central 发布版本（Central 上无历史基线），
  门禁按设计自动跳过；此后任何公开 API 的移除、签名变更、可见性收窄都会阻断发布；
- **当前没有任何内建的 japicmp 排除**：`io.github.streammq.internal.*` 这个包在仓库中并不存在（0 处 package
  声明），历史上为它配置的排除项已作为**死配置**删除。若未来需要"仅供内部使用、不承担兼容承诺"的实现包，
  必须先在根 `pom.xml` 的 japicmp 配置中显式新增对应排除规则——不要以为该前缀已被自动免检。
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

1. **更新版本号** — 升级根 `pom.xml` 与 `streammq-bom/pom.xml` 中的 `<version>` 与 `<streammq.version>`，保持一致（CI 与发布通道的 `guard` job 会校验）。
2. **更新 CHANGELOG** — 将 `[Unreleased]` 段合并入新版本，附日期。
3. **本地 dry-run（复现发布门禁）** — `mvn clean verify -Djacoco.check.skip=false`；集成测试需要本地 Redis（`localhost:6379`，无 Redis 时 IT 会被整体跳过、门禁形同虚设）。
   依赖 CVE 门禁（CI 与发布通道共用的 `sbom-scan` job，无需密钥）本地复现：`mvn -DskipTests -Dcyclonedx.skip=false -Dcyclonedx.skipNotDeployed=false org.cyclonedx:cyclonedx-maven-plugin:makeAggregateBom` 生成聚合 SBOM，再从 4 个发布构件裁剪出依赖闭包后用 `osv-scanner scan source -L <bom.cdx.json>` 扫描（High/Critical 阻断；实现与阈值口径以 `.github/workflows/ci.yml` 的 `sbom-scan` 为准，`release.yml` 以相同命令自带同一门禁）。
   如需 OWASP/NVD 深扫（每周 CI 增强项），追加 `-Dowasp.skip=false`，并建议设置 `NVD_API_KEY` 环境变量（否则匿名访问 NVD 限流、扫描可能偶发失败）。
4. **打 tag 并推送** — `git tag -s v0.x.y -m "Release v0.x.y" && git push origin v0.x.y`（签名 tag 满足 GPG 要求）。
   **tag 必须在触发发布之前就存在且指向要发布的提交**：tag 事件路径由 `release.yml` 的 `Verify tag points at the checked-out commit` 校验；`workflow_dispatch` 路径会在构建前执行 `git fetch --tags --force`，断言 `v<版本>` 已存在且 `git rev-list -n1` == `git rev-parse HEAD` —— 两条路径都拒绝"发布时现场补 tag"与"tag 指向旧提交"（否则上传 Central 的字节不绑定任何已校验 tag）。
5. **触发 `release.yml`** — 推送 tag，或在 Actions 里 `workflow_dispatch` 并填 `inputs.version`（如 `0.1.2` / `v0.1.2`）。两条路径都会先跑 `guard`（parent↔BOM 属性同步 + 发布集一致性）、`sbom-scan`（CycloneDX SBOM + `osv-scanner` 的 keyless CVE 硬门禁）与 `test`（`mvn clean verify`），三者全绿后 `publish` job 才会上传至 Central Portal。
6. **人工确认发布** — `pom.xml`（根 POM）与 `streammq-bom/pom.xml` 中均为 `<autoPublish>false</autoPublish>`，首个版本需在 [Central Portal](https://central.sonatype.com/) 人工点击 "Publish"（GitHub Release 正文会自动附带"staging 部署处于 validated、需人工 Publish"的提示）。
7. **首次发布后** — 将两处 `<autoPublish>` 翻转为 `true`，提交 PR 并在本节追加 changelog 行；后续发布由 CI 自动完成。
8. **创建 GitHub Release** — `release.yml` 把 Release 挂到已存在的 `v<版本>` tag 上（不再现场创建 tag），附带 Central 可解析的发布构件资产（`streammq-bom` 的源 POM + `core` / `redisson` / `starter` / `test` 的 jar + sources + javadoc）。
   Central 上可解析的构件共 **6 个**：`streammq-parent`（parent POM，供下游以 `<parent>` 继承；packaging=pom，无 jar/sources/javadoc 产物，故不作为资产文件列出）/ `streammq-bom` / `streammq-core` / `streammq-redisson` / `streammq-spring-boot-starter` / `streammq-test`。
   `excludeArtifacts` 中不发布到 Central 的模块（samples/benchmark/kubernetes/tracing/diagnostics/binder）**不列入** Release 资产，避免使用方误以为可从 Central 解析。

### 发布前置条件

- **CI Secrets（仓库级）**：见下方[凭据配置](#凭据配置)（Central 上传凭据、GPG 签名密钥与 `NVD_API_KEY`）。
- **GPG 密钥**：`git tag -s` 与 `mvn deploy -Pgpg` 均需可用的 GPG 私钥。
- **失败/半程发布处理**：Central Portal 采用 staging 部署，校验失败或部署中断时必须在 [Central Portal](https://central.sonatype.com/) 中**撤销（withdraw/drop）该次部署**后重新发布；不要静默重打 tag 或用同一版本号重发（Central 构件不可变，参见 CHANGELOG 中 `v0.1.0` 标签的处理说明）。

### 发布门禁

发布 job (`release.yml#publish`) 依赖 `test` / `guard` / `sbom-scan` 三个 job **全部**通过——任何单测/集成测试/Spotless/JaCoCo 失败、发布集不一致（parent↔BOM 属性漂移，或 `modules` ↔ `excludeArtifacts` ↔ BOM 三方清单漂移）、或发布构件依赖闭包中存在 CVSS ≥ 7.0 的公告，都会阻塞发布。`guard` 与 `sbom-scan` 的检查命令、插件/工具版本与 SHA-256 校验与 `.github/workflows/ci.yml` **完全一致**（发布通道自包含，不假设同 commit 的 CI 已通过，也不依赖 `secrets.NVD_API_KEY`——NVD 深扫仍只是增强项）。 `verify` job 的集成测试 tripwire 采用「分模块下限 + 全局下限 + 跳过率上限」三层校验（见 `.github/workflows/ci.yml`）：实际执行的 IT 按模块 `streammq-redisson ≥ 100` / `streammq-spring-boot-starter ≥ 30` / `streammq-test ≥ 40` / `streammq-samples/* ≥ 16`，全局 `≥ 230`，且跳过率 `≤ 20%`——防止 Redis 静默失效导致"假绿色"。 此外发布 job 内部还有：`workflow_dispatch` 的 tag↔HEAD 断言（见上文步骤 4）、版本改写后对 BOM `<version>` 与 `streammq.version` 属性的双断言、以及 staging smoke 的「解析到的 `io.github.streammq:*` 版本必须等于本次构建版本」断言。

### 凭据配置

CI 通过 GitHub Secrets 注入：

- `CENTRAL_USERNAME` / `CENTRAL_TOKEN` — Central Portal 凭据
- `GPG_PRIVATE_KEY` / `GPG_PASSPHRASE` — 签名密钥
- `NVD_API_KEY` — OWASP Dependency-Check 深扫（每周 CI 增强项）查询 NVD 的限额（可选；缺失时该 job 按设计跳过，**默认 CVE 硬门禁无需任何密钥**）

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
