# Contributing to StreamMQ

First off, thank you for considering contributing to StreamMQ! It's people like you that make StreamMQ such a great message middleware SDK.

## Table of Contents

- [Development Setup](#development-setup)
- [Git Workflow](#git-workflow)
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

# 6. Push and create PR
git push origin feat/my-feature
```

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
@ExtendWith(StreamMQTestBase.class)
class StreamMessageServiceIT {

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
| core | `StreamMessageConsumer` | Concurrent consumer interface |
| core | `StreamMessageOrderlyConsumer` | Orderly consumer interface |
| core | `StreamMessageConcurrentlyConsumer` | Annotation-driven concurrent consumer |
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
| `MessageSerializer` | `JacksonJsonSerializer` |
| `MessageConverter` | `DefaultMessageConverter` |
| `RetryPolicy` | `FixedArrayRetryPolicy` |
| `RebalanceStrategy` | `AverageRebalanceStrategy` |
| `CompressionCodec` | `GzipCompressionCodec` |
| `TraceCollector` | `Slf4jTraceCollector` |
| `ManagementAuthenticator` | `AllowAllAuthenticator` |
| `DlqFailureStrategy` | `AbstractDlqFailureStrategy` |

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