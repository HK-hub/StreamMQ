# StreamMQ 骨架阶段审查与修复报告

## 1. 文档信息

| 项目       | 内容                                   |
| ---------- | -------------------------------------- |
| 文档名称   | StreamMQ 骨架阶段审查与修复报告        |
| 文档版本   | v1.0.0                                 |
| 文档状态   | 已定稿（Approved）                     |
| 创建日期   | 2026-06-30                             |
| 审查阶段   | 项目骨架阶段（Skeleton Phase）         |
| 审查负责人 | StreamMQ 核心团队                       |
| 依据文档   | 01-PRD / 02-architecture / 03-functional-design / 04-detailed-design |
| 审查范围   | Maven 多模块结构、依赖作用域、配置示例、文件完整性 |
| 下一里程碑 | 进入 MVP 实现阶段（v0.1.0）            |

## 2. 审查方法

### 2.1 审查对象

本次审查针对 StreamMQ 项目骨架阶段产物，包括 Maven 多模块工程结构、各模块 `pom.xml` 依赖声明、Java 包结构与骨架文件、Spring Boot 自动装配注册、示例应用配置文件等。审查时项目尚未实现业务逻辑代码，仅包含模块骨架（`package-info.java`、核心 API 接口/枚举/注解占位、示例 `application.yml`）。

### 2.2 依据文档

| 文档                     | 章节          | 用途                             |
| ------------------------ | ------------- | -------------------------------- |
| docs/01-PRD.md           | 全文          | 产品需求与功能边界校验           |
| docs/02-architecture.md  | 第 4 / 11 章  | 模块清单、包名规范、依赖关系     |
| docs/03-functional-design.md | 第 9 章  | 配置示例与命名空间规范           |
| docs/04-detailed-design.md   | 全文    | 类设计与接口契约参考             |

### 2.3 检查项清单

本次审查覆盖以下 6 类检查项：

1. **模块结构合规性**：对照 [02-architecture.md](02-architecture.md) 第 4 章，校验 9 模块清单与包结构
2. **包名规范**：对照第 4.4 节，校验 7 个根包命名
3. **依赖关系**：对照第 11 章，校验各模块第三方依赖坐标、版本与作用域
4. **配置示例**：对照 [03-functional-design.md](03-functional-design.md) 第 9 章，校验 `application.yml` 命名空间与 Redisson 配置路径
5. **Maven 最佳实践**：检查 enforcer 规则、循环依赖、版本管理、插件版本声明
6. **文件完整性**：校验 Java 文件可编译、配置文件 YAML 语法、自动装配注册文件位置

### 2.4 审查方式

- 静态人工走查：逐模块对比 `pom.xml` 与设计文档
- 工具验证：`mvn validate` / `mvn compile` / `mvn enforcer:enforce`
- 一致性比对：包名、版本号、作用域三维度交叉核对

## 3. 审查范围与对象

### 3.1 审查覆盖文件清单

| 文件类别                          | 数量 | 说明                                                                 |
| --------------------------------- | ---- | -------------------------------------------------------------------- |
| `pom.xml`                         | 10   | 根聚合 pom + 8 主模块 pom + samples 父 pom（4 个 sample 子 pom 随父校验） |
| `.java`                           | 22   | 含各模块 `package-info.java`、core 核心 API 骨架（注解/枚举/异常/消息/监听器/SPI/模板/事务接口） |
| `application.yml`                 | 4    | 4 个 sample 模块配置示例（quickstart / transaction / delay / orderly）|
| `AutoConfiguration.imports`       | 1    | Spring Boot 3 自动装配注册文件                                       |
| **合计**                          | **37** |                                                                      |

### 3.2 模块清单（9 模块）

| 序号 | 模块名                          | 类型           | 状态   |
| ---- | ------------------------------- | -------------- | ------ |
| 1    | streammq-bom                    | BOM            | 已审查 |
| 2    | streammq-parent（根 pom）       | Parent POM    | 已审查 |
| 3    | streammq-core                    | 核心库         | 已审查 |
| 4    | streammq-redisson-adapter        | 适配器         | 已审查 |
| 5    | streammq-spring-boot-starter     | Spring Boot Starter | 已审查 |
| 6    | streammq-native                 | 原生 API       | 已审查 |
| 7    | streammq-kafka-compat            | Kafka 兼容     | 已审查 |
| 8    | streammq-amqp-compat             | AMQP 兼容      | 已审查 |
| 9    | streammq-test                    | 测试工具       | 已审查 |

### 3.3 示例模块（4 子模块）

| 模块名                       | 用途                 |
| ---------------------------- | -------------------- |
| streammq-sample-quickstart   | 快速开始示例         |
| streammq-sample-transaction  | 事务消息示例         |
| streammq-sample-delay        | 延时消息示例         |
| streammq-sample-orderly      | 顺序消息示例         |

## 4. 发现问题汇总

本次审查共发现 9 项问题，均已修复。按严重度分布：阻断级 2 项、严重级 5 项、一般级 2 项。

| 编号 | 严重度 | 类别           | 状态   | 涉及文件                                   |
| ---- | ------ | -------------- | ------ | ------------------------------------------ |
| P-01 | 阻断   | Maven 循环依赖 | 已修复 | streammq-bom/pom.xml、根 pom.xml           |
| P-02 | 严重   | 依赖作用域     | 已修复 | streammq-core/pom.xml                       |
| P-03 | 严重   | 依赖作用域     | 已修复 | streammq-core/pom.xml                       |
| P-04 | 严重   | 依赖作用域     | 已修复 | streammq-redisson-adapter/pom.xml           |
| P-05 | 严重   | 依赖缺失       | 已修复 | streammq-spring-boot-starter/pom.xml        |
| P-06 | 严重   | 依赖坐标       | 已修复 | streammq-spring-boot-starter/pom.xml        |
| P-07 | 严重   | 依赖坐标       | 已修复 | streammq-spring-boot-starter/pom.xml        |
| P-08 | 阻断   | BOM 版本管理   | 已修复 | streammq-bom/pom.xml                        |
| P-09 | 一般   | 插件版本缺失   | 已修复 | streammq-parent（根 pom.xml）pluginManagement |

## 5. 已修复问题详情

### 5.1 P-01：Maven BOM 循环依赖（阻断）

**问题描述**

执行 `mvn clean` 报错：

```
[ERROR] The projects in the repository contain a cycle:
[ERROR] import form a cycle: parent -> bom -> bom
```

构建无法启动，全工程受阻断。

**根因分析**

`streammq-parent` 在 `<dependencyManagement>` 中通过 `<import>` 方式引入 `streammq-bom`；而 `streammq-bom` 又将 `streammq-parent` 设为 `<parent>`。两者形成 `parent → import bom → bom.parent → parent` 的循环引用，违反 Maven 依赖解析规则。

**修复方案**

将 `streammq-bom` 改造为独立 POM：

- 移除 `<parent>` 声明，不再继承 `streammq-parent`
- 自定义 `<properties>` 维护版本号
- 自定义 `<dependencyManagement>` 管理内部模块与第三方依赖版本
- 保持对外 `import` 能力不变，外部用户可直接 `<import>` 该 BOM

**影响评估**

- 正面：消除循环依赖，构建恢复正常
- 注意：`streammq-bom` 与 `streammq-parent` 现分别维护版本属性，发版时需手动同步（详见第 8 章）

### 5.2 P-02：streammq-core SLF4J 依赖作用域与设计不符（严重）

**问题描述**

[02-architecture.md](02-architecture.md) 11.1 节明确要求 `slf4j-api` 作用域为 `provided`，实际为 `compile`（默认）。

**根因分析**

骨架初始声明遗漏 `<scope>` 标签，Maven 默认采用 `compile`，导致 SLF4J 被强制传递给所有依赖 `streammq-core` 的下游模块与最终用户，违背"日志由宿主应用决定实现"的设计意图。

**修复方案**

```xml
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-api</artifactId>
    <scope>provided</scope>
</dependency>
```

**影响评估**

下游用户可自由选择 SLF4J 绑定（logback / log4j2 / slf4j-simple 等），避免与宿主应用日志框架冲突。

### 5.3 P-03：streammq-core Jackson 依赖作用域不当（严重）

**问题描述**

`jackson-databind` 为 `compile` 作用域，会被强制传递给所有依赖 `streammq-core` 的用户。

**根因分析**

Jackson 是默认序列化器（`MessageSerializer` 的默认实现 `JacksonMessageSerializer`）所需依赖，但设计允许用户改用 JDK 原生序列化器等其他实现。强制传递 Jackson 违反"可选实现"原则。

**修复方案**

```xml
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <optional>true</optional>
</dependency>
```

**影响评估**

- 使用默认 Jackson 序列化器的用户需自行引入 `jackson-databind`（Spring Boot 默认已包含）
- 改用 JDK 序列化器的用户不再被迫引入 Jackson，依赖树更精简

### 5.4 P-04：streammq-redisson-adapter Redisson 作用域与设计不符（严重）

**问题描述**

[02-architecture.md](02-architecture.md) 11.1 节要求 `redisson` 作用域为 `provided`，实际为 `compile`。

**根因分析**

`streammq-redisson-adapter` 是 Redis Stream 的 Redisson 适配实现，编译期需要 Redisson API，但运行期应由宿主应用提供 Redisson 客户端。`compile` 作用域会强制传递 Redisson 及其全部传递依赖，造成依赖膨胀与版本冲突风险。

**修复方案**

```xml
<dependency>
    <groupId>org.redisson</groupId>
    <artifactId>redisson</artifactId>
    <scope>provided</scope>
</dependency>
```

**影响评估**

用户自行引入 Redisson 客户端（通常通过 `redisson-spring-boot-starter`），可统一管理 Redisson 版本。

### 5.5 P-05：streammq-spring-boot-starter 缺少 spring-boot-starter 依赖（严重）

**问题描述**

[02-architecture.md](02-architecture.md) 11.1 节要求声明 `spring-boot-starter (provided)`，实际未声明。

**根因分析**

作为 Spring Boot Starter，需依赖 `spring-boot-starter` 以获得自动装配、配置元数据等基础能力。骨架声明遗漏。

**修复方案**

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter</artifactId>
    <scope>provided</scope>
</dependency>
```

**影响评估**

Starter 编译期获得 Spring Boot 基础能力，运行期由宿主应用提供（Spring Boot 应用默认已含）。

### 5.6 P-06：streammq-spring-boot-starter Actuator 依赖坐标与设计不符（严重）

**问题描述**

[02-architecture.md](02-architecture.md) 11.1 节要求 `spring-boot-starter-actuator (optional)`，实际使用 `spring-boot-actuator-autoconfigure (optional)`。

**根因分析**

`spring-boot-actuator-autoconfigure` 仅包含自动装配类，不含 actuator 运行时能力。设计文档要求使用完整的 `spring-boot-starter-actuator`，以便提供指标端点、健康检查等完整可观测性能力。

**修复方案**

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
    <optional>true</optional>
</dependency>
```

**影响评估**

- 正面：更符合 Spring Boot Starter 习惯，可观测性能力完整
- `optional` 保证不强制传递，用户按需引入

### 5.7 P-07：streammq-spring-boot-starter Redisson 依赖坐标与设计不符（严重）

**问题描述**

[02-architecture.md](02-architecture.md) 11.1 节要求 `redisson-spring-boot-starter (provided)`，实际使用 `redisson (compile)`。

**根因分析**

`redisson` 仅提供 Redisson 客户端核心 API，不含 Spring Boot 自动装配。设计文档要求使用 `redisson-spring-boot-starter`，以便用户引入后自动获得 `RedissonClient` Bean 装配。当前 `compile` 作用域还会强制传递 Redisson 全部依赖。

**修复方案**

```xml
<dependency>
    <groupId>org.redisson</groupId>
    <artifactId>redisson-spring-boot-starter</artifactId>
    <scope>provided</scope>
</dependency>
```

**影响评估**

- 用户引入 `streammq-spring-boot-starter` 后，按设计引入 `redisson-spring-boot-starter` 即可获得 `RedissonClient` 自动装配
- 不再强制传递 Redisson 依赖，依赖树干净

### 5.8 P-08：streammq-bom 未管理 redisson-spring-boot-starter 版本（阻断）

**问题描述**

执行 `mvn validate` 报错：

```
[ERROR] 'dependencies.dependency.version' for org.redisson:redisson-spring-boot-starter:jar is missing.
```

**根因分析**

P-07 修复后 `streammq-spring-boot-starter` 改用 `redisson-spring-boot-starter`，但 `streammq-bom` 的 `<dependencyManagement>` 未包含该坐标的版本管理，导致依赖版本无法解析。

**修复方案**

在 `streammq-bom` 的 `<dependencyManagement>` 新增：

```xml
<dependency>
    <groupId>org.redisson</groupId>
    <artifactId>redisson-spring-boot-starter</artifactId>
    <version>${redisson.version}</version>
</dependency>
```

**影响评估**

BOM 完整管理 `redisson-spring-boot-starter` 版本，外部用户 import BOM 后无需再显式声明版本。

### 5.9 P-09：streammq-test 缺少 maven-jar-plugin 版本（一般）

**问题描述**

执行 `mvn validate` 出现警告：

```
[WARNING] maven-jar-plugin is missing version
```

**根因分析**

`streammq-parent` 的 `<pluginManagement>` 未声明 `maven-jar-plugin` 版本。`streammq-test` 模块需通过 `test-jar` 打包测试工具类供其他模块复用，依赖 `maven-jar-plugin` 的 `test-jar` goal，缺少版本声明会导致构建不可复现。

**修复方案**

在 `streammq-parent` 的 `<pluginManagement>` 新增：

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-jar-plugin</artifactId>
    <version>3.4.2</version>
</plugin>
```

**影响评估**

消除警告，保证 `test-jar` 打包行为可复现。

## 6. 未发现偏差的合规项

以下 7 项骨架实现与设计文档完全一致，审查通过：

| 序号 | 合规项                                   | 依据章节                  | 结论   |
| ---- | ---------------------------------------- | ------------------------- | ------ |
| C-01 | 9 模块清单（bom/core/adapter/starter/native/kafka/amqp/test/samples） | 02-architecture.md 4.1 节 | 一致   |
| C-02 | 7 个根包名（io.github.streammq.*）       | 02-architecture.md 4.4 节 | 一致   |
| C-03 | 4 个 sample 子模块（quickstart/transaction/delay/orderly） | 02-architecture.md 4.1 节 | 一致   |
| C-04 | 第三方依赖版本（Spring Boot 3.3.5 / Redisson 3.34.1 / Jackson 2.18.1 / SLF4J 2.0.16 / Micrometer 1.13.6） | 02-architecture.md 11.1 节 | 符合"3.3.x / 3.34.x / 2.17+ / 2.0+ / 1.13+"要求 |
| C-05 | application.yml 命名空间与 redisson.config 路径 | 03-functional-design.md 9.1 节 | 一致   |
| C-06 | Spring Boot 3 自动装配注册文件位置（META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports） | Spring Boot 3 标准 | 合规   |
| C-07 | MIT 许可证文本                           | PRD 协议声明              | 一致   |

## 7. 验证结果

### 7.1 构建验证

修复完成后执行以下命令，均成功通过：

| 命令                                          | exit code | 结果   |
| --------------------------------------------- | --------- | ------ |
| `mvn -B -DskipTests -q validate`              | 0         | 通过   |
| `mvn -B -DskipTests -q compile`               | 0         | 通过   |

全部 14 模块（含 4 个 sample 子模块）构建成功。

### 7.2 Enforcer 规则通过情况

`streammq-parent` 配置了以下 enforcer 规则，全部通过：

| Enforcer 规则                       | 要求                                   | 结果   |
| ----------------------------------- | -------------------------------------- | ------ |
| `requireJavaVersion`                | JDK 21+                                | 通过   |
| `requireMavenVersion`               | Maven 3.9+                             | 通过   |
| `banDuplicatePomDependencyVersions` | 禁止重复依赖声明                        | 通过   |
| `dependencyConvergence`             | 传递依赖版本收敛                        | 通过   |

### 7.3 验证结论

骨架阶段产物在结构、依赖、配置三维度均与设计文档对齐，Maven 构建链路畅通，可进入实现阶段。

## 8. 未来实现阶段关注事项

以下 5 项在实现阶段需持续关注：

### 8.1 streammq-test 依赖作用域说明

当前 `streammq-test` 所有依赖为 `compile` 作用域，目的是便于其他模块通过 `test-jar` 复用测试工具类。但外部用户直接依赖 `streammq-test` 时会传递获得 JUnit / Mockito 等测试框架，需在 README 中明确说明用法与适用场景。

### 8.2 maven-compiler-plugin 的 -Werror 参数

`streammq-parent` 配置了 `-Werror`，所有编译警告视为错误。实现阶段必须保证代码零警告，包括未使用导入、原始类型操作、unchecked 警告等。建议在 CI 中前置 `mvn -B compile` 检查。

### 8.3 dependencyConvergence 规则约束

enforcer 的 `dependencyConvergence` 规则要求所有传递依赖版本严格收敛。实现阶段引入新依赖时需注意：

- 优先在 `streammq-bom` 统一管理版本
- 使用 `mvn dependency:tree` 检查版本冲突
- 必要时通过 `<exclusions>` 排除冲突依赖

### 8.4 集成测试命名规范

`maven-failsafe-plugin` 已配置，集成测试需以 `*IT.java` 后缀命名（区别于单元测试 `*Test.java`）。CI 中应分离 `mvn test`（单元）与 `mvn verify`（集成）阶段，集成测试通常需要 Redis 7.2+ 实例。

### 8.5 streammq-bom 与 streammq-parent 版本同步

P-01 修复后，`streammq-bom` 与 `streammq-parent` 分别维护各自的 `<properties>` 版本属性。发版时需手动同步两份版本号，建议：

- 在发版流程文档中明确同步步骤
- 可考虑通过 CI 脚本自动比对两者版本一致性
- 当前两者版本均为 `0.1.0-SNAPSHOT`，已对齐

## 9. 审查结论

本次审查针对 StreamMQ 项目骨架阶段产物，覆盖 10 份 `pom.xml`、22 份 Java 文件、4 份 `application.yml` 与 1 份自动装配注册文件，对照 4 份设计文档进行 6 类检查项的静态走查与工具验证。

审查共发现 9 项问题（阻断 2 / 严重 5 / 一般 2），全部已修复。修复后执行 `mvn validate` 与 `mvn compile` 均 exit code 0，enforcer 4 项规则全部通过，14 模块构建成功。

同时确认 7 项骨架实现与设计文档完全一致（模块清单、包名、版本、配置、自动装配注册、许可证）。

**结论：StreamMQ 骨架阶段通过审查，可进入 MVP（v0.1.0）实现阶段。**

## 10. 附录

### 10.1 审查工具与命令

| 工具 / 命令                              | 用途                         |
| ---------------------------------------- | ---------------------------- |
| `mvn -B -DskipTests -q validate`         | 校验 POM 合规性与依赖完整性  |
| `mvn -B -DskipTests -q compile`          | 编译全工程验证可编译性       |
| `mvn enforcer:enforce`                   | 执行 enforcer 规则检查       |
| `mvn dependency:tree`                     | 依赖树分析与版本收敛验证     |
| 人工走查                                  | 对照设计文档逐项核对         |

### 10.2 审查文件清单

**POM 文件（10 份）：**

```
pom.xml                                          # 根聚合 pom (streammq-parent)
streammq-bom/pom.xml
streammq-core/pom.xml
streammq-redisson-adapter/pom.xml
streammq-spring-boot-starter/pom.xml
streammq-native/pom.xml
streammq-kafka-compat/pom.xml
streammq-amqp-compat/pom.xml
streammq-test/pom.xml
streammq-samples/pom.xml
```

**配置文件（4 份）：**

```
streammq-samples/streammq-sample-quickstart/src/main/resources/application.yml
streammq-samples/streammq-sample-transaction/src/main/resources/application.yml
streammq-samples/streammq-sample-delay/src/main/resources/application.yml
streammq-samples/streammq-sample-orderly/src/main/resources/application.yml
```

**自动装配注册文件（1 份）：**

```
streammq-spring-boot-starter/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
```

**Java 骨架文件（22 份）：**

涵盖各模块 `package-info.java` 及 `streammq-core` 核心 API 骨架，包括：
- 注解层（`annotation/`）：`@EnableStreamMq` / `@StreamMqListener` / `@StreamMqOrderlyListener` / `@StreamMqProducer` / `@StreamMqTransactionListener`
- 消费层（`consumer/`）：`StreamMqConsumer` / `StreamMqConsumerFactory` / `StreamMqListenerContainer`
- 枚举层（`enums/`）：`AcknowledgeMode` / `Action` / `ConsumeMode` / `DelayLevel` / `LocalTransactionState` / `MessageModel`
- 异常层（`exception/`）：`StreamMqException` 体系 7 个异常类
- 监听器层（`listener/`）：`StreamMqListener` / `StreamMqAckListener` / `StreamMqOrderlyListener` 及上下文
- 消息层（`message/`）：`Message` / `MessageBuilder` / `BatchMessage` / `MessageId` / `SendResult` / `SendStatus`
- 生产者层（`producer/`）：`StreamMqProducer` / `StreamMqProducerFactory` / `SendCallback`
- SPI 层（`spi/`）：8 个扩展接口
- 模板层（`template/`）：`StreamMqTemplate`
- 事务层（`transaction/`）：`TransactionCallback` / `TransactionChecker` / `TransactionContext` / `TransactionExecutor`

### 10.3 修复提交关联

本次审查 9 项问题对应一次集中修复，涉及以下文件修改：

1. `streammq-bom/pom.xml` —— 移除 parent、独立 properties、新增 redisson-spring-boot-starter 版本管理（P-01 / P-08）
2. `streammq-core/pom.xml` —— SLF4J 改 provided、Jackson 改 optional（P-02 / P-03）
3. `streammq-redisson-adapter/pom.xml` —— Redisson 改 provided（P-04）
4. `streammq-spring-boot-starter/pom.xml` —— 新增 spring-boot-starter、改用 spring-boot-starter-actuator、改用 redisson-spring-boot-starter（P-05 / P-06 / P-07）
5. `pom.xml`（根）—— pluginManagement 新增 maven-jar-plugin 3.4.2（P-09）

### 10.4 文档关联

| 文档                        | 用途                 |
| --------------------------- | -------------------- |
| [01-PRD.md](01-PRD.md)      | 产品需求文档         |
| [02-architecture.md](02-architecture.md) | 架构设计文档 |
| [03-functional-design.md](03-functional-design.md) | 功能设计文档 |
| [04-detailed-design.md](04-detailed-design.md) | 详细设计文档 |
| **05-review-report.md（本文档）** | 骨架阶段审查修复报告 |

---

*本报告由 StreamMQ 核心团队生成，作为骨架阶段交付物归档。*
