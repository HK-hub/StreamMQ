# Security Policy

## 支持版本

| 版本 | 支持状态 |
| ---- | -------- |
| 0.1.x | ✅ 支持 |

## 报告漏洞

请**不要**通过公开 GitHub Issue 报告安全漏洞。

请通过 [GitHub Security Advisories](https://github.com/HK-hub/StreamMQ/security/advisories/new) 私下报告，我们承诺在 **48 小时内**确认收到，并在修复发布前与报告者协调披露时间线。

报告时请尽量包含：

- 受影响的模块与版本
- 漏洞类型与影响范围
- 复现步骤或 PoC
- 缓解建议（如有）

## 安全设计要点

### 管理端点默认拒绝

`/actuator/streammq` 全部操作（含只读）经 `ManagementAuthenticator` 鉴权，默认 `DenyAllAuthenticator` 拒绝一切访问。开放访问需显式注册 `BasicAuthAuthenticator` / `TokenAuthenticator` / 自定义实现 Bean。

### 序列化器选择

StreamMQ 的**默认序列化器是 `JacksonJsonSerializer`**（0.1.2 起；安全优先，无 Gadget RCE 攻击面）。高性能路径可选 `FurySerializer`（需显式配置 `streammq.producer.serializer`，并添加 `org.apache.fory:fory-core` 依赖——在 `streammq-redisson` 中为 `<optional>true</optional>`，本项目使用 **1.7.3**）。

`FurySerializer` 底层库为 **Apache Fory（原 Apache Fury）**，Maven 坐标 `org.apache.fory:fory-core`（0.11.0 起由 `org.apache.fury:fury-core` 更名）。**版本下限 1.1.0**：1.1.0 之前的 fury-core / fory-core（含本项目 0.1.2 之前使用的 `org.apache.fury:fury-core:0.9.0`）存在 **CVE-2026-50076**（CVSS 9.1——反序列化时可绕过类注册校验，触发 classpath 上的 resolve/readExternal 钩子），本项目受影响并已升级。Java 类名 `FurySerializer` 与 `name()="fury"` 保持不变，以兼容既有配置写法。

**额外的高性能、安全内置序列化器（0.1.3 起）**：除 Fury/Protostuff 外，本项目新增 `FlatBuffersSerializer` 与 `SbeSerializer`，二者均以**纯数据 / 信封**方式工作，**无反序列化代码执行面（无 gadget RCE）**，适合对吞吐与确定性时延敏感、且要求不可信输入零攻击面的场景：

- `FlatBuffersSerializer`：基于 FlatBuffers 的 **FlexBuffers**（schema-less 动态格式）。读取时**零拷贝**（直接基于 ByteBuffer 偏移量寻址，不解析、不实例化任意类），免代码生成，经反射处理任意 POJO。选用需添加 `com.google.flatbuffers:flatbuffers-java`（本项目固定 **24.3.25**）。
- `SbeSerializer`：基于 **SBE（Simple Binary Encoding，FIX 社区标准）**。采用**信封模式**——业务体由严格类型 Jackson 编码为 opaque 字节后，整体放入定长 8 字节消息头 + 单一 `varData(payload)` 字段；读取 `payloadLength()`/`getPayload()` 直接基于偏移量，不解释 body 内部结构。选用需添加 `org.agrona:agrona`（本项目固定 **1.17.1**；`uk.co.real-logic:sbe-tool` **1.18.0** 仅在构建期生成信封的 Encoder/Decoder 桩，不进运行时）。

> **Cap'n Proto 暂未内置**：Cap'n Proto 同样是高性能零拷贝格式，但其 Java 绑定需要 native `capnp` 编译器生成桩代码，本仓库构建环境未预装该工具，故本轮未纳入；后续在 CI 安装 `capnp` 后可补齐。

**Fury 类注册白名单（默认开启）**：若显式选择 `FurySerializer`，其**默认强制类注册白名单（`requireClassRegistration=true`）**：只有显式注册过的类型才能反序列化，未注册类型在反序列化时被拒绝（需预注册业务消息体类型）。仅当显式设为 `false`（宽松模式）时，任意 POJO 才可反序列化，但 Redis 中被写入的字节流也可被反序列化为 classpath 上的任意类——共享/多租户 Redis 场景下是反序列化攻击面（RCE 向量），且该宽松构造受系统属性门禁保护。配置示例：

```yaml
streammq:
  producer:
    serializer: io.github.streammq.adapter.redisson.serializer.FurySerializer
    fury-require-class-registration: true # 默认即 true；显式声明以固定该安全姿态
    fury-registered-classes: com.acme.OrderCreated,com.acme.Payment # 白名单模式下需预注册的业务消息体类型
```

开启白名单后仅允许显式注册过的类反序列化，首次使用前需注册业务消息体类型（`new FurySerializer<>(OrderCreated.class)`、`register(Class)` / `registerAll(Class...)`，或在 Spring 配置中声明 `streammq.producer.fury-registered-classes`）。

**Java API 说明**：`new FurySerializer()` **等价于 `new FurySerializer(true)`，即强制类注册白名单模式**；`new FurySerializer(false)` 才是宽松模式，且必须显式设置 `-Dstreammq.security.allowUnrestrictedSerializer=true` 才能创建（否则抛 `SecurityException`，与 `JdkSerializer.unrestricted()` 同门禁）；即使设置了该属性，构造宽松实例时仍会输出 WARN 风险提醒。另外，`deserialize` 之后的 `isInstance` 校验发生在反序列化**之后**，只能发现结果类型不匹配，无法阻止 gadget 执行——防线是白名单本身。

**传输层 codec 说明（0.1.2 起已加固）**：SDK 对其自有 Redis 键（业务 Stream、DLQ、重试、延迟、事务半消息、注册表等）**显式使用 `StringCodec`**，不再继承下游 `RedissonClient` 的全局 codec，因此默认的 `Kryo5Codec`（未注册限制的 Java 反序列化）不再构成 SDK 侧的额外 gadget 攻击面；载荷本身始终以 Base64 文本写入 Stream Field。仍建议 Redis 仅对可信客户端开放（网络层控制）。

若 Redis 实例**可能被不可信方写入（共享实例、多租户场景）**，请保持 Fury 的类注册白名单开启（默认即是），并确保 Redis 访问受网络层控制。

### 依赖版本与 CVE 策略

- **SDK 自有的可选序列化器依赖由本项目钉版本**：Apache Fory（`org.apache.fory:fory-core`，当前 **1.7.3**，下限 **1.1.0**——见上文 CVE-2026-50076）、FlatBuffers（`com.google.flatbuffers:flatbuffers-java`，当前 **24.3.25**）、SBE 运行时（`org.agrona:agrona`，当前 **1.17.1**；构建期代码生成工具 `uk.co.real-logic:sbe-tool` **1.18.0**）与 Protostuff 在 `streammq-redisson` 中以 `optional` 声明，版本由本项目父 POM 管理。选用时请遵循本项目声明的版本下限，不要回退到 1.1.0 之前的 `fury-core`/`fory-core`。
- **Spring / Jackson / Netty 等宿主依赖由使用方自行管理**：本项目父 POM 中的版本（Spring Boot 3.3.5 → Spring 6.1.14、Jackson 2.17.2，Netty 4.1.x 经 Redisson 传递）**仅用于本仓库自身的构建与测试**，不会强加给使用方；`streammq-bom` 也不再导入 `spring-boot-dependencies`。请通过你自己的 BOM（如 `spring-boot-starter-parent` / `spring-boot-dependencies`）管理这些依赖的版本。
- 上述宿主依赖线（Spring 6.1.x / Jackson 2.17.x / Netty 4.1.x）在 0.1.x 周期内存在上游已披露的安全公告，升级责任与节奏由使用方掌握；本项目将在 **0.2.x** 把自身构建与测试所用的依赖线刷新到当时的新版本。

### 核心库运行时反射（非 Spring 环境）

`streammq-core` 在编译期**不依赖任何 Spring**，但运行时通过**按类名反射**加载 `org.springframework.web.context.request.RequestContextHolder`，用于 Actuator 的 CSRF 同源 / 可信代理来源校验。该反射为**失败开放（fail-open）**，且**仅当 classpath 上存在 Spring Web** 时才会激活；核心库可完全脱离 Spring 独立使用，此时请求来源安全控制自动 no-op（不执行）。

### 凭据管理

StreamMQ 从不将鉴权凭据输出到日志。生产环境建议通过环境变量或密钥管理服务注入 Redis 密码与鉴权凭据。

> **已知限制（内存中的口令）**：管理鉴权口令目前以 `String` 形式存在于 JVM 堆上（Java 字符串不可主动擦除），在堆转储 / 内存快照泄露的场景下存在被读取的理论风险。高敏感环境建议对管理端点做网络隔离（仅内网/堡垒机可达），并采用短周期令牌（`TokenAuthenticator`）以缩小泄露窗口。

## 加固建议

- 为管理端点配置强凭据，并限制 Actuator 的网络暴露面
- Redis 启用密码与 TLS（`rediss://`），遵循最小权限原则
- 及时升级依赖版本；CI 提供 OWASP Dependency-Check 扫描（`mvn verify -Dowasp.skip=false`）
