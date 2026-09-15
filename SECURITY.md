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

StreamMQ 的**默认序列化器是 `JacksonJsonSerializer`**（0.1.2 起；安全优先，无 Gadget RCE 攻击面）。高性能路径可选 `FurySerializer`（需显式配置 `streammq.producer.serializer`，并添加 `fury-core` 依赖——`<optional>true</optional>`）。

**Fury 类注册白名单（默认开启）**：若显式选择 `FurySerializer`，其**默认强制类注册白名单（`requireClassRegistration=true`）**：只有显式注册过的类型才能反序列化，未注册类型在反序列化时被拒绝（需预注册业务消息体类型）。仅当显式设为 `false`（宽松模式）时，任意 POJO 才可反序列化，但 Redis 中被写入的字节流也可被反序列化为 classpath 上的任意类——共享/多租户 Redis 场景下是反序列化攻击面（RCE 向量），且该宽松构造受系统属性门禁保护。配置示例：

```yaml
streammq:
  producer:
    serializer: io.github.streammq.adapter.redisson.serializer.FurySerializer
    fury-require-class-registration: true # 默认即 true；显式声明以固定该安全姿态
```

开启白名单后仅允许显式注册过的类反序列化，首次使用前需注册业务消息体类型（`new FurySerializer<>(OrderCreated.class)` 或 `register(Class)` / `registerAll(Class...)`）。

**Java API 说明**：`new FurySerializer()` **等价于 `new FurySerializer(true)`，即强制类注册白名单模式**；`new FurySerializer(false)` 才是宽松模式，且必须显式设置 `-Dstreammq.security.allowUnrestrictedSerializer=true` 才能创建（否则抛 `SecurityException`）。另外，`deserialize` 之后的 `isInstance` 校验发生在反序列化**之后**，只能发现结果类型不匹配，无法阻止 gadget 执行——防线是白名单本身。

**传输层 codec 说明（0.1.2 起已加固）**：SDK 对其自有 Redis 键（业务 Stream、DLQ、重试、延迟、事务半消息、注册表等）**显式使用 `StringCodec`**，不再继承下游 `RedissonClient` 的全局 codec，因此默认的 `Kryo5Codec`（未注册限制的 Java 反序列化）不再构成 SDK 侧的额外 gadget 攻击面；载荷本身始终以 Base64 文本写入 Stream Field。仍建议 Redis 仅对可信客户端开放（网络层控制）。

若 Redis 实例**可能被不可信方写入（共享实例、多租户场景）**，请保持 Fury 的类注册白名单开启（默认即是），并确保 Redis 访问受网络层控制。

### 核心库运行时反射（非 Spring 环境）

`streammq-core` 在编译期**不依赖任何 Spring**，但运行时通过**按类名反射**加载 `org.springframework.web.context.request.RequestContextHolder`，用于 Actuator 的 CSRF 同源 / 可信代理来源校验。该反射为**失败开放（fail-open）**，且**仅当 classpath 上存在 Spring Web** 时才会激活；核心库可完全脱离 Spring 独立使用，此时请求来源安全控制自动 no-op（不执行）。

### 凭据管理

StreamMQ 从不将鉴权凭据输出到日志。生产环境建议通过环境变量或密钥管理服务注入 Redis 密码与鉴权凭据。

> **已知限制（内存中的口令）**：管理鉴权口令目前以 `String` 形式存在于 JVM 堆上（Java 字符串不可主动擦除），在堆转储 / 内存快照泄露的场景下存在被读取的理论风险。高敏感环境建议对管理端点做网络隔离（仅内网/堡垒机可达），并采用短周期令牌（`TokenAuthenticator`）以缩小泄露窗口。

## 加固建议

- 为管理端点配置强凭据，并限制 Actuator 的网络暴露面
- Redis 启用密码与 TLS（`rediss://`），遵循最小权限原则
- 及时升级依赖版本；CI 提供 OWASP Dependency-Check 扫描（`mvn verify -Dowasp.skip=false`）
