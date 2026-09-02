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

StreamMQ 的**默认序列化器是 `FurySerializer`**（`streammq.producer.serializer` 默认值）。

**默认不强制类注册（宽松模式，`requireClassRegistration=false`）**：任意 POJO 开箱即用，但 Redis 中被写入的字节流可被反序列化为 classpath 上的任意类——共享/多租户 Redis 场景下是反序列化攻击面（RCE 向量）。Spring Boot 用户可通过 `streammq.producer.fury-require-class-registration` 开关控制是否强制类注册白名单：

```yaml
streammq:
  producer:
    serializer: io.github.streammq.adapter.redisson.serializer.FurySerializer
    fury-require-class-registration: true # 开启类注册白名单（生产建议）
```

开启白名单后仅允许显式注册过的类反序列化，首次使用前需注册业务消息体类型（`new FurySerializer<>(OrderCreated.class)` 或 `register(Class)` / `registerAll(Class...)`）。

**Java API 说明**：`new FurySerializer()` 为宽松模式（与 Spring 装配默认一致）；`new FurySerializer(true)` 或 `new FurySerializer<>(Xxx.class)` 为强制类注册白名单模式。宽松构造会输出一条 WARN 提醒；已评估并接受风险的场景可设置 `-Dstreammq.security.allowUnrestrictedSerializer=true` 抑制该提醒。

若 Redis 实例**可能被不可信方写入（共享实例、多租户场景），请务必开启类注册白名单**以收窄反序列化攻击面。若希望开箱即用任意 POJO 且接受 JSON 的性能与体积开销，可显式切换为 `JacksonJsonSerializer`（无需预注册类型）。

### 凭据管理

StreamMQ 从不将鉴权凭据输出到日志。生产环境建议通过环境变量或密钥管理服务注入 Redis 密码与鉴权凭据。

> **已知限制（内存中的口令）**：管理鉴权口令目前以 `String` 形式存在于 JVM 堆上（Java 字符串不可主动擦除），在堆转储 / 内存快照泄露的场景下存在被读取的理论风险。高敏感环境建议对管理端点做网络隔离（仅内网/堡垒机可达），并采用短周期令牌（`TokenAuthenticator`）以缩小泄露窗口。

## 加固建议

- 为管理端点配置强凭据，并限制 Actuator 的网络暴露面
- Redis 启用密码与 TLS（`rediss://`），遵循最小权限原则
- 及时升级依赖版本；CI 提供 OWASP Dependency-Check 扫描（`mvn verify -Dowasp.skip=false`）
