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

`FurySerializer` 默认强制类注册白名单（secure-by-default）：仅允许显式注册过的类反序列化，未注册类型在反序列化前直接拒绝。首次使用前需调用 `Fury.register(Class)` 注册业务消息体类型。仅当 Redis 实例完全可信时，才可显式关闭白名单换取任意 POJO 开箱即用：`new FurySerializer(false)`——此举会重新扩大反序列化攻击面，请谨慎评估。

### 凭据管理

StreamMQ 从不将鉴权凭据输出到日志。生产环境建议通过环境变量或密钥管理服务注入 Redis 密码与鉴权凭据。

> **已知限制（内存中的口令）**：管理鉴权口令目前以 `String` 形式存在于 JVM 堆上（Java 字符串不可主动擦除），在堆转储 / 内存快照泄露的场景下存在被读取的理论风险。高敏感环境建议对管理端点做网络隔离（仅内网/堡垒机可达），并采用短周期令牌（`TokenAuthenticator`）以缩小泄露窗口。

## 加固建议

- 为管理端点配置强凭据，并限制 Actuator 的网络暴露面
- Redis 启用密码与 TLS（`rediss://`），遵循最小权限原则
- 及时升级依赖版本；CI 提供 OWASP Dependency-Check 扫描（`mvn verify -Dowasp.skip=false`）
