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

`FurySerializer` 默认不启用类注册白名单（开箱即用优先）。若 Redis 实例可能被不可信方写入，请使用 `new FurySerializer(true)` 启用类注册白名单模式，收窄反序列化攻击面。

### 凭据管理

StreamMQ 从不将 accessKey/secretKey 输出到日志。生产环境建议通过环境变量或密钥管理服务注入 Redis 密码与鉴权凭据。

## 加固建议

- 为管理端点配置强凭据，并限制 Actuator 的网络暴露面
- Redis 启用密码与 TLS（`rediss://`），遵循最小权限原则
- 及时升级依赖版本；CI 提供 OWASP Dependency-Check 扫描（`mvn verify -Dowasp.skip=false`）
