# Security Policy

## Supported Versions

| Version | Supported |
| ------- | --------- |
| 0.1.x   | Yes       |

## Reporting a Vulnerability

Please **do not** report security vulnerabilities through public GitHub issues.

Report privately via [GitHub Security Advisories](https://github.com/HK-hub/StreamMQ/security/advisories/new).
We confirm receipt within **48 hours** and coordinate the disclosure timeline with the reporter before a fix is released.

Please include as much of the following as possible:

- Affected module(s) and version(s)
- Vulnerability type and impact
- Reproduction steps or a PoC
- Mitigation suggestions (if any)

## Security Design Highlights

### Management endpoints: deny-by-default, rate-limited, origin-checked

Every `/actuator/streammq/**` operation (including read-only ones) is authenticated by a `ManagementAuthenticator`.
The default is `DenyAllAuthenticator`, which rejects everything (HTTP 401); access must be opened explicitly by
registering `BasicAuthAuthenticator` / `TokenAuthenticator` / a custom implementation as a Bean
(`AllowAllAuthenticator` also works but triggers a start-up `SECURITY ALERT`; suppressible with
`-Dstreammq.admin.startup-warn=false`).

Additional controls:

- **Failure rate limiting** (`RateLimitedAuthenticator`): 10 authentication failures within 60s lock the client out
  for 5 minutes; a success resets the counter.
- **Client identity is not spoofable by default**: clients are aggregated by `remoteAddr`. `X-Forwarded-For` is
  ignored unless you set `streammq.admin.trust-forwarded-headers=true` **and** list the direct peer in
  `streammq.admin.trusted-proxies` (IPv4/IPv6 CIDR; loopback is trusted by default). An unvalidated XFF would let a
  single header bypass the lockout.
- **Destructive operations require explicit confirmation**: deleting a topic requires `confirm={topic}`, deleting a
  DLQ message requires `confirm={messageId}`; mismatches are rejected (HTTP 400) before touching the backend.
- **Same-origin check for writes**: POST/DELETE requests carrying a cross-site `Origin` are rejected with HTTP 403
  (fail-open for non-web contexts or requests without `Origin`); non-browser callers such as curl/SDKs are unaffected.
- **Exposure caveats**: `/streammq/diagnostics/**` (`streammq-diagnostics` module) is a plain MVC endpoint mounted on
  the **application's main port** — it is *not* governed by `management.endpoints.web.exposure.*`. If JMX exposure is
  enabled, exclude StreamMQ (`management.endpoints.jmx.exposure.exclude: streammq`). Restrict both endpoint families
  at the network layer (security group / Ingress).

### Serializer selection

The **default serializer is `JacksonJsonSerializer`** (since 0.1.2; strict types, no gadget RCE surface, human-readable
payloads in Redis). The high-throughput path is opt-in:

- **`FurySerializer`** — underlying library **Apache Fory (formerly Apache Fury)**, Maven coordinates
  `org.apache.fory:fory-core` (renamed from `org.apache.fury:fury-core` as of 0.11.0), version used by this project
  **1.7.3**, **minimum 1.1.0**: every `fury-core` / `fory-core` before 1.1.0 (including `org.apache.fury:fury-core:0.9.0`
  used by StreamMQ before 0.1.2) is affected by **CVE-2026-50076** (CVSS 9.1 — deserialization can bypass the class
  registration check and trigger `resolve`/`readExternal` hooks on the classpath). The Java class name
  `FurySerializer` and `name()="fury"` are unchanged for configuration compatibility.
  - **Class-registration whitelist is enforced by default** (`requireClassRegistration=true`): only explicitly
    registered types can be deserialized; unregistered POJOs are rejected. `new FurySerializer()` **is** the whitelist
    mode (equivalent to `new FurySerializer(true)`); `new FurySerializer(false)` is the unrestricted mode and requires
    the explicit system property `-Dstreammq.security.allowUnrestrictedSerializer=true` (otherwise it throws
    `SecurityException`; even with the property set it logs a WARN).
  - The post-deserialization `isInstance` check only validates the result type — it cannot stop gadget execution. The
    whitelist itself is the real defense. Configure it via `streammq.producer.fury-require-class-registration` and
    `streammq.producer.fury-registered-classes`, or programmatically via `new FurySerializer<>(Order.class)` /
    `register(Class)` / `registerAll(Class...)`.
- **`FlatBuffersSerializer` and `SbeSerializer`** (built in as of **0.1.2**) work as **pure data / envelopes** and have
  **no inner deserialization code-execution surface (no gadget RCE)**:
  - `FlatBuffersSerializer` uses FlatBuffers **FlexBuffers** (schema-less, no code generation). On the wire the format
    carries no class names, no inner polymorphism and no gadget surface; reads copy values into heap objects per field
    (not "zero-copy for the whole payload"). The **read side validates declared lengths** (per-field limit =
    `min(max-message-size, 64MB)`) and **nesting depth** (64 levels); malformed or deeply nested payloads throw
    `SerializationException` instead of allocating based on a 4-byte length field (the old implementation could
    amplify a 4-byte field into a ~2GB allocation). Add `com.google.flatbuffers:flatbuffers-java` (this project pins
    **24.3.25**).
  - `SbeSerializer` uses **SBE (Simple Binary Encoding)** in envelope mode: a fixed 8-byte header + a single
    `varData(payload)` field; `payloadLength()` / `getPayload()` read by offset and never interpret the body. The read
    side validates untrusted bytes: truncation, header `templateId`/`schemaId` mismatch, and declared length
    (negative, larger than the message limit, or larger than the available bytes) all throw `SerializationException`
    and never allocate by the declared length. Runtime dependencies: `org.agrona:agrona` (pinned **1.17.1**);
    `uk.co.real-logic:sbe-tool` (**1.18.0**) only generates the envelope stubs at build time and is not shipped.
  - **Caveat**: "safe for untrusted input" applies to the *inner wire format* only. The SDK outer layer still
    materializes the declared target type by reflection (the consumer's declared generic, or the type resolved from
    the payload's `bodyType`); end-to-end safety depends on the target type being trustworthy (see below).
- **`JdkSerializer`** enforces a JEP 290 `ObjectInputFilter` with a class-name allow-list (the target type + JDK
  primitives + arrays + enums). The payload-derived target type is **not** added to the per-read allow-list unless it
  passes the payload type guard (no "payload self-expanding whitelist"), and there is no blanket `java.lang.` allow
  (so `java.lang.reflect.Proxy` / `java.lang.invoke.SerializedLambda` cannot sneak in). Third-party types must be
  added via `addAllowedClasses(...)`. `JdkSerializer.unrestricted()` is gated by the same
  `-Dstreammq.security.allowUnrestrictedSerializer=true` property and should be a last resort only.
  Since 0.1.2 the filter returns `UNDECIDED` when the JDK reports a depth/reference-count check (`serialClass == null`)
  instead of `REJECTED`, so legitimate payloads containing object back-references (wire format `TC_REFERENCE`)
  deserialize correctly; the merged depth/reference/byte limits still apply.

### Payload-driven target types (PayloadTypeSafety guard)

When a consumer declares no explicit type (a raw implementation without a generic argument), the SDK resolves the
deserialization target from the payload's `bodyType` / `bodyTypeName` fields. Those fields live in Redis and are
attacker-controlled, so they pass through the `PayloadTypeSafety` guard, which normalizes JVM descriptor forms
(`[Ljava.lang.Runtime;` → `java.lang.Runtime`), rejects malformed class names (including `$$Lambda` / `$Proxy`), and
rejects JDK platform namespaces (`java.*` / `javax.*` / `jdk.*` / `sun.*` / `com.sun.*`) plus known gadget namespaces
(Spring, Commons-Collections/BeanUtils/IO, fastjson, Xalan, SnakeYAML, Groovy, cglib, Javassist, Struts2, script
engines, and more). The guard applies to **all serializers** (the consumer fallback chain and transaction check-back
half messages share it — not just `JdkSerializer`).

**The guard is a namespace deny-list: defense in depth, not an integrity boundary.** It cannot enumerate every gadget
class with a `readObject`/`readResolve` hook. In production, always declare the consumer's generic type explicitly
(annotation generic or `targetBodyType`) so the deserialization target is fixed in code. Explicitly declared types come
from code, are trusted, and bypass the guard.

### null / empty input contract (unified as of 0.1.2)

All six built-in serializers (`Jackson` / `JDK` / `Fury` / `Protostuff` / `FlatBuffers` / `SBE`) agree:
`serialize(null)` returns `null`; `deserialize(null | empty array)` returns `null` (no exception). Callers (the message
converters) skip writing the `body` field when the body is null, so a `null` return never enters the Base64 encoding
path.

### Transport codec (hardened as of 0.1.2)

The SDK uses an explicit `StringCodec` for every Redis key it owns (business streams, DLQ, retry, delay, transaction
half messages, registries, broadcast leases) instead of inheriting the downstream `RedissonClient`'s global codec.
Redisson's default `Kryo5Codec(registrationRequired=false)` is unregistered Java deserialization — previously any party
able to write to Redis could plant a gadget payload that executed on the consumer side. Payloads themselves are always
written as Base64 text in stream fields. Still, keep Redis reachable only by trusted clients (network-layer control).

### Dependency versions and CVE policy

- **Optional serializer dependencies are pinned by this project**: Apache Fory (`org.apache.fory:fory-core`, currently
  **1.7.3**, minimum **1.1.0** — see CVE-2026-50076 above), FlatBuffers (`com.google.flatbuffers:flatbuffers-java`,
  **24.3.25**), SBE runtime (`org.agrona:agrona`, **1.17.1**; build-time codegen `uk.co.real-logic:sbe-tool`
  **1.18.0**), and Protostuff (**1.8.0**). They are declared `optional` in `streammq-redisson` and their versions are
  managed by the parent POM. Follow the declared lower bounds; do not downgrade `fory-core` below 1.1.0.
- **Jackson follows the Spring Boot 3.5 baseline (2.21.4)**: the `jackson-bom` import is declared **before** the
  Spring Boot BOM so that the Boot-managed version cannot silently override it, and the pinned line is the one that
  carries the fixes for **GHSA-r7wm-3cxj-wff9** / **GHSA-72hv-8253-57qq** (2.18.8+) and the 2.19–2.21 async-parser
  advisories (2.21.4+).
- **Host dependencies (Spring / Redisson / ...) are managed by the consumer**: the versions in this project's parent
  POM (**Spring Boot 3.5.16**, **Redisson 3.52.0**, Netty 4.1.x transitively via Redisson) are **only used to build
  and test this repository** and are not imposed on consumers; `streammq-bom` deliberately does not import
  `spring-boot-dependencies`. Manage these through your own BOM (`spring-boot-starter-parent` /
  `spring-boot-dependencies`). The 0.1.x line is built against Spring Boot 3.5 and is expected to work on 3.3–3.5.
- **The 0.1.2 build baseline was selected to clear High/Critical advisories in the dependency closure we ship**:
  Spring Boot 3.5.16 (Spring Framework 6.2.x, Spring Data 3.5.x, Micrometer 1.15.x), Redisson 3.52.0 (Netty 4.1.135+),
  Jackson 2.21.4, AssertJ 3.27.7 and commons-compress 1.27.1. Remaining Medium/Low upstream advisories (if any) are
  reported but do not block the gate.
- **CVE scanning channel**: every PR/push **and every release run** a **keyless hard gate** — a CycloneDX SBOM of
  the four published artifacts' dependency closure (`bom-shipped.json`) is scanned with `osv-scanner` (OSV database).
  Any advisory with CVSS ≥ 7.0 in that closure fails the build; the full JSON report is uploaded as a build artifact.
  The release channel (`release.yml`) runs the **same** `sbom-scan` job and its `publish` job depends on it (together
  with the `guard` publish-set consistency job), so a High/Critical advisory in the shipped closure blocks the Central
  upload even for a tag that was pushed directly and never went through CI. OWASP
  Dependency-Check / NVD **deep scan** runs weekly and on demand (it requires `secrets.NVD_API_KEY` and is an
  enhancement, not the default gate). Local
  equivalent: `mvn -DskipTests -Dcyclonedx.skip=false -Dcyclonedx.skipNotDeployed=false
  org.cyclonedx:cyclonedx-maven-plugin:makeAggregateBom`. Security advisories are published through
  [GitHub Security Advisories](https://github.com/HK-hub/StreamMQ/security/advisories).

### Runtime reflection in streammq-core (non-Spring environments)

`streammq-core` has **no compile-time Spring dependency**, but it loads
`org.springframework.web.context.request.RequestContextHolder` **by class name at runtime** for the Actuator CSRF
same-origin / trusted-proxy origin checks. The reflection is **fail-open** and only activates when Spring Web is on the
classpath; the core library is fully usable without Spring, in which case request-origin security control degrades to a
no-op.

### Credential handling

StreamMQ never writes authentication credentials (Redis password, admin credentials) to logs. Inject Redis passwords
and authenticator credentials via environment variables or a secret manager.

> **Known limitation (credentials in memory)**: admin authenticator passwords currently live on the JVM heap as `String`
> (Java strings cannot be erased), a theoretical risk in heap-dump / memory-snapshot scenarios. For high-sensitivity
> environments, isolate the management endpoints at the network layer (internal network / bastion only) and prefer
> short-lived tokens (`TokenAuthenticator`) to shrink the exposure window.

## Hardening Recommendations

- Give the management endpoints strong credentials and restrict Actuator's network exposure.
- Enable Redis authentication and TLS (`rediss://`), following least privilege.
- For shared / multi-tenant Redis, keep the Fury class-registration whitelist enabled (it is the default).
- Keep dependencies up to date; run OWASP Dependency-Check (`mvn verify -Dowasp.skip=false`).
- Declare consumer payload types explicitly (annotation generics / `targetBodyType`) instead of relying on
  payload-driven type resolution.

## 中文摘要（Chinese summary）

- **报告漏洞**：请走 [GitHub Security Advisories](https://github.com/HK-hub/StreamMQ/security/advisories/new) 私下报告，
  48 小时内确认；支持版本为 0.1.x。
- **管理端点**：`/actuator/streammq/**` 默认 `DenyAllAuthenticator` 全拒绝；失败限流 60s 内 10 次即锁定 5 分钟；
  客户端身份默认只取不可伪造的 `remoteAddr`（`X-Forwarded-For` 需显式开启并配置可信代理 CIDR）；
  写/删操作有同源校验与 `confirm` 二次确认；`/streammq/diagnostics/**` 挂主端口、不受 Actuator 暴露治理。
- **序列化器**：默认 `JacksonJsonSerializer`（严格类型、无 gadget 面）；`FurySerializer` 需显式 opt-in，
  底层 Apache Fory **1.7.3**（下限 1.1.0，更早版本受 CVE-2026-50076 影响），**默认强制类注册白名单**，
  宽松构造需 `-Dstreammq.security.allowUnrestrictedSerializer=true` 且仍打 WARN。
  `FlatBuffersSerializer` / `SbeSerializer` 为 **0.1.2 内置**：内层线格式为纯数据/信封，无 gadget RCE 面；
  读取侧分别有单字段长度上限（`min(消息上限, 64MB)`）与 64 层深度上限、SBE 信封头与声明长度校验，畸形载荷抛
  `SerializationException`。`JdkSerializer` 使用 JEP 290 白名单过滤器（载荷派生的目标类型不自动放行；
  自 0.1.2 起对 JDK 的深度/引用计数检查返回 UNDECIDED，含对象回引用的合法载荷可正常反序列化）。
- **载荷驱动类型**：统一经过 `PayloadTypeSafety`（形态归一化 + 危险命名空间黑名单），对**所有**序列化器生效；
  它是纵深防御而非完整性边界，生产环境请显式声明消费者泛型。
- **依赖与 CVE**：Jackson 已升级到 **2.21.4**（修复 GHSA-r7wm-3cxj-wff9 / GHSA-72hv-8253-57qq，且 `jackson-bom`
  声明在 Boot BOM 之前以免被覆盖）；构建基线为 Spring Boot 3.5.16 / Redisson 3.52.0，宿主依赖（Spring/Netty）
  版本由使用方自管。CI **与发布通道**（`release.yml` 的 `sbom-scan` job，`publish` 依赖它）的**默认 CVE 硬门禁**
  都是**无需密钥**的发布构件依赖闭包扫描（CycloneDX SBOM + `osv-scanner`，任意 CVSS ≥ 7.0 即阻断发布；
  因此即使 tag 直接推送、从未跑过 CI，High/Critical 公告也会在 Central 上传前被拦住）；
  OWASP Dependency-Check（NVD）为**增强扫描**，仅在配置
  `secrets.NVD_API_KEY` 时于每周计划任务中执行（`mvn verify -Dowasp.skip=false`），缺失密钥不会使发布通道变红。
- **其他**：内置序列化器 `serialize(null)→null`、`deserialize(null|空)→null`；SDK 自有 Redis 键显式使用
  `StringCodec`；凭据不落日志（口令在堆上无法擦除，高敏感环境请做网络隔离并优先短周期令牌）。
