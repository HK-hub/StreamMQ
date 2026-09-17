# StreamMQ JMH 性能基准测试报告

> **测试日期**: 2026-09-02
> **测试环境**: localhost Redis 7.2, JDK 21, 笔记本级硬件 (i7 / 16GB)
> **JMH 参数**: fork=1, warmup=1×2s / 3×2s, measurement=2×3s / 5×3s
> **Redis 模式**: local (直连 localhost:6379)
>
> **⚠️ 口径说明（勿误读）**：消费者基准跑的是**裸 Redisson 读取路径**（XREADGROUP → 字段解码 → 业务回调 →
> **攒批 XACK，每 100 条 ACK 一次**），**绕过了 listener 容器**（过滤器/拦截器链、指标、重试/DLQ 处理、
> 逐消息 ACK）。因此表中的 `consumeThroughput` 是 SDK 容器路径的**下界**，不能直接当作产品端到端吞吐。
> 容器驱动的基准（含逐消息 ACK 变量）为后续待办项。

---

## 1. 序列化性能

### 测试方法
单线程序列化/反序列化 1KB Pojo，使用 JMH `Blackhole` 防止 JIT 死码消除。

### 结果 (ops/s)

| 序列化器 | Serialize | Deserialize | RoundTrip | Single Serialize | Single Deserialize |
|---|---|---|---|---|---|
| **Fory（Apache Fory，原 Fury）** | **5,205,112** | **4,542,655** | **2,123,210** | **5,215,574** | **4,630,521** |
| Jackson | 401,806 | 914,020 | 192,823 | 391,602 | 912,513 |
| JDK | 455,704 | — | — | 455,704 | — |

### 结论
- Fory 序列化吞吐是 Jackson 的 **~13x**，是 JDK 的 **~11x**
- Fory 反序列化吞吐是 Jackson 的 **~5x**
- Jackson 无需预注册，是默认序列化器（严格类型、无多态反序列化面）；Fory 需预注册类，属显式 opt-in 的高吞吐选项
- 依赖坐标：`org.apache.fory:fory-core`（>= 1.1.0，修复 CVE-2026-50076）

---

## 2. 发送吞吐

### 测试方法
单实例发送消息到 localhost Redis，测量不同负载大小下的吞吐。

### 结果 (ops/s)

| 发送模式 | 100B | 1KB | 10KB |
|---|---|---|---|
| **Async batch (batch=100)** | **12,513** | **11,780** | **8,326** |
| Sync batch (batch=10) | 3,640 | 3,765 | 2,863 |
| Sync single | 3,741 | 3,610 | 2,600 |

### 结论
- 异步批量发送约为同步单条的 **3~4x**
- 10KB 大消息由于网络传输和序列化开销，吞吐下降约 **30%**
- 异步批量是追求高吞吐的首选模式

---

## 3. 消费吞吐

### 测试方法
固定负载下的消费循环：单虚拟线程持续 `syncSend` 补货（**每条一次同步 XADD 往返**），消费侧
XREADGROUP 拉取 → 字段解码 → 业务回调 → 每 100 条攒批 XACK。**绕过 listener 容器**。

### 结果 (ops/s)

| Benchmark | 1KB | 10KB |
|---|---|---|
| **consumeThroughput** | **2,383** | **2,018** |
| serializationRoundTrip (Jackson，纯本地，无网络) | 270,705 | 19,249 |

### 结论
- `consumeThroughput` 约 **2,000~2,500 ops/s**（单线程，1KB 负载），但**该数字受补货端约束**：
  补货线程每条消息一次同步 XADD（实测单条同步发送 2,600~3,741 ops/s），消费数恰为其 0.66~0.78 倍，
  因此它**不能**用于推断消费侧容量或「XACK 同步确认是瓶颈」——本 harness 每 100 条才 ACK 一次，
  并未测量逐消息 ACK 的开销。
- 10KB 负载下 `consumeThroughput` 下降约 **15%**
- **口径**：`consumeThroughput` 是 SDK 容器路径的**下界**，且是补货端约束下的下界；容器驱动的
  端到端吞吐（含逐消息 ACK 变量、过滤器/拦截器/指标链）为 0.2.0 待办项（见 §4）。

---

## 4. 方法学与注意事项

1. **JMH 配置**: harness 注解为 `@Fork(3, warmups=2)`、`@Warmup(3×2s)`、`@Measurement(5×2s)`；
   `main()` 运行器使用 fork=3、warmup 3×2s、measurement 5×3s。**本报告中的历史数字采集于早期配置**，
   与当前 harness 参数不完全一致，0.1.2 发布前将在干净 Redis 上统一重测。
2. **Redis 实例**: 使用本地 Redis（非 Docker），避免容器化带来的额外网络开销。
3. **误差范围**: 99.9% CI 显示部分基准误差较大（尤其 10KB 负载），建议在目标生产硬件上重新实测。
4. **消费基准的已知局限**: 补货端为单线程逐条同步发送（每条一次 XADD 往返），消费数字随补货速率移动，
   未测到消费侧真实上限；且 harness 绕过 listener 容器、每 100 条攒批 ACK。**修正补货端与容器化基准为 0.2.0 待办**。
5. **序列化基准的 JMH 参数**与 README 披露值（fork=1, warmup=1×2s, measurement=2×3s）一致；
   harness 中 `messageCount` 参数未被循环使用（恒为 1000），README 中 `messageCount=1000` 仅为标注。

---

## 5. 原始数据文件

- `target/jmh-serialization.txt` — 序列化基准原始输出
- `target/jmh-template.txt` — 发送基准原始输出
- `target/jmh-consumer.txt` — 消费基准原始输出

---

*报告由 JMH 自动生成，经人工整理。*
