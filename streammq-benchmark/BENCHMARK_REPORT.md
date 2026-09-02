# StreamMQ JMH 性能基准测试报告

> **测试日期**: 2026-09-02
> **测试环境**: localhost Redis 7.2, JDK 21, 笔记本级硬件 (i7 / 16GB)
> **JMH 参数**: fork=1, warmup=1×2s / 3×2s, measurement=2×3s / 5×3s
> **Redis 模式**: local (直连 localhost:6379)

---

## 1. 序列化性能

### 测试方法
单线程序列化/反序列化 1KB Pojo，使用 JMH `Blackhole` 防止 JIT 死码消除。

### 结果 (ops/s)

| 序列化器 | Serialize | Deserialize | RoundTrip | Single Serialize | Single Deserialize |
|---|---|---|---|---|---|
| **Fury** | **5,205,112** | **4,542,655** | **2,123,210** | **5,215,574** | **4,630,521** |
| Jackson | 401,806 | 914,020 | 192,823 | 391,602 | 912,513 |
| JDK | 455,704 | — | — | 455,704 | — |

### 结论
- Fury 序列化吞吐是 Jackson 的 **~13x**，是 JDK 的 **~11x**
- Fury 反序列化吞吐是 Jackson 的 **~5x**
- Jackson 无需预注册，兼容性最佳；Fury 需预注册类，但性能极高

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
端到端完整消费路径：XREADGROUP 拉取 → 字段解码 → 业务回调 → XACK 确认。
测试前预灌 300 条消息，并通过独立虚拟线程持续补货，避免空读。

### 结果 (ops/s)

| Benchmark | 1KB | 10KB |
|---|---|---|
| **consumeThroughput** | **2,383** | **2,018** |
| serializationRoundTrip (Jackson, 含网络) | 270,705 | 19,249 |
| messageCreateAndConsume (纯内存) | 5,857,147 | 6,134,699 |

### 结论
- 端到端消费吞吐约 **2,000~2,500 ops/s**（单线程，1KB 负载）
- 消费路径瓶颈主要在 Redis 网络往返和 XACK 同步确认
- 纯内存消息处理可达 **600万 ops/s**，说明反序列化和回调开销极低
- 10KB 负载下消费吞吐下降约 **15%**

---

## 4. 方法学与注意事项

1. **JMH 配置**: 为控制总耗时，使用 fork=1 和较少的迭代次数。生产级基准建议使用 fork=2+, warmup=3×2s, measurement=5×3s 以获取更稳定的数字。
2. **Redis 实例**: 使用本地 Redis（非 Docker），避免容器化带来的额外网络开销。
3. **误差范围**: 99.9% CI 显示部分基准误差较大（尤其 10KB 负载），建议在目标生产硬件上重新实测。
4. **消费基准**: 已修正早期方法学缺陷（空读往返、缺 ACK、灌数耗尽），当前测量的是真实完整消费路径。

---

## 5. 原始数据文件

- `target/jmh-serialization.txt` — 序列化基准原始输出
- `target/jmh-template.txt` — 发送基准原始输出
- `target/jmh-consumer.txt` — 消费基准原始输出

---

*报告由 JMH 自动生成，经人工整理。*
