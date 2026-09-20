# StreamMQ JMH 性能基准测试报告

> **全部数字**: 2026-09-20 全量重跑（当前 harness，第六轮修复后的注解参数）
> **历史数字**: 2026-09-02 采集（旧 harness，口径差异见 §0.4；仅作对照，不再作为参考值）
> **测试环境**: Intel Core i7-14700KF / 32GB / Windows 10 (10.0.28000) x64 /
> JDK 21.0.11 (OpenJDK 64-Bit Server VM) / 独立本地 Redis 8.8.0（端口 6380，`--save '' --appendonly no`，
> 基准期间独占）——**非生产硬件，绝对值仅供量级参考**
> **JMH 参数（当前唯一真源 = 各 Benchmark 类注解）**: `@Fork(1)`；
> serialization / template 为 `@Warmup(2×1s)` + `@Measurement(3×2s)`，consumer 为 `@Warmup(1×2s)` + `@Measurement(3×3s)`
> **实测总时长**: 三组基准串行 **14 分 25 秒**（serialization 9:42 / template 3:17 / consumer 1:26；
> 含上游 `install -DskipTests` 步骤共 16 分 15 秒），均在 §0.2 预算内，三组退出码均为 0。
>
> ⚠️ **口径说明（勿误读）**
> 1. 消费基准跑的是**裸 Redisson 读取路径**（XREADGROUP → 字段解码 → 业务回调 → **攒批 XACK，每 100 条 ACK 一次**），
>    **绕过了 listener 容器**（过滤器/拦截器链、指标、重试/DLQ 处理、逐消息 ACK）。因此 `consumeThroughput` 是
>    SDK 容器路径的**下界**，不能直接当作产品端到端吞吐；容器驱动的基准为后续待办项。
> 2. 消费口径已改为**先灌积压再测消费**（B1）：测量期间只做低水位补货，消费端是唯一瓶颈；若测量期间积压被耗尽
>    （出现空读）该轮直接判 INVALID。本轮两次运行 `starvedReads=0`、`avgBatchSize=100.0`、`valid=true`，
>    证据见 `target/consume-validity-*.json`（§3.3 摘录）。

---

## 0. 重跑方式与实测总时长（发布前必做）

### 0.1 命令序列（干净机器；需 JDK 21 + Docker）

```bash
# 1) 先把上游构件装进本地仓库（-pl 不带 -am 时 reactor 构件解析不到）
mvn -pl streammq-benchmark -am install -DskipTests

# 2) 三类基准全量跑（docker 模式 = Testcontainers 拉起独占 Redis，不需要本地 Redis）
mvn -Pbenchmark -pl streammq-benchmark test

# 或分组执行（CI 采用这种方式，便于分别设超时）
mvn -Pbenchmark -pl streammq-benchmark exec:exec@benchmark-serialization
mvn -Pbenchmark -pl streammq-benchmark exec:exec@benchmark-template
mvn -Pbenchmark -pl streammq-benchmark exec:exec@benchmark-consumer

# 3) 直连已有 Redis（非独占实例：必须显式授权 flushdb）
mvn -Pbenchmark -pl streammq-benchmark exec:exec@benchmark-consumer \
    -Dstreammq.redis.mode=local -Dstreammq.benchmark.allowFlush=true \
    -Dstreammq.benchmark.backlog=50000
```

参数真源：fork/预热/测量**只由各 Benchmark 类注解决定**（`main()` 已不再用 `OptionsBuilder` 覆盖，
B4 修复）；需要临时覆盖时用 JMH 命令行参数（`-f/-wi/-i/-w/-r`）。系统性属性由 `streammq-benchmark/pom.xml`
的 `benchmark` profile 透传到子进程：`streammq.redis.mode/host/port`、`streammq.benchmark.backlog`、
`streammq.benchmark.feederThreads`、`streammq.benchmark.allowFlush`。

### 0.2 时间预算（声明参数下的期望时长；静态上限 45 分钟）

| 基准类 | JMH 运行次数 | 期望时长 | 2026-09-20 实测 | CI 步骤超时 |
|---|---|---|---|---|
| `SerializationBenchmark` | 30 方法 × 2 模式 | ≈10–12 分钟 | 9 分 42 秒 | 20 分钟 |
| `StreamMessageTemplateBenchmark` | 3 方法 × 2 模式 × 3 负载 | ≈5–7 分钟 | 3 分 17 秒 | 12 分钟 |
| `StreamConsumerBenchmark` | 消费 1 × 2 负载 + 本地 2 × 2 负载 | ≈2 分钟（含 2 次预灌） | 1 分 26 秒 | 10 分钟 |
| **合计** | | **≈17–21 分钟** | **14 分 25 秒** | **job 45 分钟** |

> 实测低于期望的原因：双模式下 `SampleTime` 的每轮开销低于 `Throughput` 的估算上限，且单 fork 无 JVM 重复启动；
> 期望值按上限保守估计是刻意留出的余量。静态校验 `BenchmarkBudgetTest`（`mvn test` 即跑）按
> "注解 × @Param 组合 × 模式数 × fork 数 + 每 fork 开销"换算，要求 ≤ 45 分钟。B2 修复前的
> `@Fork(3, warmups = 2)` + 30 方法 × 双模式结构性需要 **≈100 分钟**，加上模板基准 ≈35 分钟，远超 CI 的 60 分钟上限。

### 0.3 消费基准的口径有效性门禁（B1）

- 参数：`-Dstreammq.benchmark.backlog`（默认 50000，10KB 负载约 500MB Redis 内存）、
  `-Dstreammq.benchmark.feederThreads`（默认 2）。
- 判定：测量期间出现任何**空读**（积压被消费耗尽、补货没跟上）→ 打印 `INVALID RUN` 并以**非零码退出**；
  未出现 → 结果有效、退出码 0。**有效运行不得非零退出，无效运行必须非零退出**。
- 证据：每个 payloadSize 由各自 fork 落盘 `target/consume-validity-<payload>.json`，含预灌速率（补货端参考指标）、
  测量窗口、消费条数、补货条数与速率、平均批大小、空读次数；CI 会把它们一并作为产物上传。
- 处理 INVALID：首选加大 `-Dstreammq.benchmark.backlog`，或加大 `-Dstreammq.benchmark.feederThreads`。
- 单测：`ConsumeValidityReportTest`（判定规则 / JSON 往返 / 残留清理 / 非法参数回退 / fail-closed）。

### 0.4 口径变更（相对 2026-09-02 的采集配置）

| 维度 | 旧 harness（历史数字） | 当前 harness |
|---|---|---|
| 消费补货 | 每条消息一次**同步 XADD**，持续补货（消费数字被补货端封顶） | 先预灌 5 万条积压，测量期间仅**低水位批量补货**（500 条/批） |
| 有效性判定 | `feeder ≥ 3 × consume`（结构上必然失败：消费≈补货） | 测量期间是否出现空读（积压是否被耗尽） |
| fork/迭代 | `@Fork(3, warmups=2)` + 3×2s/5×2s（另被 `main()` 覆盖） | 注解单源：`@Fork(1)` + 2×1s/3×2s（consumer 1×2s/3×3s） |
| flushdb 守卫 | docker 模式也需 `-Dstreammq.benchmark.allowFlush=true` | docker 模式（独占容器）自动放行；local 模式仍需显式授权 |

---

## 1. 序列化性能

> 采集：2026-09-20，`SerializationBenchmark`，1KB `TestPayload`，单线程，双模式
> （`Throughput` 吞吐 + `SampleTime` 延迟分布），参数为类注解默认值。原始输出 `target/jmh-serialization.json`。

### 1.1 吞吐（ops/s；± 为 99.9% CI 半宽，见 §1.3）

| 序列化器 | Serialize | Deserialize | RoundTrip | Single Serialize | Single Deserialize |
|---|---|---|---|---|---|
| **Fory（Apache Fory，原 Fury）** | **4,323,664** ±4,716,551 | **4,374,579** ±3,425,750 | **2,090,991** ±1,405,021 | **4,442,831** ±5,318,555 | **4,322,816** ±2,613,395 |
| Jackson（默认） | 419,588 ±66,216 | 899,889 ±209,498 | 279,826 ±87,096 | 414,844 ±93,653 | 906,830 ±471,422 |
| JDK | 428,211 ±660,737 | 116,017 ±45,947 | 86,815 ±55,284 | 386,110 ±568,211 | 115,415 ±39,804 |
| Protostuff | 352,662 ±28,130 | 4,024,571 ±8,520,236 | 333,142 ±85,245 | 354,637 ±23,889 | 3,777,286 ±11,247,368 |
| FlatBuffers | 694,202 ±391,258 | 736,299 ±1,212,434 | 374,788 ±142,741 | 687,661 ±488,422 | 807,548 ±509,494 |
| SBE | 358,537 ±137,972 | 789,301 ±250,417 | 242,670 ±77,436 | 355,722 ±78,788 | 795,995 ±93,726 |

`Single` = 每次调用新建序列化器/不带预热复用路径；其余为复用实例。JDK 反序列化在当前过滤器下可正常测量
（见 §1.3 与 `docs/benchmarks/serialization-2026-09-17.md` 的更正说明）。

### 1.2 单次操作延迟（`SampleTime` 模式，µs）

| 序列化器 | Serialize avg / p99 | Deserialize avg / p99 | RoundTrip avg / p99 |
|---|---|---|---|
| Fory | 0.211 / 0.608 | 0.215 / 0.613 | 0.440 / 1.214 |
| Jackson | 2.382 / 3.636 | 1.013 / 1.835 | 3.613 / 5.157 |
| JDK | 2.133 / 3.431 | 8.520 / 11.885 | 11.204 / 15.147 |
| Protostuff | 2.776 / 4.208 | 0.231 / 0.652 | 2.979 / 4.430 |
| FlatBuffers | 1.399 / 2.731 | 1.251 / 2.224 | 2.604 / 4.086 |
| SBE | 2.762 / 4.136 | 1.203 / 2.076 | 4.055 / 5.560 |

### 1.3 读数说明与结论

- **± 列是 99.9% CI 半宽**（n=3 时 t≈31.6），比常见的 99% CI 宽得多；个别项（`protostuffDeserialize`、
  `furySerialize` 系列）CI 宽于点估计，是短迭代 + 单 fork 的正常现象——只按量级解读，不做精细对比。
  需要收窄时用 JMH 命令行加严（如 `-f 3 -wi 4 -i 5`）。
- Fory 序列化吞吐是 Jackson 的 **~10.3×**、JDK 的 **~10.1×**；反序列化是 Jackson 的 **~4.9×**、JDK 的 **~37.7×**；
  RoundTrip 是 Jackson 的 **~7.5×**；p99 延迟（0.6 µs 级）比 Jackson（1.8–5.2 µs 级）低约一个量级。
- Jackson 无需预注册，是默认序列化器（严格类型、无多态反序列化面）；Fory 需预注册类，属显式 opt-in。
- 依赖坐标：`org.apache.fory:fory-core`（>= 1.1.0，修复 CVE-2026-50076）。
- 与 2026-09-02/09-17 快照同量级（Fury 序列化 3.5–5.2M），差异来自 fork/迭代参数与机器负载，非代码性能变化。
  六序列化器的选型建议见 [`docs/benchmarks/serialization-2026-09-17.md`](../docs/benchmarks/serialization-2026-09-17.md)。

---

## 2. 发送吞吐

> 采集：2026-09-20，`StreamMessageTemplateBenchmark`（走完整 `StreamMessageTemplate` 生产者路径），
> 单线程、双模式、3 档负载（100B / 1KB / 10KB），参数为类注解默认值。原始输出 `target/jmh-template.json`。

### 2.1 吞吐（ops/s；± 为 99.9% CI 半宽）

| 发送模式 | 100B | 1KB | 10KB |
|---|---|---|---|
| **asyncSendThroughput（一次 100 条异步并发，全部 join）** | **17,654** ±12,852 | **16,944** ±10,955 | **12,527** ±17,251 |
| syncSendSingle（单条同步，对照基线） | 3,932 ±5,196 | 3,886 ±4,154 | 2,859 ±3,641 |
| syncSendThroughput（一次 100 条同步串行） | 3,949 ±2,274 | 3,499 ±5,366 | 2,692 ±2,882 |

### 2.2 单次发送延迟（`SampleTime`，ms）

| 发送模式 | 100B avg / p99 | 1KB avg / p99 | 10KB avg / p99 |
|---|---|---|---|
| asyncSendThroughput（单条摊薄） | 0.056 / 0.106 | 0.058 / 0.127 | 0.073 / 0.129 |
| syncSendSingle | 0.250 / 0.868 | 0.272 / 0.858 | 0.345 / 0.615 |
| syncSendThroughput（单条摊薄） | 0.241 / 0.387 | 0.263 / 0.661 | 0.339 / 0.505 |

> 批量方法带 `@OperationsPerInvocation(100)`，`SampleTime` 数字已按 100 条摊薄：异步行是**100 条并发全部完成**
> 的平均单条耗时（含等待），同步行是 100 条串行发送的平均单条耗时。

### 2.3 结论

- 异步并发发送约为同步单条的 **4.5×**（100B：17,654 vs 3,932）；10KB 仍保持 **4.4×**（12,527 vs 2,859）。
- 10KB 相对 100B：异步 -29%（17,654→12,527）、同步单条 -27%（3,932→2,859），受单实例 Redis 带宽支配。
- 同步 100 条串行与单条同步几乎同档（100B 3,949 vs 3,932）：串行路径没有摊薄收益，批量收益只在异步并发时出现。
- 与 2026-09-02 旧配置（Async 12,513 / 11,780 / 8,326）相比 +30~50%，来自独占 Redis 实例、更大内存机器与
  参数口径统一，不是代码性能变化。

---

## 3. 消费吞吐

> 采集：2026-09-20，`StreamConsumerBenchmark`，1KB / 10KB 两档负载，单线程，`@Fork(1)` + `@Warmup(1×2s)` +
> `@Measurement(3×3s)`。运行参数：`-Dstreammq.redis.mode=local -Dstreammq.redis.port=6380
> -Dstreammq.benchmark.allowFlush=true -Dstreammq.benchmark.backlog=50000 -Dstreammq.benchmark.feederThreads=2`
> （独立 Redis 8.8.0 实例，基准期间独占）。原始输出 `target/jmh-consumer.json`，口径证据 `target/consume-validity-*.json`。

### 3.1 吞吐（ops/s；± 为 99.9% CI 半宽）

| Benchmark | 1KB | 10KB | 说明 |
|---|---|---|---|
| `consumeThroughput` | **12,572** ±8,878 | **7,521** ±19,395 | 先灌 5 万条积压、测量期低水位补货，消费端是唯一瓶颈 |
| `serializationRoundTrip` | 312,685 ±121,317 | 21,813 ±2,658 | 纯本地 Jackson 往返（无 Redis），消费路径的下界参考 |
| `messageCreateAndConsume` | 7,651,608 ±12,452,619 | 8,047,889 ±15,084,578 | 消息构造 + 回调派发（无 Redis），验证本地开销不构成瓶颈 |

### 3.2 逐迭代原始值（`consumeThroughput`，3 轮 × 3s）

| 负载 | 第 1 轮 | 第 2 轮 | 第 3 轮 | 均值 |
|---|---|---|---|---|
| 1KB | 12,273 | 12,310 | 13,134 | **12,572** |
| 10KB | 8,744 | 6,819 | 6,999 | **7,521** |

> 10KB 轮间波动约 ±20%，与 99.9% CI 半宽一致；短迭代 + 单 fork 下只按量级解读。

### 3.3 口径有效性证据（B1 门禁；两次运行均 `valid=true`、退出码 0）

| 负载 | 预灌（条 / 耗时 / 速率） | 测量窗口 | 消费条数 | 补货条数（速率） | 批次数 / 平均批大小 | 空读 | 判定 |
|---|---|---|---|---|---|---|---|
| 1KB | 50,000 / 3,692ms / 13,543 msg/s | 11,147ms | 160,400 | 123,500（11,079 msg/s） | 1,604 / **100.0** | **0** | `valid` |
| 10KB | 50,000 / 4,524ms / 11,052 msg/s | 11,213ms | 88,900 | 52,000（4,637 msg/s） | 889 / **100.0** | **0** | `valid` |

- `avgBatchSize=100.0` + `starvedReads=0` ⇒ 消费端每次 XREADGROUP 都取到满批，从未因积压耗尽而空转；
  同一窗口内**消费量 > 补货量**（1KB 160,400 > 123,500；10KB 88,900 > 52,000），差额由预灌积压填补
  —— 这正是 B1 要的“消费端是唯一瓶颈”，`supplyTight=false` 亦为此结论的机器可读证据。
- 对照旧口径：同一 harness 修复前 `consumeThroughput` 只有 2,383 / 2,018 ops/s，恰为同步补货速率的 0.66–0.78×
  （被补货端封顶的伪数字）。新口径 **12,572 / 7,521 ops/s**，1KB 提升 5.3×、10KB 3.7×。

### 3.4 结论与边界

- 裸读取路径（XREADGROUP → 字段解码 → 业务回调 → 每 100 条攒批 XACK）在本机单实例 Redis 上的量级：
  **1KB ≈ 1.26 万条/s、10KB ≈ 0.75 万条/s**（约 12MB/s 的消费侧数据面）。
- 10KB 吞吐低于 1KB 约 40%：瓶颈在 Redis 侧的读放大与网络传输字节数，而非本地解码
  （`serializationRoundTrip` 10KB 仍有 21,813 ops/s，比消费快 2.9×）。
- 该数字**是 SDK 容器路径的下界**：不含过滤器/拦截器链、指标、重试/DLQ、逐消息 ACK；测量期与补货共享同一实例。
  **不可直接用于生产容量规划**，需在目标环境复测。容器驱动的端到端基准为 0.2.0 待办（§4.4）。

---

## 4. 方法学与注意事项

1. **JMH 配置**：fork/预热/测量只由各类注解决定（`main()` 不覆盖）；当前为单 fork、短迭代，全量实测 14 分 25 秒，
   上限 45 分钟由 `BenchmarkBudgetTest` 静态校验。需要更细的延迟分布时用 JMH 命令行参数临时加严
   （例如 `-f 3 -wi 4 -i 5`），并在结果里注明覆盖参数。
2. **Redis 实例**：默认 docker 模式（Testcontainers 独占实例，随机端口）；`-Dstreammq.redis.mode=local` 可直连本地
   Redis，此时 `-Dstreammq.benchmark.allowFlush=true` 为必填（否则 flushdb 守卫拒绝执行）。
   本报告的消费数字采集于**独立**的本地实例（6380 端口，独占、禁持久化），而非与开发数据共用的 6379。
3. **误差范围**：± 为 99.9% CI 半宽（n=3 时 t≈31.6），比常见的 99% CI 宽得多；10KB 负载尤甚。
   本报告数字只用于量级判断，容量规划请在目标生产硬件上重新实测。
4. **消费基准的已知局限**：仍绕过 listener 容器、每 100 条攒批 ACK；补货虽改为批量低水位补货，但测量期间与消费
   共享同一 Redis 实例（这会压低绝对值，属保守方向）。容器驱动的端到端基准为 0.2.0 待办。
5. **序列化基准的 JMH 参数**：本报告（§1）与 README 中的数字均为**类注解默认值**下的采集结果；
   `docs/benchmarks/serialization-2026-09-17.md` 是带显式 CLI 覆盖参数（`-f 1 -wi 4 -i 5 -w 2s -r 2s`）的独立快照，
   两者同量级但不逐项可比，引用时须注明来源与参数。

---

## 5. 原始数据文件

- `target/jmh-serialization.json` — 序列化基准原始输出
- `target/jmh-template.json` — 发送基准原始输出
- `target/jmh-consumer.json` — 消费基准原始输出
- `target/consume-validity-<payload>.json` — 消费口径记录（预灌/补货速率、批大小、空读次数；B1 门禁的证据）
- 每个 JSON 内嵌 `jdkVersion` / `jvmArgs` / `mode` / `forks` / `warmupIterations` / `measurementIterations`，
  引用数字时可连同这些字段一起引用（本报告 §1–§3 的采集参数均为注解默认值 + §3 开头的 `-D` 属性）。

---

*报告由 JMH 自动生成，经人工整理。*
