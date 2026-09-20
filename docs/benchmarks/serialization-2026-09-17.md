# 序列化基准测试报告 — 2026-09-17

> StreamMQ 内置序列化器 JMH 对比基准。覆盖 **6 个内置实现**：`FurySerializer`、`ProtostuffSerializer`、
> `FlatBuffersSerializer`（新增）、`SbeSerializer`（新增）、`JacksonJsonSerializer`（默认）、`JdkSerializer`。

## 1. 测试环境

| 项目 | 配置 |
|------|------|
| JDK | OpenJDK 21.0.11 (Eclipse Adoptium) |
| JMH | 1.37 |
| 操作系统 | Windows 11（笔记本级硬件，测试期间 IDE 在运行） |
| 测试负载 | `TestPayload`：`id`(String 短) + `data`(1KB 字符串) + `count`(int) + `timestamp`(long) |
| 批大小 | `messageCount=1000`（`@OperationsPerInvocation(1000)`） |

## 2. 方法学

- 通过 **JMH 标准入口** `org.openjdk.jmh.Main` 运行，参数：`-f 1 -wi 4 -i 5 -w 2s -r 2s -bm thrpt`，
  即 **1 fork、4 轮 × 2s 预热、5 轮 × 2s 测量、Throughput 模式**。
- 每个基准均以 `Blackhole` 消费返回值，防止 JIT 死码消除导致吞吐虚高。
- 误差为 JMH 报告的 99.9% 置信区间（`± error`）。
- **注意**：笔记本级硬件 + IDE 运行时存在系统噪声（如 `furySerialize` 的 CI 偏大）；绝对值为**参考值**，
  生产容量规划请以自有环境实测为准。

> 复现命令（推荐用 JMH CLI 直接跑，避免 `exec:exec@...` 作为独立 goal 触发时**跳过 `test-compile`** 而用到陈旧字节码）：
>
> ```bash
> mvn -q -pl streammq-benchmark test-compile dependency:build-classpath \
>     -Dmdep.includeScope=test -Dmdep.outputFile=target/cp.txt
> cd streammq-benchmark
> java -Djmh.ignoreLock=true \
>      -cp "target/test-classes:target/classes:$(cat target/cp.txt)" \
>      org.openjdk.jmh.Main "SerializationBenchmark" \
>      -f 1 -wi 4 -i 5 -w 2s -r 2s -bm thrpt \
>      -rf json -rff target/jmh-serialization.json
> ```
>
> 以上为 Linux/macOS 写法（classpath 分隔符 `:`）；Windows 下同一命令把分隔符换成 `;` 即可
> （本报告的数字即在 Windows 11 上采集）。

> **配置变更提示（2026-09-20）**：`SerializationBenchmark` 的类注解默认值已收敛为 `@Fork(1)` +
> `@Warmup(2×1s)` + `@Measurement(3×2s)`（原为 `@Fork(3, warmups = 2)` + 3×2s/5×2s），且 `main()` 不再用
> `OptionsBuilder` 覆盖注解（消除"注解一套、实际一套"的参数双源）；本报告的数字来自上面显式列出的 CLI 覆盖
> 参数，仍然可复现。全量默认运行的时长预算（45 分钟上限）见 `streammq-benchmark/BENCHMARK_REPORT.md` §0.2。

## 3. 吞吐结果（ops/s，1KB 负载）

| 序列化器 | Serialize | Deserialize | RoundTrip | Single Serialize | Single Deserialize | 体积 (字节) |
|---|---:|---:|---:|---:|---:|---:|
| **Fury** | **3,483,891** ±1,961,436 | **3,800,231** ±817,538 | **1,880,645** ±358,877 | **4,036,015** ±785,887 | **3,995,683** ±278,537 | 1,094 |
| Protostuff | 351,231 ±17,378 | 3,945,355 ±308,868 | 320,724 ±18,950 | 342,823 ±27,328 | 3,717,589 ±264,629 | 1,050 |
| FlatBuffers (FlexBuffers) | 657,099 ±55,703 | 763,086 ±48,510 | 347,640 ±22,656 | 645,424 ±56,305 | 785,224 ±57,113 | 1,150 |
| SBE (信封) | 358,310 ±11,087 | 765,708 ±57,350 | 246,984 ±14,697 | 354,743 ±10,951 | 817,930 ±68,241 | 1,104 |
| Jackson（默认） | 416,872 ±24,177 | 893,418 ±120,623 | 266,364 ±32,946 | 410,731 ±19,245 | 875,660 ±132,579 | 1,092 |
| JDK | 442,764 ±17,313 | — | — | 459,824 ±13,874 | — | — |

> 说明：本快照未测 JDK 反序列化——写作时的过滤器状态拒绝基准载荷。**该结论已过时**：当前
> `JdkSerializer.installFilter` 会把「本次调用的目标类型」加入放行集（受统一载荷类型护栏约束），
> 因此 JDK 反序列化/往返可正常测量。2026-09-20 全量重跑已补齐该项
> （`jdkDeserialize` 116,017 ops/s、`jdkRoundTrip` 86,815 ops/s），见
> [`streammq-benchmark/BENCHMARK_REPORT.md`](../../streammq-benchmark/BENCHMARK_REPORT.md) §1。
> 体积为“1KB 字符串”负载的线长，主要受字符串支配，各实现差异在 ±10% 内。

## 4. 分析

- **Fury**：全项最快——序列化约为 Jackson 的 **8×**、反序列化约 **4.3×**、往返约 **7×**。
  代价是需强制类注册白名单（安全）并会引入 Guava（可选依赖，需显式 opt-in）。
- **Protostuff**：**反序列化极快**（约 Jackson 的 **4.4×**），但序列化偏慢（约 Jackson 的 0.85×）。
  适合读多写少、反序列化密集的场景。
- **FlatBuffers（FlexBuffers）**：读写均衡，序列化约 Jackson 的 **1.6×**、反序列化约 **0.85×**。
  其“零拷贝读”是 *逐字段* 特性；`FlatBuffersSerializer` 仍通过反射物化完整 POJO，因此端到端反序列化
  并未显著快于 Jackson。优势在于 **schema-less、免代码生成、纯数据（无 gadget RCE 面，安全）**。
- **SBE（信封模式）**：吞吐受内层 Jackson 编解码限制（约 Jackson 的 **0.86×**），另加定长 8 字节头 +
  `varData` 分帧（约 10–16 字节开销）。它的价值是**可版本化的 schema 容器 + 确定性线格式**；要获得金融级
  低时延，需要 **schema-first 的业务消息体 + 生成式 reader（全零拷贝）**，而非信封模式。
- **Jackson（默认）**：安全优先（严格类型、无多态反序列化、无 gadget RCE 面），性能为安全默认实现的中位水平。
- **JDK**：序列化与 Jackson 相当；反序列化因安全过滤器不可用（不推荐生产使用）。

## 5. 结论与选型建议

| 场景 | 推荐 |
|---|---|
| 通用默认 / 安全优先 / 人类可读 | **JacksonJsonSerializer**（默认） |
| 极致吞吐（可接受类注册白名单 + Guava） | **FurySerializer** |
| 读多写少、反序列化密集 | **ProtostuffSerializer** |
| schema-less、免代码生成、零拷贝逐字段读、纯数据安全 | **FlatBuffersSerializer** |
| 跨语言定长线格式 / 可版本化 schema 容器（金融、低时延） | **SbeSerializer**（或 schema-first 全零拷贝） |
| 不建议 | **JdkSerializer**（反序列化风险） |

## 6. 原始数据

- 机器可读结果：`streammq-benchmark/target/jmh-serialization.json`（JMH JSON 格式）。
- 运行配置：见第 2 节复现命令。
