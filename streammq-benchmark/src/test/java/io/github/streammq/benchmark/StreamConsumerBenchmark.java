/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.benchmark;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.adapter.redisson.template.DefaultStreamMessageTemplate;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.test.ContainerizedRedisServer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.redisson.Redisson;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消费吞吐基准：真实端到端读路径（XREADGROUP → 字段解码 → 业务回调 → 攒批 XACK）。
 *
 * <p><b>口径（B1 修复后）</b>：测量前先向 topic 预灌足量积压，测量期间不允许"补货端决定消费数字"：
 *
 * <ol>
 *   <li><b>积压构建阶段</b>：一次性批量异步 XADD 预灌 {@value #DEFAULT_BACKLOG} 条（可用 {@value #BACKLOG_PROPERTY}
 *       覆盖）。该阶段吞吐只作为"补货端参考指标"打印，不参与有效性判定；
 *   <li><b>测量阶段</b>：不再持续补货，只有低水位补货线程在未读积压跌破水位时补一批（{@value #FEEDER_THREADS_PROPERTY} 个虚拟线程，每批
 *       {@value #FEEDER_BURST} 条），把消费端变成唯一瓶颈；
 *   <li><b>有效性判定</b>：改为"积压是否足以支撑测量窗口"——测量期间只要出现一次空读（积压被消费耗尽、 补货没跟上），该轮数字即被判 INVALID，{@link
 *       #main(String[])} 以非零码退出并提示加大 backlog； 未出现空读则该轮数字有效、退出码为 0。
 * </ol>
 *
 * <p>历史背景：R4-38 的"补货端 ≥ 3 × 消费端"判定在"每条消息一次同步 XADD"的口径下结构性必然失败——稳态 消费吞吐只能等于补货吞吐（仓库自有数据即
 * 0.66–0.78×），因此 CI 必然非零退出。本实现把补货端移出稳态测量 路径，用"积压是否被耗尽"这一直接证据替代该比例判定。
 *
 * <p><b>运行时参数</b>（系统属性，pom 中通过 exec:exec 透传；子进程与 fork 均继承）：
 *
 * <ul>
 *   <li>{@code -Dstreammq.benchmark.backlog=N}：预灌积压条数，默认 {@value #DEFAULT_BACKLOG}（10KB 负载时 约占用
 *       500MB Redis 内存，按可用内存/Redis 容量调整）；
 *   <li>{@code -Dstreammq.benchmark.feederThreads=N}：低水位补货线程数，默认 {@value
 *       #DEFAULT_FEEDER_THREADS}（出现 INVALID 时优先加大该值）；
 *   <li>{@code -Dstreammq.redis.mode=docker|local}、{@code -Dstreammq.redis.host/port}、 {@code
 *       -Dstreammq.benchmark.allowFlush=true}（仅 local 模式必填）。
 * </ul>
 *
 * <p>JMH 参数（fork/预热/测量）<b>只由本类注解决定</b>，{@link #main(String[])} 不再用 {@code OptionsBuilder}
 * 覆盖，避免"注解一套、实际一套"的双源；需要临时覆盖时用 JMH 命令行参数。
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 2)
@Measurement(iterations = 3, time = 3)
@Fork(1)
public class StreamConsumerBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(StreamConsumerBenchmark.class);

    private static final String TOPIC = "consumer-benchmark-stream";
    private static final String CONSUMER_GROUP = "benchmark-consumer-group";
    private static final String CONSUMER_NAME = "benchmark-consumer";
    private static final int BATCH_SIZE = 100;

    /** 预灌积压条数（正整数，非法值回退默认）：-Dstreammq.benchmark.backlog=N */
    static final String BACKLOG_PROPERTY = "streammq.benchmark.backlog";

    static final int DEFAULT_BACKLOG = 50_000;

    /** 低水位补货线程数（正整数，非法值回退默认）：-Dstreammq.benchmark.feederThreads=N */
    static final String FEEDER_THREADS_PROPERTY = "streammq.benchmark.feederThreads";

    static final int DEFAULT_FEEDER_THREADS = 2;

    /** 补货批量：一次异步管道提交的消息条数（越大越不容易被补货路径拖住，但占用更多在途内存）。 */
    private static final int FEEDER_BURST = 500;

    /** 补货线程空闲轮询间隔。 */
    private static final long FEEDER_IDLE_NANOS = TimeUnit.MILLISECONDS.toNanos(2);

    /**
     * 结果目录/文件名（相对 fork 工作目录，即模块根）：每个 payloadSize 一份口径记录，避免 fork 互相覆盖。
     *
     * <p>前缀刻意不用 {@code jmh-}：CI 的产物收集与回归脚本按 {@code jmh*.json} 匹配，口径记录不是 JMH 结果， 不应混进回归比对。
     */
    static final String RESULT_DIR = "target";

    static final String VALIDITY_FILE_PREFIX = "consume-validity-";
    static final String VALIDITY_FILE_SUFFIX = ".json";
    private static final String RESULT_FILE = RESULT_DIR + "/jmh-consumer.json";

    /**
     * 消费端必须与生产者对齐到实际落盘的 Stream Key：模板生产者写入 {@code streammq:msg:{topic}}， 而非裸 {@code TOPIC}。
     * 此前的实现直接在裸 key 上建组并 XREADGROUP，读到的永远是空流。
     */
    private static final String STREAM_KEY = StreamMQKeys.topicStream("", TOPIC);

    /** 空读退避窗口：使用服务端阻塞（XREADGROUP BLOCK），避免高频空读自旋 */
    private static final Duration EMPTY_READ_BLOCK = Duration.ofMillis(100);

    /**
     * 纯本地基准的状态（消息构造 + Jackson 往返）：不启动容器、不连 Redis，因此这两个 trial 不会付出 "预灌积压 + 容器启动"的代价（只有 {@link
     * RedisState} 承担）。
     */
    @State(Scope.Benchmark)
    public static class LocalState {

        @Param({"1024", "10240"})
        private int payloadSize;

        private String payload;

        @Setup(Level.Trial)
        public void setup() {
            payload = repeatPayload(payloadSize);
        }

        String payload() {
            return payload;
        }
    }

    /**
     * 消费基准的 Redis 状态：预灌积压 →（测量期间）低水位补货维持不空 → trial 收尾落盘口径记录。
     *
     * <p>预灌与补货都复用同一条消息（body/keys 相同）：被测消费路径的每条开销（Base64 字段解码 + Jackson 反序列化 + 回调 + 攒批
     * ACK）与消息内容无关，复用可让补货端尽可能快、不成为瓶颈。
     */
    @State(Scope.Benchmark)
    public static class RedisState {

        @Param({"1024", "10240"})
        private int payloadSize;

        private RedissonClient redisson;
        private RStream<String, String> stream;
        private DefaultMessageConverter converter;
        private StreamMessageTemplate template;
        private ContainerizedRedisServer redisServer;
        private Message<String> message;
        private ConsumeValidityReport validity;

        private final List<Thread> feeders = new ArrayList<>();

        private volatile boolean feeding = true;

        private long trialStartNanos;

        @Setup(Level.Trial)
        public void setup() {
            int backlog = intProperty(BACKLOG_PROPERTY, DEFAULT_BACKLOG);
            int feederThreads = intProperty(FEEDER_THREADS_PROPERTY, DEFAULT_FEEDER_THREADS);
            validity = new ConsumeValidityReport(payloadSize, backlog, feederThreads);

            String mode = System.getProperty("streammq.redis.mode", "docker");
            if ("docker".equalsIgnoreCase(mode)) {
                LOG.info("Starting Redis via Testcontainers...");
                redisServer = new ContainerizedRedisServer();
                redisServer.start();
            }

            String host =
                    redisServer != null
                            ? redisServer.getHost()
                            : System.getProperty("streammq.redis.host", "localhost");
            int port =
                    redisServer != null
                            ? redisServer.getPort()
                            : Integer.getInteger("streammq.redis.port", 6379);

            Config config = new Config();
            config.useSingleServer()
                    .setAddress("redis://" + host + ":" + port)
                    .setDatabase(0)
                    .setConnectionPoolSize(16)
                    .setConnectionMinimumIdleSize(4)
                    .setConnectTimeout(5000)
                    .setTimeout(5000);
            config.setCodec(StringCodec.INSTANCE);

            redisson = Redisson.create(config);
            LOG.info("RedissonClient connected to {}:{}", host, port);

            DefaultMessageConverter newConverter =
                    new DefaultMessageConverter(new JacksonJsonSerializer<>());
            this.converter = newConverter;
            RedissonStreamProducer producer =
                    RedissonStreamProducer.builder()
                            .redisson(redisson)
                            .namespace("")
                            .group("benchmark-producer")
                            .converter(newConverter)
                            .defaultTimeoutMillis(3000)
                            .maxLen(0)
                            .compressThreshold(0)
                            .maxMessageSize(512L * 1024 * 1024)
                            .build();
            template =
                    new DefaultStreamMessageTemplate(producer, "benchmark-producer", newConverter);
            stream = redisson.getStream(STREAM_KEY);

            message =
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("consumer-test")
                            .keys("key-benchmark")
                            .body(repeatPayload(payloadSize))
                            .build();

            requireFlushAllowed(redisServer);
            redisson.getKeys().flushdb();

            try {
                stream.createGroup(
                        StreamCreateGroupArgs.name(CONSUMER_GROUP)
                                .makeStream()
                                .id(new StreamMessageId(0, 0)));
            } catch (Exception ignored) {
                // 组已存在（同一 fork 的复用场景）视为就绪
            }

            preloadBacklog(backlog, feederThreads);
            startFeeders(feederThreads, backlog);
            trialStartNanos = System.nanoTime();
        }

        @TearDown(Level.Trial)
        public void teardown() {
            feeding = false;
            for (Thread thread : feeders) {
                thread.interrupt();
            }

            // setup 未走完（例如预灌失败）时不写窗口时长，避免把 nanoTime 基准的 0 值算成天文数字
            validity.recordTrialMillis(
                    trialStartNanos > 0
                            ? TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - trialStartNanos)
                            : 0);
            try {
                validity.writeTo(validityFile(payloadSize));
            } catch (IOException ex) {
                // 记录写不出去 → main() 会因"缺少记录"判 INVALID（fail closed），此处只需醒目
                LOG.error("Failed to write consume validity record: {}", ex.getMessage());
            }

            if (redisson != null) {
                requireFlushAllowed(redisServer);
                redisson.getKeys().flushdb();
                redisson.shutdown();
            }
            if (redisServer != null) {
                redisServer.stop();
            }
        }

        /** 积压构建阶段：一次性批量异步 XADD，吞吐只作为补货端参考指标打印/记录。 */
        private void preloadBacklog(int backlog, int feederThreads) {
            long start = System.nanoTime();
            int sent = 0;
            while (sent < backlog) {
                int burst = Math.min(FEEDER_BURST, backlog - sent);
                if (!sendBurst(burst)) {
                    throw new IllegalStateException(
                            "Backlog preload failed after " + sent + "/" + backlog + " messages");
                }
                sent += burst;
            }
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            validity.recordPreloadMillis(millis);
            LOG.info(
                    "Backlog preloaded: {} messages of {} B payload in {} ms ({} msg/s, batched"
                        + " async XADD, burst={}, feederThreads={}) — reference rate only, not a"
                        + " validity criterion",
                    backlog,
                    payloadSize,
                    millis,
                    String.format(Locale.ROOT, "%.0f", validity.preloadMsgPerSec()),
                    FEEDER_BURST,
                    feederThreads);
        }

        /** 低水位补货：未读积压跌破水位才补一批，正常情况下消费端始终有满批可读。 */
        private void startFeeders(int feederThreads, int backlog) {
            long lowWatermark = Math.max(backlog / 4L, (long) FEEDER_BURST * 2);
            for (int i = 0; i < feederThreads; i++) {
                Thread thread =
                        Thread.ofVirtual()
                                .name("benchmark-feeder-" + i)
                                .start(() -> feedLoop(lowWatermark));
                feeders.add(thread);
            }
            LOG.info(
                    "Keep-alive feeder started: threads={}, lowWatermark={} messages, burst={}",
                    feederThreads,
                    lowWatermark,
                    FEEDER_BURST);
        }

        private void feedLoop(long lowWatermark) {
            while (feeding) {
                if (validity.unread() >= lowWatermark) {
                    LockSupport.parkNanos(FEEDER_IDLE_NANOS);
                    continue;
                }
                if (sendBurst(FEEDER_BURST)) {
                    validity.recordReplenished(FEEDER_BURST);
                }
            }
        }

        /** 一批异步 XADD（管道化），全部完成后计数；失败只告警——补货不足会表现为空读并被判 INVALID。 */
        private boolean sendBurst(int count) {
            CompletableFuture<?>[] futures = new CompletableFuture<?>[count];
            try {
                for (int i = 0; i < count; i++) {
                    futures[i] = template.asyncSend(message);
                }
                CompletableFuture.allOf(futures).join();
                return true;
            } catch (RuntimeException ex) {
                LOG.warn("Feeder send failed: {}", ex.getMessage());
                return false;
            }
        }

        RStream<String, String> stream() {
            return stream;
        }

        DefaultMessageConverter converter() {
            return converter;
        }

        ConsumeValidityReport validity() {
            return validity;
        }
    }

    /**
     * 真实端到端消费路径：XREADGROUP 拉取 → converter 反序列化转换 → 业务回调 → XACK。
     *
     * <p>每次调用处理 {@value #BATCH_SIZE} 条完整消息（含网络与 ACK）。测量期间只有低水位补货线程维持
     * "不空"，因此本基准测的是消费侧能力；一旦读到空批（积压被耗尽），即记一次空读供有效性判定使用。
     */
    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    @SuppressWarnings("unchecked")
    public void consumeThroughput(RedisState state, Blackhole blackhole) throws Exception {
        RStream<String, String> stream = state.stream();

        StreamMessageConcurrentlyConsumer<String> consumer = (msg, ctx) -> ConsumeAction.SUCCESS;

        int processed = 0;
        while (processed < BATCH_SIZE) {
            // 空读改用服务端阻塞（XREADGROUP ... BLOCK 100），与真实消费者 pullBlock 语义一致：
            // 命令频率从约 1000 次/秒骤降至最多 10 次/秒，避免高频非阻塞空读在本地/共享 Redis 上
            // 触发并发 XADD + XREADGROUP 偶发 5s+ 响应停顿（Windows 移植版 Redis 的已知缺陷）。
            var messages =
                    stream.readGroup(
                            CONSUMER_GROUP,
                            CONSUMER_NAME,
                            StreamReadGroupArgs.neverDelivered()
                                    .count(BATCH_SIZE)
                                    .timeout(EMPTY_READ_BLOCK));
            if (messages == null || messages.isEmpty()) {
                // 积压被消费耗尽、补货没跟上：该轮数字不能代表消费能力（B1：main() 据此判 INVALID）
                state.validity().recordStarvedRead();
                continue;
            }
            List<StreamMessageId> ids = new ArrayList<>(messages.size());
            for (var entry : messages.entrySet()) {
                // 真实消费路径包含字段解码（Base64 + 反序列化），而非复用本地对象
                Message<?> msg =
                        state.converter().fromStreamFields(entry.getValue(), String.class, TOPIC);
                consumer.onMessage((Message<String>) msg, createContext(TOPIC, CONSUMER_GROUP));
                blackhole.consume(msg);
                ids.add(entry.getKey());
                processed++;
            }
            stream.ack(CONSUMER_GROUP, ids.toArray(new StreamMessageId[0]));
            state.validity().recordBatch(messages.size());
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void messageCreateAndConsume(LocalState state, Blackhole blackhole) throws Exception {
        StreamMessageConcurrentlyConsumer<String> consumer = (msg, ctx) -> ConsumeAction.SUCCESS;

        for (int i = 0; i < BATCH_SIZE; i++) {
            Message<String> msg =
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("perf")
                            .keys("key-" + i)
                            .body(state.payload())
                            .build();
            consumer.onMessage(msg, createContext(TOPIC, CONSUMER_GROUP));
            blackhole.consume(msg);
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void serializationRoundTrip(LocalState state, Blackhole blackhole) {
        JacksonJsonSerializer<String> serializer = new JacksonJsonSerializer<>();
        for (int i = 0; i < BATCH_SIZE; i++) {
            Message<String> msg =
                    MessageBuilder.<String>withTopic(TOPIC).body(state.payload()).build();
            byte[] bytes = serializer.serialize(msg.getBody(), String.class);
            blackhole.consume(serializer.deserialize(bytes, String.class));
        }
    }

    public static void main(String[] args) throws RunnerException {
        Path resultDir = Path.of(RESULT_DIR);
        try {
            clearStaleValidityRecords(resultDir);
        } catch (IOException ex) {
            LOG.warn("Could not clear stale validity records: {}", ex.getMessage());
        }

        // fork/预热/测量参数只由类注解决定（不再在此覆盖），消除"注解一套、实际一套"的参数双源；
        // 需要临时覆盖时用 JMH 命令行参数（-f/-wi/-i/-w/-r）。
        Options opt =
                new OptionsBuilder()
                        .include(StreamConsumerBenchmark.class.getSimpleName())
                        .result(RESULT_FILE)
                        .resultFormat(ResultFormatType.JSON)
                        .build();
        new Runner(opt).run();

        if (evaluateValidity(resultDir) != 0) {
            // 无效轮次必须非零退出：CI/调用方应丢弃该轮数字，不得回填文档
            System.exit(1);
        }
    }

    /**
     * 汇总本次运行落盘的口径记录并判定：全部有效返回 0；任一无效/缺失返回 1（并打印原因）。
     *
     * <p>缺失记录同样判为无效（fail closed）：无法证明有效的轮次不允许被当成有效结论使用。
     */
    static int evaluateValidity(Path resultDir) {
        List<Path> records = listValidityRecords(resultDir);
        if (records.isEmpty()) {
            System.err.println(
                    "[StreamMQ benchmark] INVALID RUN: no consume validity record ("
                            + VALIDITY_FILE_PREFIX
                            + "*"
                            + VALIDITY_FILE_SUFFIX
                            + ") under "
                            + resultDir
                            + " — consumeThroughput did not run, so the consume numbers cannot be"
                            + " certified.");
            return 1;
        }
        int exitCode = 0;
        for (Path record : records) {
            try {
                ConsumeValidityReport report = ConsumeValidityReport.read(record);
                System.out.println(
                        "[StreamMQ benchmark] "
                                + record.getFileName()
                                + ": "
                                + report.summaryLine());
                String reason = report.invalidReason();
                if (reason != null) {
                    System.err.println(
                            "[StreamMQ benchmark] INVALID RUN ("
                                    + record.getFileName()
                                    + "): "
                                    + reason);
                    exitCode = 1;
                } else if (report.supplyTight()) {
                    System.err.println(
                            "[StreamMQ benchmark] WARNING ("
                                    + record.getFileName()
                                    + "): average XREADGROUP batch is "
                                    + String.format(Locale.ROOT, "%.1f", report.avgBatchSize())
                                    + "/"
                                    + ConsumeValidityReport.BATCH_SIZE
                                    + " — supply was occasionally tight (batch got thinned); the"
                                    + " number is still valid but consider raising"
                                    + " -Dstreammq.benchmark.backlog.");
                }
            } catch (IOException | RuntimeException ex) {
                System.err.println(
                        "[StreamMQ benchmark] INVALID RUN ("
                                + record.getFileName()
                                + "): validity record unreadable ("
                                + ex.getMessage()
                                + ").");
                exitCode = 1;
            }
        }
        if (exitCode == 0) {
            System.out.println(
                    "[StreamMQ benchmark] Consume benchmark validity: OK ("
                            + records.size()
                            + " record(s); the pre-loaded backlog was never exhausted).");
        }
        return exitCode;
    }

    /** 本次运行产生的口径记录（按文件名排序；不含 JMH 结果文件 jmh-consumer.json）。 */
    static List<Path> listValidityRecords(Path resultDir) {
        if (!Files.isDirectory(resultDir)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(resultDir)) {
            return files.filter(Files::isRegularFile)
                    .filter(
                            path -> {
                                String name = path.getFileName().toString();
                                return name.startsWith(VALIDITY_FILE_PREFIX)
                                        && name.endsWith(VALIDITY_FILE_SUFFIX);
                            })
                    .sorted()
                    .toList();
        } catch (IOException ex) {
            return List.of();
        }
    }

    /** 清理上一次运行的残留记录，避免旧记录把本轮判定污染成"有效"。 */
    static void clearStaleValidityRecords(Path resultDir) throws IOException {
        for (Path record : listValidityRecords(resultDir)) {
            Files.deleteIfExists(record);
        }
    }

    /** 每个 payloadSize 一份记录（不同参数在各自 fork 中运行，落盘不可共用同一文件）。 */
    static Path validityFile(int payloadSize) {
        return Path.of(RESULT_DIR, VALIDITY_FILE_PREFIX + payloadSize + VALIDITY_FILE_SUFFIX);
    }

    /** 读取整数型基准参数：缺失/非法/非正数时回退默认值并告警——一个笔误不应该把 harness 变成空跑或 OOM。 */
    static int intProperty(String name, int defaultValue) {
        String raw = System.getProperty(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            int value = Integer.parseInt(raw.trim());
            if (value <= 0) {
                LOG.warn(
                        "Ignoring non-positive -D{}={} (using default {})",
                        name,
                        raw,
                        defaultValue);
                return defaultValue;
            }
            return value;
        } catch (NumberFormatException ex) {
            LOG.warn("Ignoring unparsable -D{}={} (using default {})", name, raw, defaultValue);
            return defaultValue;
        }
    }

    /**
     * 防误删守卫：flushdb 会清空目标 Redis 当前库的全部数据。
     *
     * <p>docker 模式由 Testcontainers 拉起<b>独占实例</b>（随机映射端口、本进程创建并销毁），清空的一定是 自己的容器，因此自动放行；切到 {@code
     * -Dstreammq.redis.mode=local} 直连本地/共享 Redis 时必须显式追加 {@code
     * -Dstreammq.benchmark.allowFlush=true} 授权，否则拒绝执行。
     */
    private static void requireFlushAllowed(ContainerizedRedisServer ownedServer) {
        if (ownedServer != null) {
            return;
        }
        if (!Boolean.getBoolean("streammq.benchmark.allowFlush")) {
            throw new IllegalStateException(
                    "Destructive operation blocked: benchmark flushdb would ERASE ALL DATA in the"
                            + " current Redis database. Re-run with"
                            + " -Dstreammq.benchmark.allowFlush=true to confirm the target Redis is"
                            + " disposable.\n"
                            + "破坏性操作已拦截：基准测试将执行 flushdb 清空当前 Redis 数据库的全部数据。请追加"
                            + " -Dstreammq.benchmark.allowFlush=true 显式确认目标 Redis 可被清空后重试。");
        }
    }

    private static String repeatPayload(int size) {
        char[] chars = new char[size];
        java.util.Arrays.fill(chars, 'x');
        return new String(chars);
    }

    private static ConsumeContext createContext(String topic, String consumerGroup) {
        return new ConsumeContext() {
            @Override
            public String topic() {
                return topic;
            }

            @Override
            public String consumerGroup() {
                return consumerGroup;
            }

            @Override
            public String consumerName() {
                return CONSUMER_NAME;
            }

            @Override
            public int reconsumeTimes() {
                return 0;
            }

            @Override
            public long bornTimestamp() {
                return System.currentTimeMillis();
            }

            @Override
            public String bornHost() {
                return "benchmark-host";
            }

            @Override
            public Map<String, String> messageTrack() {
                return Collections.emptyMap();
            }

            @Override
            public String ext(String key) {
                return null;
            }
        };
    }
}
