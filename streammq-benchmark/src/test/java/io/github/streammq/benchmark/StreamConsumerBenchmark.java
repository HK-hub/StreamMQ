/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.benchmark;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducerFactory;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import io.github.streammq.adapter.redisson.template.DefaultStreamMessageTemplate;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.test.ContainerizedRedisServer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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

@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(3)
public class StreamConsumerBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(StreamConsumerBenchmark.class);

    private static final String TOPIC = "consumer-benchmark-stream";
    private static final String CONSUMER_GROUP = "benchmark-consumer-group";
    private static final String CONSUMER_NAME = "benchmark-consumer";
    private static final int PRE_SEND_COUNT = 500;
    private static final int BATCH_SIZE = 100;

    private RedissonClient redisson;
    private StreamMessageTemplate template;
    private ContainerizedRedisServer redisServer;

    /** 持续灌数线程：保证消费基准始终有真实消息可读（避免测量空 XREADGROUP RTT） */
    private volatile Thread feeder;

    private volatile boolean feeding = true;

    private DefaultMessageConverter converter;

    @Param({"1024", "10240"})
    private int payloadSize;

    private String payload;

    @Setup(Level.Trial)
    public void setup() {
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

        DefaultMessageConverter converter =
                new DefaultMessageConverter(new JacksonJsonSerializer<>());
        this.converter = converter;
        RedissonStreamProducerFactory producerFactory =
                new RedissonStreamProducerFactory(redisson, converter);
        template =
                new DefaultStreamMessageTemplate(producerFactory, "benchmark-producer", converter);

        char[] chars = new char[payloadSize];
        java.util.Arrays.fill(chars, 'x');
        payload = new String(chars);

        requireFlushAllowed();
        redisson.getKeys().flushdb();

        RStream<String, String> stream = redisson.getStream(TOPIC);
        try {
            stream.createGroup(
                    StreamCreateGroupArgs.name(CONSUMER_GROUP)
                            .makeStream()
                            .id(new StreamMessageId(0, 0)));
        } catch (Exception ignored) {
        }

        for (int i = 0; i < PRE_SEND_COUNT; i++) {
            Message<String> msg =
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("consumer-test")
                            .keys("key-" + i)
                            .body(payload)
                            .build();
            template.syncSend(msg);
        }
        LOG.info("Pre-sent {} messages to topic {}", PRE_SEND_COUNT, TOPIC);

        // 启动持续灌数线程：消费速度通常高于单线程同步灌数，因此用独立虚拟线程
        // 不间断补货，保证基准测量的是"真实消费路径"而非空读往返
        feeder =
                Thread.ofVirtual()
                        .name("benchmark-feeder")
                        .start(
                                () -> {
                                    int i = PRE_SEND_COUNT;
                                    while (feeding) {
                                        try {
                                            Message<String> msg =
                                                    MessageBuilder.<String>withTopic(TOPIC)
                                                            .tag("consumer-test")
                                                            .keys("key-" + (i++))
                                                            .body(payload)
                                                            .build();
                                            template.syncSend(msg);
                                        } catch (RuntimeException ex) {
                                            LOG.warn("Feeder send failed: {}", ex.getMessage());
                                        }
                                    }
                                });
    }

    @TearDown(Level.Trial)
    public void teardown() {
        feeding = false;
        if (feeder != null) {
            feeder.interrupt();
        }
        if (redisson != null) {
            requireFlushAllowed();
            redisson.getKeys().flushdb();
            redisson.shutdown();
        }
        if (redisServer != null) {
            redisServer.stop();
        }
    }

    /**
     * 防误删守卫：flushdb 会清空目标 Redis 当前库的全部数据，必须显式授权后才会执行。
     *
     * <p>基准默认通过 Testcontainers 拉起独占实例，但切到 {@code -Dstreammq.redis.mode=local} 直连本地/共享 Redis 时，无守卫的
     * flushdb 可能误删业务数据。
     */
    private static void requireFlushAllowed() {
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

    /**
     * 真实端到端消费路径：XREADGROUP 拉取 → converter 反序列化转换 → 业务回调 → XACK。
     *
     * <p>每次调用处理 {@value #BATCH_SIZE} 条完整消息（含网络与 ACK），修复了旧实现不 ACK、 不做字段转换、预热数据耗尽后测量空读 RTT 的方法学缺陷。
     */
    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    @SuppressWarnings("unchecked")
    public void consumeThroughput(Blackhole blackhole) throws Exception {
        RStream<String, String> stream = redisson.getStream(TOPIC);

        StreamMessageConcurrentlyConsumer<String> consumer = (msg, ctx) -> ConsumeAction.SUCCESS;

        int processed = 0;
        while (processed < BATCH_SIZE) {
            var messages =
                    stream.readGroup(
                            CONSUMER_GROUP,
                            CONSUMER_NAME,
                            StreamReadGroupArgs.neverDelivered().count(BATCH_SIZE));
            if (messages == null || messages.isEmpty()) {
                continue;
            }
            java.util.List<StreamMessageId> ids = new java.util.ArrayList<>(messages.size());
            for (var entry : messages.entrySet()) {
                // 真实消费路径包含字段解码（Base64 + 反序列化），而非复用本地对象
                Message<?> msg = converter.fromStreamFields(entry.getValue(), String.class, TOPIC);
                consumer.onMessage((Message<String>) msg, createContext(TOPIC, CONSUMER_GROUP));
                blackhole.consume(msg);
                ids.add(entry.getKey());
                processed++;
            }
            stream.ack(CONSUMER_GROUP, ids.toArray(new StreamMessageId[0]));
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void messageCreateAndConsume(Blackhole blackhole) throws Exception {
        StreamMessageConcurrentlyConsumer<String> consumer = (msg, ctx) -> ConsumeAction.SUCCESS;

        for (int i = 0; i < BATCH_SIZE; i++) {
            Message<String> msg =
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("perf")
                            .keys("key-" + i)
                            .body(payload)
                            .build();
            consumer.onMessage(msg, createContext(TOPIC, CONSUMER_GROUP));
            blackhole.consume(msg);
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void serializationRoundTrip(Blackhole blackhole) {
        JacksonJsonSerializer<String> serializer = new JacksonJsonSerializer<>();
        for (int i = 0; i < BATCH_SIZE; i++) {
            Message<String> msg = MessageBuilder.<String>withTopic(TOPIC).body(payload).build();
            byte[] bytes = serializer.serialize(msg.getBody(), String.class);
            blackhole.consume(serializer.deserialize(bytes, String.class));
        }
    }

    private ConsumeContext createContext(String topic, String consumerGroup) {
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

    public static void main(String[] args) throws RunnerException {
        // 与类级注解保持一致（fork 数等由 @Fork/@Warmup/@Measurement 决定），消除结果来源歧义
        Options opt =
                new OptionsBuilder()
                        .include(StreamConsumerBenchmark.class.getSimpleName())
                        .result("target/jmh-consumer.txt")
                        .resultFormat(ResultFormatType.TEXT)
                        .build();
        new Runner(opt).run();
    }
}
