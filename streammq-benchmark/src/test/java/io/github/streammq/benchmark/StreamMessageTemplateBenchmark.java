/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.benchmark;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import io.github.streammq.adapter.redisson.template.DefaultStreamMessageTemplate;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.test.ContainerizedRedisServer;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 2, time = 2)
@Fork(1)
public class StreamMessageTemplateBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMessageTemplateBenchmark.class);

    private static final String TOPIC = "benchmark-stream";
    private static final String GROUP = "benchmark-group";
    private static final int BATCH_SIZE = 100;

    private RedissonClient redisson;
    private StreamMessageTemplate template;
    private Message<String> message;
    private ContainerizedRedisServer redisServer;

    @Param({"100", "1000", "10000"})
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
        RedissonStreamProducer producer =
                RedissonStreamProducer.builder()
                        .redisson(redisson)
                        .namespace("")
                        .group(GROUP)
                        .converter(converter)
                        .defaultTimeoutMillis(3000)
                        .maxLen(0)
                        .compressThreshold(0)
                        .maxMessageSize(512L * 1024 * 1024)
                        .build();
        template = new DefaultStreamMessageTemplate(producer, GROUP, converter);

        char[] chars = new char[payloadSize];
        java.util.Arrays.fill(chars, 'x');
        payload = new String(chars);

        message =
                MessageBuilder.<String>withTopic(TOPIC)
                        .tag("benchmark")
                        .keys("benchmark-key")
                        .body(payload)
                        .build();
    }

    @TearDown(Level.Trial)
    public void teardown() {
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

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void syncSendThroughput(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            blackhole.consume(template.syncSend(message));
        }
    }

    @Benchmark
    public SendResult syncSendSingle() {
        return template.syncSend(message);
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void asyncSendThroughput(Blackhole blackhole) throws Exception {
        java.util.concurrent.CompletableFuture<?>[] futures =
                new java.util.concurrent.CompletableFuture[BATCH_SIZE];
        for (int i = 0; i < BATCH_SIZE; i++) {
            futures[i] = template.asyncSend(message);
        }
        java.util.concurrent.CompletableFuture.allOf(futures).join();
        for (java.util.concurrent.CompletableFuture<?> future : futures) {
            blackhole.consume(future.join());
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .include(StreamMessageTemplateBenchmark.class.getSimpleName())
                        .warmupTime(TimeValue.seconds(2))
                        .warmupIterations(3)
                        .measurementTime(TimeValue.seconds(3))
                        .measurementIterations(5)
                        .forks(1)
                        .result("target/jmh-template.txt")
                        .resultFormat(ResultFormatType.TEXT)
                        .build();
        new Runner(opt).run();
    }
}
