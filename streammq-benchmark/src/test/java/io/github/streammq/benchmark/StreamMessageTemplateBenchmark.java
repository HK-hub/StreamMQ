package io.github.streammq.benchmark;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducerFactory;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import io.github.streammq.adapter.redisson.template.DefaultStreamMessageTemplate;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.test.EmbeddedRedisServer;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
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
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(1)
public class StreamMessageTemplateBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMessageTemplateBenchmark.class);

    private static final String TOPIC = "benchmark-stream";
    private static final String GROUP = "benchmark-group";
    private static final int BATCH_SIZE = 100;

    private RedissonClient redisson;
    private StreamMessageTemplate template;
    private Message<String> message;
    private EmbeddedRedisServer redisServer;

    @Param({"100", "1000", "10000"})
    private int payloadSize;

    private String payload;

    @Setup(Level.Trial)
    public void setup() {
        String mode = System.getProperty("streammq.redis.mode", "docker");
        if ("docker".equalsIgnoreCase(mode)) {
            LOG.info("Starting Redis via Testcontainers...");
            redisServer = new EmbeddedRedisServer();
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
        RedissonStreamProducerFactory producerFactory =
                new RedissonStreamProducerFactory(redisson, converter);
        template = new DefaultStreamMessageTemplate(producerFactory, GROUP, converter);

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
            redisson.getKeys().flushdb();
            redisson.shutdown();
        }
        if (redisServer != null) {
            redisServer.stop();
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void syncSendThroughput() {
        for (int i = 0; i < BATCH_SIZE; i++) {
            template.syncSend(message);
        }
    }

    @Benchmark
    public SendResult syncSendSingle() {
        return template.syncSend(message);
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void asyncSendThroughput() throws Exception {
        java.util.concurrent.CompletableFuture<?>[] futures =
                new java.util.concurrent.CompletableFuture[BATCH_SIZE];
        for (int i = 0; i < BATCH_SIZE; i++) {
            futures[i] = template.asyncSend(message);
        }
        java.util.concurrent.CompletableFuture.allOf(futures).join();
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
                        .build();
        new Runner(opt).run();
    }
}
