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
import io.github.streammq.test.EmbeddedRedisServer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
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
@Fork(1)
public class StreamConsumerBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(StreamConsumerBenchmark.class);

    private static final String TOPIC = "consumer-benchmark-stream";
    private static final String CONSUMER_GROUP = "benchmark-consumer-group";
    private static final String CONSUMER_NAME = "benchmark-consumer";
    private static final int PRE_SEND_COUNT = 500;
    private static final int BATCH_SIZE = 100;

    private RedissonClient redisson;
    private StreamMessageTemplate template;
    private EmbeddedRedisServer redisServer;

    @Param({"1024", "10240"})
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
        template =
                new DefaultStreamMessageTemplate(producerFactory, "benchmark-producer", converter);

        char[] chars = new char[payloadSize];
        java.util.Arrays.fill(chars, 'x');
        payload = new String(chars);

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
    public void consumeThroughput() throws Exception {
        RStream<String, String> stream = redisson.getStream(TOPIC);

        var messages =
                stream.readGroup(
                        CONSUMER_GROUP,
                        CONSUMER_NAME,
                        StreamReadGroupArgs.neverDelivered().count(BATCH_SIZE));

        if (messages != null && !messages.isEmpty()) {
            StreamMessageConcurrentlyConsumer<String> consumer =
                    (msg, ctx) -> ConsumeAction.SUCCESS;

            for (var entry : messages.entrySet()) {
                Message<String> msg =
                        MessageBuilder.<String>withTopic(TOPIC)
                                .tag("consumer-test")
                                .body(payload)
                                .build();
                consumer.onMessage(msg, createContext(TOPIC, CONSUMER_GROUP));
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void messageCreateAndConsume() throws Exception {
        StreamMessageConcurrentlyConsumer<String> consumer = (msg, ctx) -> ConsumeAction.SUCCESS;

        for (int i = 0; i < BATCH_SIZE; i++) {
            Message<String> msg =
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("perf")
                            .keys("key-" + i)
                            .body(payload)
                            .build();
            consumer.onMessage(msg, createContext(TOPIC, CONSUMER_GROUP));
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void serializationRoundTrip() {
        JacksonJsonSerializer<String> serializer = new JacksonJsonSerializer<>();
        for (int i = 0; i < BATCH_SIZE; i++) {
            Message<String> msg = MessageBuilder.<String>withTopic(TOPIC).body(payload).build();
            byte[] bytes = serializer.serialize(msg.getBody(), String.class);
            serializer.deserialize(bytes, String.class);
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
        Options opt =
                new OptionsBuilder()
                        .include(StreamConsumerBenchmark.class.getSimpleName())
                        .warmupTime(TimeValue.seconds(2))
                        .warmupIterations(3)
                        .measurementTime(TimeValue.seconds(3))
                        .measurementIterations(5)
                        .forks(1)
                        .build();
        new Runner(opt).run();
    }
}
