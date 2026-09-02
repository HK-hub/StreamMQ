/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.benchmark;

import io.github.streammq.adapter.redisson.serializer.FurySerializer;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import io.github.streammq.adapter.redisson.serializer.JdkSerializer;
import io.github.streammq.core.serializer.MessageSerializer;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 2, time = 2)
@Fork(1)
public class SerializationBenchmark {

    private static final int PAYLOAD_SIZE = 1024;
    private static final int BATCH_SIZE = 1000;

    @Param({"100", "1000", "10000"})
    private int messageCount;

    private TestPayload payload;
    private byte[] jacksonBytes;
    private byte[] jdkBytes;
    private byte[] furyBytes;

    private MessageSerializer<TestPayload> jacksonSerializer;
    private MessageSerializer<TestPayload> jdkSerializer;
    private MessageSerializer<TestPayload> furySerializer;

    @Setup(Level.Trial)
    public void setup() {
        char[] chars = new char[PAYLOAD_SIZE];
        java.util.Arrays.fill(chars, 'x');
        String data = new String(chars);

        payload = new TestPayload("benchmark-id", data, 42, System.currentTimeMillis());

        jacksonSerializer = new JacksonJsonSerializer<>();
        jdkSerializer = new JdkSerializer<>();
        furySerializer = new FurySerializer<>(TestPayload.class);

        jacksonBytes = jacksonSerializer.serialize(payload, TestPayload.class);
        jdkBytes = jdkSerializer.serialize(payload, TestPayload.class);
        furyBytes = furySerializer.serialize(payload, TestPayload.class);
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void jacksonSerialize(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            blackhole.consume(jacksonSerializer.serialize(payload, TestPayload.class));
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void jacksonDeserialize(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            blackhole.consume(jacksonSerializer.deserialize(jacksonBytes, TestPayload.class));
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void jdkSerialize(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            blackhole.consume(jdkSerializer.serialize(payload, TestPayload.class));
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void jdkDeserialize(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            blackhole.consume(jdkSerializer.deserialize(jdkBytes, TestPayload.class));
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void furySerialize(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            blackhole.consume(furySerializer.serialize(payload, TestPayload.class));
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void furyDeserialize(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            blackhole.consume(furySerializer.deserialize(furyBytes, TestPayload.class));
        }
    }

    @Benchmark
    public byte[] jacksonSerializeSingle() {
        return jacksonSerializer.serialize(payload, TestPayload.class);
    }

    @Benchmark
    public byte[] jdkSerializeSingle() {
        return jdkSerializer.serialize(payload, TestPayload.class);
    }

    @Benchmark
    public byte[] furySerializeSingle() {
        return furySerializer.serialize(payload, TestPayload.class);
    }

    @Benchmark
    public TestPayload jacksonDeserializeSingle() {
        return jacksonSerializer.deserialize(jacksonBytes, TestPayload.class);
    }

    @Benchmark
    public TestPayload jdkDeserializeSingle() {
        return jdkSerializer.deserialize(jdkBytes, TestPayload.class);
    }

    @Benchmark
    public TestPayload furyDeserializeSingle() {
        return furySerializer.deserialize(furyBytes, TestPayload.class);
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void jacksonRoundTrip(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            byte[] bytes = jacksonSerializer.serialize(payload, TestPayload.class);
            blackhole.consume(jacksonSerializer.deserialize(bytes, TestPayload.class));
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void jdkRoundTrip(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            byte[] bytes = jdkSerializer.serialize(payload, TestPayload.class);
            blackhole.consume(jdkSerializer.deserialize(bytes, TestPayload.class));
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void furyRoundTrip(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            byte[] bytes = furySerializer.serialize(payload, TestPayload.class);
            blackhole.consume(furySerializer.deserialize(bytes, TestPayload.class));
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .include(SerializationBenchmark.class.getSimpleName())
                        .warmupTime(TimeValue.seconds(2))
                        .warmupIterations(3)
                        .measurementTime(TimeValue.seconds(3))
                        .measurementIterations(5)
                        .forks(1)
                        .result("target/jmh-serialization.txt")
                        .resultFormat(ResultFormatType.TEXT)
                        .build();
        new Runner(opt).run();
    }

    public static class TestPayload implements Serializable {

        private static final long serialVersionUID = 1L;

        private String id;
        private String data;
        private int count;
        private long timestamp;

        public TestPayload() {}

        public TestPayload(String id, String data, int count, long timestamp) {
            this.id = id;
            this.data = data;
            this.count = count;
            this.timestamp = timestamp;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }
}
