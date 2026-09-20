/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.benchmark;

import io.github.streammq.adapter.redisson.serializer.FlatBuffersSerializer;
import io.github.streammq.adapter.redisson.serializer.FurySerializer;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import io.github.streammq.adapter.redisson.serializer.JdkSerializer;
import io.github.streammq.adapter.redisson.serializer.ProtostuffSerializer;
import io.github.streammq.adapter.redisson.serializer.SbeSerializer;
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

/**
 * 序列化器横向对比基准（6 个内置实现 × 吞吐/采样两种模式 × 30 个方法）。
 *
 * <p>参数真源：fork/预热/测量<b>只由本类注解决定</b>（{@code main()} 不再用 {@code OptionsBuilder} 覆盖，
 * 消除"注解一套、实际一套"的双源）。默认值按 CI 的 60 分钟 job 预算收敛——旧的 {@code @Fork(3, warmups = 2)} + 30 方法 × 双模式结构性需要
 * ≈100 分钟：现在单 fork、不再单独的 warmup fork、更短迭代， 全量默认运行约 12 分钟。预算校验见 {@code
 * BenchmarkBudgetTest}；需要更细的分布可用 JMH 命令行参数 （{@code -f}/{@code -wi}/{@code -i}/{@code -w}/{@code
 * -r}）临时覆盖。
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class SerializationBenchmark {

    private static final int PAYLOAD_SIZE = 1024;
    private static final int BATCH_SIZE = 1000;

    private TestPayload payload;
    private byte[] jacksonBytes;
    private byte[] jdkBytes;
    private byte[] furyBytes;
    private byte[] protostuffBytes;
    private byte[] flatBuffersBytes;
    private byte[] sbeBytes;

    private MessageSerializer<TestPayload> jacksonSerializer;
    private MessageSerializer<TestPayload> jdkSerializer;
    private MessageSerializer<TestPayload> furySerializer;
    private MessageSerializer<TestPayload> protostuffSerializer;
    private MessageSerializer<TestPayload> flatBuffersSerializer;
    private MessageSerializer<TestPayload> sbeSerializer;

    @Setup(Level.Trial)
    public void setup() {
        char[] chars = new char[PAYLOAD_SIZE];
        java.util.Arrays.fill(chars, 'x');
        String data = new String(chars);

        payload = new TestPayload("benchmark-id", data, 42, System.currentTimeMillis());

        jacksonSerializer = new JacksonJsonSerializer<>();
        jdkSerializer = new JdkSerializer<>();
        furySerializer = new FurySerializer<>(TestPayload.class);
        protostuffSerializer = new ProtostuffSerializer<>();
        flatBuffersSerializer = new FlatBuffersSerializer<>();
        sbeSerializer = new SbeSerializer<>();

        jacksonBytes = jacksonSerializer.serialize(payload, TestPayload.class);
        jdkBytes = jdkSerializer.serialize(payload, TestPayload.class);
        furyBytes = furySerializer.serialize(payload, TestPayload.class);
        protostuffBytes = protostuffSerializer.serialize(payload, TestPayload.class);
        flatBuffersBytes = flatBuffersSerializer.serialize(payload, TestPayload.class);
        sbeBytes = sbeSerializer.serialize(payload, TestPayload.class);
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
    @OperationsPerInvocation(BATCH_SIZE)
    public void protostuffSerialize(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            blackhole.consume(protostuffSerializer.serialize(payload, TestPayload.class));
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void protostuffDeserialize(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            blackhole.consume(protostuffSerializer.deserialize(protostuffBytes, TestPayload.class));
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
    public byte[] protostuffSerializeSingle() {
        return protostuffSerializer.serialize(payload, TestPayload.class);
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
    public TestPayload protostuffDeserializeSingle() {
        return protostuffSerializer.deserialize(protostuffBytes, TestPayload.class);
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

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void protostuffRoundTrip(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            byte[] bytes = protostuffSerializer.serialize(payload, TestPayload.class);
            blackhole.consume(protostuffSerializer.deserialize(bytes, TestPayload.class));
        }
    }

    // ===== FlatBuffers（FlexBuffers 动态反射，零拷贝读） =====

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void flatBuffersSerialize(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            blackhole.consume(flatBuffersSerializer.serialize(payload, TestPayload.class));
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void flatBuffersDeserialize(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            blackhole.consume(
                    flatBuffersSerializer.deserialize(flatBuffersBytes, TestPayload.class));
        }
    }

    @Benchmark
    public byte[] flatBuffersSerializeSingle() {
        return flatBuffersSerializer.serialize(payload, TestPayload.class);
    }

    @Benchmark
    public TestPayload flatBuffersDeserializeSingle() {
        return flatBuffersSerializer.deserialize(flatBuffersBytes, TestPayload.class);
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void flatBuffersRoundTrip(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            byte[] bytes = flatBuffersSerializer.serialize(payload, TestPayload.class);
            blackhole.consume(flatBuffersSerializer.deserialize(bytes, TestPayload.class));
        }
    }

    // ===== SBE（Simple Binary Encoding，信封模式） =====

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void sbeSerialize(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            blackhole.consume(sbeSerializer.serialize(payload, TestPayload.class));
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void sbeDeserialize(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            blackhole.consume(sbeSerializer.deserialize(sbeBytes, TestPayload.class));
        }
    }

    @Benchmark
    public byte[] sbeSerializeSingle() {
        return sbeSerializer.serialize(payload, TestPayload.class);
    }

    @Benchmark
    public TestPayload sbeDeserializeSingle() {
        return sbeSerializer.deserialize(sbeBytes, TestPayload.class);
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void sbeRoundTrip(Blackhole blackhole) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            byte[] bytes = sbeSerializer.serialize(payload, TestPayload.class);
            blackhole.consume(sbeSerializer.deserialize(bytes, TestPayload.class));
        }
    }

    public static void main(String[] args) throws RunnerException {
        // fork/预热/测量参数只由类注解决定（此处不再覆盖），需要临时覆盖请用 JMH 命令行参数
        Options opt =
                new OptionsBuilder()
                        .include(SerializationBenchmark.class.getSimpleName())
                        .result("target/jmh-serialization.json")
                        .resultFormat(ResultFormatType.JSON)
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
