/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.benchmark;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * B2 回归用例：三类基准的<b>声明参数</b>（注解值 × @Param 组合 × 模式数 × fork 数）换算出的全量默认运行时长 必须落在 45 分钟预算内——CI 的
 * benchmark job 超时是 60 分钟，历史配置（30 方法 × 双模式 × {@code @Fork(3, warmups = 2)}）结构性超过该预算（≈100 分钟 + 模板基准
 * ≈35 分钟）。
 *
 * <p>本用例是对"默认参数"的静态预算校验（确定性、不依赖机器速度）：若后续有人放大 fork/迭代/参数组合， 这里会先红，而不是等 CI 跑到 60 分钟被砍。
 */
class BenchmarkBudgetTest {

    /** 全量默认运行预算：45 分钟（含每 fork 的 JVM/容器开销估算）。 */
    private static final long BUDGET_SECONDS = TimeUnit.MINUTES.toSeconds(45);

    /** 每个 fork 的固定开销（JVM 启动 + setup/teardown）保守估计。 */
    private static final long FORK_OVERHEAD_SECONDS = 4;

    /** 需要拉起 Testcontainers Redis 的基准类：每个 fork 额外算上容器启动/清理开销。 */
    private static final Set<Class<?>> CONTAINER_BACKED =
            Set.of(StreamMessageTemplateBenchmark.class, StreamConsumerBenchmark.class);

    private static final long CONTAINER_OVERHEAD_SECONDS = 10;

    @Test
    void declaredHarnessBudgetStaysWithinCiWindow() {
        StringBuilder detail = new StringBuilder();
        long totalSeconds = 0;
        for (Class<?> benchmarkClass :
                List.of(
                        SerializationBenchmark.class,
                        StreamMessageTemplateBenchmark.class,
                        StreamConsumerBenchmark.class)) {
            long seconds = declaredSeconds(benchmarkClass);
            totalSeconds += seconds;
            detail.append(
                    String.format(
                            "%n  %-32s %6.1f min", benchmarkClass.getSimpleName(), seconds / 60.0));
        }

        long measuredSeconds = totalSeconds;
        assertTrue(
                measuredSeconds <= BUDGET_SECONDS,
                () ->
                        "声明参数的全量默认运行预算 "
                                + measuredSeconds / 60.0
                                + " min 超出 "
                                + BUDGET_SECONDS / 60.0
                                + " min 预算（CI benchmark job 上限 60 min）；明细："
                                + detail);
    }

    /** 单个基准类的声明时长：Σ(runs) × forks × (预热 + 测量 + 每 fork 开销)。 */
    private static long declaredSeconds(Class<?> benchmarkClass) {
        BenchmarkMode benchmarkMode = benchmarkClass.getAnnotation(BenchmarkMode.class);
        int modes = benchmarkMode == null ? 1 : benchmarkMode.value().length;

        long iterationsSeconds = warmupSeconds(benchmarkClass) + measurementSeconds(benchmarkClass);

        Fork fork = benchmarkClass.getAnnotation(Fork.class);
        // 注意 JMH 语义：Fork.warmups 默认 -1（表示"不额外增加 warmup fork"），不能直接相加，
        // 否则 @Fork(1) 会被算成 0 个 fork，预算校验就会静默失效。
        long warmupForks = fork == null ? 0 : Math.max(fork.warmups(), 0);
        long forks = (fork == null ? 1 : fork.value()) + warmupForks;
        assertTrue(forks > 0, "fork 数换算必须为正数，否则预算校验会静默失效");

        long overheadSeconds =
                FORK_OVERHEAD_SECONDS
                        + (CONTAINER_BACKED.contains(benchmarkClass)
                                ? CONTAINER_OVERHEAD_SECONDS
                                : 0);

        long runs = 0;
        for (Method method : benchmarkClass.getDeclaredMethods()) {
            if (method.getAnnotation(Benchmark.class) == null) {
                continue;
            }
            runs += paramCombinations(method) * modes;
        }
        assertTrue(runs > 0, benchmarkClass.getSimpleName() + " 必须至少有一个 @Benchmark 运行组合");
        return runs * forks * (iterationsSeconds + overheadSeconds);
    }

    private static long warmupSeconds(Class<?> benchmarkClass) {
        Warmup warmup = benchmarkClass.getAnnotation(Warmup.class);
        return warmup == null
                ? 0
                : warmup.timeUnit().toSeconds(warmup.time()) * warmup.iterations();
    }

    private static long measurementSeconds(Class<?> benchmarkClass) {
        Measurement measurement = benchmarkClass.getAnnotation(Measurement.class);
        return measurement == null
                ? 0
                : measurement.timeUnit().toSeconds(measurement.time()) * measurement.iterations();
    }

    /** 一个 @Benchmark 方法的运行次数 = 其所有 @State（外层 + 参数）里 @Param 取值数的乘积。 */
    private static long paramCombinations(Method method) {
        Set<Class<?>> states = new LinkedHashSet<>();
        for (Class<?> parameterType : method.getParameterTypes()) {
            if (parameterType.isAnnotationPresent(State.class)) {
                states.add(parameterType);
            }
        }
        if (method.getDeclaringClass().isAnnotationPresent(State.class)) {
            states.add(method.getDeclaringClass());
        }
        long combinations = 1;
        for (Class<?> state : states) {
            combinations *= paramValues(state);
        }
        return combinations;
    }

    /** 一个 @State 内全部 @Param 取值数乘积（含继承字段）。 */
    private static long paramValues(Class<?> stateClass) {
        long values = 1;
        for (Class<?> current = stateClass;
                current != null && current != Object.class;
                current = current.getSuperclass()) {
            for (Field field : current.getDeclaredFields()) {
                Param param = field.getAnnotation(Param.class);
                if (param != null) {
                    values *= param.value().length;
                }
            }
        }
        return values;
    }
}
