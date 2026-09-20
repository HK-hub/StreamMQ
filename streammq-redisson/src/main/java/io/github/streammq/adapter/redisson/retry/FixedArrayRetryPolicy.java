/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.retry;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.policy.RetryPolicy;
import java.time.Duration;
import java.util.Objects;

/**
 * 固定数组重试策略，对齐 RocketMQ 16 级延时数组。
 *
 * <p>重试级别（共 16 级）：
 *
 * <pre>
 * [10s, 30s, 1m, 2m, 3m, 4m, 5m, 6m, 7m, 8m, 9m, 10m, 20m, 30m, 1h, 2h]
 * </pre>
 *
 * <p><b>预算与退避序列的解耦（0.1.2 定稿）：</b>重试预算<b>唯一</b>由 {@code max-reconsume-times}（构造参数 {@code
 * maxReconsumeTimes}，由消费者的 maxReconsumeTimes / 全局配置决定）提供；延时数组只提供<b>退避序列</b>，
 * <b>不再</b>隐式充当预算上限（历史行为：{@code new FixedArrayRetryPolicy(delayArray)} 把 {@code maxReconsumeTimes =
 * delayArray.length}，导致 {@code delay-array} 长度静默截断重试次数）。 数组耗尽后使用<b>最后一档</b>延期（保持非 null），停止条件交给预算或
 * {@link #shouldStopRetry(int, Message)}。
 *
 * <p>当 {@code reconsumeTimes >= maxReconsumeTimes} 时，{@link #shouldStopRetry} 返回 true， {@link
 * #nextRetryDelay} 返回 null，消息将进入 DLQ。
 *
 * <p>对应架构设计文档决策 D7（RetryPolicy SPI）：默认重试实现。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class FixedArrayRetryPolicy implements RetryPolicy {

    /** RocketMQ 风格 16 级固定延时数组（毫秒） */
    public static final long[] DELAY_MILLIS = {
        10_000L, // 10s
        30_000L, // 30s
        60_000L, // 1m
        120_000L, // 2m
        180_000L, // 3m
        240_000L, // 4m
        300_000L, // 5m
        360_000L, // 6m
        420_000L, // 7m
        480_000L, // 8m
        540_000L, // 9m
        600_000L, // 10m
        1_200_000L, // 20m
        1_800_000L, // 30m
        3_600_000L, // 1h
        7_200_000L // 2h
    };

    /** 默认延时数组长度（= 默认重试预算，仅作为默认值来源；自定义数组长度不影响预算） */
    public static final int MAX_RECONSUME_TIMES = DELAY_MILLIS.length;

    /**
     * "预算完全交给消费者配置"的哨兵值：仅自定义延时数组时使用。
     *
     * <p>预算的唯一真源是消费者的 {@code max-reconsume-times}（框架在 handler / RetryScheduler 两层执行）， 策略实例（可能是全局单例
     * Bean，无法感知 per-consumer 预算）不得再叠加一个内部预算。 取值 {@code Integer.MAX_VALUE} 表示"策略自身不停止"，数组耗尽后恒用最后一档。
     */
    static final int BUDGET_DELEGATED_TO_CONSUMER = Integer.MAX_VALUE;

    private final long[] delayMillis;
    private final int maxReconsumeTimes;

    /** 使用默认最大重试次数（16，与 {@code max-reconsume-times} 默认值一致）。 */
    public FixedArrayRetryPolicy() {
        this(DELAY_MILLIS, StreamMQConstants.DEFAULT_MAX_RECONSUME_TIMES);
    }

    /**
     * 自定义最大重试次数（使用默认 16 级延时数组）。
     *
     * @param maxReconsumeTimes 最大重试次数，必须 > 0
     */
    public FixedArrayRetryPolicy(int maxReconsumeTimes) {
        this(DELAY_MILLIS, maxReconsumeTimes);
    }

    /**
     * 自定义延时数组（对应 {@code streammq.retry.delay-array} 配置，逗号分隔毫秒值）。
     *
     * <p><b>重试预算不取数组长度，也不由策略自设：</b>预算的唯一真源是消费者的 {@code max-reconsume-times} （框架在 handler 与
     * RetryScheduler 两层执行）。本构造器把策略内部预算委托给该配置（数组耗尽后恒用最后一档）， 因此 {@code delay-array}
     * 的长度只影响退避节奏，绝不会静默截断重试次数。 需要策略实例自带预算时使用 {@link #FixedArrayRetryPolicy(long[], int)}。
     *
     * @param delayMillis 延时数组（毫秒），非空
     */
    public FixedArrayRetryPolicy(long[] delayMillis) {
        this(delayMillis, delayMillis == null ? 0 : BUDGET_DELEGATED_TO_CONSUMER);
    }

    /**
     * 自定义延时数组与最大重试次数。
     *
     * @param delayMillis 延时数组（毫秒），非空
     * @param maxReconsumeTimes 最大重试次数，必须 > 0
     */
    public FixedArrayRetryPolicy(long[] delayMillis, int maxReconsumeTimes) {
        if (delayMillis == null || delayMillis.length == 0) {
            throw new IllegalArgumentException("delayMillis must not be null or empty");
        }
        if (maxReconsumeTimes <= 0) {
            throw new IllegalArgumentException(
                    "maxReconsumeTimes must be positive: " + maxReconsumeTimes);
        }
        this.delayMillis = delayMillis.clone();
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    @Override
    public Duration nextRetryDelay(int reconsumeTimes, Message<?> message) {
        Objects.requireNonNull(message, "message");
        if (reconsumeTimes < 0) {
            reconsumeTimes = 0;
        }
        if (reconsumeTimes >= maxReconsumeTimes) {
            // 预算耗尽：null 为停止信号（与 shouldStopRetry=true 语义一致）
            return null;
        }
        // 退避序列耗尽后保持在最后一档（保持非 null），而非提前停止——
        // 停止只由 maxReconsumeTimes 预算或 shouldStopRetry 决定（0.1.2 定稿）。
        int index = Math.min(reconsumeTimes, delayMillis.length - 1);
        return Duration.ofMillis(delayMillis[index]);
    }

    @Override
    public boolean shouldStopRetry(int reconsumeTimes, Message<?> message) {
        Objects.requireNonNull(message, "message");
        return reconsumeTimes >= maxReconsumeTimes;
    }

    /**
     * 返回最大重试次数。
     *
     * @return 最大重试次数
     */
    public int getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }
}
