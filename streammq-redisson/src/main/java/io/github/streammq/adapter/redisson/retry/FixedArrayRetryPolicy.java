package io.github.streammq.adapter.redisson.retry;

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
 * <p>当 {@code reconsumeTimes >= 16} 时，{@link #shouldStopRetry} 返回 true，消息将进入 DLQ。
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

    /** 最大重试次数（与数组长度一致） */
    public static final int MAX_RECONSUME_TIMES = DELAY_MILLIS.length;

    private final long[] delayMillis;
    private final int maxReconsumeTimes;

    /** 使用默认最大重试次数（16）。 */
    public FixedArrayRetryPolicy() {
        this(DELAY_MILLIS, DELAY_MILLIS.length);
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
     * @param delayMillis 延时数组（毫秒），非空
     */
    public FixedArrayRetryPolicy(long[] delayMillis) {
        this(delayMillis, delayMillis == null ? 0 : delayMillis.length);
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
            return null;
        }
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
