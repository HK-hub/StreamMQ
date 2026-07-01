package io.github.streammq.adapter.redisson.retry;

import io.github.streammq.core.StreamMqConstants;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.spi.RetryPolicy;

import java.time.Duration;
import java.util.Objects;

/**
 * 指数退避重试策略。
 *
 * <p>计算公式：{@code delay = min(initial * (multiplier ^ reconsumeTimes), max)}。
 *
 * <p>默认参数：
 * <ul>
 *   <li>{@code initial = 1s}</li>
 *   <li>{@code multiplier = 2.0}</li>
 *   <li>{@code max = 2h}</li>
 *   <li>{@code maxReconsumeTimes = 16}</li>
 * </ul>
 *
 * <p>对应架构设计文档决策 D7（RetryPolicy SPI）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class ExponentialBackoffRetryPolicy implements RetryPolicy {

    /** 默认初始延时 1s */
    public static final Duration DEFAULT_INITIAL = Duration.ofSeconds(1);
    /** 默认乘数 2.0 */
    public static final double DEFAULT_MULTIPLIER = 2.0;
    /** 默认最大延时 2h */
    public static final Duration DEFAULT_MAX = Duration.ofHours(2);
    /** 默认最大重试次数 16 */
    public static final int DEFAULT_MAX_RECONSUME_TIMES = StreamMqConstants.DEFAULT_MAX_RECONSUME_TIMES;

    private final long initialMillis;
    private final double multiplier;
    private final long maxMillis;
    private final int maxReconsumeTimes;

    /**
     * 使用默认参数构造（initial=1s, multiplier=2.0, max=2h, maxReconsumeTimes=16）。
     */
    public ExponentialBackoffRetryPolicy() {
        this(DEFAULT_INITIAL.toMillis(), DEFAULT_MULTIPLIER, DEFAULT_MAX.toMillis(), DEFAULT_MAX_RECONSUME_TIMES);
    }

    /**
     * 自定义参数构造。
     *
     * @param initialMillis 初始延时（毫秒），必须 > 0
     * @param multiplier 乘数，必须 > 1.0
     * @param maxMillis 最大延时（毫秒），必须 >= initialMillis
     * @param maxReconsumeTimes 最大重试次数，必须 > 0
     */
    public ExponentialBackoffRetryPolicy(long initialMillis, double multiplier,
                                         long maxMillis, int maxReconsumeTimes) {
        if (initialMillis <= 0) {
            throw new IllegalArgumentException("initialMillis must be positive: " + initialMillis);
        }
        if (multiplier <= 1.0) {
            throw new IllegalArgumentException("multiplier must be > 1.0: " + multiplier);
        }
        if (maxMillis < initialMillis) {
            throw new IllegalArgumentException("maxMillis must be >= initialMillis: " + maxMillis);
        }
        if (maxReconsumeTimes <= 0) {
            throw new IllegalArgumentException("maxReconsumeTimes must be positive: " + maxReconsumeTimes);
        }
        this.initialMillis = initialMillis;
        this.multiplier = multiplier;
        this.maxMillis = maxMillis;
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    @Override
    public Duration nextRetryDelay(int reconsumeTimes, Message<?> message) {
        Objects.requireNonNull(message, "message");
        if (reconsumeTimes < 0) {
            reconsumeTimes = 0;
        }
        // delay = min(initial * (multiplier ^ reconsumeTimes), max)
        // 为防止 reconsumeTimes 过大导致溢出，使用 Math.pow 后转 long
        double raw = initialMillis * Math.pow(multiplier, reconsumeTimes);
        long delay = (long) Math.min(raw, maxMillis);
        if (delay < 0) {
            // 溢出保护
            delay = maxMillis;
        }
        return Duration.ofMillis(delay);
    }

    @Override
    public boolean shouldStopRetry(int reconsumeTimes, Message<?> message) {
        Objects.requireNonNull(message, "message");
        return reconsumeTimes >= maxReconsumeTimes;
    }

    /**
     * 返回初始延时（毫秒）。
     *
     * @return 初始延时
     */
    public long getInitialMillis() {
        return initialMillis;
    }

    /**
     * 返回乘数。
     *
     * @return 乘数
     */
    public double getMultiplier() {
        return multiplier;
    }

    /**
     * 返回最大延时（毫秒）。
     *
     * @return 最大延时
     */
    public long getMaxMillis() {
        return maxMillis;
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
