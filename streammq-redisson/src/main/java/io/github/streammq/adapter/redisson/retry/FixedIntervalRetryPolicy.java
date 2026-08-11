package io.github.streammq.adapter.redisson.retry;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.policy.RetryPolicy;
import java.time.Duration;
import java.util.Objects;

/**
 * 固定间隔重试策略。
 *
 * <p>每次重试使用相同的延时 {@code intervalMs}，当重试次数达到 {@code maxRetries} 时停止。
 *
 * <p>默认参数：
 *
 * <ul>
 *   <li>{@code intervalMs = 10000}（10 秒）
 *   <li>{@code maxRetries = 16}
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class FixedIntervalRetryPolicy implements RetryPolicy {

    /** 默认重试间隔（毫秒） */
    public static final long DEFAULT_INTERVAL_MS = 10_000L;

    /** 默认最大重试次数 */
    public static final int DEFAULT_MAX_RETRIES = 16;

    private final long intervalMs;
    private final int maxRetries;

    /** 使用默认参数构造（intervalMs=10000, maxRetries=16）。 */
    public FixedIntervalRetryPolicy() {
        this(DEFAULT_INTERVAL_MS, DEFAULT_MAX_RETRIES);
    }

    /**
     * 自定义参数构造。
     *
     * @param intervalMs 重试间隔（毫秒），必须 > 0
     * @param maxRetries 最大重试次数，必须 > 0
     */
    public FixedIntervalRetryPolicy(long intervalMs, int maxRetries) {
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("intervalMs must be positive: " + intervalMs);
        }
        if (maxRetries <= 0) {
            throw new IllegalArgumentException("maxRetries must be positive: " + maxRetries);
        }
        this.intervalMs = intervalMs;
        this.maxRetries = maxRetries;
    }

    @Override
    public Duration nextRetryDelay(int reconsumeTimes, Message<?> message) {
        Objects.requireNonNull(message, "message");
        if (reconsumeTimes < 0) {
            reconsumeTimes = 0;
        }
        if (reconsumeTimes >= maxRetries) {
            return null;
        }
        return Duration.ofMillis(intervalMs);
    }

    @Override
    public boolean shouldStopRetry(int reconsumeTimes, Message<?> message) {
        Objects.requireNonNull(message, "message");
        if (reconsumeTimes < 0) {
            reconsumeTimes = 0;
        }
        return reconsumeTimes >= maxRetries;
    }

    /**
     * 返回重试间隔（毫秒）。
     *
     * @return 重试间隔
     */
    public long getIntervalMs() {
        return intervalMs;
    }

    /**
     * 返回最大重试次数。
     *
     * @return 最大重试次数
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    @Override
    public String name() {
        return "fixed-interval";
    }
}
