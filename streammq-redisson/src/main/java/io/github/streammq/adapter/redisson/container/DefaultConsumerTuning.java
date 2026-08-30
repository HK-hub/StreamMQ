/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.StreamMQConstants;

/** {@link ConsumerTuning} 默认实现：volatile 字段 + 写入下界保护。 */
public class DefaultConsumerTuning implements ConsumerTuning {

    private volatile int defaultPullBatchSize = StreamMQConstants.DEFAULT_CONSUME_BATCH_SIZE;

    private volatile long defaultPullBlockTimeoutMillis =
            StreamMQConstants.DEFAULT_PULL_BLOCK_TIMEOUT_MS;

    private volatile long defaultPullIntervalMillis = StreamMQConstants.DEFAULT_PULL_INTERVAL_MS;

    /**
     * 全局顺序消费超时（毫秒），0 表示不启用。
     *
     * <p>来自 {@code streammq.consumer.orderly-consume-timeout}。仅作为注解未显式声明时的回落值—— 注解 {@code
     * orderlyConsumeTimeout > 0} 时始终优先，保证 per-consumer 可覆盖全局。
     */
    private volatile long defaultOrderlyConsumeTimeoutMillis;

    private volatile int maxBatchSizeLimit = StreamMQConstants.MAX_BATCH_SIZE_LIMIT;

    private volatile int inflightCapacity;

    /** 暂停状态下消费循环的休眠间隔（毫秒）——来自 {@code streammq.consumer.paused-sleep-millis} */
    private volatile long pausedSleepMillis = StreamMQConstants.DEFAULT_PAUSED_SLEEP_MS;

    /** Broker 异常后消费循环的退避休眠间隔（毫秒）——来自 {@code streammq.consumer.broker-error-backoff-millis} */
    private volatile long brokerErrorBackoffMillis =
            StreamMQConstants.DEFAULT_BROKER_ERROR_BACKOFF_MS;

    @Override
    public int defaultPullBatchSize() {
        return defaultPullBatchSize;
    }

    @Override
    public long defaultPullBlockTimeoutMillis() {
        return defaultPullBlockTimeoutMillis;
    }

    @Override
    public int inflightCapacity() {
        return inflightCapacity;
    }

    /** 暴露 pausedSleepMillis 给容器装配 LoopContext 时使用。 */
    public long getPausedSleepMillis() {
        return pausedSleepMillis;
    }

    /** 暴露 brokerErrorBackoffMillis 给容器装配 LoopContext 时使用。 */
    public long getBrokerErrorBackoffMillis() {
        return brokerErrorBackoffMillis;
    }

    public void setDefaultPullBatchSize(int batchSize) {
        if (batchSize > 0) {
            this.defaultPullBatchSize = batchSize;
        }
    }

    public void setDefaultPullBlockTimeoutMillis(long millis) {
        if (millis > 0) {
            this.defaultPullBlockTimeoutMillis = millis;
        }
    }

    public void setDefaultPullIntervalMillis(long millis) {
        if (millis >= 0) {
            this.defaultPullIntervalMillis = millis;
        }
    }

    /**
     * 注入全局顺序消费超时（毫秒），{@code >= 0} 才生效；0 表示不启用（默认）。
     *
     * @param millis 超时毫秒数
     */
    public void setDefaultOrderlyConsumeTimeoutMillis(long millis) {
        if (millis >= 0) {
            this.defaultOrderlyConsumeTimeoutMillis = millis;
        }
    }

    public void setMaxBatchSizeLimit(int limit) {
        if (limit > 0) {
            this.maxBatchSizeLimit = limit;
        }
    }

    public void setInflightCapacity(int capacity) {
        this.inflightCapacity = Math.max(0, capacity);
    }

    /** 注入暂停休眠间隔（毫秒），{@code > 0} 才生效。 */
    public void setPausedSleepMillis(long millis) {
        if (millis > 0) {
            this.pausedSleepMillis = millis;
        }
    }

    /** 注入 Broker 异常退避间隔（毫秒），{@code > 0} 才生效。 */
    public void setBrokerErrorBackoffMillis(long millis) {
        if (millis > 0) {
            this.brokerErrorBackoffMillis = millis;
        }
    }

    @Override
    public int effectivePullBatchSize(int annotationValue) {
        int effective =
                annotationValue != StreamMQConstants.DEFAULT_CONSUME_BATCH_SIZE
                        ? annotationValue
                        : defaultPullBatchSize;
        return Math.max(1, Math.min(effective, maxBatchSizeLimit));
    }

    @Override
    public long effectivePullInterval(long annotationValue) {
        return annotationValue != 0 ? annotationValue : defaultPullIntervalMillis;
    }

    @Override
    public long effectiveOrderlyConsumeTimeoutMillis(long annotationValue) {
        return annotationValue > 0 ? annotationValue : defaultOrderlyConsumeTimeoutMillis;
    }
}
