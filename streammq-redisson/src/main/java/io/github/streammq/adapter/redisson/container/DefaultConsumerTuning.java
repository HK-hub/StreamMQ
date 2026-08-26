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

    private volatile int maxBatchSizeLimit = StreamMQConstants.MAX_BATCH_SIZE_LIMIT;

    private volatile int inflightCapacity;

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

    public void setMaxBatchSizeLimit(int limit) {
        if (limit > 0) {
            this.maxBatchSizeLimit = limit;
        }
    }

    public void setInflightCapacity(int capacity) {
        this.inflightCapacity = Math.max(0, capacity);
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
}
