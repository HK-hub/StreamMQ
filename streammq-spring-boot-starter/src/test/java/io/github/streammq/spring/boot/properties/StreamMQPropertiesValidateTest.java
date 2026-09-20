/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.exception.StreamMQClientException;
import io.github.streammq.spring.boot.StreamMQSpringConstants;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link StreamMQProperties#validate()} 参数范围校验回归测试。
 *
 * <p>覆盖新增的范围约束：retry.maxReconsumeTimes / retry.batchSize / delay.batchSize /
 * producer.maxMessageSize / dlq.dlqRetryDelayMs，以及默认配置必须合法通过。
 */
@DisplayName("StreamMQProperties validate 范围校验测试")
class StreamMQPropertiesValidateTest {

    @Test
    @DisplayName("默认配置应通过校验")
    void defaults_shouldPassValidation() {
        StreamMQProperties properties = new StreamMQProperties();
        assertThatCode(properties::validate).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("producer.serializer 默认值为 JacksonJsonSerializer，且自动装配回退常量与之保持一致")
    void defaultSerializer_isJackson() {
        StreamMQProperties properties = new StreamMQProperties();
        assertThat(properties.getProducer().getSerializer().getName())
                .isEqualTo(StreamMQConstants.DEFAULT_SERIALIZER);
        assertThat(StreamMQSpringConstants.DEFAULT_SERIALIZER_CLASS.getName())
                .isEqualTo(StreamMQConstants.DEFAULT_SERIALIZER);
    }

    @Test
    @DisplayName("retry.maxReconsumeTimes < 0 应被拒绝")
    void negativeMaxReconsumeTimes_rejected() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getRetry().setMaxReconsumeTimes(-1);
        assertThatThrownBy(properties::validate)
                .isInstanceOf(StreamMQClientException.class)
                .hasMessageContaining("streammq.retry.max-reconsume-times must be >= 0");
    }

    @Test
    @DisplayName("retry.maxReconsumeTimes = 0 应被接受（禁用重试）")
    void zeroMaxReconsumeTimes_accepted() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getRetry().setMaxReconsumeTimes(0);
        assertThatCode(properties::validate).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("retry.batchSize <= 0 应被拒绝")
    void nonPositiveRetryBatchSize_rejected() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getRetry().setBatchSize(0);
        assertThatThrownBy(properties::validate)
                .isInstanceOf(StreamMQClientException.class)
                .hasMessageContaining("streammq.retry.batch-size must be > 0");
    }

    @Test
    @DisplayName("delay.batchSize <= 0 应被拒绝")
    void nonPositiveDelayBatchSize_rejected() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getDelay().setBatchSize(-5);
        assertThatThrownBy(properties::validate)
                .isInstanceOf(StreamMQClientException.class)
                .hasMessageContaining("streammq.delay.batch-size must be > 0");
    }

    @Test
    @DisplayName("producer.maxMessageSize <= 0 应被拒绝")
    void nonPositiveMaxMessageSize_rejected() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getProducer().setMaxMessageSize(0);
        assertThatThrownBy(properties::validate)
                .isInstanceOf(StreamMQClientException.class)
                .hasMessageContaining("streammq.producer.max-message-size must be > 0");
    }

    @Test
    @DisplayName("dlq.dlqRetryDelayMs < 0 应被拒绝")
    void negativeDlqRetryDelayMs_rejected() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getDlq().setDlqRetryDelayMs(-1);
        assertThatThrownBy(properties::validate)
                .isInstanceOf(StreamMQClientException.class)
                .hasMessageContaining("streammq.dlq.dlq-retry-delay-ms must be >= 0");
    }

    @Test
    @DisplayName("admin.failureRetryCooldownMillis < 0 的报错必须给出真实配置键名 -millis（D-13 回归）")
    void negativeFailureRetryCooldown_reportsRealKeyName() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getAdmin().setFailureRetryCooldownMillis(-1);
        assertThatThrownBy(properties::validate)
                .isInstanceOf(StreamMQClientException.class)
                .hasMessageContaining("streammq.admin.failure-retry-cooldown-millis must be >= 0")
                .hasMessageNotContaining("failure-retry-cooldown-ms ");
    }

    // ===================== R6-S5：管理面列表参数上界 =====================

    @Test
    @DisplayName("admin.list-page-size 超过硬上限 10000 应被拒绝")
    void adminListPageSizeAboveUpperBound_rejected() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getAdmin().setListPageSize(StreamMQSpringConstants.MAX_ADMIN_LIST_LIMIT + 1);

        assertThatThrownBy(properties::validate)
                .isInstanceOf(StreamMQClientException.class)
                .hasMessageContaining("streammq.admin.list-page-size must be <= 10000");
    }

    @Test
    @DisplayName("admin.max-pending-query-size 超过硬上限 10000 应被拒绝")
    void adminMaxPendingQuerySizeAboveUpperBound_rejected() {
        StreamMQProperties properties = new StreamMQProperties();
        properties
                .getAdmin()
                .setMaxPendingQuerySize(StreamMQSpringConstants.MAX_ADMIN_LIST_LIMIT + 1);

        assertThatThrownBy(properties::validate)
                .isInstanceOf(StreamMQClientException.class)
                .hasMessageContaining("streammq.admin.max-pending-query-size must be <= 10000");
    }

    @Test
    @DisplayName("admin 列表参数恰为上限 10000 时应被接受")
    void adminListParamsAtUpperBound_accepted() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getAdmin().setListPageSize(StreamMQSpringConstants.MAX_ADMIN_LIST_LIMIT);
        properties.getAdmin().setMaxPendingQuerySize(StreamMQSpringConstants.MAX_ADMIN_LIST_LIMIT);

        assertThatCode(properties::validate).doesNotThrowAnyException();
    }

    // ===================== R6-S6：rebalance.virtual-nodes =====================

    @Test
    @DisplayName("rebalance.virtual-nodes <= 0 应被拒绝（此前静默回退默认 160）")
    void nonPositiveVirtualNodes_rejected() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getRebalance().setVirtualNodes(0);

        assertThatThrownBy(properties::validate)
                .isInstanceOf(StreamMQClientException.class)
                .hasMessageContaining("streammq.rebalance.virtual-nodes must be > 0");
    }

    // ===================== R6-S7：batch-size 与 max-batch-size-limit 交叉校验 =====================

    @Test
    @DisplayName("consumer.batch-size 超过 max-batch-size-limit 应被拒绝（此前被静默夹取）")
    void batchSizeAboveLimit_rejected() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getConsumer().setMaxBatchSizeLimit(1000);
        properties.getConsumer().setBatchSize(1001);

        assertThatThrownBy(properties::validate)
                .isInstanceOf(StreamMQClientException.class)
                .hasMessageContaining("streammq.consumer.batch-size")
                .hasMessageContaining("must be <= streammq.consumer.max-batch-size-limit");
    }

    @Test
    @DisplayName("consumer.batch-size 等于 max-batch-size-limit 应被接受")
    void batchSizeEqualToLimit_accepted() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getConsumer().setMaxBatchSizeLimit(1000);
        properties.getConsumer().setBatchSize(1000);

        assertThatCode(properties::validate).doesNotThrowAnyException();
    }

    // ===================== R6-S8：orderly-shard-lock-lease-millis =====================

    @Test
    @DisplayName("consumer.orderly-shard-lock-lease-millis < 0 应被拒绝")
    void negativeOrderlyShardLockLease_rejected() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getConsumer().setOrderlyShardLockLeaseMillis(-1);

        assertThatThrownBy(properties::validate)
                .isInstanceOf(StreamMQClientException.class)
                .hasMessageContaining(
                        "streammq.consumer.orderly-shard-lock-lease-millis must be >= 0");
    }

    @Test
    @DisplayName("orderly-shard-lock-lease-millis: 0（默认看门狗）与 >= 5000（推荐值）均通过校验")
    void orderlyShardLockLease_validValuesAccepted() {
        StreamMQProperties properties = new StreamMQProperties();
        assertThatCode(properties::validate).doesNotThrowAnyException();

        properties.getConsumer().setOrderlyShardLockLeaseMillis(0L);
        assertThatCode(properties::validate).doesNotThrowAnyException();

        properties.getConsumer().setOrderlyShardLockLeaseMillis(30_000L);
        assertThatCode(properties::validate).doesNotThrowAnyException();
    }
}
