/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.properties;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    @DisplayName("retry.maxReconsumeTimes < 0 应被拒绝")
    void negativeMaxReconsumeTimes_rejected() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getRetry().setMaxReconsumeTimes(-1);
        assertThatThrownBy(properties::validate)
                .isInstanceOf(IllegalArgumentException.class)
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
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("streammq.retry.batch-size must be > 0");
    }

    @Test
    @DisplayName("delay.batchSize <= 0 应被拒绝")
    void nonPositiveDelayBatchSize_rejected() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getDelay().setBatchSize(-5);
        assertThatThrownBy(properties::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("streammq.delay.batch-size must be > 0");
    }

    @Test
    @DisplayName("producer.maxMessageSize <= 0 应被拒绝")
    void nonPositiveMaxMessageSize_rejected() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getProducer().setMaxMessageSize(0);
        assertThatThrownBy(properties::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("streammq.producer.max-message-size must be > 0");
    }

    @Test
    @DisplayName("dlq.dlqRetryDelayMs < 0 应被拒绝")
    void negativeDlqRetryDelayMs_rejected() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getDlq().setDlqRetryDelayMs(-1);
        assertThatThrownBy(properties::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("streammq.dlq.dlq-retry-delay-ms must be >= 0");
    }
}
