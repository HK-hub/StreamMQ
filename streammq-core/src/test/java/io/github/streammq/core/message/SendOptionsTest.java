/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.streammq.core.template.StreamMessageTemplate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link SendOptions} 单元测试，覆盖哨兵语义、Builder 参数校验与有效值换算（F-09 回归）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("SendOptions 发送选项测试")
class SendOptionsTest {

    @Test
    @DisplayName("defaults() 未设置任何字段，生效值取模板默认")
    void defaultsUseTemplateDefaults() {
        SendOptions options = SendOptions.defaults();
        assertThat(options.getTimeoutMillis()).isEqualTo(-1L);
        assertThat(options.getRetryTimes()).isEqualTo(-1);
        assertThat(options.effectiveTimeoutMillis())
                .isEqualTo(StreamMessageTemplate.DEFAULT_SEND_TIMEOUT_MILLIS);
        assertThat(options.effectiveRetryTimes())
                .isEqualTo(StreamMessageTemplate.DEFAULT_SYNC_RETRY_TIMES);
    }

    @Test
    @DisplayName("builder 显式设置后按字面值生效（retryTimes=0 表示零次重试）")
    void explicitValuesTakeEffect() {
        SendOptions options = SendOptions.builder().timeoutMillis(5000).retryTimes(0).build();
        assertThat(options.getTimeoutMillis()).isEqualTo(5000L);
        assertThat(options.getRetryTimes()).isZero();
        assertThat(options.effectiveTimeoutMillis()).isEqualTo(5000L);
        assertThat(options.effectiveRetryTimes()).isZero();
    }

    @Test
    @DisplayName("builder timeoutMillis <= 0 抛 IllegalArgumentException（F-09 回归：不再静默回退默认）")
    void builderRejectsNonPositiveTimeout() {
        for (long bad : new long[] {0L, -1L, -5000L}) {
            assertThatThrownBy(() -> SendOptions.builder().timeoutMillis(bad))
                    .as("timeoutMillis=%s 应被拒绝", bad)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("timeoutMillis must be positive");
        }
    }

    @Test
    @DisplayName("builder retryTimes < 0 抛 IllegalArgumentException")
    void builderRejectsNegativeRetry() {
        assertThatThrownBy(() -> SendOptions.builder().retryTimes(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryTimes must be >= 0");
    }

    @Test
    @DisplayName("of(timeout, retry) 接受 -1 哨兵或合法值，拒绝 0 超时与负重试")
    void ofContract() {
        assertThat(SendOptions.of(-1L, -1)).isEqualTo(SendOptions.defaults());
        assertThat(SendOptions.of(1000L, 2).getTimeoutMillis()).isEqualTo(1000L);

        assertThatThrownBy(() -> SendOptions.of(0L, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timeoutMillis");
        assertThatThrownBy(() -> SendOptions.of(-2L, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timeoutMillis");
        assertThatThrownBy(() -> SendOptions.of(1000L, -3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryTimes");
    }

    @Test
    @DisplayName("equals/hashCode 基于原始字段值")
    void equalsAndHashCode() {
        assertThat(SendOptions.builder().timeoutMillis(1).retryTimes(1).build())
                .isEqualTo(SendOptions.builder().timeoutMillis(1).retryTimes(1).build());
        assertThat(SendOptions.builder().timeoutMillis(1).retryTimes(1).build())
                .isNotEqualTo(SendOptions.builder().timeoutMillis(2).retryTimes(1).build());
    }
}
