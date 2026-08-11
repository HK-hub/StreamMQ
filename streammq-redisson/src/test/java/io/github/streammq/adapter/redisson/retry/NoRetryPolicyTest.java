package io.github.streammq.adapter.redisson.retry;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.core.message.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link NoRetryPolicy} 单元测试。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("NoRetryPolicy 不重试策略测试")
class NoRetryPolicyTest {

    private final NoRetryPolicy policy = new NoRetryPolicy();
    private final Message<String> msg = new Message<>();

    @Test
    @DisplayName("nextRetryDelay 始终返回 null")
    void nextRetryDelayAlwaysNull() {
        assertThat(policy.nextRetryDelay(0, msg)).isNull();
        assertThat(policy.nextRetryDelay(1, msg)).isNull();
        assertThat(policy.nextRetryDelay(100, msg)).isNull();
    }

    @Test
    @DisplayName("shouldStopRetry 始终返回 true")
    void shouldStopRetryAlwaysTrue() {
        assertThat(policy.shouldStopRetry(0, msg)).isTrue();
        assertThat(policy.shouldStopRetry(1, msg)).isTrue();
        assertThat(policy.shouldStopRetry(100, msg)).isTrue();
    }

    @Test
    @DisplayName("name 返回 no-retry")
    void name() {
        assertThat(policy.name()).isEqualTo("no-retry");
    }

    @Test
    @DisplayName("message 为 null 时不抛异常（nextRetryDelay）")
    void nextRetryDelayNullMessage() {
        // NoRetryPolicy 不读取 message，应容忍 null
        assertThat(policy.nextRetryDelay(0, null)).isNull();
    }

    @Test
    @DisplayName("message 为 null 时不抛异常（shouldStopRetry）")
    void shouldStopRetryNullMessage() {
        assertThat(policy.shouldStopRetry(0, null)).isTrue();
    }
}
