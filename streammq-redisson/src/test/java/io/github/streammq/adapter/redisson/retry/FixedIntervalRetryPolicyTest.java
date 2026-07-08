package io.github.streammq.adapter.redisson.retry;

import io.github.streammq.core.message.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link FixedIntervalRetryPolicy} 单元测试，覆盖默认/自定义参数、
 * nextRetryDelay 边界、shouldStopRetry 行为与构造校验。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("FixedIntervalRetryPolicy 固定间隔重试策略测试")
class FixedIntervalRetryPolicyTest {

    private final Message<String> msg = new Message<>();

    @Test
    @DisplayName("默认构造 intervalMs=10000, maxRetries=16")
    void defaultConstructor() {
        FixedIntervalRetryPolicy policy = new FixedIntervalRetryPolicy();
        assertThat(policy.getIntervalMs()).isEqualTo(10_000L);
        assertThat(policy.getMaxRetries()).isEqualTo(16);
    }

    @Test
    @DisplayName("自定义构造 intervalMs=5000, maxRetries=3")
    void customConstructor() {
        FixedIntervalRetryPolicy policy = new FixedIntervalRetryPolicy(5_000L, 3);
        assertThat(policy.getIntervalMs()).isEqualTo(5_000L);
        assertThat(policy.getMaxRetries()).isEqualTo(3);
    }

    @Test
    @DisplayName("intervalMs <= 0 抛出 IllegalArgumentException")
    void invalidIntervalMs() {
        assertThatThrownBy(() -> new FixedIntervalRetryPolicy(0, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("intervalMs");
        assertThatThrownBy(() -> new FixedIntervalRetryPolicy(-1, 3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("maxRetries <= 0 抛出 IllegalArgumentException")
    void invalidMaxRetries() {
        assertThatThrownBy(() -> new FixedIntervalRetryPolicy(1_000L, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetries");
        assertThatThrownBy(() -> new FixedIntervalRetryPolicy(1_000L, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("nextRetryDelay(0) 默认参数返回 10s")
    void nextRetryDelayZero() {
        FixedIntervalRetryPolicy policy = new FixedIntervalRetryPolicy();
        assertThat(policy.nextRetryDelay(0, msg)).isEqualTo(Duration.ofMillis(10_000L));
    }

    @Test
    @DisplayName("nextRetryDelay 在 maxRetries 之前返回固定间隔")
    void nextRetryDelayBeforeMax() {
        FixedIntervalRetryPolicy policy = new FixedIntervalRetryPolicy(2_000L, 5);
        for (int i = 0; i < 5; i++) {
            assertThat(policy.nextRetryDelay(i, msg)).isEqualTo(Duration.ofMillis(2_000L));
        }
    }

    @Test
    @DisplayName("nextRetryDelay 达到 maxRetries 返回 null")
    void nextRetryDelayAtMax() {
        FixedIntervalRetryPolicy policy = new FixedIntervalRetryPolicy(1_000L, 3);
        assertThat(policy.nextRetryDelay(3, msg)).isNull();
        assertThat(policy.nextRetryDelay(4, msg)).isNull();
        assertThat(policy.nextRetryDelay(100, msg)).isNull();
    }

    @Test
    @DisplayName("nextRetryDelay 负数归零处理")
    void nextRetryDelayNegative() {
        FixedIntervalRetryPolicy policy = new FixedIntervalRetryPolicy(1_000L, 3);
        assertThat(policy.nextRetryDelay(-1, msg)).isEqualTo(Duration.ofMillis(1_000L));
    }

    @Test
    @DisplayName("nextRetryDelay message 为 null 抛出 NullPointerException")
    void nextRetryDelayNullMessage() {
        FixedIntervalRetryPolicy policy = new FixedIntervalRetryPolicy();
        assertThatThrownBy(() -> policy.nextRetryDelay(0, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("message");
    }

    @Test
    @DisplayName("shouldStopRetry 小于 maxRetries 返回 false")
    void shouldStopRetryBeforeMax() {
        FixedIntervalRetryPolicy policy = new FixedIntervalRetryPolicy(1_000L, 5);
        assertThat(policy.shouldStopRetry(0, msg)).isFalse();
        assertThat(policy.shouldStopRetry(4, msg)).isFalse();
    }

    @Test
    @DisplayName("shouldStopRetry 达到 maxRetries 返回 true")
    void shouldStopRetryAtMax() {
        FixedIntervalRetryPolicy policy = new FixedIntervalRetryPolicy(1_000L, 5);
        assertThat(policy.shouldStopRetry(5, msg)).isTrue();
        assertThat(policy.shouldStopRetry(6, msg)).isTrue();
    }

    @Test
    @DisplayName("shouldStopRetry 负数归零处理")
    void shouldStopRetryNegative() {
        FixedIntervalRetryPolicy policy = new FixedIntervalRetryPolicy(1_000L, 5);
        assertThat(policy.shouldStopRetry(-1, msg)).isFalse();
    }

    @Test
    @DisplayName("shouldStopRetry message 为 null 抛出 NullPointerException")
    void shouldStopRetryNullMessage() {
        FixedIntervalRetryPolicy policy = new FixedIntervalRetryPolicy();
        assertThatThrownBy(() -> policy.shouldStopRetry(0, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("name 返回 fixed-interval")
    void name() {
        assertThat(new FixedIntervalRetryPolicy().name()).isEqualTo("fixed-interval");
    }
}
