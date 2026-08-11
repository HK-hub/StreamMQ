package io.github.streammq.adapter.redisson.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.streammq.core.message.Message;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link FixedArrayRetryPolicy} 单元测试，覆盖固定延时数组、构造校验、 nextRetryDelay 边界与 shouldStopRetry 行为。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("FixedArrayRetryPolicy 固定数组重试策略测试")
class FixedArrayRetryPolicyTest {

    private final Message<String> msg = new Message<>();

    @Test
    @DisplayName("DELAY_MILLIS 数组长度为 16")
    void delayMillisLength() {
        assertThat(FixedArrayRetryPolicy.DELAY_MILLIS).hasSize(16);
    }

    @Test
    @DisplayName("DELAY_MILLIS 首元素为 10000ms（10s）")
    void delayMillisFirst() {
        assertThat(FixedArrayRetryPolicy.DELAY_MILLIS[0]).isEqualTo(10_000L);
    }

    @Test
    @DisplayName("DELAY_MILLIS 末元素为 7200000ms（2h）")
    void delayMillisLast() {
        assertThat(FixedArrayRetryPolicy.DELAY_MILLIS[15]).isEqualTo(7_200_000L);
    }

    @Test
    @DisplayName("默认构造 maxReconsumeTimes 为 16")
    void defaultMaxReconsumeTimes() {
        FixedArrayRetryPolicy policy = new FixedArrayRetryPolicy();
        assertThat(policy.getMaxReconsumeTimes()).isEqualTo(16);
    }

    @Test
    @DisplayName("自定义 maxReconsumeTimes(8)")
    void customMaxReconsumeTimes() {
        FixedArrayRetryPolicy policy = new FixedArrayRetryPolicy(8);
        assertThat(policy.getMaxReconsumeTimes()).isEqualTo(8);
    }

    @Test
    @DisplayName("maxReconsumeTimes <= 0 抛出 IllegalArgumentException")
    void invalidMaxReconsumeTimes() {
        assertThatThrownBy(() -> new FixedArrayRetryPolicy(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxReconsumeTimes");
        assertThatThrownBy(() -> new FixedArrayRetryPolicy(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("nextRetryDelay(0) = 10s")
    void nextRetryDelayFirstLevel() {
        FixedArrayRetryPolicy policy = new FixedArrayRetryPolicy();
        assertThat(policy.nextRetryDelay(0, msg)).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("nextRetryDelay(15) = 2h（最后一个级别）")
    void nextRetryDelayLastLevel() {
        FixedArrayRetryPolicy policy = new FixedArrayRetryPolicy();
        assertThat(policy.nextRetryDelay(15, msg)).isEqualTo(Duration.ofHours(2));
    }

    @Test
    @DisplayName("nextRetryDelay(16) = null（达到 maxReconsumeTimes）")
    void nextRetryDelayAtMax() {
        FixedArrayRetryPolicy policy = new FixedArrayRetryPolicy();
        assertThat(policy.nextRetryDelay(16, msg)).isNull();
    }

    @Test
    @DisplayName("nextRetryDelay(100) = null（超出 maxReconsumeTimes 返回 null）")
    void nextRetryDelayOverflow() {
        FixedArrayRetryPolicy policy = new FixedArrayRetryPolicy();
        assertThat(policy.nextRetryDelay(100, msg)).isNull();
    }

    @Test
    @DisplayName("nextRetryDelay(-1) = 10s（负数归零）")
    void nextRetryDelayNegative() {
        FixedArrayRetryPolicy policy = new FixedArrayRetryPolicy();
        assertThat(policy.nextRetryDelay(-1, msg)).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("nextRetryDelay message 为 null 抛出 NullPointerException")
    void nextRetryDelayNullMessage() {
        FixedArrayRetryPolicy policy = new FixedArrayRetryPolicy();
        assertThatThrownBy(() -> policy.nextRetryDelay(0, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("message");
    }

    @Test
    @DisplayName("shouldStopRetry(15) 默认策略下不停止（15 < 16）")
    void shouldStopRetryBelowMax() {
        FixedArrayRetryPolicy policy = new FixedArrayRetryPolicy();
        assertThat(policy.shouldStopRetry(15, msg)).isFalse();
    }

    @Test
    @DisplayName("shouldStopRetry(16) 默认策略下停止（达到上限）")
    void shouldStopRetryAtMax() {
        FixedArrayRetryPolicy policy = new FixedArrayRetryPolicy();
        assertThat(policy.shouldStopRetry(16, msg)).isTrue();
    }

    @Test
    @DisplayName("shouldStopRetry(0) 不停止")
    void shouldStopRetryZero() {
        FixedArrayRetryPolicy policy = new FixedArrayRetryPolicy();
        assertThat(policy.shouldStopRetry(0, msg)).isFalse();
    }

    @Test
    @DisplayName("shouldStopRetry(15) 自定义 maxReconsumeTimes=15 时停止")
    void shouldStopRetryCustomMax() {
        FixedArrayRetryPolicy policy = new FixedArrayRetryPolicy(15);
        assertThat(policy.shouldStopRetry(15, msg)).isTrue();
    }

    @Test
    @DisplayName("shouldStopRetry message 为 null 抛出 NullPointerException")
    void shouldStopRetryNullMessage() {
        FixedArrayRetryPolicy policy = new FixedArrayRetryPolicy();
        assertThatThrownBy(() -> policy.shouldStopRetry(0, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("getMaxReconsumeTimes 返回构造值")
    void getMaxReconsumeTimes() {
        assertThat(new FixedArrayRetryPolicy().getMaxReconsumeTimes()).isEqualTo(16);
        assertThat(new FixedArrayRetryPolicy(5).getMaxReconsumeTimes()).isEqualTo(5);
    }

    @Test
    @DisplayName("name 返回类名")
    void name() {
        assertThat(new FixedArrayRetryPolicy().name()).isEqualTo("FixedArrayRetryPolicy");
    }
}
