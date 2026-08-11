package io.github.streammq.adapter.redisson.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.streammq.core.message.Message;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link ExponentialBackoffRetryPolicy} 单元测试，覆盖默认参数、自定义参数构造校验、 指数退避计算边界、shouldStopRetry 与 getter
 * 方法。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("ExponentialBackoffRetryPolicy 指数退避重试策略测试")
class ExponentialBackoffRetryPolicyTest {

  private final Message<String> msg = new Message<>();

  @Test
  @DisplayName("默认参数: initial=1000ms, multiplier=2.0, max=7200000ms, maxReconsumeTimes=16")
  void defaultParameters() {
    ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy();
    assertThat(policy.getInitialMillis()).isEqualTo(1000L);
    assertThat(policy.getMultiplier()).isEqualTo(2.0);
    assertThat(policy.getMaxMillis()).isEqualTo(7_200_000L);
    assertThat(policy.getMaxReconsumeTimes()).isEqualTo(16);
  }

  @Test
  @DisplayName("自定义参数构造")
  void customParameters() {
    ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy(500L, 1.5, 60_000L, 8);
    assertThat(policy.getInitialMillis()).isEqualTo(500L);
    assertThat(policy.getMultiplier()).isEqualTo(1.5);
    assertThat(policy.getMaxMillis()).isEqualTo(60_000L);
    assertThat(policy.getMaxReconsumeTimes()).isEqualTo(8);
  }

  @Test
  @DisplayName("initialMillis <= 0 抛出 IllegalArgumentException")
  void invalidInitial() {
    assertThatThrownBy(() -> new ExponentialBackoffRetryPolicy(0, 2.0, 1000L, 16))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("initialMillis");
    assertThatThrownBy(() -> new ExponentialBackoffRetryPolicy(-1, 2.0, 1000L, 16))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("multiplier <= 1.0 抛出 IllegalArgumentException")
  void invalidMultiplier() {
    assertThatThrownBy(() -> new ExponentialBackoffRetryPolicy(1000L, 1.0, 10_000L, 16))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("multiplier");
    assertThatThrownBy(() -> new ExponentialBackoffRetryPolicy(1000L, 0.5, 10_000L, 16))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("maxMillis < initialMillis 抛出 IllegalArgumentException")
  void invalidMaxLessThanInitial() {
    assertThatThrownBy(() -> new ExponentialBackoffRetryPolicy(1000L, 2.0, 999L, 16))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxMillis");
  }

  @Test
  @DisplayName("maxReconsumeTimes <= 0 抛出 IllegalArgumentException")
  void invalidMaxReconsumeTimes() {
    assertThatThrownBy(() -> new ExponentialBackoffRetryPolicy(1000L, 2.0, 10_000L, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxReconsumeTimes");
    assertThatThrownBy(() -> new ExponentialBackoffRetryPolicy(1000L, 2.0, 10_000L, -5))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("maxMillis == initialMillis 合法（边界）")
  void maxEqualsInitial() {
    ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy(1000L, 2.0, 1000L, 4);
    assertThat(policy.getMaxMillis()).isEqualTo(policy.getInitialMillis());
  }

  @Test
  @DisplayName("nextRetryDelay(0) = 1000ms（initial）")
  void nextRetryDelayZero() {
    ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy();
    assertThat(policy.nextRetryDelay(0, msg)).isEqualTo(Duration.ofMillis(1000L));
  }

  @Test
  @DisplayName("nextRetryDelay(1) = 2000ms（initial * 2）")
  void nextRetryDelayOne() {
    ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy();
    assertThat(policy.nextRetryDelay(1, msg)).isEqualTo(Duration.ofMillis(2000L));
  }

  @Test
  @DisplayName("nextRetryDelay(2) = 4000ms（initial * 4）")
  void nextRetryDelayTwo() {
    ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy();
    assertThat(policy.nextRetryDelay(2, msg)).isEqualTo(Duration.ofMillis(4000L));
  }

  @Test
  @DisplayName("nextRetryDelay(100) = null（超出 maxReconsumeTimes 返回 null）")
  void nextRetryDelayCappedAtMax() {
    ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy();
    assertThat(policy.nextRetryDelay(100, msg)).isNull();
  }

  @Test
  @DisplayName("nextRetryDelay(16) = null（达到 maxReconsumeTimes）")
  void nextRetryDelayAtMax() {
    ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy();
    assertThat(policy.nextRetryDelay(16, msg)).isNull();
  }

  @Test
  @DisplayName("nextRetryDelay(-1) = 1000ms（负数归零）")
  void nextRetryDelayNegative() {
    ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy();
    assertThat(policy.nextRetryDelay(-1, msg)).isEqualTo(Duration.ofMillis(1000L));
  }

  @Test
  @DisplayName("nextRetryDelay message 为 null 抛出 NullPointerException")
  void nextRetryDelayNullMessage() {
    ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy();
    assertThatThrownBy(() -> policy.nextRetryDelay(0, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("message");
  }

  @Test
  @DisplayName("shouldStopRetry(15) 默认策略不停止（15 < 16）")
  void shouldStopRetryBelowMax() {
    ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy();
    assertThat(policy.shouldStopRetry(15, msg)).isFalse();
  }

  @Test
  @DisplayName("shouldStopRetry(16) 默认策略停止（达到上限）")
  void shouldStopRetryAtMax() {
    ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy();
    assertThat(policy.shouldStopRetry(16, msg)).isTrue();
  }

  @Test
  @DisplayName("shouldStopRetry(0) 不停止")
  void shouldStopRetryZero() {
    ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy();
    assertThat(policy.shouldStopRetry(0, msg)).isFalse();
  }

  @Test
  @DisplayName("shouldStopRetry message 为 null 抛出 NullPointerException")
  void shouldStopRetryNullMessage() {
    ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy();
    assertThatThrownBy(() -> policy.shouldStopRetry(0, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  @DisplayName("getter 方法返回构造值")
  void getters() {
    ExponentialBackoffRetryPolicy policy =
        new ExponentialBackoffRetryPolicy(2000L, 3.0, 100_000L, 10);
    assertThat(policy.getInitialMillis()).isEqualTo(2000L);
    assertThat(policy.getMultiplier()).isEqualTo(3.0);
    assertThat(policy.getMaxMillis()).isEqualTo(100_000L);
    assertThat(policy.getMaxReconsumeTimes()).isEqualTo(10);
  }

  @Test
  @DisplayName("name 返回类名")
  void name() {
    assertThat(new ExponentialBackoffRetryPolicy().name())
        .isEqualTo("ExponentialBackoffRetryPolicy");
  }
}
