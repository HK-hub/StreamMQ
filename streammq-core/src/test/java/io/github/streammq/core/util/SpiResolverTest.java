package io.github.streammq.core.util;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.policy.RetryPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link SpiResolver} 单元测试。
 */
@DisplayName("SpiResolver 测试")
class SpiResolverTest {

    /** 测试用 SPI：无参构造 */
    public static class NoArgPolicy implements RetryPolicy {
        @Override
        public Duration nextRetryDelay(int reconsumeTimes, Message<?> message) {
            return Duration.ZERO;
        }

        @Override
        public boolean shouldStopRetry(int reconsumeTimes, Message<?> message) {
            return false;
        }
    }

    /** 测试用 SPI：仅含带参构造（无无参构造） */
    public static class NoNoArgPolicy implements RetryPolicy {
        private final int x;
        public NoNoArgPolicy(int x) { this.x = x; }
        @Override
        public Duration nextRetryDelay(int reconsumeTimes, Message<?> message) { return Duration.ZERO; }
        @Override
        public boolean shouldStopRetry(int reconsumeTimes, Message<?> message) { return false; }
    }

    @Test
    @DisplayName("clazz == spiType（marker）时返回全局默认")
    void markerReturnsGlobal() {
        RetryPolicy global = new NoArgPolicy();
        RetryPolicy resolved = SpiResolver.resolveOrInstantiate(RetryPolicy.class, RetryPolicy.class, global);
        assertThat(resolved).isSameAs(global);
    }

    @Test
    @DisplayName("clazz 为 null 时返回全局默认")
    void nullReturnsGlobal() {
        RetryPolicy global = new NoArgPolicy();
        RetryPolicy resolved = SpiResolver.resolveOrInstantiate(null, RetryPolicy.class, global);
        assertThat(resolved).isSameAs(global);
    }

    @Test
    @DisplayName("自定义类无参实例化，不返回全局默认")
    void customInstantiated() {
        RetryPolicy global = new NoArgPolicy();
        RetryPolicy resolved = SpiResolver.resolveOrInstantiate(NoArgPolicy.class, RetryPolicy.class, global);
        assertThat(resolved).isInstanceOf(NoArgPolicy.class).isNotSameAs(global);
    }

    @Test
    @DisplayName("自定义类无无参构造时抛 IllegalArgumentException")
    void noNoArgThrows() {
        RetryPolicy global = new NoArgPolicy();
        assertThatThrownBy(() -> SpiResolver.resolveOrInstantiate(NoNoArgPolicy.class, RetryPolicy.class, global))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("no-arg constructor");
    }
}
