package io.github.streammq.adapter.redisson.trace;

import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.spi.TraceCollector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link NoopTraceCollector} 单元测试，覆盖空操作行为、isEnabled 与 name。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("NoopTraceCollector 空操作追踪收集器测试")
class NoopTraceCollectorTest {

    private final NoopTraceCollector collector = new NoopTraceCollector();

    @Test
    @DisplayName("isEnabled 返回 false")
    void isEnabled() {
        assertThat(collector.isEnabled()).isFalse();
    }

    @Test
    @DisplayName("name 返回 noop")
    void name() {
        assertThat(collector.name()).isEqualTo("noop");
    }

    @Test
    @DisplayName("recordSend 不抛异常")
    void recordSend() {
        TraceCollector.SendTraceContext ctx = new TraceCollector.SendTraceContext(
                "topic-1", "tag-1", new MessageId("1-0"), "pg",
                System.currentTimeMillis(), true, 10L,
                "trace-1", new HashMap<>());
        // 多次调用不应抛异常
        collector.recordSend(ctx);
        collector.recordSend(null);
    }

    @Test
    @DisplayName("recordConsume 不抛异常")
    void recordConsume() {
        TraceCollector.ConsumeTraceContext ctx = new TraceCollector.ConsumeTraceContext(
                "topic-1", "tag-1", new MessageId("1-0"), "cg", "c1",
                0, true, 10L, "trace-1", new HashMap<>());
        // 多次调用不应抛异常
        collector.recordConsume(ctx);
        collector.recordConsume(null);
    }
}
