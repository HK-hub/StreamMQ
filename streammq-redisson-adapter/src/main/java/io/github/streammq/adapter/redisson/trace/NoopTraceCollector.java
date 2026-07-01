package io.github.streammq.adapter.redisson.trace;

import io.github.streammq.core.spi.TraceCollector;

/**
 * 空操作链路追踪收集器，默认实现。
 *
 * <p>{@link #isEnabled()} 返回 false，所有记录方法不做任何操作。
 * 适用于不需要链路追踪的场景，作为安全兜底避免空指针。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class NoopTraceCollector implements TraceCollector {

    @Override
    public void recordSend(SendTraceContext context) {
        // 空操作，不记录任何追踪数据
    }

    @Override
    public void recordConsume(ConsumeTraceContext context) {
        // 空操作，不记录任何追踪数据
    }

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public String name() {
        return "noop";
    }
}
