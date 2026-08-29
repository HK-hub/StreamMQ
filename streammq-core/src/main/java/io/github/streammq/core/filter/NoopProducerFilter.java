/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.filter;

import io.github.streammq.core.message.Message;

/**
 * 空操作生产者过滤器（默认实现），接受所有消息。
 *
 * <p>v0.1.0 起作为 {@link ProducerFilter} SPI 的默认实现存在，使得未配置任何 {@code ProducerFilter} Bean 时过滤器链不会
 * NPE。业务方可注册自定义 Bean（如标签白名单、Schema 校验）覆盖本默认。
 *
 * <p>本类为单例，线程安全。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class NoopProducerFilter implements ProducerFilter {

    /** 共享单例。 */
    public static final NoopProducerFilter INSTANCE = new NoopProducerFilter();

    private NoopProducerFilter() {}

    @Override
    public boolean accept(Message<?> message) {
        return true;
    }

    @Override
    public String name() {
        return "noop";
    }
}
