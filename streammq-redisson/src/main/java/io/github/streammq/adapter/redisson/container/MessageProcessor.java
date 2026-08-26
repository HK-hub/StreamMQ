/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.metrics.StreamMQMetrics;

/**
 * 单条消息消费管线。
 *
 * <p><b>SPI：</b>容器与读循环仅依赖本接口；默认实现 {@link DefaultMessageProcessor}。 职责：过滤器/拦截器前置检查、DLQ / 顺序 /
 * 并发三类消费分发、超时取消与宽限期、 拦截器 after 与消费指标。无状态、可并发调用。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface MessageProcessor {

    /** 处理单条消息：以 {@code onMessage} 返回值路由 ACK/重试/DLQ。 */
    void processMessage(Message<?> message, ListenerRegistration<?> reg, StreamMQListener listener);

    /** 注入指标收集器（null 时为 no-op）。 */
    void setMetrics(StreamMQMetrics metrics);

    /** 设置消费超时取消后的业务线程等待宽限期（毫秒）。 */
    void setTimeoutCancelGraceMillis(long millis);
}
