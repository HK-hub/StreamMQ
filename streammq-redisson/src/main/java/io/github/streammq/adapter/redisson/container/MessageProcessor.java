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
import java.util.concurrent.ExecutorService;

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

    /**
     * 替换执行业务消费回调的执行器。
     *
     * <p>存在原因：本接口的实现会在构造时捕获一个执行器引用，而容器的执行器可在 INIT 阶段被 {@code setConsumeExecutor}
     * 替换。容器必须把新执行器同步过来——否则实现仍指向旧的、已被 关闭的执行器，消费时会抛 {@code RejectedExecutionException}（且该异常只体现为"消费者静默
     * 不消费"，极难排查）。
     *
     * @param executor 新的执行器
     */
    void setExecutor(ExecutorService executor);
}
