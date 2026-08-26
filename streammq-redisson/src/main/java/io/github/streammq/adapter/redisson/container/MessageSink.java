/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;

/**
 * 拉取消息的派发策略（God class 拆分第二轮）。
 *
 * <p><b>设计模式：Strategy。</b>读循环拉到消息后有两种互斥的派发方式，由 {@link #forCapacity} 按背压容量选择：
 *
 * <ul>
 *   <li>{@code InlineSink} - 逐条同步交给 {@link MessageProcessor}（背压禁用）
 *   <li>{@code InflightSink} - 投入有界阻塞队列由独立虚拟线程泵消费（背压启用： 队列满则 {@code put} 阻塞，实现拉取与处理解耦；泵 Future 登记到
 *       {@link ConsumeLoopSupervisor} 以便 unregister/stop 取消）
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
interface MessageSink {

    /** 派发一条消息。Inflight 策略下队列满时阻塞（背压）。 */
    void dispatch(Message<?> message) throws InterruptedException;

    /** 工厂方法：按背压容量选择策略。 */
    static MessageSink forCapacity(
            int inflightCapacity,
            ListenerRegistration<?> reg,
            StreamMQListener listener,
            MessageProcessor processor,
            ConsumeLoopSupervisor supervisor,
            java.util.concurrent.ExecutorService executor,
            java.util.function.BooleanSupplier running) {
        if (inflightCapacity > 0) {
            return new InflightSink(
                    inflightCapacity, reg, listener, processor, supervisor, executor, running);
        }
        return (message) -> processor.processMessage(message, reg, listener);
    }
}

/** 有界队列解耦策略（含独立泵线程的生命周期管理）。 */
final class InflightSink implements MessageSink {

    private final java.util.concurrent.BlockingQueue<Message<?>> queue =
            new java.util.concurrent.LinkedBlockingQueue<>();

    InflightSink(
            int capacity,
            ListenerRegistration<?> reg,
            StreamMQListener listener,
            MessageProcessor processor,
            ConsumeLoopSupervisor supervisor,
            java.util.concurrent.ExecutorService executor,
            java.util.function.BooleanSupplier running) {
        // 泵线程提交到容器执行器并登记 Future：否则 unregister/stop 时无法取消该线程，
        // 会残留持有 listener 引用的孤儿虚拟线程
        java.util.concurrent.Future<?> pumpFuture =
                executor.submit(
                        () -> {
                            while (running.getAsBoolean()) {
                                try {
                                    Message<?> message =
                                            queue.poll(1, java.util.concurrent.TimeUnit.SECONDS);
                                    if (message != null) {
                                        processor.processMessage(message, reg, listener);
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            }
                        });
        supervisor.registerInflightPump(reg.key(), pumpFuture);
    }

    @Override
    public void dispatch(Message<?> message) throws InterruptedException {
        queue.put(message);
    }
}
