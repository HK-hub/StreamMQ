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
 *   <li>{@code InflightSink} - 投入有界阻塞队列由独立虚拟线程泵消费（背压启用： 队列满则自旋等待，实现拉取与处理解耦；泵 Future 登记到 {@link
 *       ConsumeLoopSupervisor} 以便 unregister/stop 取消）
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
interface MessageSink {

    /** 派发一条消息。Inflight 策略下队列满时等待（背压）；容器停止后立即返回（不无限挂起）。 */
    void dispatch(Message<?> message) throws InterruptedException;

    /** 工厂方法：按背压容量选择策略。 */
    static MessageSink forCapacity(
            int inflightCapacity,
            ListenerRegistration<?> reg,
            StreamMQListener listener,
            MessageProcessor processor,
            ConsumeLoopSupervisor supervisor,
            java.util.concurrent.ExecutorService executor,
            java.util.function.BooleanSupplier running,
            String pumpKey) {
        if (inflightCapacity > 0) {
            return new InflightSink(
                    inflightCapacity,
                    reg,
                    listener,
                    processor,
                    supervisor,
                    executor,
                    running,
                    pumpKey);
        }
        return (message) -> processor.processMessage(message, reg, listener);
    }
}

/** 有界队列解耦策略（含独立泵线程的生命周期管理）。 */
final class InflightSink implements MessageSink {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(InflightSink.class);

    /** 泵线程处理异常后的退避间隔（毫秒）：防止毒丸消息以 CPU 速度热循环 */
    private static final long PUMP_ERROR_BACKOFF_MILLIS = 200;

    /** dispatch 自旋等待的单次 park 时长（纳秒，约 1ms） */
    private static final long DISPATCH_PARK_NANOS = 1_000_000L;

    private final java.util.concurrent.BlockingQueue<Message<?>> queue;

    private final java.util.function.BooleanSupplier running;

    InflightSink(
            int capacity,
            ListenerRegistration<?> reg,
            StreamMQListener listener,
            MessageProcessor processor,
            ConsumeLoopSupervisor supervisor,
            java.util.concurrent.ExecutorService executor,
            java.util.function.BooleanSupplier running,
            String pumpKey) {
        this.running = running;
        // 队列以声明的背压容量为界：满时 dispatch 自旋等待（尊重 running 标志）
        this.queue = new java.util.concurrent.LinkedBlockingQueue<>(capacity);
        // 泵线程提交到容器执行器并登记 Future：否则 unregister/stop 时无法取消该线程，
        // 会残留持有 listener 引用的孤儿虚拟线程。
        // 登记键必须按循环唯一（pumpKey 含循环序号）：此前固定 key 在 consumeThreadMin>1 时
        // 后注册的泵覆盖前一个的 Future，导致旧泵泄漏无法取消。
        java.util.concurrent.Future<?> pumpFuture =
                executor.submit(() -> pumpLoop(reg, listener, processor));
        supervisor.registerInflightPump(pumpKey, pumpFuture);
    }

    /**
     * 泵主循环：从队列取出消息交给处理器。
     *
     * <p><b>脆弱性修复：</b>整个循环体包裹在兜底异常处理中——用户过滤器/处理器抛出的任何 {@code Throwable}（除中断）只记录 ERROR
     * 并退避重试，绝不杀死泵线程（旧实现一条消息的处理异常即令 该注册的全部后续消息永久滞留队列）。
     */
    private void pumpLoop(
            ListenerRegistration<?> reg, StreamMQListener listener, MessageProcessor processor) {
        while (running.getAsBoolean()) {
            Message<?> message = null;
            try {
                message = queue.poll(1, java.util.concurrent.TimeUnit.SECONDS);
                if (message != null) {
                    processor.processMessage(message, reg, listener);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                LOG.error(
                        "Inflight pump swallowed processor error, backing off {}ms:"
                                + " topic={}, group={}, messageId={}",
                        PUMP_ERROR_BACKOFF_MILLIS,
                        reg != null ? reg.getTopic() : "?",
                        reg != null ? reg.getGroup() : "?",
                        message != null ? message.getMessageId() : "?",
                        t);
                io.github.streammq.adapter.redisson.container.ContainerSupport.sleepQuietly(
                        PUMP_ERROR_BACKOFF_MILLIS);
            }
        }
    }

    /** 仅供同包测试观察内部队列状态。 */
    boolean dispatchQueueEmpty() {
        return queue.isEmpty();
    }

    @Override
    public void dispatch(Message<?> message) throws InterruptedException {
        // 有界背压：offer 失败时自旋等待，但始终尊重 running 标志——容器停止后立即返回，
        // 不再永久阻塞读循环（未投递的消息仍在 PEL 中，由恢复路径补齐 at-least-once）
        while (!queue.offer(message)) {
            if (!running.getAsBoolean()) {
                LOG.debug(
                        "Sink cancelled while queue full, dropped buffered dispatch of"
                                + " messageId={} (stays in PEL for redelivery)",
                        message.getMessageId());
                return;
            }
            java.util.concurrent.locks.LockSupport.parkNanos(DISPATCH_PARK_NANOS);
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("dispatch interrupted");
            }
        }
    }
}
