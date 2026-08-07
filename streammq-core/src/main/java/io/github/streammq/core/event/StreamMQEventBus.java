package io.github.streammq.core.event;

import java.util.function.Consumer;

/**
 * 事件总线，用于模块间解耦通信。
 *
 * <p>核心处理流程（发送、消费、重试、死信、事务等）在关键节点发布事件，
 * 扩展模块（Tracing、Metrics、Diagnostics）订阅事件后异步处理，
 * 避免核心流程直接依赖扩展模块。
 *
 * <p>事件处理不应阻塞或抛出异常影响主流程。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMQEventBus {

    /**
     * 发布事件（异步，不阻塞主流程）。
     *
     * @param event 事件对象
     * @param <E>   事件类型
     */
    <E> void publish(E event);

    /**
     * 订阅指定类型的事件。
     *
     * @param eventType 事件类型
     * @param subscriber 事件消费者（在异步线程中执行）
     * @param <E>        事件类型
     */
    <E> void subscribe(Class<E> eventType, Consumer<E> subscriber);
}
