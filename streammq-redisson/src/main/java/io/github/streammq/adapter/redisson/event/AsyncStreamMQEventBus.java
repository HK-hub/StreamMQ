/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.event;

import io.github.streammq.core.event.StreamMQEventBus;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 异步事件总线默认实现，使用虚拟线程处理事件订阅。
 *
 * <p>线程安全，支持运行时动态注册订阅者。 事件异步分发，不影响发布者主流程。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class AsyncStreamMQEventBus implements StreamMQEventBus {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncStreamMQEventBus.class);

    /**
     * 单订阅者最大排队事件数：超过后丢弃并告警。
     *
     * <p>事件总线仅承载可观测性（Tracing/审计），丢弃事件不影响消息收发正确性； 反之若不设上限，消费速率骤增而订阅者处理缓慢时，无界任务提交会放大内存压力。
     */
    static final int MAX_PENDING_PER_SUBSCRIBER = 10_000;

    private final Map<Class<?>, List<Consumer<?>>> subscribers = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    /** 各订阅者的在途任务计数（信号量式背压） */
    private final Map<Consumer<?>, java.util.concurrent.atomic.AtomicInteger> pendingCounts =
            new ConcurrentHashMap<>();

    @Override
    public <E> void publish(E event) {
        Objects.requireNonNull(event, "event");
        List<Consumer<?>> list = subscribers.get(event.getClass());
        if (list == null || list.isEmpty()) {
            return;
        }
        for (Consumer<?> subscriber : list) {
            @SuppressWarnings("unchecked")
            Consumer<E> typed = (Consumer<E>) subscriber;
            var pending =
                    pendingCounts.computeIfAbsent(
                            subscriber, k -> new java.util.concurrent.atomic.AtomicInteger());
            // 背压：超出上限时丢弃本条事件（可观测性数据允许有损）
            if (pending.get() >= MAX_PENDING_PER_SUBSCRIBER) {
                LOG.warn(
                        "Event dropped (subscriber backlog exceeded {}): event={}",
                        MAX_PENDING_PER_SUBSCRIBER,
                        event.getClass().getSimpleName());
                continue;
            }
            pending.incrementAndGet();
            try {
                executor.submit(
                        () -> {
                            try {
                                typed.accept(event);
                            } catch (Exception ex) {
                                LOG.warn(
                                        "Event subscriber error for {}: {}",
                                        event.getClass().getSimpleName(),
                                        ex.getMessage(),
                                        ex);
                            } finally {
                                pending.decrementAndGet();
                            }
                        });
            } catch (RejectedExecutionException ex) {
                // close() 后的发布不得向业务主流程抛异常（事件仅承载可观测性数据）
                pending.decrementAndGet();
                LOG.debug(
                        "Event dropped, event bus is closed: {}", event.getClass().getSimpleName());
            }
        }
    }

    @Override
    public <E> void subscribe(Class<E> eventType, Consumer<E> subscriber) {
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(subscriber, "subscriber");
        subscribers.computeIfAbsent(eventType, k -> new CopyOnWriteArrayList<>()).add(subscriber);
        LOG.debug("Subscribed to event: {}", eventType.getSimpleName());
    }

    /** 关闭事件总线，释放异步分发线程池。 */
    public void close() {
        executor.shutdown();
    }
}
