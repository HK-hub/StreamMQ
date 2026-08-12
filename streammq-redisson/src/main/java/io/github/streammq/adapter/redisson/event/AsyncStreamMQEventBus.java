package io.github.streammq.adapter.redisson.event;

import io.github.streammq.core.event.StreamMQEventBus;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    private final Map<Class<?>, List<Consumer<?>>> subscribers = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

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
            executor.submit(
                    () -> {
                        try {
                            typed.accept(event);
                        } catch (Exception ex) {
                            LOG.debug(
                                    "Event subscriber error for {}: {}",
                                    event.getClass().getSimpleName(),
                                    ex.getMessage());
                        }
                    });
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
