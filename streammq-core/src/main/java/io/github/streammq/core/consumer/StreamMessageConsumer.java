package io.github.streammq.core.consumer;

/**
 * StreamMQ 消费者回调根接口（所有消费者回调接口的公共父类型）。
 *
 * <p>子接口 {@link StreamMessageConcurrentlyConsumer}（并发消费）与
 * {@link StreamMessageOrderlyConsumer}（顺序消费）分别定义 {@code onMessage} 方法，
 * 返回 {@link io.github.streammq.core.enums.ConsumeAction} 作为唯一消费结果表达。
 *
 * <p>框架内部以 {@link StreamMessageConsumer} 引用持有所有 Consumer 实例，
 * 便于在容器层面统一调度与扩展。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMessageConsumer<T> {
}
