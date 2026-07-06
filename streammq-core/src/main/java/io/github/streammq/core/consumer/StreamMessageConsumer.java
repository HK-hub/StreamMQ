package io.github.streammq.core.consumer;

import io.github.streammq.core.message.Message;

/**
 * StreamMQ 消费者基类接口。
 *
 * <p>所有 StreamMQ 消费者回调接口的根接口，统一消费入口为 {@link #consumeMessage}。
 * 子接口（{@link StreamMessageConcurrentlyConsumer} / {@link StreamMessageOrderlyConsumer}）
 * 通过 default 方法将自身 {@code onMessage} 委派到本方法。
 *
 * <p>框架内部以 {@link StreamMessageConsumer} 引用持有所有 Consumer 实例，
 * 便于在容器层面统一调度与扩展。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMessageConsumer<T> {

    /**
     * 处理单条消息（统一入口）。
     *
     * <p>由容器在拉取消息后调用，实现类内部决定是否 ACK、重试或挂起。
     *
     * @param message 消息载体
     * @param context 消费上下文
     * @throws Exception 业务异常，框架按 Consumer 类型转换为对应的失败动作
     */
    void consumeMessage(Message<T> message, ConsumeContext context) throws Exception;
}
