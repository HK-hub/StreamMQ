package io.github.streammq.core.consumer;

import io.github.streammq.core.message.Message;

/**
 * 手动 ACK 消费回调接口（并发消费）。
 *
 * <p>使用此接口的 Consumer 需在 {@code @StreamMqConsumer} 中显式设置 {@code acknowledgeMode = MANUAL}，
 * 并通过 {@link ConsumeConcurrentlyContext#acknowledge()} 显式 ACK：
 * <ul>
 *   <li>{@link Acknowledgment#acknowledge()} - 确认成功</li>
 *   <li>{@link Acknowledgment#nack()} - 立即重投</li>
 *   <li>{@link Acknowledgment#defer(java.time.Duration)} - 延迟重投</li>
 * </ul>
 *
 * <p>若 onMessage 退出时未调用任何 ACK 方法，框架视为失败，进入重试。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMessageManualAckConsumer<T> {

    /**
     * 处理单条消息（无返回值，通过 context.acknowledge() 显式 ACK）。
     *
     * @param message 消息载体
     * @param context 消费上下文
     * @throws Exception 业务异常，框架将其视为消费失败
     */
    void onMessage(Message<T> message, ConsumeConcurrentlyContext context) throws Exception;
}
