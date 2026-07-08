package io.github.streammq.core.consumer;

import io.github.streammq.core.message.Message;

/**
 * 死信队列（DLQ）消费回调接口，专用于处理进入 DLQ Stream 的死信消息。
 *
 * <p>与普通消费接口 {@link StreamMessageConcurrentlyConsumer} 的区别：
 * <ul>
 *   <li>返回 void（非 {@code ConsumeAction}）：DLQ 消费者的失败处理由 {@code DlqFailureStrategy} 策略决定，
 *       而非返回值驱动重试链路，避免死信无限循环</li>
 *   <li>成功时框架自动 ACK，失败（throw）时进入 {@code DlqFailureStrategy#decide} 决策</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * @Component
 * @StreamMQDlqConsumer(consumerGroup = "order-cg", maxDlqRetryAttempts = 3)
 * public class OrderDlqConsumer extends AbstractDlqMessageConsumer<Order> {
 *     @Override
 *     public void onDlqMessage(Message<Order> msg, ConsumeContext ctx) {
 *         notifyOps("DLQ message from topic=" + ctx.topic() + ": " + msg.getBody());
 *     }
 * }
 * }</pre>
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@FunctionalInterface
public interface DlqMessageConsumer<T> {

    /**
     * 处理单条死信消息（成功=自动 ACK，异常=触发 {@code DlqFailureStrategy} 决策）。
     *
     * @param message 死信消息载体
     * @param context 消费上下文（仅元数据）
     * @throws Exception 业务异常，框架捕获后将上下文传给 {@code DlqFailureStrategy#decide}
     */
    void onDlqMessage(Message<T> message, ConsumeContext context) throws Exception;
}
