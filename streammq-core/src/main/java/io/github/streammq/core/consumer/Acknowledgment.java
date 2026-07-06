package io.github.streammq.core.consumer;

import java.time.Duration;

/**
 * ACK 操作接口（手动 ACK 模式专用）。
 *
 * <p>仅在 {@link io.github.streammq.core.enums.AcknowledgeMode#MANUAL} 模式下生效，
 * 通过 {@link ConsumeContext#acknowledge()} 获取。
 * AUTO 模式下不应调用 {@link ConsumeContext#acknowledge()}，返回值可能为 null 或不可用。
 *
 * <p>使用示例：
 * <pre>{@code
 * @StreamMQConsumer(topic = "order-topic", consumerGroup = "order-cg", acknowledgeMode = MANUAL)
 * public class OrderAckConsumer implements StreamMessageConcurrentlyConsumer<Order> {
 *     @Override
 *     public ConsumeAction onMessage(Message<Order> message, ConsumeContext context) {
 *         try {
 *             processOrder(message.getBody());
 *             context.acknowledge().acknowledge();   // 成功 ACK
 *             return ConsumeAction.SUCCESS;          // MANUAL 模式下返回值被忽略
 *         } catch (Exception ex) {
 *             context.acknowledge().defer(Duration.ofSeconds(30));   // 延迟重投
 *             return ConsumeAction.RECONSUME_LATER;
 *         }
 *     }
 * }
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface Acknowledgment {

    /**
     * 确认消费成功，从 PEL 中移除该消息。
     *
     * @throws io.github.streammq.core.exception.StreamMQBrokerException 如果 XACK 失败
     */
    void acknowledge();

    /**
     * 否定 ACK，立即重新投递该消息。
     * 等价于返回 {@link io.github.streammq.core.enums.ConsumeAction#RECONSUME_LATER}，但跳过 retry ZSet 直接重投。
     */
    void nack();

    /**
     * 延迟一段时间后重新投递该消息。
     *
     * @param delay 延迟时长，必须为正
     * @throws IllegalArgumentException 如果 delay 为 null 或非正
     */
    void defer(Duration delay);
}
