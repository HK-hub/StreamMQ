package io.github.streammq.core.consumer;

import java.time.Duration;

/**
 * ACK 操作接口（手动 ACK 模式专用）。
 *
 * <p>仅在 {@code AcknowledgeMode.MANUAL} 模式下生效，通过 {@link ConsumeConcurrentlyContext#acknowledge()} 获取。
 *
 * <p>使用示例：
 * <pre>{@code
 * @StreamMqConsumer(topic = "order-topic", consumerGroup = "order-cg", acknowledgeMode = MANUAL)
 * public class OrderAckConsumer implements StreamMqAckConsumer<Order> {
 *     @Override
 *     public void onMessage(Message<Order> message, ConsumerContext context) {
 *         try {
 *             processOrder(message.getBody());
 *             context.acknowledge().acknowledge();   // 成功 ACK
 *         } catch (Exception ex) {
 *             context.acknowledge().defer(Duration.ofSeconds(30));   // 延迟重投
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
     * @throws io.github.streammq.core.exception.StreamMqBrokerException 如果 XACK 失败
     */
    void acknowledge();

    /**
     * 否定 ACK，立即重新投递该消息。
     * 等价于返回 {@link io.github.streammq.core.enums.Action#RECONSUME_LATER}，但跳过 retry ZSet 直接重投。
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
