package io.github.streammq.sample.orderly;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 顺序消息生产者示例。
 *
 * <p>演示顺序消息发送：通过设置 {@code shardingKey} 确保同一业务实体的消息
 * 在消费者端按发送顺序消费。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class OrderlyMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderlyMessageProducer.class);

    private final StreamMessageTemplate template;

    public OrderlyMessageProducer(StreamMessageTemplate template) {
        this.template = template;
    }

    /**
     * 发送订单顺序消息。
     *
     * <p>通过 {@code shardingKey} 指定分片键，确保同一订单的消息
     * 在同一分片内按顺序消费。
     *
     * @param orderId 订单 ID（作为 shardingKey）
     * @param content 消息内容
     * @param sequence 消息序号
     * @return 发送结果
     */
    public SendResult sendOrderlyMessage(String orderId, String content, int sequence) {
        log.info("Sending orderly message: orderId={}, sequence={}, content={}",
                orderId, sequence, content);

        Message<String> message = MessageBuilder.<String>withTopic("orderly-order-topic")
                .tag("orderly")
                .keys(orderId)
                .shardingKey(orderId)
                .body(content)
                .userProperty("sequence", String.valueOf(sequence))
                .userProperty("source", "orderly-sample")
                .build();

        SendResult result = template.syncSend(message);
        log.info("Orderly message sent: orderId={}, sequence={}, msgId={}",
                orderId, sequence, result.getMessageId());
        return result;
    }

    /**
     * 模拟订单状态流转消息。
     *
     * <p>发送创建、支付、发货、完成等顺序消息，确保同一订单按正确顺序处理。
     *
     * @param orderId 订单 ID
     */
    public void sendOrderStatusFlow(String orderId) {
        sendOrderlyMessage(orderId, "{\"status\":\"created\"}", 1);
        sendOrderlyMessage(orderId, "{\"status\":\"paid\"}", 2);
        sendOrderlyMessage(orderId, "{\"status\":\"shipped\"}", 3);
        sendOrderlyMessage(orderId, "{\"status\":\"completed\"}", 4);

        log.info("Order status flow sent: orderId={}", orderId);
    }

    /**
     * 批量发送顺序消息。
     *
     * @param orderId 订单 ID
     * @param count 消息数量
     */
    public void sendBatchOrderlyMessages(String orderId, int count) {
        for (int i = 1; i <= count; i++) {
            String content = String.format("{\"orderId\":\"%s\",\"sequence\":%d,\"data\":\"item-%d\"}",
                    orderId, i, i);
            sendOrderlyMessage(orderId, content, i);
        }
        log.info("Batch orderly messages sent: orderId={}, count={}", orderId, count);
    }
}