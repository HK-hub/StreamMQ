package io.github.streammq.sample.quickstart;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.service.StreamMessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 订单消息生产者示例。
 *
 * <p>演示最基本的同步发送能力：通过 {@link StreamMessageService} 发送消息到指定 Topic，
 * 携带 Tag 和业务键（Keys）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final StreamMessageService service;

    /**
     * 构造 OrderProducer，注入 {@link StreamMessageService}。
     *
     * @param service 消息发送服务
     */
    public OrderProducer(StreamMessageService service) {
        this.service = service;
    }

    /**
     * 发送一条订单创建消息。
     *
     * @param orderId 订单 ID（作为消息的 Keys，用于幂等 / 查询）
     * @param content 订单内容（消息体）
     * @return 发送结果，包含消息 ID、状态等信息
     */
    public SendResult createOrder(String orderId, String content) {
        log.info("Producing order message: orderId={}, content={}", orderId, content);
        SendResult result = service.send("order-topic", content, "created", orderId);
        log.info("Order message sent successfully: orderId={}, msgId={}, status={}",
                orderId, result.getMessageId(), result.getSendStatus());
        return result;
    }

    /**
     * 使用 {@link MessageBuilder} 构造消息后发送。
     *
     * <p>适用于需要更多消息属性（如用户属性、shardingKey）的场景。
     *
     * @param orderId 订单 ID
     * @param content 订单内容
     * @return 发送结果
     */
    public SendResult createOrderWithBuilder(String orderId, String content) {
        Message<String> message = MessageBuilder.<String>withTopic("order-topic")
                .tag("created")
                .keys(orderId)
                .body(content)
                .userProperty("source", "quickstart-sample")
                .build();
        log.info("Producing order message via builder: orderId={}", orderId);
        SendResult result = service.send(message);
        log.info("Order message sent successfully: msgId={}", result.getMessageId());
        return result;
    }
}
