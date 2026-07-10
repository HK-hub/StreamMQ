package io.github.streammq.sample.interceptor;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.service.StreamMessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 订单消息生产者示例（配合拦截器使用）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final StreamMessageService service;

    public OrderProducer(StreamMessageService service) {
        this.service = service;
    }

    public SendResult sendOrder(String orderId, String content) {
        log.info("Producing order message with interceptors: orderId={}", orderId);

        Message<String> message = MessageBuilder.<String>withTopic("interceptor-order-topic")
                .tag("order")
                .keys(orderId)
                .body(content)
                .build();

        SendResult result = service.send(message);
        log.info("Order message sent: orderId={}, msgId={}, success={}",
                orderId, result.getMessageId(), result.isSuccess());
        return result;
    }
}