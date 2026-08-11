package io.github.streammq.sample.dlq;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.service.StreamMessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 订单消息生产者示例（用于生成可能进入死信的消息）。
 *
 * <p>演示正常消息发送，当消费者处理失败超过重试次数后，消息进入死信队列。
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

  /**
   * 发送订单消息（可能触发死信）。
   *
   * @param orderId 订单 ID
   * @param content 订单内容
   * @return 发送结果
   */
  public SendResult sendOrder(String orderId, String content) {
    log.info("Producing order message (may enter DLQ): orderId={}", orderId);

    Message<String> message =
        MessageBuilder.<String>withTopic("order-topic")
            .tag("dlq-test")
            .keys(orderId)
            .body(content)
            .withUserProperty("source", "dlq-sample")
            .build();

    SendResult result = service.send(message);
    log.info("Order message sent: orderId={}, msgId={}", orderId, result.getMessageId());
    return result;
  }
}
