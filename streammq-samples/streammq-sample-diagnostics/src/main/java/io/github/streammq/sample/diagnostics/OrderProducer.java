package io.github.streammq.sample.diagnostics;

import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import org.springframework.stereotype.Component;

/**
 * 订单生产者，通过 {@link StreamMessageTemplate} 发送模拟订单消息。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class OrderProducer {

  private static final String TOPIC = "order-events";

  private final StreamMessageTemplate template;

  public OrderProducer(StreamMessageTemplate template) {
    this.template = template;
  }

  public SendResult createOrder(String orderId, String content) {
    return template.syncSend(
        MessageBuilder.<String>withTopic(TOPIC)
            .tag("created")
            .keys(orderId)
            .body(content)
            .withUserProperty("source", "diagnostics-sample")
            .build());
  }
}
