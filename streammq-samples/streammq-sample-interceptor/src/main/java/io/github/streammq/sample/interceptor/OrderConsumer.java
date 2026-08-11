package io.github.streammq.sample.interceptor;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 订单消息消费者示例（配合拦截器使用）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
@StreamMQConsumer(
    topic = "interceptor-order-topic",
    consumerGroup = "interceptor-order-consumer-group")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {

  private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

  @Override
  public ConsumeAction onMessage(Message<String> message, ConsumeContext context) throws Exception {
    log.info(
        "Processing order message: keys={}, body={}, traceId={}",
        message.getKeys(),
        message.getBody(),
        message.getUserProperties().get("traceId"));

    processOrder(message);
    return ConsumeAction.SUCCESS;
  }

  private void processOrder(Message<String> message) {
    log.debug("Processing order: body={}", message.getBody());
  }
}
