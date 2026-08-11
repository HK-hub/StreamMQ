package io.github.streammq.sample.tracing;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 事件消费者，验证 traceparent 头在消费端可用。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
@StreamMQConsumer(topic = "tracing-events", consumerGroup = "tracing-sample-consumer")
public class EventConsumer implements StreamMessageConcurrentlyConsumer<String> {

  private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

  @Override
  public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
    String traceparent = message.getUserProperties().get("traceparent");
    log.info("Received event: keys={}, traceparent={}", message.getKeys(), traceparent);
    return ConsumeAction.SUCCESS;
  }
}
