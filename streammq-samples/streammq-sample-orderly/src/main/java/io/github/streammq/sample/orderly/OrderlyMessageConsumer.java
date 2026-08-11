package io.github.streammq.sample.orderly;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeOrderlyContext;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.message.Message;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 顺序消息消费者示例。
 *
 * <p>通过 {@link MessageModel#ORDERLY} 指定顺序消费模式， 框架保证同一 {@code shardingKey} 的消息在单线程内串行消费。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
@StreamMQConsumer(
    topic = "orderly-order-topic",
    consumerGroup = "orderly-order-consumer-group",
    messageModel = MessageModel.ORDERLY,
    shardCount = 8)
public class OrderlyMessageConsumer implements StreamMessageOrderlyConsumer<String> {

  private static final Logger log = LoggerFactory.getLogger(OrderlyMessageConsumer.class);

  private final AtomicInteger processedCount = new AtomicInteger(0);

  @Override
  public ConsumeAction onMessage(Message<String> message, ConsumeOrderlyContext context)
      throws Exception {
    String orderId = message.getKeys();
    String sequence = message.getUserProperties().get("sequence");

    log.info(
        "Received orderly message: orderId={}, sequence={}, shardingKey={}, body={}, count={}",
        orderId,
        sequence,
        message.getShardingKey(),
        message.getBody(),
        processedCount.incrementAndGet());

    try {
      processOrderStatus(message);

      log.info(
          "Orderly message processed successfully: orderId={}, sequence={}", orderId, sequence);
      return ConsumeAction.SUCCESS;
    } catch (Exception e) {
      log.error(
          "Failed to process orderly message: orderId={}, sequence={}, error={}",
          orderId,
          sequence,
          e.getMessage(),
          e);

      return ConsumeAction.RECONSUME_LATER;
    }
  }

  private void processOrderStatus(Message<String> message) {
    String body = message.getBody();
    log.debug("Processing order status: body={}", body);
  }

  public int getProcessedCount() {
    return processedCount.get();
  }
}
