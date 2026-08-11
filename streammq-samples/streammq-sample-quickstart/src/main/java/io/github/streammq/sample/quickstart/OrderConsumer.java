package io.github.streammq.sample.quickstart;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 订单消息消费者示例。
 *
 * <p>演示最基本的并发消费：通过 {@link StreamMQConsumer} 注解注册消费者， 实现 {@link StreamMessageConcurrentlyConsumer}
 * 接口处理消息。 消费成功返回 {@link ConsumeAction#SUCCESS}，失败返回 {@link ConsumeAction#RECONSUME_LATER}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-consumer-group")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {

  private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

  /**
   * 处理单条订单消息。
   *
   * <p>返回 {@link ConsumeAction#SUCCESS} 表示消费成功，框架自动 ACK； 返回 {@link ConsumeAction#RECONSUME_LATER}
   * 表示消费失败，框架按 {@code RetryPolicy} 重试。
   *
   * @param message 消息载体，包含 topic、tag、keys、body 等信息
   * @param context 消费上下文，提供 reconsumeTimes、consumerGroup 等元信息
   * @return 消费结果动作
   * @throws Exception 业务异常，框架将其视为 RECONSUME_LATER
   */
  @Override
  public ConsumeAction onMessage(Message<String> message, ConsumeContext context) throws Exception {
    log.info(
        "Received order message: id={}, topic={}, tag={}, body={}, retryTimes={}",
        message.getKeys(),
        message.getTopic(),
        message.getTag(),
        message.getBody(),
        context.reconsumeTimes());

    try {
      // 模拟业务处理：解析订单内容并处理
      String orderContent = message.getBody();
      processOrder(message.getKeys(), orderContent);

      log.info(
          "Order processed successfully: id={}, consumerGroup={}",
          message.getKeys(),
          context.consumerGroup());
      return ConsumeAction.SUCCESS;
    } catch (Exception e) {
      log.error(
          "Failed to process order: id={}, retryTimes={}, error={}",
          message.getKeys(),
          context.reconsumeTimes(),
          e.getMessage(),
          e);

      // 重试超过一定次数后仍然失败，可以记录到死信或告警
      if (context.reconsumeTimes() >= 3) {
        log.error(
            "Order processing exhausted retries: id={}, totalRetries={}",
            message.getKeys(),
            context.reconsumeTimes());
        // 超过重试次数仍然失败，返回 SUCCESS 避免无限重试（实际生产应使用 DLQ）
        return ConsumeAction.SUCCESS;
      }

      return ConsumeAction.RECONSUME_LATER;
    }
  }

  /**
   * 模拟订单处理逻辑。
   *
   * @param orderId 订单 ID
   * @param content 订单内容
   */
  private void processOrder(String orderId, String content) {
    log.debug("Processing order: orderId={}, content={}", orderId, content);
  }
}
