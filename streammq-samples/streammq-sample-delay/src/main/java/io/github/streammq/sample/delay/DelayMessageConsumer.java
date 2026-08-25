/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.delay;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 延时消息消费者示例。
 *
 * <p>延时消息在到达指定时间后会被投递到目标 Topic，本消费者接收并处理这些消息。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
@StreamMQConsumer(
        topic = SampleConstants.TOPIC,
        consumerGroup = SampleConstants.CONSUMER_GROUP,
        maxReconsumeTimes = 3)
public class DelayMessageConsumer implements StreamMessageConcurrentlyConsumer<String> {

    private static final Logger log = LoggerFactory.getLogger(DelayMessageConsumer.class);

    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context)
            throws Exception {
        log.info(
                "Received delay message: orderId={}, tag={}, body={}, reconsumeTimes={}",
                message.getKeys(),
                message.getTag(),
                message.getBody(),
                context.reconsumeTimes());

        try {
            if ("delay".equals(message.getTag())) {
                handleFixedDelayMessage(message);
            } else if ("custom-delay".equals(message.getTag())) {
                handleCustomDelayMessage(message);
            } else {
                handleGenericDelayMessage(message);
            }

            log.info("Delay message processed successfully: orderId={}", message.getKeys());
            return ConsumeAction.SUCCESS;
        } catch (Exception e) {
            log.error(
                    "Failed to process delay message: orderId={}, error={}",
                    message.getKeys(),
                    e.getMessage(),
                    e);

            if (context.reconsumeTimes() >= 3) {
                log.error("Delay message exhausted retries: orderId={}", message.getKeys());
                return ConsumeAction.SUCCESS;
            }

            return ConsumeAction.RECONSUME_LATER;
        }
    }

    private void handleFixedDelayMessage(Message<String> message) {
        log.debug("Handling fixed delay message: orderId={}", message.getKeys());
    }

    private void handleCustomDelayMessage(Message<String> message) {
        log.debug("Handling custom delay message: orderId={}", message.getKeys());
    }

    private void handleGenericDelayMessage(Message<String> message) {
        log.debug("Handling generic delay message: orderId={}", message.getKeys());
    }
}
