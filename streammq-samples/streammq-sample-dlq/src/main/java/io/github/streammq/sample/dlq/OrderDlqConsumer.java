/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.dlq;

import io.github.streammq.core.annotation.StreamMQDlqConsumer;
import io.github.streammq.core.consumer.AbstractDlqMessageConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.message.Message;
import org.springframework.stereotype.Component;

/**
 * 订单死信队列消费者示例。
 *
 * <p>演示死信队列消费：
 *
 * <ul>
 *   <li>使用 {@link StreamMQDlqConsumer} 注解注册死信消费者
 *   <li>继承 {@link AbstractDlqMessageConsumer} 实现死信消息处理
 *   <li>配置 DLQ 失败策略（默认 LogAndDrop）
 * </ul>
 *
 * <p>死信消息来源：当 {@code order-consumer-group} 的消息消费失败超过 maxReconsumeTimes 后， 消息会被转移到死信队列 {@code
 * streammq:{ns}:dlq:order-consumer-group}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
@StreamMQDlqConsumer(
        consumerGroup = SampleConstants.CONSUMER_GROUP,
        namespace = SampleConstants.NAMESPACE,
        maxDlqRetryAttempts = 3,
        dlqRetryDelayMs = 10000)
public class OrderDlqConsumer extends AbstractDlqMessageConsumer<String> {

    @Override
    public void onDlqMessage(Message<String> message, ConsumeContext context) throws Exception {
        log.info(
                "Received DLQ message: topic={}, keys={}, body={}, reconsumeTimes={},"
                        + " consumerGroup={}",
                message.getTopic(),
                message.getKeys(),
                message.getBody(),
                context.reconsumeTimes(),
                context.consumerGroup());

        try {
            processDlqMessage(message);
            log.info("DLQ message processed successfully: keys={}", message.getKeys());
        } catch (Exception e) {
            log.error(
                    "Failed to process DLQ message: keys={}, error={}",
                    message.getKeys(),
                    e.getMessage(),
                    e);
            throw e;
        }
    }

    private void processDlqMessage(Message<String> message) {
        log.debug("Processing DLQ message: body={}", message.getBody());
    }
}
