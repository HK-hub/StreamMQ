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
 * <p><b>失败处理（推荐模式）：</b>消费成功返回 {@link ConsumeAction#SUCCESS}；失败返回 {@link
 * ConsumeAction#RECONSUME_LATER} 或抛出异常，由框架按 {@code maxReconsumeTimes}（本示例为 3）重试， 重试耗尽后自动路由到死信队列
 * {@code streammq:{ns}:dlq:{consumerGroup}}——<b>不静默 ACK 吞消息</b>。 如需消费死信请参考 {@code
 * streammq-sample-dlq}。
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

    /** 失败注入（演示/测试用）：该 orderId 的消息消费时抛出异常，触发重试与 DLQ 路由 */
    private volatile String failOrderId;

    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context)
            throws Exception {
        log.info(
                "Received delay message: orderId={}, tag={}, body={}, reconsumeTimes={}",
                message.getKeys(),
                message.getTag(),
                message.getBody(),
                context.reconsumeTimes());

        if (failOrderId != null && failOrderId.equals(message.getKeys())) {
            throw new RuntimeException("Simulated delay-message failure: " + message.getKeys());
        }

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
                    "Failed to process delay message: orderId={}, reconsumeTimes={}, error={}",
                    message.getKeys(),
                    context.reconsumeTimes(),
                    e.getMessage(),
                    e);
            // 不吞消息：返回 RECONSUME_LATER 交给框架重试；重试耗尽后自动进入 DLQ
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

    /**
     * 注入一个必然失败的 orderId（演示 / 集成测试用，用于观察「失败 → 重试 → DLQ」链路）。
     *
     * @param orderId 订单 ID；为 null 表示取消失败注入
     */
    public void setFailOrderId(String orderId) {
        this.failOrderId = orderId;
    }

    /** 取消失败注入。 */
    public void clearFailOrderId() {
        this.failOrderId = null;
    }
}
