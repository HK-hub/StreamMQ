/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
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
 * 接口处理消息。
 *
 * <p><b>失败处理（推荐模式）：</b>消费成功返回 {@link ConsumeAction#SUCCESS}（框架自动 ACK）； 失败返回 {@link
 * ConsumeAction#RECONSUME_LATER} 或抛出异常（框架视为 RECONSUME_LATER），由框架按 {@code max-reconsume-times}
 * 重试，重试耗尽后自动把消息路由到死信队列 {@code streammq:{namespace}:dlq:{consumerGroup}}，<b>绝不在消费者里静默 ACK 吞掉</b>。
 * 需要消费死信请参考 {@code streammq-sample-dlq}（{@code @StreamMQDlqConsumer}）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
@StreamMQConsumer(topic = SampleConstants.TOPIC, consumerGroup = SampleConstants.CONSUMER_GROUP)
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    /** 失败注入（演示/测试用）：该订单号的消息消费时抛出异常，触发重试与 DLQ 路由 */
    private volatile String failOrderId;

    /**
     * 处理单条订单消息。
     *
     * <p>返回 {@link ConsumeAction#SUCCESS} 表示消费成功，框架自动 ACK； 返回 {@link ConsumeAction#RECONSUME_LATER}
     * 表示消费失败，框架按 {@code RetryPolicy} 重试，耗尽重试预算后进入 DLQ。
     *
     * @param message 消息载体，包含 topic、tag、keys、body 等信息
     * @param context 消费上下文，提供 reconsumeTimes、consumerGroup 等元信息
     * @return 消费结果动作
     * @throws Exception 业务异常，框架将其视为 RECONSUME_LATER
     */
    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context)
            throws Exception {
        log.info(
                "Received order message: id={}, topic={}, tag={}, body={}, retryTimes={}",
                message.getKeys(),
                message.getTopic(),
                message.getTag(),
                message.getBody(),
                context.reconsumeTimes());

        // 模拟业务处理：解析订单内容并处理
        String orderContent = message.getBody();
        if (failOrderId != null && failOrderId.equals(message.getKeys())) {
            throw new RuntimeException(
                    "Simulated business failure for order: " + message.getKeys());
        }
        try {
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
            // 不吞消息：交给框架重试；重试耗尽后由框架路由到 DLQ（消息不会凭空消失）
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

    /**
     * 注入一个必然失败的订单号（演示 / 集成测试用，用于观察「失败 → 重试 → DLQ」链路）。
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
