/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.dlq;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 订单消息消费者示例（模拟消费失败触发死信）。
 *
 * <p>演示死信机制：消费失败时抛出异常（或返回 {@link ConsumeAction#RECONSUME_LATER}）， 框架按 {@code maxReconsumeTimes}
 * 重试，重试耗尽后把消息路由到死信队列 {@code streammq:{ns}:dlq:order-consumer-group}， 由 {@link OrderDlqConsumer}
 * 消费——失败消息不会在消费者里被静默 ACK 丢弃。
 *
 * <p>{@link #setFailOrderId(String)} 提供失败注入：默认演示（{@link DemoRunner}）与集成测试用它把指定订单号变成 必然失败的订单。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
@StreamMQConsumer(
        topic = SampleConstants.TOPIC,
        consumerGroup = SampleConstants.CONSUMER_GROUP,
        maxReconsumeTimes = 3)
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private final AtomicInteger failCount = new AtomicInteger(0);
    private volatile boolean shouldFail = false;
    private volatile String failOrderId = null;

    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context)
            throws Exception {
        log.info(
                "Received order message: keys={}, tag={}, body={}, reconsumeTimes={}",
                message.getKeys(),
                message.getTag(),
                message.getBody(),
                context.reconsumeTimes());

        if (shouldFail && failOrderId != null && failOrderId.equals(message.getKeys())) {
            int count = failCount.incrementAndGet();
            log.error(
                    "Simulating order processing failure: orderId={}, failCount={},"
                            + " reconsumeTimes={}",
                    message.getKeys(),
                    count,
                    context.reconsumeTimes());
            // 抛出异常：框架按 maxReconsumeTimes=3 重试，重试耗尽后自动路由到 DLQ
            throw new RuntimeException(
                    "Intentional failure for DLQ demo: orderId=" + message.getKeys());
        }

        try {
            processOrder(message);
            log.info("Order processed successfully: keys={}", message.getKeys());
            return ConsumeAction.SUCCESS;
        } catch (Exception e) {
            log.error(
                    "Failed to process order: keys={}, reconsumeTimes={}, error={}",
                    message.getKeys(),
                    context.reconsumeTimes(),
                    e.getMessage(),
                    e);
            // 不吞消息：返回 RECONSUME_LATER，由框架重试并在耗尽 maxReconsumeTimes 后路由到 DLQ
            // （streammq:{ns}:dlq:{consumerGroup}），保证失败消息不丢
            return ConsumeAction.RECONSUME_LATER;
        }
    }

    private void processOrder(Message<String> message) {
        log.debug("Processing order: body={}", message.getBody());
    }

    /**
     * 设置指定订单 ID 消费失败（用于测试死信机制）。
     *
     * @param orderId 订单 ID
     */
    public void setFailOrderId(String orderId) {
        this.shouldFail = true;
        this.failOrderId = orderId;
        this.failCount.set(0);
    }

    /** 取消消费失败模拟。 */
    public void clearFailOrderId() {
        this.shouldFail = false;
        this.failOrderId = null;
        this.failCount.set(0);
    }
}
