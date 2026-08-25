/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.delay;

import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 延时消息生产者示例。
 *
 * <p>演示两种延时消息发送方式：
 *
 * <ul>
 *   <li>固定延时级别：通过 {@link DelayLevel} 指定 18 级固定延时
 *   <li>任意延时：通过 {@code delayTimeMillis} 指定精确延时时间
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class DelayMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(DelayMessageProducer.class);

    private final StreamMessageTemplate template;

    public DelayMessageProducer(StreamMessageTemplate template) {
        this.template = template;
    }

    /**
     * 发送固定延时消息（使用 {@link DelayLevel}）。
     *
     * <p>DelayLevel 支持 18 级固定延时：
     *
     * <ul>
     *   <li>SECOND_1 / SECOND_5 / SECOND_10 / SECOND_30: 1s, 5s, 10s, 30s
     *   <li>MINUTE_1 ~ MINUTE_10: 1m, 2m, 3m, 4m, 5m, 6m, 7m, 8m, 9m, 10m
     *   <li>MINUTE_20 / MINUTE_30 / HOUR_1 / HOUR_2: 20m, 30m, 1h, 2h
     * </ul>
     *
     * @param orderId 订单 ID
     * @param content 消息内容
     * @param delayLevel 延时级别
     * @return 发送结果
     */
    public SendResult sendFixedDelayMessage(String orderId, String content, DelayLevel delayLevel) {
        log.info(
                "Sending fixed delay message: orderId={}, delayLevel={}, content={}",
                orderId,
                delayLevel,
                content);

        Message<String> message =
                MessageBuilder.<String>withTopic(SampleConstants.TOPIC)
                        .tag(SampleConstants.TAG_DELAY)
                        .keys(orderId)
                        .body(content)
                        .delayLevel(delayLevel)
                        .withUserProperty("source", "delay-sample")
                        .build();

        SendResult result = template.syncSend(message);
        log.info(
                "Fixed delay message sent: orderId={}, msgId={}, delayLevel={}",
                orderId,
                result.getMessageId(),
                delayLevel);
        return result;
    }

    /**
     * 发送任意延时消息（使用毫秒数）。
     *
     * <p>适用于需要精确控制延时时间的场景，优先级高于 {@link DelayLevel}。
     *
     * @param orderId 订单 ID
     * @param content 消息内容
     * @param delayMillis 延时毫秒数
     * @return 发送结果
     */
    public SendResult sendCustomDelayMessage(String orderId, String content, long delayMillis) {
        log.info(
                "Sending custom delay message: orderId={}, delayMillis={}, content={}",
                orderId,
                delayMillis,
                content);

        Message<String> message =
                MessageBuilder.<String>withTopic(SampleConstants.TOPIC)
                        .tag(SampleConstants.TAG_CUSTOM_DELAY)
                        .keys(orderId)
                        .body(content)
                        .delayTimeMillis(delayMillis)
                        .withUserProperty("source", "delay-sample")
                        .build();

        SendResult result = template.syncSend(message);
        log.info(
                "Custom delay message sent: orderId={}, msgId={}, delayMillis={}",
                orderId,
                result.getMessageId(),
                delayMillis);
        return result;
    }

    /**
     * 发送订单超时提醒（延时 30 分钟）。
     *
     * @param orderId 订单 ID
     * @param content 提醒内容
     * @return 发送结果
     */
    public SendResult sendOrderTimeoutReminder(String orderId, String content) {
        return sendFixedDelayMessage(orderId, content, DelayLevel.MINUTE_30);
    }

    /**
     * 发送订单支付超时（延时 15 分钟）。
     *
     * @param orderId 订单 ID
     * @param content 消息内容
     * @return 发送结果
     */
    public SendResult sendPaymentTimeout(String orderId, String content) {
        return sendCustomDelayMessage(orderId, content, 15 * 60 * 1000L);
    }
}
