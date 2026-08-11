package io.github.streammq.core.service;

import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.message.MessageMetadataBuilder;
import io.github.streammq.core.message.SendResult;

/**
 * 延时消息发送服务接口。
 *
 * <p>提供固定延时级别和自定义延时时间两种模式。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface DelayMessageService {

    <T> SendResult sendDelay(String topic, T body, DelayLevel delayLevel);

    <T> SendResult sendDelay(String topic, T body, long delayTimeMillis);

    <T> SendResult sendDelay(String topic, T body, String tag, DelayLevel delayLevel);

    <T> SendResult sendDelay(String topic, T body, String tag, long delayTimeMillis);

    <T> SendResult sendDelay(String topic, T body, MessageMetadataBuilder metadata);

    <T> SendResult sendDelay(
            String topic, T body, MessageMetadataBuilder metadata, long timeoutMillis);
}
