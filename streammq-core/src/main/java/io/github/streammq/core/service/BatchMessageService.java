/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.service;

import io.github.streammq.core.message.BatchMessage;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageMetadataBuilder;
import io.github.streammq.core.message.SendResult;
import java.util.List;

/**
 * 批量发送服务接口。
 *
 * <p>提供批量消息发送能力，支持多种参数组合。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface BatchMessageService {

    <T> List<SendResult> sendBatch(BatchMessage<T> batch);

    <T> List<SendResult> sendBatch(String topic, List<T> bodies);

    <T> List<SendResult> sendBatch(String topic, String tag, List<T> bodies);

    <T> List<SendResult> sendBatch(String topic, List<T> bodies, MessageMetadataBuilder metadata);

    <T> List<SendResult> sendBatch(List<Message<T>> messages);

    <T> List<SendResult> sendBatch(List<Message<T>> messages, long timeoutMillis, int retryTimes);

    @SuppressWarnings("unchecked")
    <T> List<SendResult> sendBatch(Message<T>... messages);

    @SuppressWarnings("unchecked")
    <T> List<SendResult> sendBatch(String topic, Message<T>... messages);

    @SuppressWarnings("unchecked")
    <T> List<SendResult> sendBatch(
            String topic, long timeoutMillis, int retryTimes, Message<T>... messages);
}
