package io.github.streammq.core.service;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageMetadataBuilder;

/**
 * 单向发送服务接口（fire-and-forget）。
 *
 * <p>不等待响应，不抛异常，性能最高。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface OnewayMessageService {

    <T> void sendOneway(Message<T> message);

    <T> void sendOneway(String topic, T body);

    <T> void sendOneway(String topic, T body, String tag);

    <T> void sendOneway(String topic, T body, String tag, String keys);

    <T> void sendOneway(String topic, T body, String tag, String keys, String shardingKey);

    <T> void sendOneway(String topic, T body, MessageMetadataBuilder metadata);
}