package io.github.streammq.core.service;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageMetadataBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.producer.SendCallback;

import java.util.concurrent.CompletableFuture;

/**
 * 异步发送服务接口。
 *
 * <p>提供异步消息发送能力，支持 CompletableFuture 和回调两种模式。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface AsyncMessageService {

    <T> CompletableFuture<SendResult> asyncSend(Message<T> message);

    <T> CompletableFuture<SendResult> asyncSend(String topic, T body);

    <T> CompletableFuture<SendResult> asyncSend(String topic, T body, String tag);

    <T> CompletableFuture<SendResult> asyncSend(String topic, T body, String tag, String keys);

    <T> CompletableFuture<SendResult> asyncSend(String topic, T body, String tag,
                                                String keys, String shardingKey);

    <T> CompletableFuture<SendResult> asyncSend(String topic, T body, long timeoutMillis);

    <T> CompletableFuture<SendResult> asyncSend(String topic, T body,
                                                MessageMetadataBuilder metadata);

    <T> CompletableFuture<SendResult> asyncSend(String topic, T body,
                                                MessageMetadataBuilder metadata, long timeoutMillis);

    <T> void asyncSend(String topic, T body, SendCallback callback);

    <T> void asyncSend(String topic, T body, String tag, SendCallback callback);

    <T> void asyncSend(String topic, T body, SendCallback callback, long timeoutMillis);

    <T> void asyncSend(String topic, T body, MessageMetadataBuilder metadata,
                       SendCallback callback);

    <T> void asyncSend(String topic, T body, MessageMetadataBuilder metadata,
                       SendCallback callback, long timeoutMillis);
}