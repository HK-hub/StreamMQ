/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.service;

import io.github.streammq.core.message.BatchMessage;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageMetadataBuilder;
import io.github.streammq.core.message.SendOptions;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.producer.SendCallback;
import io.github.streammq.core.transaction.TransactionExecutor;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * 业务友好的消息发送门面，封装 {@link io.github.streammq.core.template.StreamMessageTemplate}。
 *
 * <p><b>API 收敛（0.1.0）：</b>此前的六个子接口（Basic / Async / Oneway / Batch / Delay /
 * TransactionMessageService）与数十个 topic/body/tag/keys/timeout 伸缩重载已合并删除，
 * 统一收敛为以下三种正交维度：
 *
 * <ol>
 *   <li><b>发送模式</b>：{@code send}（同步）/ {@code asyncSend}（异步，Future 与回调两种接收方式）/
 *       {@code sendOneway}（单向）/ {@code sendBatch}（批量）
 *   <li><b>载体形态</b>：完整 {@link Message} 或 {@code (topic, body[, MessageMetadataBuilder])}
 *   <li><b>参数</b>：超时与重试统一由 {@link SendOptions}（Message 形态）或
 *       {@link MessageMetadataBuilder#timeoutMillis(long)}（Topic 形态）表达
 * </ol>
 *
 * <p>延时、属性、Tag、Keys 等消息元数据一律通过 {@link MessageMetadataBuilder} 表达。
 *
 * <p>遵循「依赖接口而非实现」原则，业务代码应注入本接口；Spring 环境默认注册
 * {@link DefaultStreamMessageService}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMessageService extends TransactionExecutor {

    // ===================== Message 形态 =====================

    /**
     * 同步发送完整 Message（规范形）。
     *
     * @param message 消息
     * @param options 发送选项，null 按默认值处理
     * @param <T> body 类型
     * @return 发送结果
     */
    <T> SendResult send(Message<T> message, SendOptions options);

    /** 同步发送完整 Message（默认参数）。 */
    default <T> SendResult send(Message<T> message) {
        return send(message, SendOptions.defaults());
    }

    /**
     * 异步发送完整 Message（规范形）。
     *
     * @param message 消息
     * @param options 发送选项，null 按默认值处理
     * @param <T> body 类型
     * @return 异步结果
     */
    <T> CompletableFuture<SendResult> asyncSend(Message<T> message, SendOptions options);

    /** 异步发送完整 Message（默认参数）。 */
    default <T> CompletableFuture<SendResult> asyncSend(Message<T> message) {
        return asyncSend(message, SendOptions.defaults());
    }

    /**
     * 单向发送完整 Message（fire-and-forget，不抛异常）。
     *
     * @param message 消息
     * @param <T> body 类型
     */
    <T> void sendOneway(Message<T> message);

    /** 单向发送（topic + body，默认元数据）。 */
    default <T> void sendOneway(String topic, T body) {
        sendOneway(topic, body, null);
    }

    /**
     * 单向发送（topic + body + 元数据）。
     *
     * @param topic 主题（必填）
     * @param body 消息体
     * @param metadata 附加元数据，可为 null
     * @param <T> body 类型
     */
    default <T> void sendOneway(String topic, T body, MessageMetadataBuilder metadata) {
        sendOneway(assemble(topic, body, metadata));
    }

    // ===================== Topic + Metadata 形态 =====================

    /**
     * 同步发送（topic + body + 元数据，规范形）。
     *
     * <p>{@code metadata} 中的超时/重试设置同样生效（转换为内部 {@link SendOptions}）； null 视为空元数据。
     *
     * @param topic 主题（必填）
     * @param body 消息体
     * @param metadata 附加元数据（Tag/Keys/ShardingKey/延时/属性/超时/重试），可为 null
     * @param <T> body 类型
     * @return 发送结果
     */
    <T> SendResult send(String topic, T body, MessageMetadataBuilder metadata);

    /** 同步发送（topic + body）。 */
    default <T> SendResult send(String topic, T body) {
        return send(topic, body, null);
    }

    /**
     * 异步发送（topic + body + 元数据，规范形）。
     *
     * @param topic 主题（必填）
     * @param body 消息体
     * @param metadata 附加元数据，可为 null
     * @param <T> body 类型
     * @return 异步结果
     */
    <T> CompletableFuture<SendResult> asyncSend(
            String topic, T body, MessageMetadataBuilder metadata);

    /** 异步发送（topic + body）。 */
    default <T> CompletableFuture<SendResult> asyncSend(String topic, T body) {
        return asyncSend(topic, body, null);
    }

    /**
     * 异步发送（topic + body + 元数据 + 回调通知）。
     *
     * @param topic 主题（必填）
     * @param body 消息体
     * @param metadata 附加元数据，可为 null
     * @param callback 回调
     * @param <T> body 类型
     */
    default <T> void asyncSend(
            String topic, T body, MessageMetadataBuilder metadata, SendCallback callback) {
        asyncSend(topic, body, metadata)
                .whenComplete(
                        (result, ex) -> {
                            if (Objects.isNull(ex)) {
                                callback.onSuccess(result);
                            } else {
                                callback.onException(
                                        ex instanceof RuntimeException re
                                                ? re
                                                : new io.github.streammq.core.exception
                                                        .StreamMQException("async send failed", ex));
                            }
                        });
    }

    // ===================== 批量 =====================

    /**
     * 批量同步发送（规范形）。
     *
     * @param batch 批量消息
     * @param options 发送选项（超时、整体重试），null 按默认值处理
     * @param <T> body 类型
     * @return 与输入顺序一致的发送结果列表
     */
    <T> List<SendResult> sendBatch(BatchMessage<T> batch, SendOptions options);

    /** 批量同步发送（默认参数）。 */
    default <T> List<SendResult> sendBatch(BatchMessage<T> batch) {
        return sendBatch(batch, SendOptions.defaults());
    }

    /**
     * 将 topic + body + 元数据装配为 {@link Message}（供各 default 方法与实现复用）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param metadata 附加元数据，可为 null
     * @param <T> body 类型
     * @return 装配后的不可变消息
     */
    static <T> Message<T> assemble(
            String topic, T body, MessageMetadataBuilder metadata) {
        java.util.Objects.requireNonNull(topic, "topic");
        io.github.streammq.core.message.MessageBuilder<T> builder =
                io.github.streammq.core.message.MessageBuilder.<T>withTopic(topic).body(body);
        if (Objects.nonNull(metadata)) {
            metadata.applyTo(builder);
        }
        return builder.build();
    }
}
