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
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.core.transaction.TransactionCallback;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * {@link StreamMessageService} 默认实现：对 {@link StreamMessageTemplate} 的薄封装。
 *
 * <p>职责仅剩两件事：① 将 Topic+Metadata 形态装配为 {@link Message}；② 透传 Message 形态与事务调用。0.1.0 起 API 已收敛，此前的 601
 * 行重载转发层不再存在。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultStreamMessageService implements StreamMessageService {

    private final StreamMessageTemplate template;

    public DefaultStreamMessageService(StreamMessageTemplate template) {
        this.template = Objects.requireNonNull(template, "template");
    }

    /** 返回内部模板（供需要完整 SPI 能力的高级场景使用）。 */
    public StreamMessageTemplate getTemplate() {
        return template;
    }

    // ===================== Message 形态 =====================

    @Override
    public <T> SendResult send(Message<T> message, SendOptions options) {
        return template.syncSend(message, options);
    }

    @Override
    public <T> CompletableFuture<SendResult> asyncSend(Message<T> message, SendOptions options) {
        return template.asyncSend(message, options);
    }

    @Override
    public <T> void sendOneway(Message<T> message) {
        template.sendOneway(message);
    }

    // ===================== Topic + Metadata 形态 =====================

    @Override
    public <T> SendResult send(String topic, T body, MessageMetadataBuilder metadata) {
        return template.syncSend(
                StreamMessageService.assemble(topic, body, metadata), toOptions(metadata));
    }

    @Override
    public <T> CompletableFuture<SendResult> asyncSend(
            String topic, T body, MessageMetadataBuilder metadata) {
        return template.asyncSend(
                StreamMessageService.assemble(topic, body, metadata), toOptions(metadata));
    }

    // ===================== 批量 =====================

    @Override
    public <T> List<SendResult> sendBatch(BatchMessage<T> batch, SendOptions options) {
        return template.syncSendBatch(batch, options);
    }

    // ===================== 事务透传 =====================

    @Override
    public <T> SendResult executeInTransaction(
            Message<T> message, TransactionCallback<T> callback) {
        return template.executeInTransaction(message, callback);
    }

    // ===================== 内部工具 =====================

    private static SendOptions toOptions(MessageMetadataBuilder metadata) {
        if (Objects.isNull(metadata)) {
            return SendOptions.defaults();
        }
        long timeout = metadata.getTimeoutMillis();
        int retries = metadata.getRetryTimes();
        if (timeout <= 0 && retries < 0) {
            return SendOptions.defaults();
        }
        return SendOptions.of(timeout, retries);
    }
}
