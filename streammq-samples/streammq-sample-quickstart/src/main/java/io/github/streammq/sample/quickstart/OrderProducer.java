/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.quickstart;

import io.github.streammq.core.message.BatchMessage;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.service.StreamMessageService;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 订单消息生产者（最小可运行示例）。
 *
 * <p>演示 StreamMQ 最常用的四种发送方式：同步 / Builder / 异步 / 批量。 完整功能（单向发送、超时重试、回调、MetadataBuilder）
 * 见 {@code streammq-sample-interceptor} 与 {@code streammq-sample-delay}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final StreamMessageService service;

    public OrderProducer(StreamMessageService service) {
        this.service = service;
    }

    /** 同步发送：最简形式（topic + body + 元数据）。 */
    public SendResult createOrder(String orderId, String content) {
        log.info("Producing order message: orderId={}", orderId);
        SendResult result =
                service.send(
                        SampleConstants.TOPIC,
                        content,
                        io.github.streammq.core.message.MessageMetadataBuilder.create()
                                .tag(SampleConstants.TAG_CREATED)
                                .keys(orderId));
        log.info("Order sent: orderId={}, msgId={}", orderId, result.getMessageId());
        return result;
    }

    /** Builder 模式：显式构造 {@link Message} 携带完整字段。 */
    public SendResult createOrderWithBuilder(String orderId, String content) {
        Message<String> message =
                MessageBuilder.<String>withTopic(SampleConstants.TOPIC)
                        .tag(SampleConstants.TAG_CREATED)
                        .keys(orderId)
                        .body(content)
                        .withUserProperty(SampleConstants.PROP_SOURCE, SampleConstants.SOURCE)
                        .build();
        log.info("Producing order via builder: orderId={}", orderId);
        SendResult result = service.send(message);
        log.info("Order sent: msgId={}", result.getMessageId());
        return result;
    }

    /** 异步发送：返回 {@link CompletableFuture}，调用方自行编排后续。 */
    public CompletableFuture<SendResult> createOrderAsync(String orderId, String content) {
        log.info("Producing order asynchronously: orderId={}", orderId);
        return service.asyncSend(
                SampleConstants.TOPIC,
                content,
                io.github.streammq.core.message.MessageMetadataBuilder.create()
                        .tag(SampleConstants.TAG_ASYNC)
                        .keys(orderId));
    }

    /** 批量发送：底层走 RBatch Pipeline 一次性 XADD。 */
    public List<SendResult> createOrdersBatch(List<String> orderIds, List<String> contents) {
        log.info("Producing batch: count={}", orderIds.size());
        BatchMessage.Builder<String> builder = BatchMessage.<String>withTopic(SampleConstants.TOPIC);
        for (int i = 0; i < orderIds.size(); i++) {
            builder.add(
                    MessageBuilder.<String>withTopic(SampleConstants.TOPIC)
                            .tag(SampleConstants.TAG_BATCH)
                            .keys(orderIds.get(i))
                            .body(contents.get(i))
                            .withUserProperty("batchIndex", String.valueOf(i))
                            .build());
        }
        return service.sendBatch(builder.build());
    }
}
