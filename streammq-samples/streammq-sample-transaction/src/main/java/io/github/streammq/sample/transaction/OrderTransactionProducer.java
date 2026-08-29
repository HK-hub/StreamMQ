/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.transaction;

import io.github.streammq.core.enums.LocalTransactionState;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.core.transaction.TransactionCallback;
import io.github.streammq.core.transaction.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 订单事务消息生产者示例。
 *
 * <p>演示事务消息发送流程：通过 {@link StreamMessageTemplate#executeInTransaction} 发送半消息并执行本地事务，根据本地事务执行结果决定
 * COMMIT 或 ROLLBACK。 当本地事务返回 {@link LocalTransactionState#UNKNOWN} 时，框架会触发 {@link
 * TransactionChecker} 进行回查。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class OrderTransactionProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderTransactionProducer.class);

    private final StreamMessageTemplate template;

    /**
     * 构造 OrderTransactionProducer，注入 {@link StreamMessageTemplate}。
     *
     * @param template 事务消息模板
     */
    public OrderTransactionProducer(StreamMessageTemplate template) {
        this.template = template;
    }

    /**
     * 发送事务消息：创建订单。
     *
     * <p>事务流程：
     *
     * <ol>
     *   <li>发送半消息到 Broker（消费者暂时不可见）
     *   <li>执行本地事务（例如：写数据库）
     *   <li>本地事务成功则 COMMIT，失败则 ROLLBACK，未知则等待回查
     * </ol>
     *
     * @param orderContent 订单内容（消息体）
     * @return 发送结果
     */
    public SendResult sendOrderTransaction(String orderContent) {
        Message<String> msg =
                MessageBuilder.<String>withTopic(SampleConstants.TOPIC)
                        .tag(SampleConstants.TAG)
                        .body(orderContent)
                        .withUserProperty("bizType", "order-create")
                        .build();

        log.info("Sending transaction message: body={}", orderContent);

        TransactionCallback<String> callback =
                new TransactionCallback<String>() {
                    @Override
                    public LocalTransactionState execute(
                            Message<String> message, TransactionContext ctx) {
                        log.info(
                                "Executing local transaction: txId={}, transactionGroup={},"
                                        + " body={}",
                                ctx.getTransactionId(),
                                ctx.getTransactionGroup(),
                                message.getBody());

                        try {
                            // 模拟执行本地事务（例如：写入订单数据库）
                            executeLocalTransaction(message.getBody(), ctx.getTransactionId());

                            log.info(
                                    "Local transaction committed: txId={}", ctx.getTransactionId());
                            return LocalTransactionState.COMMIT_MESSAGE;
                        } catch (Exception e) {
                            log.error(
                                    "Local transaction failed, rolling back: txId={}, error={}",
                                    ctx.getTransactionId(),
                                    e.getMessage(),
                                    e);
                            return LocalTransactionState.ROLLBACK_MESSAGE;
                        }
                    }
                };

        SendResult result = template.executeInTransaction(msg, callback);
        log.info(
                "Transaction message sent: msgId={}, status={}",
                result.getMessageId(),
                result.getSendStatus());
        return result;
    }

    /**
     * 使用 {@link StreamMessageService} 的便捷方式发送事务消息。
     *
     * <p>与上述方法功能相同，但通过 {@code StreamMessageService.sendTransaction} 简化。 适用于不需要直接操作 {@link
     * StreamMessageTemplate} 的场景。
     *
     * @param orderContent 订单内容
     * @return 发送结果
     */
    public SendResult sendOrderTransactionSimple(String orderContent) {
        log.info("Sending transaction message (simple API): body={}", orderContent);

        TransactionCallback<String> callback =
                (message, ctx) -> {
                    log.info(
                            "Executing local transaction (simple): txId={}",
                            ctx.getTransactionId());
                    executeLocalTransaction(message.getBody(), ctx.getTransactionId());
                    return LocalTransactionState.COMMIT_MESSAGE;
                };

        // 此处演示 TransactionCallback 作为 lambda 的写法
        return template.executeInTransaction(
                MessageBuilder.<String>withTopic(SampleConstants.TOPIC)
                        .tag(SampleConstants.TAG)
                        .body(orderContent)
                        .build(),
                callback);
    }

    /**
     * 模拟本地事务执行（实际场景中应操作数据库，如插入订单记录）。
     *
     * @param content 订单内容
     * @param transactionId 事务 ID
     */
    private void executeLocalTransaction(String content, String transactionId) {
        log.debug("Simulating local transaction for txId={}: content={}", transactionId, content);
        // 实际场景中：
        //   orderMapper.insert(order);
        //   transactionLogMapper.insert(transactionId, orderId, "PENDING");
    }
}
