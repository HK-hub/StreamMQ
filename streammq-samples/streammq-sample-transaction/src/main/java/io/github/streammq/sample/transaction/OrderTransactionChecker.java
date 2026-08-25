/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.transaction;

import io.github.streammq.core.annotation.StreamMQTransactionConsumer;
import io.github.streammq.core.enums.LocalTransactionState;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.transaction.TransactionChecker;
import io.github.streammq.core.transaction.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 订单事务回查器示例。
 *
 * <p>当 {@link OrderTransactionProducer} 中本地事务返回 {@link LocalTransactionState#UNKNOW}
 * 或执行超时时，框架会在指定间隔后调用本回查器的 {@link #check} 方法查询本地事务的最终状态。
 *
 * <p>典型应用场景：
 *
 * <ul>
 *   <li>本地事务执行时间过长，先返回 UNKNOW
 *   <li>网络抖动导致半消息 COMMIT/ROLLBACK 通知丢失
 *   <li>服务宕机恢复后，回查未确认的事务消息
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
@StreamMQTransactionConsumer(transactionGroup = SampleConstants.TRANSACTION_GROUP)
public class OrderTransactionChecker implements TransactionChecker<String> {

    private static final Logger log = LoggerFactory.getLogger(OrderTransactionChecker.class);

    /**
     * 回查本地事务的最终状态。
     *
     * <p>通过 {@code context.getTransactionId()} 查询本地事务日志表， 判断事务是否已提交、已回滚或仍处于未知状态。
     *
     * @param message 半消息载体，包含原始业务数据
     * @param context 事务上下文，提供 transactionId、transactionGroup 等信息
     * @return 本地事务状态：
     *     <ul>
     *       <li>{@link LocalTransactionState#COMMIT_MESSAGE} - 本地事务已提交，提交半消息
     *       <li>{@link LocalTransactionState#ROLLBACK_MESSAGE} - 本地事务已回滚，删除半消息
     *       <li>{@link LocalTransactionState#UNKNOW} - 本地事务状态仍未知，等待下次回查
     *     </ul>
     *
     * @throws Exception 业务异常，框架将其视为 UNKNOW，等待下次回查
     */
    @Override
    public LocalTransactionState check(Message<String> message, TransactionContext context)
            throws Exception {
        log.info(
                "Checking transaction: txId={}, transactionGroup={}, body={}",
                context.getTransactionId(),
                context.getTransactionGroup(),
                message.getBody());

        try {
            // 模拟查询本地事务状态
            // 实际场景中通过 transactionId 查询本地事务日志表
            //   TransactionLog log = transactionLogMapper.selectByTxId(context.getTransactionId());
            //   if (log == null) return LocalTransactionState.UNKNOW;
            //   return log.isCommitted() ? COMMIT_MESSAGE : ROLLBACK_MESSAGE;
            LocalTransactionState state = checkLocalTransaction(context.getTransactionId());

            log.info(
                    "Transaction check result: txId={}, state={}",
                    context.getTransactionId(),
                    state);
            return state;
        } catch (Exception e) {
            log.error(
                    "Transaction check failed: txId={}, error={}",
                    context.getTransactionId(),
                    e.getMessage(),
                    e);
            // 查询异常，返回 UNKNOW 等待下次回查
            return LocalTransactionState.UNKNOW;
        }
    }

    /**
     * 模拟查询本地事务状态。
     *
     * <p>实际场景中应根据 transactionId 查询本地事务日志表或业务表。
     *
     * @param transactionId 事务 ID
     * @return 本地事务最终状态
     */
    private LocalTransactionState checkLocalTransaction(String transactionId) {
        log.debug("Checking local transaction status: txId={}", transactionId);
        // 模拟：所有事务均已提交
        // 实际场景：
        //   1. 查询事务日志表：select status from t_transaction_log where tx_id = ?
        //   2. 根据 status 返回对应的 LocalTransactionState
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
