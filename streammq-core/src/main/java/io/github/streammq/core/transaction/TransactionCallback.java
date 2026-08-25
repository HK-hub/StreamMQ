/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.transaction;

import io.github.streammq.core.enums.LocalTransactionState;
import io.github.streammq.core.message.Message;

/**
 * 事务消息本地事务执行回调。
 *
 * <p>对齐 Spring TransactionTemplate 风格——单方法 execute 接收回调。
 *
 * <p>使用示例：
 *
 * <pre>{@code
 * TransactionCallback<Order> callback = new TransactionCallback<>() {
 *     @Override
 *     public LocalTransactionState execute(Message<Order> message, TransactionContext context) {
 *         try {
 *             orderService.createOrder(message.getBody());
 *             return LocalTransactionState.COMMIT_MESSAGE;
 *         } catch (Exception ex) {
 *             return LocalTransactionState.ROLLBACK_MESSAGE;
 *         }
 *     }
 * };
 * SendResult result = template.executeInTransaction(msg, callback);
 * }</pre>
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@FunctionalInterface
public interface TransactionCallback<T> {

    /**
     * 执行本地事务。
     *
     * @param message 半消息载体
     * @param context 事务上下文
     * @return 本地事务状态：
     *     <ul>
     *       <li>{@link LocalTransactionState#COMMIT_MESSAGE} - 提交半消息
     *       <li>{@link LocalTransactionState#ROLLBACK_MESSAGE} - 回滚半消息
     *       <li>{@link LocalTransactionState#UNKNOW} - 等待事务回查
     *     </ul>
     *
     * @throws Exception 业务异常，框架将其视为 {@link LocalTransactionState#UNKNOW}
     */
    LocalTransactionState execute(Message<T> message, TransactionContext context) throws Exception;
}
