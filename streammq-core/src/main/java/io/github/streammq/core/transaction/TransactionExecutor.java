/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.transaction;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;

/**
 * 事务执行器接口，封装事务消息发送 + 本地事务执行的完整流程。
 *
 * <p>核心方法 {@link #executeInTransaction} 流程：
 *
 * <ol>
 *   <li>发送半消息到 {@code streammq:half:{transactionGroup}} Stream
 *   <li>调用 {@link TransactionCallback#execute} 执行本地事务
 *   <li>根据返回值：
 *       <ul>
 *         <li>{@link io.github.streammq.core.enums.LocalTransactionState#COMMIT_MESSAGE} -
 *             提交半消息（转投到目标 Topic Stream）
 *         <li>{@link io.github.streammq.core.enums.LocalTransactionState#ROLLBACK_MESSAGE} -
 *             回滚半消息（标记删除）
 *         <li>{@link io.github.streammq.core.enums.LocalTransactionState#UNKNOWN} - 保留半消息，等待事务回查
 *       </ul>
 * </ol>
 *
 * <p><b>实现要求：</b>完整的半消息 + 回查语义依赖 {@code TransactionScanner}。若实现方未注入 Scanner，{@code
 * executeInTransaction} 必须<b>快速失败</b>（抛出 {@code TransactionException}）， 而不是静默降级为"先发送后回滚"的简化路径——后者在
 * JVM 崩溃时会留下永久悬挂的半消息， 且没有任何回查机制能够补偿。
 *
 * <p><b>可靠性保证：</b>
 *
 * <ul>
 *   <li>简化模式：消息持久化到 Redis（取决于 Redis AOF 配置），但本地事务与消息发送不是原子操作
 *   <li>完整模式：通过回查机制保证最终一致性，即使 JVM 崩溃也能通过回查恢复
 * </ul>
 *
 * <p><b>泛型设计</b>：泛型参数 {@code <T>} 声明在方法级别，支持同一事务执行器 处理不同 body 类型的事务消息。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface TransactionExecutor {

    /**
     * 在事务中执行消息发送。
     *
     * @param message 半消息载体（topic 必填，body 必填）
     * @param callback 本地事务回调
     * @param <T> body 类型
     * @return 发送结果（仅在 COMMIT_MESSAGE 时为 SEND_OK，其他状态为 SEND_FAILED）
     * @throws io.github.streammq.core.exception.TransactionException 如果半消息发送失败
     */
    <T> SendResult executeInTransaction(Message<T> message, TransactionCallback<T> callback);
}
