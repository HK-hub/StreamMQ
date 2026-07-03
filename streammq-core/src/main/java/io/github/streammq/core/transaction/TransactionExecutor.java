package io.github.streammq.core.transaction;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;

/**
 * 事务执行器接口，封装事务消息发送 + 本地事务执行的完整流程。
 *
 * <p>核心方法 {@link #executeInTransaction} 流程：
 * <ol>
 *   <li>发送半消息到 {@code streammq:half:{transactionGroup}} Stream</li>
 *   <li>调用 {@link TransactionCallback#execute} 执行本地事务</li>
 *   <li>根据返回值：
 *     <ul>
 *       <li>{@link io.github.streammq.core.enums.LocalTransactionState#COMMIT_MESSAGE} - 提交半消息（转投到目标 Topic Stream）</li>
 *       <li>{@link io.github.streammq.core.enums.LocalTransactionState#ROLLBACK_MESSAGE} - 回滚半消息（标记删除）</li>
 *       <li>{@link io.github.streammq.core.enums.LocalTransactionState#UNKNOW} - 保留半消息，等待事务回查</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <p><b>泛型设计</b>：泛型参数 {@code <T>} 声明在方法级别，支持同一事务执行器
 * 处理不同 body 类型的事务消息。
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
