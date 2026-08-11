package io.github.streammq.core.transaction;

import io.github.streammq.core.annotation.StreamMQTransactionConsumer;
import io.github.streammq.core.enums.LocalTransactionState;
import io.github.streammq.core.message.Message;

/**
 * 事务消息回查接口。
 *
 * <p>实现此接口并在类上标注 {@link StreamMQTransactionConsumer} 注解即可注册为事务回查器。 框架将在半消息超时未确认时调用 {@link #check}
 * 回查本地事务状态。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@FunctionalInterface
public interface TransactionChecker<T> {

  /**
   * 回查本地事务状态。
   *
   * @param message 半消息载体
   * @param context 事务上下文（含 transactionId，可据此查询本地事务表）
   * @return 本地事务状态：
   *     <ul>
   *       <li>{@link LocalTransactionState#COMMIT_MESSAGE} - 提交半消息
   *       <li>{@link LocalTransactionState#ROLLBACK_MESSAGE} - 回滚半消息
   *       <li>{@link LocalTransactionState#UNKNOW} - 仍未知，等待下次回查
   *     </ul>
   *
   * @throws Exception 业务异常，框架将其视为 {@link LocalTransactionState#UNKNOW}
   */
  LocalTransactionState check(Message<T> message, TransactionContext context) throws Exception;
}
