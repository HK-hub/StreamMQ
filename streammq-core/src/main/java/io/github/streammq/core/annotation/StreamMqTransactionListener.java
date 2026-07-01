package io.github.streammq.core.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * StreamMQ 事务回查监听注解（类级），标注在 {@code TransactionChecker} 实现类上。
 *
 * <p>框架将通过注解参数注册事务回查任务，定时扫描超时未确认的半消息并调用回查。
 *
 * <p>使用示例：
 * <pre>{@code
 * @Component
 * @StreamMqTransactionListener(transactionGroup = "order-tx-group")
 * public class OrderTransactionChecker implements TransactionChecker<Order> {
 *     @Override
 *     public LocalTransactionState check(Message<Order> message, TransactionContext context) {
 *         String txId = context.getTransactionId();
 *         // 查询本地事务状态
 *         return orderService.isTransactionCommitted(txId)
 *             ? LocalTransactionState.COMMIT_MESSAGE
 *             : LocalTransactionState.ROLLBACK_MESSAGE;
 *     }
 * }
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface StreamMqTransactionListener {

    /**
     * 事务组名（必填），与发送端 {@code executeInTransaction} 的 transactionGroup 对应。
     *
     * @return 事务组名
     */
    String transactionGroup();

    /**
     * 单次回查超时（秒），默认 60。
     *
     * @return 超时秒数
     */
    int checkTimeout() default 60;

    /**
     * 命名空间，默认使用全局配置。
     *
     * @return 命名空间
     */
    String namespace() default "";
}
