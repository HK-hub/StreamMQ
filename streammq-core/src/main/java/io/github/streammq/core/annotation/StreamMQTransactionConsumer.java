package io.github.streammq.core.annotation;

import io.github.streammq.core.StreamMQConstants;
import java.lang.annotation.*;

/**
 * StreamMQ 事务回查消费者注解（类级），标注在 {@code TransactionChecker} 实现类上。
 *
 * <p>框架将通过注解参数注册事务回查任务，定时扫描超时未确认的半消息并调用回查。
 *
 * <p>使用示例：
 *
 * <pre>{@code
 * @Component
 * @StreamMQTransactionConsumer(transactionGroup = "order-tx-group")
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
public @interface StreamMQTransactionConsumer {

    /**
     * 事务组名（必填），与发送端 {@code executeInTransaction} 的 transactionGroup 对应。
     *
     * @return 事务组名
     */
    String transactionGroup();

    /**
     * 单次回查超时（毫秒），默认 60000（60 秒）。
     *
     * @return 超时毫秒数
     */
    long checkTimeout() default 60000L;

    /**
     * 回查间隔（毫秒），默认 {@link StreamMQConstants#DEFAULT_CHECK_INTERVAL_MS}。
     *
     * @return 回查间隔毫秒数
     */
    long checkIntervalMillis() default StreamMQConstants.DEFAULT_CHECK_INTERVAL_MS;

    /**
     * 最大回查次数，默认 {@link StreamMQConstants#DEFAULT_MAX_CHECK_TIMES}。
     *
     * @return 最大回查次数
     */
    int maxCheckTimes() default StreamMQConstants.DEFAULT_MAX_CHECK_TIMES;

    /**
     * 扫描批量，默认 {@link StreamMQConstants#DEFAULT_BATCH_SIZE}。
     *
     * @return 扫描批量
     */
    int batchSize() default StreamMQConstants.DEFAULT_BATCH_SIZE;

    /**
     * 命名空间，默认使用全局配置。
     *
     * @return 命名空间
     */
    String namespace() default "";
}
