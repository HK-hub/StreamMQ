package io.github.streammq.core.exception;

/**
 * 事务消息异常：半消息发送失败、本地事务执行失败、事务回查失败等。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class TransactionException extends StreamMqException {

    private static final long serialVersionUID = 1L;

    /** 事务 ID */
    private final String transactionId;

    /** 事务 Group */
    private final String transactionGroup;

    /**
     * 构造异常。
     *
     * @param message 错误信息
     * @param transactionId 事务 ID
     * @param transactionGroup 事务组名
     */
    public TransactionException(String message, String transactionId, String transactionGroup) {
        super(message);
        this.transactionId = transactionId;
        this.transactionGroup = transactionGroup;
    }

    /**
     * 构造异常。
     *
     * @param message 错误信息
     * @param transactionId 事务 ID
     * @param transactionGroup 事务组名
     * @param cause 原始异常
     */
    public TransactionException(String message, String transactionId, String transactionGroup, Throwable cause) {
        super(message, cause);
        this.transactionId = transactionId;
        this.transactionGroup = transactionGroup;
    }

    /**
     * 返回事务 ID。
     *
     * @return 事务 ID
     */
    public String getTransactionId() {
        return transactionId;
    }

    /**
     * 返回事务组名。
     *
     * @return 事务组名
     */
    public String getTransactionGroup() {
        return transactionGroup;
    }
}
