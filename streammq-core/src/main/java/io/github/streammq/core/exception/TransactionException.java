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

    public TransactionException(String message, String transactionId, String transactionGroup) {
        super(message);
        this.transactionId = transactionId;
        this.transactionGroup = transactionGroup;
    }

    public TransactionException(String message, String transactionId, String transactionGroup, Throwable cause) {
        super(message, cause);
        this.transactionId = transactionId;
        this.transactionGroup = transactionGroup;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getTransactionGroup() {
        return transactionGroup;
    }
}
