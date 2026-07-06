package io.github.streammq.core.exception;

/**
 * Broker 异常：Redis 服务端返回错误。
 *
 * <p>含 errorCode 用于区分 Redis 错误码（如 OOM、LOADING、NOSCRIPT 等）。
 * 此类异常通常可重试。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMQBrokerException extends StreamMQException {

    private static final long serialVersionUID = 1L;

    /** Redis 返回的错误码（如 "OOM" / "LOADING"），可为 null */
    private final String errorCode;

    public StreamMQBrokerException(String message) {
        this(message, null, null);
    }

    public StreamMQBrokerException(String message, String errorCode) {
        this(message, errorCode, null);
    }

    public StreamMQBrokerException(String message, String errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    /**
     * 返回 Redis 错误码。
     *
     * @return 错误码，可能为 null
     */
    public String getErrorCode() {
        return errorCode;
    }
}
