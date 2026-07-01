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
public class StreamMqBrokerException extends StreamMqException {

    private static final long serialVersionUID = 1L;

    /** Redis 返回的错误码（如 "OOM" / "LOADING"），可为 null */
    private final String errorCode;

    /**
     * 构造异常（无错误码、无原始异常）。
     *
     * @param message 错误信息
     */
    public StreamMqBrokerException(String message) {
        this(message, null, null);
    }

    /**
     * 构造异常（带错误码、无原始异常）。
     *
     * @param message 错误信息
     * @param errorCode Redis 错误码
     */
    public StreamMqBrokerException(String message, String errorCode) {
        this(message, errorCode, null);
    }

    /**
     * 构造异常（全参）。
     *
     * @param message 错误信息
     * @param errorCode Redis 错误码，可为 null
     * @param cause 原始异常，可为 null
     */
    public StreamMqBrokerException(String message, String errorCode, Throwable cause) {
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
