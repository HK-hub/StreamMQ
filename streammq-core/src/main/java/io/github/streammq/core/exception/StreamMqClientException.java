package io.github.streammq.core.exception;

/**
 * 客户端异常：配置错误、参数错误、调用时序错误等。
 *
 * <p>此类异常通常无法通过重试解决，需修复业务代码或配置。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMqClientException extends StreamMqException {

    private static final long serialVersionUID = 1L;

    /**
     * 构造异常。
     *
     * @param message 错误信息
     */
    public StreamMqClientException(String message) {
        super(message);
    }

    /**
     * 构造异常。
     *
     * @param message 错误信息
     * @param cause 原始异常
     */
    public StreamMqClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
