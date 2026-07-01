package io.github.streammq.core.exception;

/**
 * StreamMQ 异常基类，所有 StreamMQ 内部异常均继承此类。
 *
 * <p>继承 {@link RuntimeException}，所有方法签名不强制声明 throws。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMqException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * 构造异常。
     *
     * @param message 错误信息
     */
    public StreamMqException(String message) {
        super(message);
    }

    /**
     * 构造异常。
     *
     * @param message 错误信息
     * @param cause 原始异常
     */
    public StreamMqException(String message, Throwable cause) {
        super(message, cause);
    }
}
