package io.github.streammq.core.exception;

/**
 * 序列化异常：Message body 序列化或反序列化失败。
 *
 * <p>不可重试，业务方需修正 body 类型或更换序列化器。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class SerializationException extends StreamMqException {

    private static final long serialVersionUID = 1L;

    public SerializationException(String message) {
        super(message);
    }

    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
