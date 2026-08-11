package io.github.streammq.core.exception;

import java.io.Serial;

/**
 * StreamMQ 异常基类，所有 StreamMQ 内部异常均继承此类。
 *
 * <p>继承 {@link RuntimeException}，所有方法签名不强制声明 throws。
 *
 * <p><b>异常层次结构（06-1.3）：</b>
 *
 * <ul>
 *   <li>{@link StreamMQException} - 基类，所有 StreamMQ 异常的父类
 *   <li>{@link StreamMQClientException} - 客户端错误（参数校验失败、配置错误等）
 *   <li>{@link StreamMQBrokerException} - Broker 端错误（Redis 操作失败、连接超时等）
 *   <li>{@link TransactionException} - 事务消息相关异常
 *   <li>{@link SerializationException} - 序列化/反序列化异常
 *   <li>{@link ProducerTimeoutException} - 生产者发送超时
 *   <li>{@link ConsumerInterruptedException} - 消费者线程被中断
 * </ul>
 *
 * <p><b>使用约定：</b>
 *
 * <ul>
 *   <li>所有公开 API 方法应抛出 {@link StreamMQException} 或其子类，不应返回 null
 *   <li>内部异常应通过 cause 链传递，保留原始异常信息
 *   <li>业务层应捕获 {@link StreamMQException} 处理框架错误
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMQException extends RuntimeException {

    @Serial private static final long serialVersionUID = 1L;

    /**
     * 构造异常。
     *
     * @param message 错误信息
     */
    public StreamMQException(String message) {
        super(message);
    }

    /**
     * 构造异常。
     *
     * @param message 错误信息
     * @param cause 原始异常
     */
    public StreamMQException(String message, Throwable cause) {
        super(message, cause);
    }
}
