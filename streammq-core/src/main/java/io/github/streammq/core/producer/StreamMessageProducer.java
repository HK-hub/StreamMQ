package io.github.streammq.core.producer;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * StreamMQ 生产者接口（底层抽象）。
 *
 * <p>提供原始的发送 API，{@link StreamMessageTemplate} 在此基础上做业务友好封装。 实现类位于 {@code
 * streammq-redisson-adapter} 模块。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMessageProducer {

    /**
     * 同步发送单条消息。
     *
     * @param message 消息
     * @return 发送结果
     * @throws io.github.streammq.core.exception.StreamMQException 发送失败
     */
    SendResult syncSend(Message<?> message);

    /**
     * 同步发送单条消息（带超时）。
     *
     * @param message 消息
     * @param timeoutMillis 超时毫秒数
     * @return 发送结果
     * @throws io.github.streammq.core.exception.ProducerTimeoutException 超时
     */
    SendResult syncSend(Message<?> message, long timeoutMillis);

    /**
     * 异步发送单条消息。
     *
     * @param message 消息
     * @return 异步结果
     */
    CompletableFuture<SendResult> asyncSend(Message<?> message);

    /**
     * 单向发送：不等待响应，不抛异常，性能最高。
     *
     * @param message 消息
     */
    void sendOneway(Message<?> message);

    /**
     * 批量发送（基于 RBatch / Pipeline）。
     *
     * @param messages 消息列表（必须同 Topic）
     * @return 每条消息的发送结果
     * @throws IllegalArgumentException 如果消息列表为空或 Topic 不一致
     */
    List<SendResult> syncSendBatch(List<? extends Message<?>> messages);

    /** 关闭生产者，释放资源。 */
    void close();
}
