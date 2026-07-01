package io.github.streammq.core.consumer;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;

import java.time.Duration;
import java.util.List;

/**
 * StreamMQ 消费者接口（底层抽象）。
 *
 * <p>提供 PULL 模式主动拉取 API。PUSH 模式（伪推送）由 {@link StreamMqListenerContainer} 内部基于此接口实现。
 * 实现类位于 {@code streammq-redisson-adapter} 模块。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMqConsumer {

    /**
     * 非阻塞拉取消息。
     *
     * @param batchSize 批量大小（1-1000）
     * @return 消息列表，可能为空
     */
    List<Message<?>> pull(int batchSize);

    /**
     * 阻塞拉取消息。
     *
     * @param batchSize 批量大小
     * @param timeout 阻塞超时时长
     * @return 消息列表，超时后可能为空
     */
    List<Message<?>> pullBlock(int batchSize, Duration timeout);

    /**
     * 确认单条消息（从 PEL 中移除）。
     *
     * @param messageId 消息 ID
     * @throws io.github.streammq.core.exception.StreamMqBrokerException 如果 XACK 失败
     */
    void ack(MessageId messageId);

    /**
     * 批量确认消息。
     *
     * @param messageIds 消息 ID 列表
     * @throws io.github.streammq.core.exception.StreamMqBrokerException 如果 XACK 失败
     */
    void ackBatch(List<MessageId> messageIds);

    /**
     * 关闭消费者，释放资源。
     */
    void close();
}
