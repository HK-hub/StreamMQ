package io.github.streammq.core.listener;

import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;

import java.time.Duration;
import java.util.List;

/**
 * StreamMQ 监听器接口（底层 PULL 抽象）。
 *
 * <p>负责"监听" Redis Stream，从 Stream 拉取消息并确认（ACK）。
 * 该接口是底层 API，由 {@link StreamMQListenerContainer} 内部调用，
 * 将拉取到的消息分发给业务层实现的 {@link StreamMessageConcurrentlyConsumer} 处理。
 *
 * <p>命名说明：对齐 RocketMQ 的 {@code PullConsumer}，
 * "监听"（Listen）Stream 获取消息的角色是 Listener，与业务消费回调（Consumer）分离：
 * <ul>
 *   <li>{@code StreamMqListener}（本接口）- 框架内部使用，PULL + ACK</li>
 *   <li>{@code StreamMqConsumer<T>} - 用户实现，onMessage 业务处理</li>
 * </ul>
 *
 * <p>实现类位于 {@code streammq-redisson-adapter} 模块。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMQListener {

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
     * 关闭监听器，释放资源。
     */
    void close();
}
