/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.listener;

import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import java.time.Duration;
import java.util.List;

/**
 * StreamMQ 监听器接口（底层 PULL 抽象）。
 *
 * <p>负责"监听" Redis Stream，从 Stream 拉取消息并确认（ACK）。 该接口是底层 API，由 {@link StreamMQListenerContainer}
 * 内部调用， 将拉取到的消息分发给业务层实现的 {@link StreamMessageConcurrentlyConsumer} 处理。
 *
 * <p>命名说明：对齐 RocketMQ 的 {@code PullConsumer}， "监听"（Listen）Stream 获取消息的角色是
 * Listener，与业务消费回调（Consumer）分离：
 *
 * <ul>
 *   <li>{@code StreamMQListener}（本接口）- 框架内部使用，PULL + ACK
 *   <li>{@code StreamMessageConcurrentlyConsumer<T>} - 用户实现，onMessage 业务处理
 * </ul>
 *
 * <p>实现类位于 {@code streammq-redisson} 模块。
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
     * 排空当前消费者 PEL 中已投递未确认的消息（XREADGROUP id=0 语义），用于实例重启后的恢复。
     *
     * <p>崩溃/停止时已进入本消费者 PEL 但未处理完的消息，会在下次启动时通过本方法重新交付， 保证 at-least-once。
     *
     * <p><b>返回值契约（0.1.2 定稿，发布即冻结）：</b>三种结果语义不同，调用方必须区分：
     *
     * <ul>
     *   <li>{@code null}——<b>本监听器未实现 PEL 恢复</b>（默认实现即返回 {@code null}）；调用方应记录 WARN 提示
     *       "不支持恢复"，而不是把它当作"PEL 已清空"
     *   <li>空列表——<b>PEL 已清空</b>（恢复完成，已无待处理消息）
     *   <li>非空列表——本轮恢复出的待处理消息
     * </ul>
     *
     * @param maxMessages 单次最大恢复条数
     * @return 待处理消息列表；{@code null} 表示该监听器不支持 PEL 恢复，空列表表示 PEL 已清空
     */
    default List<Message<?>> drainPendingOnce(int maxMessages) {
        return null;
    }

    /**
     * 确认单条消息（从 PEL 中移除）。
     *
     * <p><b>实现契约：</b>方法返回<b>不代表</b> XACK 已在 Redis 端完成——默认实现采用<b>有界异步流水线</b>
     * （避免每条消息一次阻塞往返，这是消费吞吐的硬上限）。XACK 失败会记录 ERROR，消息保留在 PEL 中由 PEL
     * 认领调度器兜底重投，因此<b>消费端必须幂等</b>；优雅停机（{@code close()}）会对在途 ACK 做有界排空。 需要"返回即已确认"语义时请使用 {@link
     * #ackBatch(List)}（同步）。
     *
     * @param messageId 消息 ID
     * @throws io.github.streammq.core.exception.StreamMQBrokerException 参数非法、等待 ACK 窗口被中断，
     *     或提交动作本身失败（如监听器已关闭）
     */
    void ack(MessageId messageId);

    /**
     * 批量确认消息。
     *
     * @param messageIds 消息 ID 列表
     * @throws io.github.streammq.core.exception.StreamMQBrokerException 如果 XACK 失败
     */
    void ackBatch(List<MessageId> messageIds);

    /** 关闭监听器，释放资源。 */
    void close();
}
