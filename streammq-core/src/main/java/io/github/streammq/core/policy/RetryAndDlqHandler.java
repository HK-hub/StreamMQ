package io.github.streammq.core.policy;

import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;

import java.time.Duration;

/**
 * ACK / 重试 / DLQ 路由处理器策略接口。
 *
 * <p>封装消息消费后的动作路由逻辑：
 * <ul>
 *   <li>{@link ConsumeAction#SUCCESS} - ACK 消息（从 PEL 移除）</li>
 *   <li>{@link ConsumeAction#RECONSUME_LATER} - 写入 retry ZSet + payload Hash 后 ACK 原消息；
 *       DLQ 模式下直接 ACK 丢弃，避免死信消息无限循环</li>
 * </ul>
 *
 * <p>顺序消费的 {@link io.github.streammq.core.enums.OrderlyAction#SUSPEND_CURRENT_QUEUE_A_MOMENT}
 * 由容器直接处理（消息留在 PEL），不进入本处理器。
 *
 * <p>重试超时路由：当 {@link RetryPolicy#nextRetryDelay} 返回 null（不再重试）时，
 * 路由到 DLQ Stream。
 *
 * <p>设计模式：策略模式，将 ACK/重试/DLQ 路由逻辑从容器中分离。
 * 默认实现位于 {@code streammq-redisson-adapter} 模块，可通过容器构造器注入自定义实现。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface RetryAndDlqHandler {

    /**
     * 根据消费动作路由消息。
     *
     * @param action 消费动作
     * @param message 消息
     * @param reg Listener 注册信息
     * @param listener 监听器实例
     */
    void handleAction(ConsumeAction action, Message<?> message, ListenerRegistration<?> reg, StreamMQListener listener);

    /**
     * 处理 RECONSUME_LATER：将消息写入 retry ZSet + payload Hash，并 ACK 原消息。
     *
     * <p>流程：
     * <ol>
     *   <li>将 {@link Message} 转换回 Stream Entry 字段</li>
     *   <li>调用 {@link RetryPolicy#nextRetryDelay} 计算下一次重试延迟</li>
     *   <li>若延迟为 null（不再重试），路由到 DLQ Stream</li>
     *   <li>否则写入 payload Hash + retry ZSet，ACK 原消息</li>
     * </ol>
     *
     * @param message 消息
     * @param reg Listener 注册信息
     * @param listener 监听器实例
     * @param messageId 消息 ID
     */
    void handleReconsumeLater(Message<?> message, ListenerRegistration<?> reg,
                              StreamMQListener listener, MessageId messageId);

    /**
     * 处理 defer：将消息写入 retry ZSet + payload Hash（使用指定延迟），并 ACK 原消息。
     *
     * <p>流程类似 {@link #handleReconsumeLater}，但用指定的 delay 而非
     * {@link RetryPolicy#nextRetryDelay} 计算的延迟。当重试次数达到
     * {@link ListenerRegistration#getMaxReconsumeTimes()} 时路由到 DLQ Stream。
     *
     * @param message 消息
     * @param reg Listener 注册信息
     * @param listener 监听器实例
     * @param messageId 消息 ID
     * @param delay 指定的延迟时长
     */
    void handleDefer(Message<?> message, ListenerRegistration<?> reg,
                     StreamMQListener listener, MessageId messageId, Duration delay);

    /**
     * 将消息路由到 DLQ Stream。
     *
     * @param message 原始消息
     * @param reg Listener 注册信息
     * @param messageId 消息 ID
     * @param reason 进入 DLQ 的原因
     * @return true 表示 DLQ 写入成功；false 表示失败，调用方不应 ACK
     */
    boolean routeToDlq(Message<?> message, ListenerRegistration<?> reg,
                       MessageId messageId, String reason);
}
