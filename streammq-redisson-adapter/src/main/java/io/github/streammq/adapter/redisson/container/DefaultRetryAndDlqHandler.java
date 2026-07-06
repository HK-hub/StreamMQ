package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.interceptor.ConsumerInterceptorChain;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.policy.DlqFailureHandler;
import io.github.streammq.core.policy.RetryAndDlqHandler;
import io.github.streammq.core.policy.RetryPolicy;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamAddArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * ACK / 重试 / DLQ 路由处理器默认实现（策略类）。
 *
 * <p>封装消息消费后的动作路由逻辑（消费结果以 {@link ConsumeAction} 返回值为唯一标准）：
 * <ul>
 *   <li>{@link ConsumeAction#SUCCESS} - ACK 消息（从 PEL 移除）</li>
 *   <li>{@link ConsumeAction#RECONSUME_LATER} - 写入 retry ZSet + payload Hash 后 ACK 原消息；
 *       DLQ 模式下调用 {@link DlqFailureHandler} 后 ACK 丢弃，避免死信消息无限循环</li>
 *   <li>{@code ConsumeAction.defer(Duration)} - 使用指定延迟写入 retry ZSet + payload Hash 后 ACK；
 *       DLQ 模式下同 RECONSUME_LATER 处理（调用 DlqFailureHandler 后丢弃）</li>
 * </ul>
 *
 * <p>顺序消费的 {@link io.github.streammq.core.enums.OrderlyAction#SUSPEND_CURRENT_QUEUE_A_MOMENT}
 * 由容器直接处理（消息留在 PEL），不进入本处理器。
 *
 * <p>重试终止路由（任一条件命中即进 DLQ）：
 * <ul>
 *   <li>{@link RetryPolicy#shouldStopRetry} 返回 true</li>
 *   <li>{@code retryCount >= reg.getMaxReconsumeTimes()}</li>
 *   <li>{@link RetryPolicy#nextRetryDelay} 返回 null</li>
 * </ul>
 *
 * <p>设计模式：策略模式，将 ACK/重试/DLQ 路由逻辑从容器中分离。
 * 本实例持有的 {@link RetryPolicy} / {@link MessageConverter} / {@link DlqFailureHandler}
 * 可为 per-consumer 实例（由容器在注册时按注解实例化），实现高度可配置。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@RequiredArgsConstructor
public class DefaultRetryAndDlqHandler implements RetryAndDlqHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRetryAndDlqHandler.class);

    /** DLQ Stream Entry 字段：原始消息 ID */
    private static final String FIELD_ORIGINAL_MESSAGE_ID = "originalMessageId";

    @NonNull
    private final RedissonClient redisson;
    @NonNull
    private final MessageConverter messageConverter;
    @NonNull
    private final RetryPolicy retryPolicy;
    @NonNull
    private final ConsumerInterceptorChain interceptorChain;
    @NonNull
    private final DlqFailureHandler dlqFailureHandler;

    /**
     * 根据消费动作路由消息。
     *
     * @param action 消费动作（null 视为 RECONSUME_LATER）
     * @param message 消息
     * @param reg Listener 注册信息
     * @param listener 监听器实例
     * @param cause 失败原因；返回 RECONSUME_LATER/DEFER 时为 null，抛出异常时为该异常
     */
    @Override
    public void handleAction(ConsumeAction action, Message<?> message, ListenerRegistration<?> reg,
                             StreamMQListener listener, Throwable cause) {
        MessageId messageId = message.getMessageId();
        if (messageId == null) {
            LOG.warn("Message has no messageId, cannot ack/retry: topic={}, group={}", reg.getTopic(), reg.getGroup());
            return;
        }
        if (action == null) {
            action = ConsumeAction.RECONSUME_LATER;
        }
        if (action.isSuccess()) {
            try {
                listener.ack(messageId);
            } catch (RuntimeException ex) {
                LOG.warn("ACK failed (messageId={}): {}", messageId, ex.getMessage(), ex);
            }
            return;
        }
        if (action.isDefer()) {
            if (reg.isDlqMode()) {
                handleDlqFailure(message, reg, listener, messageId, cause);
            } else {
                handleDefer(message, reg, listener, messageId, action.getDeferDelay());
            }
            return;
        }
        // RECONSUME_LATER
        if (reg.isDlqMode()) {
            handleDlqFailure(message, reg, listener, messageId, cause);
        } else {
            handleReconsumeLater(message, reg, listener, messageId);
        }
    }

    /**
     * DLQ 消费失败处理：调用 {@link DlqFailureHandler} 后 ACK 丢弃，避免死信无限循环。
     */
    private void handleDlqFailure(Message<?> message, ListenerRegistration<?> reg,
                                  StreamMQListener listener, MessageId messageId, Throwable cause) {
        try {
            dlqFailureHandler.handleFailure(message, reg, cause);
        } catch (RuntimeException ex) {
            LOG.warn("DlqFailureHandler {} threw, proceeding to drop: {}", dlqFailureHandler.name(), ex.getMessage(), ex);
        }
        try {
            listener.ack(messageId);
            LOG.warn("DLQ message dropped after failure handler (topic={}, group={}, messageId={})",
                reg.getTopic(), reg.getGroup(), messageId);
        } catch (RuntimeException ex) {
            LOG.warn("ACK failed for DLQ message (messageId={}): {}", messageId, ex.getMessage(), ex);
        }
    }

    /**
     * 处理 RECONSUME_LATER：将消息写入 retry ZSet + payload Hash，并 ACK 原消息。
     *
     * <p>流程：
     * <ol>
     *   <li>若 {@link RetryPolicy#shouldStopRetry} 返回 true 或 {@code retryCount >= maxReconsumeTimes}，路由到 DLQ</li>
     *   <li>否则将 {@link Message} 转换回 Stream Entry 字段</li>
     *   <li>调用 {@link RetryPolicy#nextRetryDelay} 计算下一次重试延迟</li>
     *   <li>若延迟为 null（不再重试），路由到 DLQ Stream</li>
     *   <li>否则写入 payload Hash + retry ZSet，ACK 原消息</li>
     * </ol>
     */
    @Override
    public void handleReconsumeLater(Message<?> message, ListenerRegistration<?> reg,
                                     StreamMQListener listener, MessageId messageId) {
        try {
            Map<String, String> fields = messageConverter.toStreamFields(message);
            int retryCount = message.getReconsumeTimes();
            if (retryPolicy.shouldStopRetry(retryCount, message) || retryCount >= reg.getMaxReconsumeTimes()) {
                LOG.warn("Retry stopped by policy/maxReconsumeTimes, routing to DLQ " +
                        "(topic={}, group={}, messageId={}, retryCount={}, max={})",
                    reg.getTopic(), reg.getGroup(), messageId, retryCount, reg.getMaxReconsumeTimes());
                if (routeToDlq(message, reg, messageId, RetryScheduler.DLQ_REASON_MAX_RETRY)) {
                    listener.ack(messageId);
                } else {
                    LOG.error("DLQ routing failed, message kept in PEL for re-delivery " +
                        "(topic={}, group={}, messageId={})", reg.getTopic(), reg.getGroup(), messageId);
                }
                return;
            }
            Duration delay = retryPolicy.nextRetryDelay(retryCount, message);
            if (delay == null) {
                LOG.warn("RetryPolicy returned null delay, routing to DLQ " +
                        "(topic={}, group={}, messageId={}, retryCount={})",
                    reg.getTopic(), reg.getGroup(), messageId, retryCount);
                if (routeToDlq(message, reg, messageId, RetryScheduler.DLQ_REASON_MAX_RETRY)) {
                    listener.ack(messageId);
                } else {
                    LOG.error("DLQ routing failed, message kept in PEL for re-delivery " +
                        "(topic={}, group={}, messageId={})", reg.getTopic(), reg.getGroup(), messageId);
                }
                return;
            }
            scheduleRetry(message, reg, listener, messageId, fields, retryCount, delay);
        } catch (RuntimeException ex) {
            LOG.error("Failed to schedule retry for message (topic={}, group={}, messageId={}): {}",
                reg.getTopic(), reg.getGroup(), messageId, ex.getMessage(), ex);
        }
    }

    /**
     * 处理 defer：将消息写入 retry ZSet + payload Hash（使用指定延迟），并 ACK 原消息。
     *
     * <p>当重试次数达到 {@link ListenerRegistration#getMaxReconsumeTimes()} 或
     * {@link RetryPolicy#shouldStopRetry} 返回 true 时路由到 DLQ Stream。
     */
    @Override
    public void handleDefer(Message<?> message, ListenerRegistration<?> reg,
                            StreamMQListener listener, MessageId messageId, Duration delay) {
        try {
            int retryCount = message.getReconsumeTimes();
            if (retryPolicy.shouldStopRetry(retryCount, message) || retryCount >= reg.getMaxReconsumeTimes()) {
                LOG.warn("Defer stopped by policy/maxReconsumeTimes, routing to DLQ " +
                        "(topic={}, group={}, messageId={}, retryCount={}, max={})",
                    reg.getTopic(), reg.getGroup(), messageId, retryCount, reg.getMaxReconsumeTimes());
                if (routeToDlq(message, reg, messageId, RetryScheduler.DLQ_REASON_MAX_RETRY)) {
                    listener.ack(messageId);
                } else {
                    LOG.error("DLQ routing failed, message kept in PEL for re-delivery " +
                        "(topic={}, group={}, messageId={})", reg.getTopic(), reg.getGroup(), messageId);
                }
                return;
            }

            Map<String, String> fields = messageConverter.toStreamFields(message);
            scheduleRetry(message, reg, listener, messageId, fields, retryCount, delay);
        } catch (RuntimeException ex) {
            LOG.error("Failed to defer message (topic={}, group={}, messageId={}): {}",
                reg.getTopic(), reg.getGroup(), messageId, ex.getMessage(), ex);
        }
    }

    /**
     * 写入 retry ZSet + payload Hash 并 ACK 原消息。
     */
    private void scheduleRetry(Message<?> message, ListenerRegistration<?> reg, StreamMQListener listener,
                               MessageId messageId, Map<String, String> fields, int retryCount, Duration delay) {
        long nextRetryAt = System.currentTimeMillis() + delay.toMillis();
        String msgIdStr = messageId.getStreamEntryId();
        String payloadKey = StreamMQKeys.delayPayloadHash(reg.getNamespace(), msgIdStr);
        Map<String, String> payload = new HashMap<>(fields.size() + 2);
        payload.putAll(fields);
        payload.put(RetryScheduler.FIELD_RETRY_COUNT, Integer.toString(retryCount));
        payload.put(RetryScheduler.FIELD_TARGET_TOPIC, reg.getTopic());
        RMap<String, String> payloadMap = redisson.getMap(payloadKey);
        payloadMap.putAll(payload);

        String retryKey = StreamMQKeys.retryZSet(reg.getNamespace(), reg.getTopic(), reg.getGroup());
        RScoredSortedSet<String> zset = redisson.getScoredSortedSet(retryKey);
        zset.add(nextRetryAt, msgIdStr);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Message scheduled for retry: topic={}, group={}, messageId={}, " +
                    "retryCount={}, delayMs={}, nextRetryAt={}",
                reg.getTopic(), reg.getGroup(), messageId, retryCount, delay.toMillis(), nextRetryAt);
        }
        listener.ack(messageId);
    }

    /**
     * 将消息路由到 DLQ Stream。
     *
     * @param message 原始消息
     * @param reg Listener 注册信息
     * @param messageId 消息 ID
     * @param reason 进入 DLQ 的原因
     * @return true 表示 DLQ 写入成功；false 表示失败，调用方不应 ACK
     */
    @Override
    public boolean routeToDlq(Message<?> message, ListenerRegistration<?> reg,
                             MessageId messageId, String reason) {
        try {
            Map<String, String> fields = messageConverter.toStreamFields(message);
            fields.put(RetryScheduler.FIELD_DLQ_REASON, reason);
            fields.put(FIELD_ORIGINAL_MESSAGE_ID, messageId.getStreamEntryId());
            String dlqKey = StreamMQKeys.dlqStream(reg.getNamespace(), reg.getGroup());
            RStream<String, String> dlqStream = redisson.getStream(dlqKey);
            dlqStream.add(StreamAddArgs.entries(fields));
            LOG.info("Message routed to DLQ: topic={}, group={}, messageId={}, reason={}",
                reg.getTopic(), reg.getGroup(), messageId, reason);
            return true;
        } catch (RuntimeException ex) {
            LOG.error("Failed to route message to DLQ (topic={}, group={}, messageId={}): {}",
                reg.getTopic(), reg.getGroup(), messageId, ex.getMessage(), ex);
            return false;
        }
    }
}
