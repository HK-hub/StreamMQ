package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.enums.Action;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.spi.MessageConverter;
import io.github.streammq.core.spi.RetryPolicy;
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
 * ACK / 重试 / DLQ 路由处理器（策略类）。
 *
 * <p>封装消息消费后的动作路由逻辑：
 * <ul>
 *   <li>{@link Action#SUCCESS} / {@link Action#COMMIT} - ACK 消息（从 PEL 移除）</li>
 *   <li>{@link Action#RECONSUME_LATER} / {@link Action#ROLLBACK} - 写入 retry ZSet + payload Hash 后 ACK 原消息；
 *       DLQ 模式下直接 ACK 丢弃，避免死信消息无限循环</li>
 *   <li>{@link Action#SUSPEND_CURRENT_QUEUE_A_MOMENT} - 不 ACK，消息留在 PEL 等待 XAUTOCLAIM</li>
 * </ul>
 *
 * <p>重试超时路由：当 {@link RetryPolicy#nextRetryDelay} 返回 null（不再重试）时，
 * 路由到 DLQ Stream（{@code streammq:{ns}:dlq:{topic}:{group}}）。
 *
 * <p>设计模式：策略模式，将 ACK/重试/DLQ 路由逻辑从容器中分离。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@RequiredArgsConstructor
public class RetryAndDlqHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RetryAndDlqHandler.class);

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

    /**
     * 根据消费动作路由消息。
     *
     * @param action 消费动作
     * @param message 消息
     * @param reg Listener 注册信息
     * @param listener 监听器实例
     */
    public void handleAction(Action action, Message<?> message, ListenerRegistration<?> reg, StreamMQListener listener) {
        MessageId messageId = message.getMessageId();
        if (messageId == null) {
            LOG.warn("Message has no messageId, cannot ack/retry: topic={}, group={}", reg.getTopic(), reg.getGroup());
            return;
        }
        switch (action) {
            case SUCCESS, COMMIT -> {
                try {
                    listener.ack(messageId);
                } catch (RuntimeException ex) {
                    LOG.warn("ACK failed (messageId={}): {}", messageId, ex.getMessage(), ex);
                }
            }
            case RECONSUME_LATER, ROLLBACK -> {
                if (reg.isDlqMode()) {
                    // DLQ 模式：消费失败直接 ACK 丢弃，不再进入重试/DLQ 循环
                    LOG.warn("DLQ message consume failed, dropping (topic={}, group={}, messageId={})",
                        reg.getTopic(), reg.getGroup(), messageId);
                    try {
                        listener.ack(messageId);
                    } catch (RuntimeException ex) {
                        LOG.warn("ACK failed for DLQ message (messageId={}): {}", messageId, ex.getMessage(), ex);
                    }
                } else {
                    handleReconsumeLater(message, reg, listener, messageId);
                }
            }
            case SUSPEND_CURRENT_QUEUE_A_MOMENT -> {
                LOG.debug("Suspend current shard (messageId={}): message stays in PEL", messageId);
            }
            default -> {
                LOG.warn("Unknown action {} for messageId={}", action, messageId);
            }
        }
    }

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
     */
    public void handleReconsumeLater(Message<?> message, ListenerRegistration<?> reg,
                                     StreamMQListener listener, MessageId messageId) {
        try {
            Map<String, String> fields = messageConverter.toStreamFields(message);
            int retryCount = message.getReconsumeTimes();
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
        } catch (RuntimeException ex) {
            LOG.error("Failed to schedule retry for message (topic={}, group={}, messageId={}): {}",
                reg.getTopic(), reg.getGroup(), messageId, ex.getMessage(), ex);
        }
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
    public boolean routeToDlq(Message<?> message, ListenerRegistration<?> reg,
                             MessageId messageId, String reason) {
        try {
            Map<String, String> fields = messageConverter.toStreamFields(message);
            fields.put(RetryScheduler.FIELD_DLQ_REASON, reason);
            fields.put(FIELD_ORIGINAL_MESSAGE_ID, messageId.getStreamEntryId());
            String dlqKey = StreamMQKeys.dlqStream(reg.getNamespace(), reg.getTopic(), reg.getGroup());
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
