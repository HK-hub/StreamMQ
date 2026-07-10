package io.github.streammq.adapter.redisson.handler;

import io.github.streammq.adapter.redisson.dlq.DefaultDlqFailureContext;
import io.github.streammq.adapter.redisson.dlq.LimitedRetryDlqFailureStrategy;
import io.github.streammq.adapter.redisson.dlq.LogAndDropDlqFailureStrategy;
import io.github.streammq.adapter.redisson.dlq.SecondaryDlqFailureStrategy;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.interceptor.ConsumerInterceptorChain;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.metrics.StreamMQMetrics;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureContext;
import io.github.streammq.core.policy.DlqFailureDecision;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RetryAndDlqHandler;
import io.github.streammq.core.policy.RetryPolicy;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
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
 * <p>封装消息消费后的动作路由逻辑。DLQ 消费失败时，
 * 使用 {@link DlqFailureStrategy} 决策 drop / retry / secondaryDlq 三种去向。
 *
 * <p>内置策略：
 * <ul>
 *   <li>{@link LogAndDropDlqFailureStrategy} - 始终丢弃（默认）</li>
 *   <li>{@link LimitedRetryDlqFailureStrategy} - 有限次重试后丢弃</li>
 *   <li>{@link SecondaryDlqFailureStrategy} - 有限次重试后转投二级死信</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@RequiredArgsConstructor
public class DefaultRetryAndDlqHandler implements RetryAndDlqHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRetryAndDlqHandler.class);

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
    private final DlqFailureStrategy dlqFailureStrategy;
    @NonNull
    private final DlqConfig dlqConfig;

    /** 指标收集器（可选注入，用于记录重试 / 死信指标，null 时为 no-op） */
    @Setter
    private volatile StreamMQMetrics metrics;

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
            try { listener.ack(messageId); } catch (RuntimeException ex) {
                LOG.warn("ACK failed (messageId={}): {}", messageId, ex.getMessage(), ex); }
            return;
        }
        if (action.isDefer()) {
            if (reg.isDlqMode()) {
                handleDlqFailureWithStrategy(message, reg, listener, messageId, cause);
            } else {
                handleDefer(message, reg, listener, messageId, action.getDeferDelay());
            }
            return;
        }
        if (reg.isDlqMode()) {
            handleDlqFailureWithStrategy(message, reg, listener, messageId, cause);
        } else {
            handleReconsumeLater(message, reg, listener, messageId);
        }
    }

    /**
     * DLQ 消费失败处理（基于策略决策）。
     *
     * <p>流程：
     * <ol>
     *   <li>从消息中解析 dlqRetryCount</li>
     *   <li>构造 {@link DlqFailureContext} 传给策略</li>
     *   <li>执行策略返回的决策（drop/retry/secondaryDlq）</li>
     * </ol>
     */
    private void handleDlqFailureWithStrategy(Message<?> message, ListenerRegistration<?> reg,
                                              StreamMQListener listener, MessageId messageId, Throwable cause) {
        try {
            Map<String, String> fields = messageConverter.toStreamFields(message);
            int dlqRetryCount = parseDlqRetryCount(fields);
            String dlqReason = fields.getOrDefault(RetryScheduler.FIELD_DLQ_REASON, "unknown");
            String originalMsgId = fields.getOrDefault(FIELD_ORIGINAL_MESSAGE_ID, messageId.getStreamEntryId());

            DlqFailureContext ctx = new DefaultDlqFailureContext(
                dlqRetryCount, dlqReason, reg.getTopic(), originalMsgId,
                cause, fields, dlqConfig.getMaxDlqRetryAttempts(), dlqConfig.getDlqRetryDelayMs());

            DlqFailureDecision decision = dlqFailureStrategy.decide(message, ctx);
            if (decision == null) { decision = DlqFailureDecision.drop(); }

            

            // dispatch decision
            switch (decision.type()) {
                case RETRY -> scheduleDlqRetry(message, reg, listener, messageId, fields, dlqRetryCount, decision.retryDelay());
                case SECONDARY_DLQ -> {
                    routeToSecondaryDlq(message, reg, messageId, fields);
                    listener.ack(messageId);
                }
                default -> {
                    LOG.warn("DLQ message dropped (topic={}, group={}, messageId={}, dlqRetryCount={})",
                        reg.getTopic(), reg.getGroup(), messageId, dlqRetryCount);
                    listener.ack(messageId);
                }
            }
        } catch (RuntimeException ex) {
            LOG.error("DLQ failure strategy error, falling back to drop (topic={}, group={}, messageId={}): {}",
                reg.getTopic(), reg.getGroup(), messageId, ex.getMessage(), ex);
            try { listener.ack(messageId); } catch (RuntimeException ackEx) {
                LOG.warn("Fallback ACK failed: {}", ackEx.getMessage()); }
        }
    }

    

    /** 将 DLQ 消息写入 retry ZSet（以哨兵 topic 标识，RetryScheduler 检测后 XADD 回 DLQ Stream） */
    private void scheduleDlqRetry(Message<?> message, ListenerRegistration<?> reg, StreamMQListener listener,
                                  MessageId messageId, Map<String, String> fields, int dlqRetryCount, Duration delay) {
        long nextRetryAt = System.currentTimeMillis() + delay.toMillis();
        String msgIdStr = messageId.getStreamEntryId();
        String payloadKey = StreamMQKeys.delayPayloadHash(reg.getNamespace(), msgIdStr);
        int newDlqRetryCount = dlqRetryCount + 1;
        Map<String, String> payload = new HashMap<>(fields.size() + 3);
        payload.putAll(fields);
        payload.put(RetryScheduler.FIELD_RETRY_COUNT, Integer.toString(newDlqRetryCount));
        payload.put(RetryScheduler.FIELD_TARGET_TOPIC, StreamMQConstants.DLQ_RETRY_TARGET_TOPIC_SENTINEL);
        RMap<String, String> payloadMap = redisson.getMap(payloadKey);
        payloadMap.putAll(payload);

        String retryKey = StreamMQKeys.retryZSet(reg.getNamespace(), reg.getTopic(), reg.getGroup());
        RScoredSortedSet<String> zset = redisson.getScoredSortedSet(retryKey);
        zset.add(nextRetryAt, msgIdStr);

        LOG.info("DLQ retry scheduled: topic={}, group={}, dlqRetryCount={}/{}, delayMs={}",
            reg.getTopic(), reg.getGroup(), newDlqRetryCount, dlqConfig.getMaxDlqRetryAttempts(), delay.toMillis());
        listener.ack(messageId);
    }

    /** 路由到二级死信队列 */
    private void routeToSecondaryDlq(Message<?> message, ListenerRegistration<?> reg,
                                      MessageId messageId, Map<String, String> fields) {
        try {
            fields.put(RetryScheduler.FIELD_DLQ_REASON, "secondaryDlq");
            fields.put(FIELD_ORIGINAL_MESSAGE_ID, messageId.getStreamEntryId());
            String dlq2Key = StreamMQKeys.secondaryDlqStream(reg.getNamespace(), reg.getGroup(),
                dlqConfig.getSecondaryDlqKeyPrefix());
            RStream<String, String> dlq2Stream = redisson.getStream(dlq2Key);
            dlq2Stream.add(StreamAddArgs.entries(fields));
            LOG.warn("Message routed to secondary DLQ: topic={}, group={}, messageId={}, dlq2Key={}",
                reg.getTopic(), reg.getGroup(), messageId, dlq2Key);
        } catch (RuntimeException ex) {
            LOG.error("Failed to route to secondary DLQ, falling back to drop (messageId={}): {}",
                messageId, ex.getMessage(), ex);
        }
    }

    private int parseDlqRetryCount(Map<String, String> fields) {
        String v = fields.get(StreamMQConstants.FIELD_DLQ_RETRY_COUNT);
        if (v != null && !v.isEmpty()) {
            try { return Integer.parseInt(v); } catch (NumberFormatException ignored) {}
        }
        return 0;
    }

    // ===================== 原方法（不变） =====================

    @Override
    public void handleReconsumeLater(Message<?> message, ListenerRegistration<?> reg,
                                     StreamMQListener listener, MessageId messageId) {
        try {
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
            Map<String, String> fields = messageConverter.toStreamFields(message);
            scheduleRetry(message, reg, listener, messageId, fields, retryCount, delay);
        } catch (RuntimeException ex) {
            LOG.error("Failed to schedule retry for message (topic={}, group={}, messageId={}): {}",
                reg.getTopic(), reg.getGroup(), messageId, ex.getMessage(), ex);
        }
    }

    @Override
    public void handleDefer(Message<?> message, ListenerRegistration<?> reg,
                            StreamMQListener listener, MessageId messageId, Duration delay) {
        try {
            int retryCount = message.getReconsumeTimes();
            Map<String, String> fields = messageConverter.toStreamFields(message);
            scheduleRetry(message, reg, listener, messageId, fields, retryCount, delay);
        } catch (RuntimeException ex) {
            LOG.error("Failed to defer message (topic={}, group={}, messageId={}): {}",
                reg.getTopic(), reg.getGroup(), messageId, ex.getMessage(), ex);
        }
    }

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

        recordRetryMetrics(reg.getTopic(), reg.getGroup());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Message scheduled for retry: topic={}, group={}, messageId={}, " +
                    "retryCount={}, delayMs={}, nextRetryAt={}",
                reg.getTopic(), reg.getGroup(), messageId, retryCount, delay.toMillis(), nextRetryAt);
        }
        listener.ack(messageId);
    }

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
            recordDlqMetrics(reg.getTopic(), reg.getGroup());
            return true;
        } catch (RuntimeException ex) {
            LOG.error("Failed to route message to DLQ (topic={}, group={}, messageId={}): {}",
                reg.getTopic(), reg.getGroup(), messageId, ex.getMessage(), ex);
            return false;
        }
    }

    // ===================== 指标收集 =====================

    /**
     * 记录重试指标（null 安全，指标异常不影响业务主流程）。
     *
     * @param topic 消息主题
     * @param group 消费者组
     */
    private void recordRetryMetrics(String topic, String group) {
        if (metrics != null) {
            try {
                metrics.recordRetry(topic, group);
            } catch (Exception ignored) {
                // 指标收集失败不得影响业务主流程
            }
        }
    }

    /**
     * 记录死信指标（null 安全，指标异常不影响业务主流程）。
     *
     * @param topic 消息主题
     * @param group 消费者组
     */
    private void recordDlqMetrics(String topic, String group) {
        if (metrics != null) {
            try {
                metrics.recordDlq(topic, group);
            } catch (Exception ignored) {
                // 指标收集失败不得影响业务主流程
            }
        }
    }
}
