/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.handler;

import io.github.streammq.adapter.redisson.dlq.DefaultDlqFailureContext;
import io.github.streammq.adapter.redisson.dlq.LimitedRetryDlqFailureStrategy;
import io.github.streammq.adapter.redisson.dlq.LogAndDropDlqFailureStrategy;
import io.github.streammq.adapter.redisson.dlq.SecondaryDlqFailureStrategy;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.DlqReason;
import io.github.streammq.core.interceptor.ConsumerInterceptorChain;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.metrics.StreamMQMetrics;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureContext;
import io.github.streammq.core.policy.DlqFailureDecision;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RetryAndDlqHandler;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.util.StringUtils;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.redisson.api.BatchOptions;
import org.redisson.api.RBatch;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamAddArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ACK / 重试 / DLQ 路由处理器默认实现（策略类）。
 *
 * <p>封装消息消费后的动作路由逻辑。DLQ 消费失败时， 使用 {@link DlqFailureStrategy} 决策 drop / retry / secondaryDlq 三种去向。
 *
 * <p>内置策略：
 *
 * <ul>
 *   <li>{@link LogAndDropDlqFailureStrategy} - 始终丢弃（默认）
 *   <li>{@link LimitedRetryDlqFailureStrategy} - 有限次重试后丢弃
 *   <li>{@link SecondaryDlqFailureStrategy} - 有限次重试后转投二级死信
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@RequiredArgsConstructor
public class DefaultRetryAndDlqHandler implements RetryAndDlqHandler {

    /** 重试/DLQ payload Hash 的保留时长：超期自动过期，防止孤儿 payload 无限累积 */
    static final java.time.Duration RETRY_PAYLOAD_TTL = java.time.Duration.ofDays(7);

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRetryAndDlqHandler.class);

    private static final String FIELD_ORIGINAL_MESSAGE_ID =
            StreamMQConstants.FIELD_ORIGINAL_MESSAGE_ID;

    @NonNull private final RedissonClient redisson;
    @NonNull private final MessageConverter messageConverter;
    @NonNull private final RetryPolicy retryPolicy;
    @NonNull private final ConsumerInterceptorChain interceptorChain;
    @NonNull private final DlqFailureStrategy dlqFailureStrategy;
    @NonNull private final DlqConfig dlqConfig;

    /** 指标收集器（可选注入，用于记录重试 / 死信指标，null 时为 no-op） */
    @Setter private volatile StreamMQMetrics metrics;

    @Override
    public void handleAction(
            ConsumeAction action,
            Message<?> message,
            ListenerRegistration<?> reg,
            StreamMQListener listener,
            Throwable cause) {
        MessageId messageId = message.getMessageId();
        if (Objects.isNull(messageId)) {
            LOG.warn(
                    "Message has no messageId, cannot ack/retry: topic={}, group={}",
                    reg.getTopic(),
                    reg.getGroup());
            return;
        }
        if (Objects.isNull(action)) {
            action = ConsumeAction.RECONSUME_LATER;
        }
        LOG.info(
                "handleAction: action={}, isSuccess={}, isDefer={}, dlqMode={}, topic={}, group={},"
                        + " messageId={}",
                action,
                action.isSuccess(),
                action.isDefer(),
                reg.isDlqMode(),
                reg.getTopic(),
                reg.getGroup(),
                messageId);
        if (action.isSuccess()) {
            try {
                listener.ack(messageId);
            } catch (RuntimeException ex) {
                // ACK 失败不重试：消息仍留在 PEL 中，后续会被 PEL 认领调度器重新投递，
                // at-least-once 语义得以保持（消费端必须幂等）。这里刻意提升为 ERROR 并说明后果，
                // 因为"ACK 失败"在 Redis 抖动期间会直接表现为重复消费，是需要被运维看到的信号。
                LOG.error(
                        "ACK failed (messageId={}): the message stays in PEL and will be"
                                + " redelivered by PelClaimScheduler once idle exceeds the PEL"
                                + " min-idle threshold (default {}ms) — consumers must be"
                                + " idempotent. cause={}",
                        messageId,
                        StreamMQConstants.DEFAULT_PEL_CLAIM_MIN_IDLE_MS,
                        ex.getMessage(),
                        ex);
            }
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
            LOG.info(
                    "Routing to handleDlqFailureWithStrategy: topic={}, group={}, messageId={},"
                            + " cause={}",
                    reg.getTopic(),
                    reg.getGroup(),
                    messageId,
                    cause != null ? cause.getMessage() : "null");
            handleDlqFailureWithStrategy(message, reg, listener, messageId, cause);
        } else {
            handleReconsumeLater(message, reg, listener, messageId);
        }
    }

    /**
     * DLQ 消费失败处理（基于策略决策）。
     *
     * <p>流程：
     *
     * <ol>
     *   <li>从消息中解析 dlqRetryCount
     *   <li>构造 {@link DlqFailureContext} 传给策略
     *   <li>执行策略返回的决策（drop/retry/secondaryDlq）
     * </ol>
     */
    private void handleDlqFailureWithStrategy(
            Message<?> message,
            ListenerRegistration<?> reg,
            StreamMQListener listener,
            MessageId messageId,
            Throwable cause) {
        LOG.info(
                "handleDlqFailureWithStrategy called: topic={}, group={}, messageId={}, cause={}",
                reg.getTopic(),
                reg.getGroup(),
                messageId,
                cause != null ? cause.getMessage() : "null");
        try {
            Map<String, String> fields = messageConverter.toStreamFields(message);
            LOG.info("handleDlqFailureWithStrategy: fields.size={}", fields.size());
            int dlqRetryCount = resolveDlqRetryCount(message, fields);
            String dlqReason =
                    fields.getOrDefault(
                            RetryScheduler.FIELD_DLQ_REASON, DlqReason.UNKNOWN.getCode());
            String originalMsgId =
                    fields.getOrDefault(FIELD_ORIGINAL_MESSAGE_ID, messageId.getStreamEntryId());

            DlqFailureContext ctx =
                    new DefaultDlqFailureContext(
                            dlqRetryCount,
                            dlqReason,
                            reg.getTopic(),
                            originalMsgId,
                            cause,
                            fields,
                            dlqConfig.getMaxDlqRetryAttempts(),
                            dlqConfig.getDlqRetryDelayMs(),
                            reg.getGroup());

            LOG.info(
                    "Calling dlqFailureStrategy.decide: strategy={}, dlqRetryCount={},"
                            + " dlqReason={}",
                    dlqFailureStrategy.name(),
                    dlqRetryCount,
                    dlqReason);
            DlqFailureDecision decision = dlqFailureStrategy.decide(message, ctx);
            LOG.info("dlqFailureStrategy.decide returned: decision={}", decision.type());
            if (Objects.isNull(decision)) {
                decision = DlqFailureDecision.drop();
            }

            // dispatch decision
            switch (decision.type()) {
                case RETRY ->
                        scheduleDlqRetry(
                                message,
                                reg,
                                listener,
                                messageId,
                                fields,
                                dlqRetryCount,
                                decision.retryDelay());
                case SECONDARY_DLQ -> {
                    // 仅在成功写入二级 DLQ 后才 ACK；失败保留 PEL 等待重试（否则消息既不在
                    // 二级 DLQ 也不在 PEL，造成静默丢失）
                    if (routeToSecondaryDlq(message, reg, messageId, fields)) {
                        listener.ack(messageId);
                    } else {
                        LOG.error(
                                "Secondary DLQ routing failed, keeping message in PEL:"
                                        + " topic={}, group={}, messageId={}",
                                reg.getTopic(),
                                reg.getGroup(),
                                messageId);
                    }
                }
                default -> {
                    LOG.warn(
                            "DLQ message dropped (topic={}, group={}, messageId={},"
                                    + " dlqRetryCount={})",
                            reg.getTopic(),
                            reg.getGroup(),
                            messageId,
                            dlqRetryCount);
                    listener.ack(messageId);
                }
            }
        } catch (RuntimeException ex) {
            // 安全兜底：策略/序列化等内部异常时不得丢弃死信（死信是最后一副本）。
            // 不 ACK，消息保留在 DLQ Stream 的 PEL 中——DLQ 组注册的 PelClaim DLQ 目标会在
            // idle 超时后将条目尾部复制重投（copy-tail + ACK 旧条目），与 SECONDARY_DLQ
            // 写入失败分支保持一致的"宁可滞留、不可丢失"语义。
            LOG.error(
                    "DLQ failure strategy error, keeping message in PEL (topic={}, group={},"
                            + " messageId={}): {}",
                    reg.getTopic(),
                    reg.getGroup(),
                    messageId,
                    ex.getMessage(),
                    ex);
        }
    }

    /** 将 DLQ 消息写入 retry ZSet（以哨兵 topic 标识，RetryScheduler 检测后 XADD 回 DLQ Stream） */
    private void scheduleDlqRetry(
            Message<?> message,
            ListenerRegistration<?> reg,
            StreamMQListener listener,
            MessageId messageId,
            Map<String, String> fields,
            int dlqRetryCount,
            Duration delay) {
        long nextRetryAt = System.currentTimeMillis() + delay.toMillis();
        String msgIdStr = messageId.getStreamEntryId();
        // DLQ 流按 group 命名（与业务 topic 无关），重试调度条目必须统一挂到 {group}:{group}
        // 维度——此前使用 reg.getTopic()（生产路径下为 group，但自定义注册时可能是业务 topic），
        // 会写入一个没有任何 RetryScheduler 扫描目标覆盖的 ZSet，重试永不发生。
        String scopeTopic = reg.getGroup();
        String payloadKey =
                StreamMQKeys.retryPayloadHash(
                        reg.getNamespace(), scopeTopic, reg.getGroup(), msgIdStr);
        int newDlqRetryCount = dlqRetryCount + 1;
        Map<String, String> payload = new HashMap<>(fields.size() + 3);
        payload.putAll(fields);
        payload.put(RetryScheduler.FIELD_RETRY_COUNT, Integer.toString(newDlqRetryCount));
        payload.put(
                RetryScheduler.FIELD_TARGET_TOPIC,
                StreamMQConstants.DLQ_RETRY_TARGET_TOPIC_SENTINEL);

        // 原子写入：payload Hash（带 TTL）+ 调度 ZSet 必须同生同死——拆成两条命令时，
        // 第二条失败会导致消息既不在 PEL 也不再调度，造成静默丢失
        String retryKey = StreamMQKeys.retryZSet(reg.getNamespace(), scopeTopic, reg.getGroup());
        RBatch batch =
                redisson.createBatch(
                        BatchOptions.defaults()
                                .executionMode(BatchOptions.ExecutionMode.REDIS_WRITE_ATOMIC));
        batch.<String, String>getMap(payloadKey).putAllAsync(payload);
        batch.<String, String>getMap(payloadKey).expireAsync(RETRY_PAYLOAD_TTL);
        batch.<String>getScoredSortedSet(retryKey).addAsync(nextRetryAt, msgIdStr);
        try {
            batch.execute();
        } catch (RuntimeException ex) {
            // 原子批未生效，调度条目未写入：保留 PEL 等待恢复。DLQ 组注册的 PelClaim DLQ
            // 目标会在 idle 超时后尾部复制重投该条目，消息不会滞留丢失。
            LOG.error(
                    "Failed to schedule DLQ retry, keeping message in PEL (topic={}, group={},"
                            + " messageId={}): {}",
                    reg.getTopic(),
                    reg.getGroup(),
                    messageId,
                    ex.getMessage(),
                    ex);
            return;
        }

        LOG.info(
                "DLQ retry scheduled: topic={}, group={}, dlqRetryCount={}/{}, delayMs={}",
                reg.getTopic(),
                reg.getGroup(),
                newDlqRetryCount,
                dlqConfig.getMaxDlqRetryAttempts(),
                delay.toMillis());
        listener.ack(messageId);
    }

    /**
     * 路由到二级死信队列。
     *
     * @return true 写入成功（调用方应 ACK）；false 写入失败（调用方必须保留 PEL）
     */
    private boolean routeToSecondaryDlq(
            Message<?> message,
            ListenerRegistration<?> reg,
            MessageId messageId,
            Map<String, String> fields) {
        try {
            fields.put(RetryScheduler.FIELD_DLQ_REASON, DlqReason.SECONDARY_DLQ.getCode());
            fields.put(FIELD_ORIGINAL_MESSAGE_ID, messageId.getStreamEntryId());
            String dlq2Key =
                    StreamMQKeys.secondaryDlqStream(
                            reg.getNamespace(),
                            reg.getGroup(),
                            dlqConfig.getSecondaryDlqKeyPrefix());
            RStream<String, String> dlq2Stream = redisson.getStream(dlq2Key);
            dlq2Stream.add(StreamAddArgs.entries(fields));
            LOG.warn(
                    "Message routed to secondary DLQ: topic={}, group={}, messageId={}, dlq2Key={}",
                    reg.getTopic(),
                    reg.getGroup(),
                    messageId,
                    dlq2Key);
            return true;
        } catch (RuntimeException ex) {
            LOG.error(
                    "Failed to route to secondary DLQ, caller must keep PEL (messageId={}): {}",
                    messageId,
                    ex.getMessage(),
                    ex);
            return false;
        }
    }

    /**
     * 解析 DLQ 重试计数。
     *
     * <p>查找顺序：消息保留属性（{@code __} 前缀，由解码器从 Entry 字段/props JSON 捕获，可随 decode → encode 往返存活）→ 原始 Entry
     * 顶层字段（仅本进程刚写入时存在）。 此前只查顶层字段且经 converter 重编码后丢失，导致计数恒为 0、DLQ 重试上限与二级 DLQ 策略失效。
     *
     * @param message 当前 DLQ 消息
     * @param fields 由当前消息重新编码的 Entry 字段
     * @return 已重试次数（无记录时为 0）
     */
    private int resolveDlqRetryCount(Message<?> message, Map<String, String> fields) {
        String fromProps =
                Objects.isNull(message.getUserProperties())
                        ? null
                        : message.getUserProperties().get(StreamMQConstants.FIELD_DLQ_RETRY_COUNT);
        String v =
                StringUtils.isNotEmpty(fromProps)
                        ? fromProps
                        : fields.get(StreamMQConstants.FIELD_DLQ_RETRY_COUNT);
        if (StringUtils.isNotEmpty(v)) {
            try {
                return Integer.parseInt(v);
            } catch (NumberFormatException ignored) {
                LOG.debug("Failed to parse DLQ retry count: {}", v);
            }
        }
        return 0;
    }

    // ===================== 原方法（不变） =====================

    @Override
    public void handleReconsumeLater(
            Message<?> message,
            ListenerRegistration<?> reg,
            StreamMQListener listener,
            MessageId messageId) {
        try {
            int retryCount = message.getReconsumeTimes();
            if (retryCount >= reg.getMaxReconsumeTimes()) {
                LOG.warn(
                        "Retry count exceeded consumer maxReconsumeTimes, routing to DLQ (topic={},"
                                + " group={}, messageId={}, retryCount={}, maxReconsumeTimes={})",
                        reg.getTopic(),
                        reg.getGroup(),
                        messageId,
                        retryCount,
                        reg.getMaxReconsumeTimes());
                if (routeToDlq(message, reg, messageId, RetryScheduler.DLQ_REASON_MAX_RETRY)) {
                    listener.ack(messageId);
                } else {
                    LOG.error(
                            "DLQ routing failed, message kept in PEL for re-delivery "
                                    + "(topic={}, group={}, messageId={})",
                            reg.getTopic(),
                            reg.getGroup(),
                            messageId);
                }
                return;
            }
            Duration delay = retryPolicy.nextRetryDelay(retryCount, message);
            if (Objects.isNull(delay)) {
                LOG.warn(
                        "RetryPolicy returned null delay, routing to DLQ "
                                + "(topic={}, group={}, messageId={}, retryCount={})",
                        reg.getTopic(),
                        reg.getGroup(),
                        messageId,
                        retryCount);
                if (routeToDlq(message, reg, messageId, RetryScheduler.DLQ_REASON_MAX_RETRY)) {
                    listener.ack(messageId);
                } else {
                    LOG.error(
                            "DLQ routing failed, message kept in PEL for re-delivery "
                                    + "(topic={}, group={}, messageId={})",
                            reg.getTopic(),
                            reg.getGroup(),
                            messageId);
                }
                return;
            }
            Map<String, String> fields = messageConverter.toStreamFields(message);
            scheduleRetry(message, reg, listener, messageId, fields, retryCount, delay);
        } catch (RuntimeException ex) {
            LOG.error(
                    "Failed to schedule retry for message (topic={}, group={}, messageId={}): {}",
                    reg.getTopic(),
                    reg.getGroup(),
                    messageId,
                    ex.getMessage(),
                    ex);
        }
    }

    @Override
    public void handleDefer(
            Message<?> message,
            ListenerRegistration<?> reg,
            StreamMQListener listener,
            MessageId messageId,
            Duration delay) {
        try {
            int retryCount = message.getReconsumeTimes();
            Map<String, String> fields = messageConverter.toStreamFields(message);
            // 标记 DEFER 调度：RetryScheduler 转投时不递增 retryTimes、不做 MAX_RETRY 判定，
            // 避免"业务合法延迟重试"侵占失败重试预算、被误标为 MAX_RETRY 进入 DLQ。
            // DEFER 不设上限，节奏由业务自行控制（文档已声明）。
            fields.put(StreamMQConstants.FIELD_DEFERRED, Boolean.TRUE.toString());
            scheduleRetry(message, reg, listener, messageId, fields, retryCount, delay);
        } catch (RuntimeException ex) {
            LOG.error(
                    "Failed to defer message (topic={}, group={}, messageId={}): {}",
                    reg.getTopic(),
                    reg.getGroup(),
                    messageId,
                    ex.getMessage(),
                    ex);
        }
    }

    private void scheduleRetry(
            Message<?> message,
            ListenerRegistration<?> reg,
            StreamMQListener listener,
            MessageId messageId,
            Map<String, String> fields,
            int retryCount,
            Duration delay) {
        long nextRetryAt = System.currentTimeMillis() + delay.toMillis();
        String msgIdStr = messageId.getStreamEntryId();
        String payloadKey =
                StreamMQKeys.retryPayloadHash(
                        reg.getNamespace(), reg.getTopic(), reg.getGroup(), msgIdStr);
        Map<String, String> payload = new HashMap<>(fields.size() + 2);
        payload.putAll(fields);
        payload.put(RetryScheduler.FIELD_RETRY_COUNT, Integer.toString(retryCount));
        payload.put(RetryScheduler.FIELD_TARGET_TOPIC, reg.getTopic());

        // 原子写入：payload Hash（带 TTL）+ 调度 ZSet 同生同死（见 scheduleDlqRetry 注释）
        String retryKey =
                StreamMQKeys.retryZSet(reg.getNamespace(), reg.getTopic(), reg.getGroup());
        RBatch batch =
                redisson.createBatch(
                        BatchOptions.defaults()
                                .executionMode(BatchOptions.ExecutionMode.REDIS_WRITE_ATOMIC));
        batch.<String, String>getMap(payloadKey).putAllAsync(payload);
        batch.<String, String>getMap(payloadKey).expireAsync(RETRY_PAYLOAD_TTL);
        batch.<String>getScoredSortedSet(retryKey).addAsync(nextRetryAt, msgIdStr);
        try {
            batch.execute();
        } catch (RuntimeException ex) {
            // 原子批未生效，调度条目未写入：保留 PEL 等待恢复。并发集群消费组注册的
            // PelClaim TOPIC/RETRY 目标会在 idle 超时后重投（超限转 DLQ），重启后的
            // 自身 PEL 排空亦会补齐，消息不会滞留丢失。
            LOG.error(
                    "Failed to schedule retry, keeping message in PEL (topic={}, group={},"
                            + " messageId={}): {}",
                    reg.getTopic(),
                    reg.getGroup(),
                    messageId,
                    ex.getMessage(),
                    ex);
            return;
        }

        recordRetryMetrics(reg.getTopic(), reg.getGroup());

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Message scheduled for retry: topic={}, group={}, messageId={}, "
                            + "retryCount={}, delayMs={}, nextRetryAt={}",
                    reg.getTopic(),
                    reg.getGroup(),
                    messageId,
                    retryCount,
                    delay.toMillis(),
                    nextRetryAt);
        }
        listener.ack(messageId);
    }

    @Override
    public boolean routeToDlq(
            Message<?> message, ListenerRegistration<?> reg, MessageId messageId, String reason) {
        try {
            Map<String, String> fields = messageConverter.toStreamFields(message);
            fields.put(RetryScheduler.FIELD_DLQ_REASON, reason);
            fields.put(FIELD_ORIGINAL_MESSAGE_ID, messageId.getStreamEntryId());
            String dlqKey = StreamMQKeys.dlqStream(reg.getNamespace(), reg.getGroup());
            RStream<String, String> dlqStream = redisson.getStream(dlqKey);
            dlqStream.add(StreamAddArgs.entries(fields));
            LOG.info(
                    "Message routed to DLQ: topic={}, group={}, messageId={}, reason={}",
                    reg.getTopic(),
                    reg.getGroup(),
                    messageId,
                    reason);
            recordDlqMetrics(reg.getTopic(), reg.getGroup());
            return true;
        } catch (RuntimeException ex) {
            LOG.error(
                    "Failed to route message to DLQ (topic={}, group={}, messageId={}): {}",
                    reg.getTopic(),
                    reg.getGroup(),
                    messageId,
                    ex.getMessage(),
                    ex);
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
        if (Objects.nonNull(metrics)) {
            try {
                metrics.recordRetry(topic, group);
            } catch (Exception ignored) {
                // 指标收集失败不得影响业务主流程
                LOG.debug("Metrics collection failed", ignored);
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
        if (Objects.nonNull(metrics)) {
            try {
                metrics.recordDlq(topic, group);
            } catch (Exception ignored) {
                // 指标收集失败不得影响业务主流程
                LOG.debug("Metrics collection failed", ignored);
            }
        }
    }
}
