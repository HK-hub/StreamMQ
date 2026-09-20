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
import io.github.streammq.adapter.redisson.support.RedisClusterCompatibility;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.DlqReason;
import io.github.streammq.core.interceptor.ConsumerInterceptorChain;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.ListenerType;
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
import java.util.LinkedHashMap;
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
import org.redisson.client.codec.StringCodec;
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

    /**
     * payload TTL 的「投递后宽限」（毫秒）：与延时链路共用同一常量（1 小时）。
     *
     * <p>重试/DLQ 重试链路此前把 payload TTL 固定为 7 天，而重试延迟可被策略配置为超过 7 天：到期扫描时 payload 已先过期 → 消息进隔离区
     * ZSet（而非投递），事实丢失。修复后 TTL = max({@link #RETRY_PAYLOAD_TTL}, 延迟 +
     * 本宽限)，覆盖扫描间隔、转投耗时与节点间时钟偏差（与延时链路同一姿态）。
     */
    static final long RETRY_PAYLOAD_TTL_GRACE_MS =
            StreamMQConstants.DEFAULT_DELAY_PAYLOAD_TTL_GRACE_MS;

    /** secondary-dlq-enabled=false 时"按 drop 处理"告警的限频窗口（毫秒） */
    private static final long SECONDARY_DISABLED_WARN_INTERVAL_MS = 60_000L;

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRetryAndDlqHandler.class);

    private static final String FIELD_ORIGINAL_MESSAGE_ID =
            StreamMQConstants.FIELD_ORIGINAL_MESSAGE_ID;

    /** 上次 secondary 开关关闭告警时间（限频 WARN，避免死信风暴刷爆日志） */
    private final java.util.concurrent.atomic.AtomicLong lastSecondaryDisabledWarnAt =
            new java.util.concurrent.atomic.AtomicLong(Long.MIN_VALUE);

    @NonNull private final RedissonClient redisson;
    @NonNull private final MessageConverter messageConverter;
    @NonNull private final RetryPolicy retryPolicy;
    @NonNull private final ConsumerInterceptorChain interceptorChain;
    @NonNull private final DlqFailureStrategy dlqFailureStrategy;
    @NonNull private final DlqConfig dlqConfig;

    /** 指标收集器（可选注入，用于记录重试 / 死信指标，null 时为 no-op） */
    @Setter private volatile StreamMQMetrics metrics;

    /**
     * 进程内运行时统计登记表（可选注入）。
     *
     * <p>发布前修复 P1-3：为 {@code /actuator/streammq/stats} 提供真实的重试/死信计数来源。
     */
    @Setter
    private volatile io.github.streammq.adapter.redisson.metrics.RuntimeStatsRegistry runtimeStats;

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
        LOG.debug(
                "handleAction: action={}, isSuccess={}, isDefer={}, dlqMode={}, topic={}, group={},"
                        + " messageId={}",
                action,
                action.isSuccess(),
                action.isDefer(),
                reg.isDlqMode(),
                reg.getTopic(),
                reg.getGroup(),
                messageId);
        if (!action.isSuccess() && !reg.isDlqMode() && reg.getType() == ListenerType.ORDERLY) {
            handleOrderlyFailure(message, reg, listener, messageId, cause);
            return;
        }
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
            LOG.debug(
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
     * 顺序消费失败的统一收口：直接路由到 DLQ（不写重试调度）。
     *
     * <p><b>为什么必须收口在这里（发布前红队审查 R5）：</b>顺序消费者<b>没有</b> retry 消费循环 （{@code
     * DefaultConsumeLoopSupervisor} 只为 AUTO_ACK 提交 retry 循环），失败重试在分片锁内原地进行、 耗尽后由 {@code
     * DefaultMessageProcessor#consumeOrderlyWithRetry} 直接转 DLQ。因此任何"未被处理的 ORDERLY 消息"若走到本类的 {@code
     * handleReconsumeLater}，只会被写入 retry ZSet → 由 {@code RetryScheduler} 转投进 retry
     * Stream，而<b>没有任何循环读取该 Stream</b>——消息在 retry Stream 中 静默沉没直到被裁剪，属于静默丢失。
     *
     * <p>可达路径（修复前均会漏到这个坑）：过滤器求值异常（{@code DefaultMessageProcessor} 的 filterEx 分支）、 {@code
     * processMessage} 的 {@code Throwable} 兜底（{@code Error}/路由二次故障）。它们在 ORDERLY 语义下的
     * 正确归宿与"业务异常"完全一致：进 DLQ 并 ACK，由运维介入。
     *
     * <p>失败语义：DLQ 写入成功才 ACK；写入失败则保留在 PEL，等 PEL 认领兜底（宁可重复，不可丢失）。
     */
    private void handleOrderlyFailure(
            Message<?> message,
            ListenerRegistration<?> reg,
            StreamMQListener listener,
            MessageId messageId,
            Throwable cause) {
        LOG.warn(
                "Orderly consumer failure routed to DLQ (orderly consumers have no retry loop):"
                        + " topic={}, group={}, messageId={}, cause={}",
                reg.getTopic(),
                reg.getGroup(),
                messageId,
                Objects.isNull(cause) ? "null" : cause.toString(),
                cause);
        if (routeToDlq(message, reg, messageId, DlqReason.MAX_RETRY_ORDERLY.getCode())) {
            try {
                listener.ack(messageId);
            } catch (RuntimeException ex) {
                LOG.error(
                        "ACK failed after orderly DLQ routing (messageId={}): the message stays in"
                                + " PEL and will be redelivered by PelClaimScheduler — consumers"
                                + " must be idempotent. cause={}",
                        messageId,
                        ex.getMessage(),
                        ex);
            }
        } else {
            LOG.error(
                    "Orderly DLQ routing failed, message kept in PEL (topic={}, group={},"
                            + " messageId={})",
                    reg.getTopic(),
                    reg.getGroup(),
                    messageId);
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
        LOG.debug(
                "handleDlqFailureWithStrategy called: topic={}, group={}, messageId={}, cause={}",
                reg.getTopic(),
                reg.getGroup(),
                messageId,
                cause != null ? cause.getMessage() : "null");
        try {
            // 防御性拷贝：转换器返回的 Map 可能是不可变实现（见 routeToDlq 说明），
            // 而下游 routeToSecondaryDlq 需要写入 DLQ 元数据字段。
            Map<String, String> fields =
                    new LinkedHashMap<>(messageConverter.toStreamFields(message));
            LOG.debug("handleDlqFailureWithStrategy: fields.size={}", fields.size());
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
                            reg.getGroup(),
                            // R3-5：把"按消费者合并全局后的"生效配置随上下文交给策略。
                            // 策略实例可能由反射无参构造（配置恒为 builder 默认），若不携带，
                            // streammq.dlq.* 全局/注解配置对策略完全不可见（三份真源互相矛盾）。
                            dlqConfig);

            LOG.debug(
                    "Calling dlqFailureStrategy.decide: strategy={}, dlqRetryCount={},"
                            + " dlqReason={}",
                    dlqFailureStrategy.name(),
                    dlqRetryCount,
                    dlqReason);
            DlqFailureDecision decision = dlqFailureStrategy.decide(message, ctx);
            // 必须先判 null 再读 decision.type()：策略返回 null 属于合法兜底输入（旧实现先调用
            // decision.type()，null 时直接 NPE 并被外层 catch 吞成 ERROR，等价于"消息滞留 PEL"），
            // 这里显式降级为默认策略 DROP，与既有兜底语义一致且可观测。
            if (Objects.isNull(decision)) {
                LOG.warn(
                        "DlqFailureStrategy returned null decision, falling back to DROP:"
                                + " strategy={}, topic={}, group={}, messageId={},"
                                + " dlqRetryCount={}",
                        dlqFailureStrategy.name(),
                        reg.getTopic(),
                        reg.getGroup(),
                        messageId,
                        dlqRetryCount);
                decision = DlqFailureDecision.drop();
            }
            LOG.debug("dlqFailureStrategy.decide returned: decision={}", decision.type());

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
                                clampRetryDelay(
                                        decision.retryDelay(), "DlqFailureDecision.retryDelay"));
                case SECONDARY_DLQ -> {
                    // R3-6：secondary-dlq-enabled 是二级路由的权威门控——策略返回 secondaryDlq
                    // 但开关为 false 时不得写入 dlq2（否则开关形同虚设，且与配置文档矛盾）。
                    // 关闭时按 drop 处理（ACK），WARN 限频 + DEBUG 留痕，保证可观测。
                    if (!dlqConfig.isSecondaryDlqEnabled()) {
                        warnSecondaryDisabled(reg, messageId, dlqRetryCount);
                        listener.ack(messageId);
                        return;
                    }
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
        // payload Hash + 调度 ZSet 必须同生同死（Cluster 下原子批退化为按节点拆分），前置拒绝
        RedisClusterCompatibility.requireCrossKeyAtomicity(
                redisson, "DLQ retry scheduling (payload hash + schedule ZSet)");
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
        // R3-7：独立 scope 标记——RetryScheduler 只在"哨兵 targetTopic + scope=dlq"同时成立时
        // 才按 DLQ 重试转投，避免任何来源的 targetTopic="__dlq__"（如历史/第三方写入的业务 topic）
        // 被误判为 DLQ 重试目标。
        payload.put(RetryScheduler.FIELD_RETRY_SCOPE, RetryScheduler.RETRY_SCOPE_DLQ);

        // 原子写入：payload Hash（带 TTL）+ 调度 ZSet 必须同生同死——拆成两条命令时，
        // 第二条失败会导致消息既不在 PEL 也不再调度，造成静默丢失
        String retryKey = StreamMQKeys.retryZSet(reg.getNamespace(), scopeTopic, reg.getGroup());
        RBatch batch =
                redisson.createBatch(
                        BatchOptions.defaults()
                                .executionMode(BatchOptions.ExecutionMode.REDIS_WRITE_ATOMIC));
        batch.<String, String>getMap(payloadKey, StringCodec.INSTANCE).putAllAsync(payload);
        batch.<String, String>getMap(payloadKey, StringCodec.INSTANCE)
                .expireAsync(payloadTtlFor(delay.toMillis()));
        batch.<String>getScoredSortedSet(retryKey, StringCodec.INSTANCE)
                .addAsync(nextRetryAt, msgIdStr);
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
            RStream<String, String> dlq2Stream = redisson.getStream(dlq2Key, StringCodec.INSTANCE);
            StreamAddArgs<String, String> dlq2Args = StreamAddArgs.entries(fields);
            if (dlqConfig.getStreamMaxLen() > 0) {
                dlq2Args = dlq2Args.trimNonStrict().maxLen(dlqConfig.getStreamMaxLen()).noLimit();
            }
            dlq2Stream.add(dlq2Args);
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

    // ===================== 消费失败重试 / DEFER 调度 =====================

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
                routeToDlqThenAck(
                        message, reg, listener, messageId, RetryScheduler.DLQ_REASON_MAX_RETRY);
                return;
            }
            // R3-4：接线 core 的 RetryPolicy.shouldStopRetry（0.1.2 契约：返回 true → 直接进
            // DLQ，reason=MAX_RETRY，不再调用 nextRetryDelay）。预算唯一由策略/消费者配置决定，
            // 与"nextRetryDelay 返回 null"这一既有停止信号并存。
            if (shouldStopRetry(retryCount, message)) {
                LOG.warn(
                        "RetryPolicy.shouldStopRetry=true, routing to DLQ (topic={}, group={},"
                                + " messageId={}, retryCount={}, policy={})",
                        reg.getTopic(),
                        reg.getGroup(),
                        messageId,
                        retryCount,
                        retryPolicy.name());
                routeToDlqThenAck(
                        message, reg, listener, messageId, RetryScheduler.DLQ_REASON_MAX_RETRY);
                return;
            }
            Duration delay =
                    clampRetryDelay(
                            retryPolicy.nextRetryDelay(retryCount, message),
                            "retryPolicy.nextRetryDelay");
            if (Objects.isNull(delay)) {
                LOG.warn(
                        "RetryPolicy returned null delay, routing to DLQ "
                                + "(topic={}, group={}, messageId={}, retryCount={})",
                        reg.getTopic(),
                        reg.getGroup(),
                        messageId,
                        retryCount);
                routeToDlqThenAck(
                        message, reg, listener, messageId, RetryScheduler.DLQ_REASON_MAX_RETRY);
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

    /**
     * 调用 {@link RetryPolicy#shouldStopRetry(int, Message)}，对第三方实现做防御。
     *
     * <p>SPI 契约要求实现不抛异常；这里仍兜底捕获 RuntimeException 并视为"不停止"，避免一个坏策略 中断消费失败后的 ACK/重投路由（消息滞留 PEL 直到 PEL
     * 认领，代价是被动恢复）。
     *
     * @return true 表示应停止重试（消息进 DLQ，reason=MAX_RETRY）
     */
    private boolean shouldStopRetry(int retryCount, Message<?> message) {
        RetryPolicy policy = this.retryPolicy;
        if (Objects.isNull(policy)) {
            return false;
        }
        try {
            return policy.shouldStopRetry(retryCount, message);
        } catch (RuntimeException ex) {
            LOG.warn(
                    "RetryPolicy.shouldStopRetry threw, treating as false and continuing with"
                            + " nextRetryDelay (policy={}): {}",
                    policy.name(),
                    ex.getMessage(),
                    ex);
            return false;
        }
    }

    /**
     * 重试延迟上界校验：超过 {@link StreamMQConstants#MAX_DELAY_TIME_MILLIS}（7 天）时<b>夹取</b>到上界并 WARN。
     *
     * <p>选择"夹取"而非"拒绝该次延迟（转 DLQ）"：拒绝会让一条本可重试的消息因策略配置笔误被直接丢弃； 夹取保证消息仍在有限时间内重投（并可通过 WARN
     * 定位配置问题）。负延迟夹取为 0（立即重试）。
     *
     * @param delay 策略返回值，可为 null
     * @param source 调用来源（日志定位用）
     * @return 夹取后的延迟（null 原样返回，表示"停止重试"）
     */
    static Duration clampRetryDelay(Duration delay, String source) {
        if (Objects.isNull(delay)) {
            return null;
        }
        if (delay.isNegative()) {
            LOG.warn("{} returned negative delay {}ms, treating as immediate retry", source, delay);
            return Duration.ZERO;
        }
        if (delay.compareTo(Duration.ofMillis(StreamMQConstants.MAX_DELAY_TIME_MILLIS)) > 0) {
            LOG.warn(
                    "{} returned delay {}ms which exceeds MAX_DELAY_TIME_MILLIS ({}ms);"
                            + " clamping to the upper bound so the payload TTL can cover it",
                    source,
                    delay,
                    StreamMQConstants.MAX_DELAY_TIME_MILLIS);
            return Duration.ofMillis(StreamMQConstants.MAX_DELAY_TIME_MILLIS);
        }
        return delay;
    }

    /**
     * 计算重试/DLQ-retry payload Hash 的 TTL：{@code max(RETRY_PAYLOAD_TTL, delay + 1h 宽限)}。
     *
     * <p>不变式：<b>TTL 必须覆盖"延迟 + 宽限"</b>，否则延迟超过 7 天的调度条目到期时 payload 已过期， RetryScheduler 读不到 payload →
     * 进隔离区（事实丢失）而非投递（R3-3）。宽限与延时链路共用同一常量。
     */
    static Duration payloadTtlFor(long delayMs) {
        long covered =
                delayMs > Long.MAX_VALUE - RETRY_PAYLOAD_TTL_GRACE_MS
                        ? Long.MAX_VALUE
                        : delayMs + RETRY_PAYLOAD_TTL_GRACE_MS;
        return Duration.ofMillis(Math.max(RETRY_PAYLOAD_TTL.toMillis(), covered));
    }

    /**
     * {@link Duration#toMillis()} 的饱和版本：超大 Duration 不抛 ArithmeticException（按 Long.MAX_VALUE 处理）。
     */
    static long delayToMillisSaturated(Duration delay) {
        try {
            return delay.toMillis();
        } catch (ArithmeticException ex) {
            return Long.MAX_VALUE;
        }
    }

    /** 停止/耗尽重试后统一收口：写 DLQ 成功才 ACK；失败保留 PEL（宁可重复，不可丢失）。 */
    private void routeToDlqThenAck(
            Message<?> message,
            ListenerRegistration<?> reg,
            StreamMQListener listener,
            MessageId messageId,
            String reason) {
        if (routeToDlq(message, reg, messageId, reason)) {
            listener.ack(messageId);
        } else {
            LOG.error(
                    "DLQ routing failed, message kept in PEL for re-delivery "
                            + "(topic={}, group={}, messageId={})",
                    reg.getTopic(),
                    reg.getGroup(),
                    messageId);
        }
    }

    /** secondary 开关关闭时的限频 WARN（窗口 {@link #SECONDARY_DISABLED_WARN_INTERVAL_MS}），其余走 DEBUG。 */
    private void warnSecondaryDisabled(
            ListenerRegistration<?> reg, MessageId messageId, int dlqRetryCount) {
        long now = System.currentTimeMillis();
        long last = lastSecondaryDisabledWarnAt.get();
        boolean warned = false;
        if (last == Long.MIN_VALUE || now - last >= SECONDARY_DISABLED_WARN_INTERVAL_MS) {
            if (lastSecondaryDisabledWarnAt.compareAndSet(last, now)) {
                warned = true;
            }
        }
        if (warned) {
            LOG.warn(
                    "DlqFailureStrategy decided SECONDARY_DLQ but"
                            + " streammq.dlq.secondary-dlq-enabled=false; message is dropped"
                            + " instead of being written to dlq2 (topic={}, group={},"
                            + " messageId={}, dlqRetryCount={}). Set secondary-dlq-enabled=true"
                            + " to enable the secondary DLQ.",
                    reg.getTopic(),
                    reg.getGroup(),
                    messageId,
                    dlqRetryCount);
        } else {
            LOG.debug(
                    "SECONDARY_DLQ decision suppressed by secondary-dlq-enabled=false, dropped"
                            + " (topic={}, group={}, messageId={})",
                    reg.getTopic(),
                    reg.getGroup(),
                    messageId);
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
            // 防御性拷贝：见 routeToDlq 中关于不可变 Map 的说明。
            Map<String, String> fields =
                    new LinkedHashMap<>(messageConverter.toStreamFields(message));
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
        // payload Hash + 调度 ZSet 必须同生同死（Cluster 下原子批退化为按节点拆分），前置拒绝
        RedisClusterCompatibility.requireCrossKeyAtomicity(
                redisson, "Retry scheduling (payload hash + schedule ZSet)");
        long delayMs = delayToMillisSaturated(delay);
        long now = System.currentTimeMillis();
        // 饱和加法：非法超大延迟不得回绕成"过去时刻"（否则立即重试形成热循环）
        long nextRetryAt = delayMs > Long.MAX_VALUE - now ? Long.MAX_VALUE : now + delayMs;
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
        batch.<String, String>getMap(payloadKey, StringCodec.INSTANCE).putAllAsync(payload);
        batch.<String, String>getMap(payloadKey, StringCodec.INSTANCE)
                .expireAsync(payloadTtlFor(delayMs));
        batch.<String>getScoredSortedSet(retryKey, StringCodec.INSTANCE)
                .addAsync(nextRetryAt, msgIdStr);
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
                    delayMs,
                    nextRetryAt);
        }
        listener.ack(messageId);
    }

    @Override
    public boolean routeToDlq(
            Message<?> message, ListenerRegistration<?> reg, MessageId messageId, String reason) {
        try {
            // 防御性拷贝：MessageConverter 是用户可替换 SPI，返回不可变 Map（如 Map.of）完全合法。
            // 直接在其返回值上 put 会抛 UnsupportedOperationException 并被下方 catch 吞成
            // "DLQ routing failed" → 消息永久滞留 PEL，重投/DLQ 全部失效（静默降级）。
            Map<String, String> fields =
                    new LinkedHashMap<>(messageConverter.toStreamFields(message));
            fields.put(RetryScheduler.FIELD_DLQ_REASON, reason);
            fields.put(FIELD_ORIGINAL_MESSAGE_ID, messageId.getStreamEntryId());
            String dlqKey = StreamMQKeys.dlqStream(reg.getNamespace(), reg.getGroup());
            RStream<String, String> dlqStream = redisson.getStream(dlqKey, StringCodec.INSTANCE);
            StreamAddArgs<String, String> dlqArgs = StreamAddArgs.entries(fields);
            if (dlqConfig.getStreamMaxLen() > 0) {
                dlqArgs = dlqArgs.trimNonStrict().maxLen(dlqConfig.getStreamMaxLen()).noLimit();
            }
            dlqStream.add(dlqArgs);
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
        var stats = runtimeStats;
        if (Objects.nonNull(stats)) {
            stats.recordRetry(group, topic);
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
        var stats = runtimeStats;
        if (Objects.nonNull(stats)) {
            stats.recordDlq(group, topic);
        }
    }
}
