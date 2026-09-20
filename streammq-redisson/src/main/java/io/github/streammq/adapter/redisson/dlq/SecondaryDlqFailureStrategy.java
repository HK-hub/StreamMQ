/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.dlq;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.policy.AbstractDlqFailureStrategy;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureContext;
import io.github.streammq.core.policy.DlqFailureDecision;

/**
 * 策略三：有限次重试后转投二级死信（Secondary-DLQ）。
 *
 * <p>DLQ 消费失败时：
 *
 * <ul>
 *   <li>若 {@code dlqAttempts < maxDlqRetryAttempts} → 按退避延迟重试
 *   <li>否则 → 转投到二级死信队列（{@code streammq:{ns}:dlq2:{group}}）
 * </ul>
 *
 * <p>适用于需要多级死信归档的场景：一级 DLQ = 正常重试耗尽时进入， 二级 DLQ = 一级 DLQ 消费也失败时进入，可配合人工审核系统。
 *
 * <p><b>配置真源（0.1.2）：</b>重试次数/延迟/告警阈值优先读取决策上下文携带的生效配置（{@link
 * DefaultDlqFailureContext#resolveEffectiveConfig}，由 handler 按消费者合并全局后填充）；仅当上下文未携带时
 * 才回退到本实例构造参数。二级路由本身还受生效配置的 {@code secondary-dlq-enabled} 门控（由 handler 在分派 {@code SECONDARY_DLQ}
 * 决策前检查）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class SecondaryDlqFailureStrategy extends AbstractDlqFailureStrategy {

    public static final String STRATEGY_NAME = "secondary-dlq";

    public SecondaryDlqFailureStrategy() {
        super(DlqConfig.builder().build());
    }

    public SecondaryDlqFailureStrategy(DlqConfig config) {
        super(config);
    }

    @Override
    protected DlqFailureDecision doDecide(Message<?> message, DlqFailureContext context) {
        DlqConfig effective = DefaultDlqFailureContext.resolveEffectiveConfig(context, config);
        int attempts = context.dlqAttempts();
        int maxRetries = effective.getMaxDlqRetryAttempts();

        if (attempts >= maxRetries) {
            log.warn(
                    "DLQ retry exhausted, routing to secondary DLQ: attempts={}/{}, topic={}",
                    attempts,
                    maxRetries,
                    context.originalTopic());
            return DlqFailureDecision.secondaryDlq();
        }

        long base = effective.getDlqRetryDelayMs();
        double multiplier = effective.getDlqRetryBackoffMultiplier();
        long delayMs = (long) (base * Math.pow(multiplier, attempts));
        delayMs = Math.min(delayMs, effective.getDlqRetryMaxDelayMs());
        delayMs = Math.max(delayMs, effective.getMinRetryDelayMs());
        log.info(
                "DLQ retry scheduled: attempt={}/{}, delay={}ms (topic={})",
                attempts + 1,
                maxRetries,
                delayMs,
                context.originalTopic());
        return DlqFailureDecision.retry(java.time.Duration.ofMillis(delayMs));
    }

    /** 告警阈值同样以生效配置为真源（全局 {@code dlq-alert-threshold} 对策略可见）。 */
    @Override
    protected boolean shouldAlert(DlqFailureContext context) {
        DlqConfig effective = DefaultDlqFailureContext.resolveEffectiveConfig(context, config);
        return effective.getDlqAlertThreshold() > 0
                && context.dlqAttempts() + 1 >= effective.getDlqAlertThreshold();
    }

    @Override
    public String name() {
        return STRATEGY_NAME;
    }
}
