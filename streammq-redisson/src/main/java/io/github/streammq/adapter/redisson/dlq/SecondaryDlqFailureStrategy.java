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
        int attempts = context.dlqAttempts();
        int maxRetries = config.getMaxDlqRetryAttempts();

        if (attempts >= maxRetries) {
            log.warn(
                    "DLQ retry exhausted, routing to secondary DLQ: attempts={}/{}, topic={}",
                    attempts,
                    maxRetries,
                    context.originalTopic());
            return DlqFailureDecision.secondaryDlq();
        }

        long base = config.getDlqRetryDelayMs();
        double multiplier = config.getDlqRetryBackoffMultiplier();
        long delayMs = (long) (base * Math.pow(multiplier, attempts));
        delayMs = Math.min(delayMs, config.getDlqRetryMaxDelayMs());
        delayMs = Math.max(delayMs, 1000L);
        log.info(
                "DLQ retry scheduled: attempt={}/{}, delay={}ms (topic={})",
                attempts + 1,
                maxRetries,
                delayMs,
                context.originalTopic());
        return DlqFailureDecision.retry(java.time.Duration.ofMillis(delayMs));
    }

    @Override
    public String name() {
        return STRATEGY_NAME;
    }
}
