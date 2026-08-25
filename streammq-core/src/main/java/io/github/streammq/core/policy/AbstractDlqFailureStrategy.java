/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.policy;

import io.github.streammq.core.message.Message;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link DlqFailureStrategy} 抽象基类，提供日志、告警判断等公共逻辑。
 *
 * <p>子类只需实现 {@link #doDecide(Message, DlqFailureContext)}， 本类在调用前后统一处理日志记录与告警判断。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public abstract class AbstractDlqFailureStrategy implements DlqFailureStrategy {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    /** 配置 */
    protected final DlqConfig config;

    protected AbstractDlqFailureStrategy(DlqConfig config) {
        this.config = Objects.nonNull(config) ? config : DlqConfig.builder().build();
    }

    @Override
    public final DlqFailureDecision decide(Message<?> message, DlqFailureContext context) {
        log.warn(
                "DLQ consume failed: topic={}, group={}, dlqAttempts={}/{}, reason={}, cause={}",
                context.originalTopic(),
                context.dlqReason(),
                context.dlqAttempts(),
                context.maxDlqRetryAttempts(),
                context.dlqReason(),
                Objects.nonNull(context.lastFailureCause())
                        ? context.lastFailureCause().getMessage()
                        : "returned non-SUCCESS");

        DlqFailureDecision decision = doDecide(message, context);
        if (Objects.isNull(decision)) {
            decision = DlqFailureDecision.drop();
        }

        if (shouldAlert(context)) {
            log.error(
                    "DLQ alert threshold reached: topic={}, dlqAttempts={}, decision={}",
                    context.originalTopic(),
                    context.dlqAttempts(),
                    decision.type());
        }

        return decision;
    }

    /**
     * 子类实现核心决策逻辑。
     *
     * @param message DLQ 消息
     * @param context 失败上下文
     * @return 决策（返回 null 时视为 drop）
     */
    protected abstract DlqFailureDecision doDecide(Message<?> message, DlqFailureContext context);

    /** 判断是否应触发告警。 默认：dlqAttempts + 1 >= alertThreshold 时触发。 */
    protected boolean shouldAlert(DlqFailureContext context) {
        return config.getDlqAlertThreshold() > 0
                && context.dlqAttempts() + 1 >= config.getDlqAlertThreshold();
    }
}
