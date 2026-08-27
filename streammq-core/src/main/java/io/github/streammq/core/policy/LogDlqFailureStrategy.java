/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.policy;

import io.github.streammq.core.message.Message;
import java.util.Objects;

/**
 * DLQ 消费失败的核心级安全默认策略：记录告警日志后丢弃死信消息。
 *
 * <p>作为 {@link DlqConfig#failureStrategyClass} 的程序化默认值， 保证 {@code DlqConfig.builder().build()}
 * 在任何环境下都能实例化出可工作的策略（此前默认值为 {@link DlqFailureStrategy} 接口本身， 反射实例化必然失败）。
 *
 * <p>与 redisson 适配层的 {@code LogAndDropDlqFailureStrategy} 语义一致（WARN 日志 + drop 决策）， 但不依赖适配层，可安全地在纯
 * core 环境下使用。注解上下文（如 {@code @StreamMQDlqConsumer}）中该字段为接口类型时表示「使用全局 Bean」， 与本类的语义不同。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class LogDlqFailureStrategy extends AbstractDlqFailureStrategy {

    /** 构造无配置实例（使用内置默认 DlqConfig）。 */
    public LogDlqFailureStrategy() {
        this(null);
    }

    /**
     * 构造带配置的实例。
     *
     * @param config DLQ 配置（null 时使用内置默认值）
     */
    public LogDlqFailureStrategy(DlqConfig config) {
        super(config);
    }

    @Override
    protected DlqFailureDecision doDecide(Message<?> message, DlqFailureContext context) {
        log.warn(
                "DLQ message dropped by core default strategy: topic={}, messageId={}, decision={}",
                message.getTopic(),
                Objects.nonNull(message.getMessageId()) ? message.getMessageId() : "unknown",
                "DROP");
        return DlqFailureDecision.drop();
    }
}
