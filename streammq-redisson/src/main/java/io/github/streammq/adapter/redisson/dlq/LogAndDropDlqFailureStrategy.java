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
 * 策略一：始终丢弃（Log-And-Drop）。
 *
 * <p>DLQ 消费失败后始终返回 {@link DlqFailureDecision#drop()}。 在 {@link AbstractDlqFailureStrategy} 基类中已记录
 * ERROR 日志和告警判断， 本策略不再重复记录。
 *
 * <p>通过配置 {@link DlqConfig#getDlqAlertThreshold()} 可控制告警触发。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class LogAndDropDlqFailureStrategy extends AbstractDlqFailureStrategy {

    /** 策略名称常量 */
    public static final String STRATEGY_NAME = "log-and-drop";

    /** 无参构造（全局默认工厂使用） */
    public LogAndDropDlqFailureStrategy() {
        super(DlqConfig.builder().build());
    }

    /** 带配置构造 */
    public LogAndDropDlqFailureStrategy(DlqConfig config) {
        super(config);
    }

    @Override
    protected DlqFailureDecision doDecide(Message<?> message, DlqFailureContext context) {
        return DlqFailureDecision.drop();
    }

    @Override
    public String name() {
        return STRATEGY_NAME;
    }
}
