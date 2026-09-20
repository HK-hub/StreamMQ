/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.dlq;

import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureContext;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * {@link DlqFailureContext} 默认实现。
 *
 * <p><b>生效配置携带（0.1.2，R3-5 配置真源收口）：</b>上下文可选携带"按消费者合并全局后的" {@link DlqConfig} （{@link
 * #effectiveDlqConfig()}）。策略实例可能由反射无参构造（配置恒为 builder 默认），因此策略必须优先读取本字段， 缺失时才回退到自身实例配置——否则全局/注解配置（如
 * {@code max-dlq-retry-attempts}）对策略完全不可见。 读取请使用 {@link #resolveEffectiveConfig(DlqFailureContext,
 * DlqConfig)}，不要在策略中直接做 {@code instanceof} 判定。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultDlqFailureContext implements DlqFailureContext {

    private final int dlqAttempts;
    private final String dlqReason;
    private final String originalTopic;
    private final String originalMessageId;
    private final Throwable lastFailureCause;
    private final Map<String, String> dlqFields;
    private final int maxDlqRetryAttempts;
    private final long dlqRetryDelayMs;
    private final String consumerGroup;

    /**
     * 决策时生效的 DlqConfig（按消费者合并全局后的值）。
     *
     * <p>为 {@code null} 表示调用方未携带配置（旧调用方/测试桩），此时策略回退到实例配置。
     */
    private final DlqConfig effectiveDlqConfig;

    public DefaultDlqFailureContext(
            int dlqAttempts,
            String dlqReason,
            String originalTopic,
            String originalMessageId,
            Throwable lastFailureCause,
            Map<String, String> dlqFields,
            int maxDlqRetryAttempts,
            long dlqRetryDelayMs,
            String consumerGroup) {
        this(
                dlqAttempts,
                dlqReason,
                originalTopic,
                originalMessageId,
                lastFailureCause,
                dlqFields,
                maxDlqRetryAttempts,
                dlqRetryDelayMs,
                consumerGroup,
                null);
    }

    /**
     * 全参构造（携带生效 DLQ 配置）。
     *
     * @param effectiveDlqConfig 按消费者合并全局后的生效配置，可为 null（策略回退到实例配置）
     */
    public DefaultDlqFailureContext(
            int dlqAttempts,
            String dlqReason,
            String originalTopic,
            String originalMessageId,
            Throwable lastFailureCause,
            Map<String, String> dlqFields,
            int maxDlqRetryAttempts,
            long dlqRetryDelayMs,
            String consumerGroup,
            DlqConfig effectiveDlqConfig) {
        this.dlqAttempts = dlqAttempts;
        this.dlqReason = dlqReason;
        this.originalTopic = originalTopic;
        this.originalMessageId = originalMessageId;
        this.lastFailureCause = lastFailureCause;
        this.dlqFields = dlqFields;
        this.maxDlqRetryAttempts = maxDlqRetryAttempts;
        this.dlqRetryDelayMs = dlqRetryDelayMs;
        this.consumerGroup = consumerGroup;
        this.effectiveDlqConfig = effectiveDlqConfig;
    }

    /**
     * 返回决策时生效的 DLQ 配置（按消费者合并全局后的值），可能为 null。
     *
     * @return 生效配置，null 表示未携带（策略应回退到自身实例配置）
     */
    public DlqConfig effectiveDlqConfig() {
        return effectiveDlqConfig;
    }

    /**
     * 从决策上下文解析生效 DLQ 配置（策略侧唯一读取入口）。
     *
     * <p>优先级：上下文携带的合并配置 > 策略实例配置 > 内置默认配置。策略实例由反射无参构造时其配置恒为 builder 默认，因此上下文携带的配置是全局/注解配置真正生效的唯一通道。
     *
     * @param context 决策上下文（可为 null 的第三方实现）
     * @param fallbackConfig 策略实例配置（可为 null）
     * @return 生效配置，永不为 null
     */
    public static DlqConfig resolveEffectiveConfig(
            DlqFailureContext context, DlqConfig fallbackConfig) {
        if (context instanceof DefaultDlqFailureContext defaultContext) {
            DlqConfig carried = defaultContext.effectiveDlqConfig();
            if (Objects.nonNull(carried)) {
                return carried;
            }
        }
        return Objects.nonNull(fallbackConfig) ? fallbackConfig : DlqConfig.builder().build();
    }

    @Override
    public int dlqAttempts() {
        return dlqAttempts;
    }

    @Override
    public int maxDlqRetryAttempts() {
        // 携带生效配置时以其为唯一真源（与策略读取的配置保持一致）
        return Objects.nonNull(effectiveDlqConfig)
                ? effectiveDlqConfig.getMaxDlqRetryAttempts()
                : maxDlqRetryAttempts;
    }

    @Override
    public String dlqReason() {
        return dlqReason;
    }

    @Override
    public String originalTopic() {
        return originalTopic;
    }

    @Override
    public String originalMessageId() {
        return originalMessageId;
    }

    @Override
    public Throwable lastFailureCause() {
        return lastFailureCause;
    }

    @Override
    public long dlqRetryDelayMs() {
        // 同 maxDlqRetryAttempts：携带生效配置时以生效配置为准
        return Objects.nonNull(effectiveDlqConfig)
                ? effectiveDlqConfig.getDlqRetryDelayMs()
                : dlqRetryDelayMs;
    }

    @Override
    public String consumerGroup() {
        return consumerGroup;
    }

    @Override
    public Map<String, String> dlqFields() {
        return Objects.nonNull(dlqFields)
                ? Collections.unmodifiableMap(dlqFields)
                : Collections.emptyMap();
    }
}
