/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.dlq;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureContext;
import io.github.streammq.core.policy.DlqFailureDecision;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * DLQ 策略"生效配置来自决策上下文"契约测试（第六轮红队 R3-5）。
 *
 * <p>缺陷背景：策略实例由 starter 以 {@code BeanUtils.instantiateClass(clazz)} 无参构造，实例配置恒为 builder 默认；{@code
 * streammq.dlq.max-dlq-retry-attempts} 等全局/注解配置在主代码零读取， 行为与配置、日志三方矛盾。修复后 handler 把"按消费者合并全局后的"
 * DlqConfig 随 {@link DefaultDlqFailureContext} 交给策略，策略优先以它决策。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("DLQ 策略按决策上下文中的生效配置决策")
class DlqFailureStrategyEffectiveConfigTest {

    private static final Message<String> MESSAGE =
            MessageBuilder.<String>withTopic("dlq-topic").body("payload").build();

    private static DefaultDlqFailureContext contextWith(
            int dlqAttempts, DlqConfig effectiveConfig) {
        return new DefaultDlqFailureContext(
                dlqAttempts,
                "maxRetry",
                "dlq-topic",
                "1700000000000-0",
                new RuntimeException("boom"),
                Map.of("body", "cGF5bG9hZA=="),
                effectiveConfig.getMaxDlqRetryAttempts(),
                effectiveConfig.getDlqRetryDelayMs(),
                "dlq-group",
                effectiveConfig);
    }

    @Test
    @DisplayName("全局 max-dlq-retry-attempts=5：策略按 5 决策（第 3 次仍重试，第 5 次才转二级）")
    void secondaryDlq_usesEffectiveMaxFromContext() {
        // 无参构造 = 实例配置为默认 3；若读取实例配置，attempts=3 就会转二级 DLQ（错误）
        SecondaryDlqFailureStrategy strategy = new SecondaryDlqFailureStrategy();
        DlqConfig effective =
                DlqConfig.builder()
                        .maxDlqRetryAttempts(5)
                        .dlqRetryDelayMs(2_000L)
                        .dlqRetryBackoffMultiplier(1.0)
                        .build();

        DlqFailureDecision atThree = strategy.decide(MESSAGE, contextWith(3, effective));
        assertThat(atThree.isRetry()).as("attempts=3 < 生效配置 5，必须继续重试").isTrue();
        assertThat(atThree.retryDelay()).isEqualTo(java.time.Duration.ofSeconds(2));

        DlqFailureDecision atFive = strategy.decide(MESSAGE, contextWith(5, effective));
        assertThat(atFive.isSecondaryDlq()).as("attempts=5 >= 生效配置 5，转二级 DLQ").isTrue();
    }

    @Test
    @DisplayName("生效配置的延迟/退避参数对策略可见（LimitedRetry）")
    void limitedRetry_usesEffectiveDelayFromContext() {
        LimitedRetryDlqFailureStrategy strategy = new LimitedRetryDlqFailureStrategy();
        DlqConfig effective =
                DlqConfig.builder()
                        .maxDlqRetryAttempts(5)
                        .dlqRetryDelayMs(1_234L)
                        .dlqRetryBackoffMultiplier(1.0)
                        .build();

        DlqFailureDecision decision = strategy.decide(MESSAGE, contextWith(0, effective));

        assertThat(decision.isRetry()).isTrue();
        assertThat(decision.retryDelay())
                .as("延迟必须取自生效配置（1.234s），而非实例默认 10s")
                .isEqualTo(java.time.Duration.ofMillis(1_234L));
        assertThat(contextWith(0, effective).maxDlqRetryAttempts())
                .as("上下文访问器同样以生效配置为真源")
                .isEqualTo(5);
    }

    @Test
    @DisplayName("上下文未携带生效配置时回退到实例配置（兼容直接 new 策略的既有用法）")
    void withoutEffectiveConfig_fallsBackToInstanceConfig() {
        DlqConfig instanceConfig =
                DlqConfig.builder().maxDlqRetryAttempts(1).dlqRetryDelayMs(200L).build();
        SecondaryDlqFailureStrategy strategy = new SecondaryDlqFailureStrategy(instanceConfig);

        DlqFailureContext legacyContext =
                new DefaultDlqFailureContext(
                        1,
                        "maxRetry",
                        "dlq-topic",
                        "1700000000000-0",
                        null,
                        Map.of(),
                        1,
                        200L,
                        "dlq-group");

        assertThat(DefaultDlqFailureContext.resolveEffectiveConfig(legacyContext, instanceConfig))
                .isSameAs(instanceConfig);
        assertThat(strategy.decide(MESSAGE, legacyContext).isSecondaryDlq())
                .as("attempts=1 >= 实例配置 1，转二级 DLQ")
                .isTrue();
    }
}
