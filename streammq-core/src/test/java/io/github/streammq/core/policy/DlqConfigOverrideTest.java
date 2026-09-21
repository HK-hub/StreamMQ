/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.policy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.streammq.core.StreamMQConstants;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link DlqConfigOverride} 合并语义与构造期校验。
 *
 * <p>锁定「注解 &gt; 全局配置 &gt; 框架默认」这一优先级在 DLQ 数值参数上的真实实现点：未声明的字段（{@code null}）必须保持全局值，已声明的字段必须覆盖全局值。此前
 * {@code @StreamMQDlqConsumer} 的 7 个数值属性从未被读取， 用户按文档设置后静默无效。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("DlqConfigOverride 合并与校验")
class DlqConfigOverrideTest {

    @Test
    @DisplayName("全部未声明时既不覆盖也不改变基线")
    void emptyOverrideKeepsBaseline() {
        DlqConfigOverride override =
                new DlqConfigOverride(null, null, null, null, null, null, null);
        DlqConfig base = DlqConfig.builder().maxDlqRetryAttempts(7).build();

        assertThat(override.isEmpty()).isTrue();
        DlqConfig effective = override.applyTo(base);
        assertThat(effective).isSameAs(base);
        assertThat(effective.getMaxDlqRetryAttempts()).isEqualTo(7);
    }

    @Test
    @DisplayName("已声明字段覆盖全局值，未声明字段保持全局值")
    void declaredFieldsOverrideBaseOnlyDeclared() {
        DlqConfig base =
                DlqConfig.builder()
                        .maxDlqRetryAttempts(9)
                        .dlqRetryDelayMs(1_000L)
                        .secondaryDlqEnabled(true)
                        .secondaryDlqKeyPrefix("dlq-custom")
                        .dlqAlertThreshold(4)
                        .dlqRetryBackoffMultiplier(2.0d)
                        .dlqRetryMaxDelayMs(60_000L)
                        .build();
        DlqConfigOverride override =
                new DlqConfigOverride(3, null, Boolean.FALSE, null, 1, null, null);

        DlqConfig effective = override.applyTo(base);

        // 已声明 → 覆盖
        assertThat(effective.getMaxDlqRetryAttempts()).isEqualTo(3);
        assertThat(effective.isSecondaryDlqEnabled()).isFalse();
        assertThat(effective.getDlqAlertThreshold()).isEqualTo(1);
        // 未声明 → 跟随全局
        assertThat(effective.getDlqRetryDelayMs()).isEqualTo(1_000L);
        assertThat(effective.getSecondaryDlqKeyPrefix()).isEqualTo("dlq-custom");
        assertThat(effective.getDlqRetryBackoffMultiplier()).isEqualTo(2.0d);
        assertThat(effective.getDlqRetryMaxDelayMs()).isEqualTo(60_000L);
        // 基线对象本身不得被修改
        assertThat(base.getMaxDlqRetryAttempts()).isEqualTo(9);
    }

    @Test
    @DisplayName("空串前缀视为未声明（跟随全局），不会把 Key 段清空")
    void blankPrefixFallsBackToBase() {
        DlqConfig base = DlqConfig.builder().secondaryDlqKeyPrefix("dlq2").build();
        DlqConfig effective =
                new DlqConfigOverride(null, null, null, "", null, null, null).applyTo(base);
        assertThat(effective.getSecondaryDlqKeyPrefix()).isEqualTo("dlq2");
    }

    @Test
    @DisplayName("基线为 null 时以框架默认值为基线")
    void nullBaseUsesFrameworkDefaults() {
        DlqConfig effective =
                new DlqConfigOverride(5, null, null, null, null, null, null).applyTo(null);
        assertThat(effective.getMaxDlqRetryAttempts()).isEqualTo(5);
        assertThat(effective.getDlqRetryDelayMs())
                .isEqualTo(StreamMQConstants.DEFAULT_DLQ_RETRY_DELAY_MS);
        assertThat(effective.isSecondaryDlqEnabled())
                .isEqualTo(StreamMQConstants.DEFAULT_SECONDARY_DLQ_ENABLED);
    }

    @Test
    @DisplayName("构造期拒绝非法值（与全局配置校验同口径）")
    void rejectsInvalidValues() {
        assertThatThrownBy(() -> new DlqConfigOverride(-1, null, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxDlqRetryAttempts");
        assertThatThrownBy(() -> new DlqConfigOverride(null, -1L, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dlqRetryDelayMs");
        assertThatThrownBy(() -> new DlqConfigOverride(null, null, null, null, 0, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dlqAlertThreshold");
        assertThatThrownBy(() -> new DlqConfigOverride(null, null, null, null, null, 0.5d, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dlqRetryBackoffMultiplier");
        assertThatThrownBy(() -> new DlqConfigOverride(null, null, null, null, null, null, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dlqRetryMaxDelayMs");
    }
}
