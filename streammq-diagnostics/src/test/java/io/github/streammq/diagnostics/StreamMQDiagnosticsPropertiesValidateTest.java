/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link StreamMQDiagnosticsProperties#validate()} 启动期校验回归守卫。
 *
 * <p>锁定历史缺陷：诊断模块的全部阈值此前<b>零校验</b>，非法值以难以定位的形式在运行期暴露 —— {@code backlog-warning-threshold >
 * backlog-critical-threshold} 会让 {@code /streammq/diagnostics/health} <b>常态返回 DOWN</b>；{@code
 * recent-window-ms = 0} 会让 {@code produceRate/consumeRate} 变成 {@code Infinity} 并随响应体返回。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("StreamMQDiagnosticsProperties#validate")
class StreamMQDiagnosticsPropertiesValidateTest {

    @Test
    @DisplayName("默认配置必须合法")
    void defaultsAreValid() {
        assertThatCode(() -> new StreamMQDiagnosticsProperties().validate())
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("窗口为 0 或负值一律拒绝（0 会产生 Infinity 速率）")
    void rejectsNonPositiveWindows() {
        StreamMQDiagnosticsProperties properties = new StreamMQDiagnosticsProperties();
        properties.setRecentWindowMs(0);
        assertThatThrownBy(properties::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recent-window-ms");

        properties = new StreamMQDiagnosticsProperties();
        properties.setDlqWindowMs(-1);
        assertThatThrownBy(properties::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dlq-window-ms");
    }

    @Test
    @DisplayName("积压阈值倒挂必须拒绝（否则中等积压被判 CRITICAL，健康面常态 DOWN）")
    void rejectsInvertedBacklogThresholds() {
        StreamMQDiagnosticsProperties properties = new StreamMQDiagnosticsProperties();
        properties.setBacklogWarningThreshold(10_000);
        properties.setBacklogCriticalThreshold(1_000);

        assertThatThrownBy(properties::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("backlog-critical-threshold");
    }

    @Test
    @DisplayName("相等阈值同样拒绝（warning == critical 时 WARNING 级别不可达）")
    void rejectsEqualBacklogThresholds() {
        StreamMQDiagnosticsProperties properties = new StreamMQDiagnosticsProperties();
        properties.setBacklogWarningThreshold(500);
        properties.setBacklogCriticalThreshold(500);

        assertThatThrownBy(properties::validate).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("dlq-max-retry-count < 1 必须拒绝（否则任何一次失败都被判死信）")
    void rejectsZeroDlqMaxRetryCount() {
        StreamMQDiagnosticsProperties properties = new StreamMQDiagnosticsProperties();
        properties.setDlqMaxRetryCount(0);

        assertThatThrownBy(properties::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dlq-max-retry-count");
    }

    @Test
    @DisplayName("max-profile-query-size 必须为正（0 会让画像查询恒为空）")
    void rejectsNonPositiveProfileQuerySize() {
        StreamMQDiagnosticsProperties properties = new StreamMQDiagnosticsProperties();
        properties.setMaxProfileQuerySize(0);

        assertThatThrownBy(properties::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-profile-query-size");
    }

    @Test
    @DisplayName("namespace 含非法字符必须拒绝（会直接拼进积压探针的 Redis Key）")
    void rejectsInvalidNamespace() {
        StreamMQDiagnosticsProperties properties = new StreamMQDiagnosticsProperties();
        properties.setNamespace("bad:ns");

        assertThatThrownBy(properties::validate).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("合法自定义值可以通过")
    void acceptsValidCustomValues() {
        StreamMQDiagnosticsProperties properties = new StreamMQDiagnosticsProperties();
        properties.setRecentWindowMs(60_000);
        properties.setDlqWindowMs(120_000);
        properties.setSlowConsumeThresholdMs(0);
        properties.setBacklogWarningThreshold(100);
        properties.setBacklogCriticalThreshold(1_000);
        properties.setDlqMaxRetryCount(5);
        properties.setMaxProfileQuerySize(200);
        properties.setNamespace("diagnostics-it");

        assertThatCode(properties::validate).doesNotThrowAnyException();
        assertThat(properties.getBacklogCriticalThreshold()).isEqualTo(1_000);
    }
}
