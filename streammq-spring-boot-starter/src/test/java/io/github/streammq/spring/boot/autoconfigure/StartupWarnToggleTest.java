/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

/**
 * {@link StartupWarnToggle} 容错解析回归守卫。
 *
 * <p>锁定历史缺陷：两处 {@code ApplicationReadyEvent} 监听器此前用 {@code
 * environment.getProperty("streammq.admin.startup-warn", Boolean.class)} 读取该开关， 对无法转换的值（YAML 中很自然的
 * {@code off} / {@code no} / {@code disable}）会抛 {@link IllegalArgumentException}，并穿透 {@code
 * SpringApplication.run} —— 一个纯日志开关能把应用启动搞挂。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("StartupWarnToggle")
class StartupWarnToggleTest {

    private static MockEnvironment env(String value) {
        MockEnvironment environment = new MockEnvironment();
        if (value != null) {
            environment.setProperty(StartupWarnToggle.KEY, value);
        }
        return environment;
    }

    @Test
    @DisplayName("未配置时提醒保持启用")
    void unsetKeepsWarningEnabled() {
        assertThat(StartupWarnToggle.isDisabled(env(null))).isFalse();
        assertThat(StartupWarnToggle.isDisabled(env(""))).isFalse();
        assertThat(StartupWarnToggle.isDisabled(null)).isFalse();
    }

    @Test
    @DisplayName("常见假值写法均可关闭提醒（含标准转换器不认识的 disable/disabled）")
    void recognisesFalsySpellings() {
        for (String value :
                new String[] {"false", "FALSE", "off", "no", "0", "disable", "disabled"}) {
            assertThat(StartupWarnToggle.isDisabled(env(value))).as("value=%s", value).isTrue();
        }
    }

    @Test
    @DisplayName("常见真值写法保持提醒启用")
    void recognisesTruthySpellings() {
        for (String value : new String[] {"true", "TRUE", "on", "yes", "1", "enable", "enabled"}) {
            assertThat(StartupWarnToggle.isDisabled(env(value))).as("value=%s", value).isFalse();
        }
    }

    @Test
    @DisplayName("无法识别的取值不得抛异常（否则会阻断应用启动），按启用处理")
    void unrecognisedValueFallsBackToEnabledWithoutThrowing() {
        assertThat(StartupWarnToggle.isDisabled(env("maybe"))).isFalse();
        assertThat(StartupWarnToggle.isDisabled(env("  flase  "))).isFalse();
    }
}
