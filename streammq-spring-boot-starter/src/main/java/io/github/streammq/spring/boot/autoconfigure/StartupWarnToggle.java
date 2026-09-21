/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

/**
 * {@code streammq.admin.startup-warn} 的容错解析。
 *
 * <p><b>为什么不能直接用 {@code getProperty(key, Boolean.class)}：</b>该键是<b>直读精确键</b>（不走宽松绑定、也不在
 * {@code @ConfigurationProperties} 里），而 {@code Environment#getProperty(String, Class)} 对无法转换的值 （如
 * YAML 中很自然的 {@code off} / {@code no} / {@code disable}）会抛 {@link IllegalArgumentException}。 调用点在
 * {@code ApplicationReadyEvent} 监听器里，异常会穿透 {@code SpringApplication.run}
 * ——一个纯日志开关能阻断应用启动，且错误文案与"关闭提醒"毫无关联。
 *
 * <p>解析规则：{@code true/on/yes/1/enable/enabled} → 启用提醒；{@code false/off/no/0/disable/disabled} →
 * 关闭提醒；空白或无法识别的值 → <b>按启用处理</b>并限频 WARN（绝不因日志开关让应用启动失败）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
final class StartupWarnToggle {

    /** 关闭管理端点启动提醒的配置键。 */
    static final String KEY = "streammq.admin.startup-warn";

    private static final Logger LOG = LoggerFactory.getLogger(StartupWarnToggle.class);

    private static final Set<String> TRUTHY = Set.of("true", "on", "yes", "1", "enable", "enabled");
    private static final Set<String> FALSY =
            Set.of("false", "off", "no", "0", "disable", "disabled");

    /** 非法取值只告警一次，避免每个监听器各刷一条。 */
    private static final AtomicBoolean INVALID_VALUE_WARNED = new AtomicBoolean(false);

    private StartupWarnToggle() {
        // 工具类
    }

    /**
     * 判断是否应关闭启动提醒。
     *
     * @param environment Spring 环境，可为 null（视为未配置）
     * @return true 表示关闭提醒
     */
    static boolean isDisabled(Environment environment) {
        if (environment == null) {
            return false;
        }
        String raw = environment.getProperty(KEY);
        if (raw == null || raw.isBlank()) {
            return false;
        }
        String normalized = raw.trim().toLowerCase(Locale.ROOT);
        if (FALSY.contains(normalized)) {
            return true;
        }
        if (TRUTHY.contains(normalized)) {
            return false;
        }
        if (INVALID_VALUE_WARNED.compareAndSet(false, true)) {
            LOG.warn(
                    "{}='{}' is not a recognised boolean; treating the startup warning as ENABLED."
                            + " Use true/false (or on/off, yes/no, 1/0).",
                    KEY,
                    raw);
        }
        return false;
    }
}
