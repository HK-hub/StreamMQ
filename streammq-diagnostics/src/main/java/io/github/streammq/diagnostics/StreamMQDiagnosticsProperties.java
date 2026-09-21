/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * StreamMQ 诊断模块配置属性。
 *
 * <p>通过 {@code streammq.diagnostics.*} 前缀配置诊断模块行为。
 *
 * <p>典型配置示例：
 *
 * <pre>{@code
 * streammq:
 *   diagnostics:
 *     enabled: true
 *     slow-consume-threshold-ms: 3000
 *     backlog-warning-threshold: 500
 *     backlog-critical-threshold: 5000
 *     max-profile-query-size: 500
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
@Setter
@ConfigurationProperties(prefix = StreamMQDiagnosticsDefaults.PROP_PREFIX)
public class StreamMQDiagnosticsProperties {

    /** 是否启用诊断模块，默认关闭 */
    private boolean enabled = false;

    /**
     * 命名空间（用于真实积压探测的 Redis Key 前缀，需与 {@code streammq.namespace} 保持一致）。
     *
     * <p>仅影响基于 Redisson 的积压探针（XLEN/XPENDING）；追踪窗口估算不依赖该值。 默认空字符串。
     */
    private String namespace = "";

    /** 近期诊断时间窗口（毫秒） */
    private long recentWindowMs = StreamMQDiagnosticsDefaults.RECENT_WINDOW_MS;

    /** DLQ 诊断时间窗口（毫秒） */
    private long dlqWindowMs = StreamMQDiagnosticsDefaults.DLQ_WINDOW_MS;

    /** 慢消费耗时阈值（毫秒），超过此值判定为慢消费 */
    private long slowConsumeThresholdMs = StreamMQDiagnosticsDefaults.SLOW_CONSUME_THRESHOLD_MS;

    /** 积压警告阈值，超过此值触发 WARNING 级别 */
    private long backlogWarningThreshold = StreamMQDiagnosticsDefaults.BACKLOG_WARNING_THRESHOLD;

    /** 积压严重阈值，超过此值触发 CRITICAL 级别 */
    private long backlogCriticalThreshold = StreamMQDiagnosticsDefaults.BACKLOG_CRITICAL_THRESHOLD;

    /** DLQ 主题标识关键字（小写匹配） */
    private String dlqTopicMarker = StreamMQDiagnosticsDefaults.DLQ_TOPIC_MARKER;

    /** DLQ 最大重试次数阈值，超过此值判定为死信 */
    private int dlqMaxRetryCount = StreamMQDiagnosticsDefaults.DLQ_MAX_RETRY_COUNT;

    /** 单次画像查询最大消息数，防止大范围查询导致 OOM */
    private int maxProfileQuerySize = StreamMQDiagnosticsDefaults.MAX_PROFILE_QUERY_SIZE;

    /**
     * 配置校验（启动期 fail-fast）。
     *
     * <p>为什么必须有：诊断模块此前对全部阈值零校验，非法值在运行期以难以定位的形式暴露——
     *
     * <ul>
     *   <li>{@code backlog-warning-threshold > backlog-critical-threshold}：中等积压被判 CRITICAL， {@code
     *       /streammq/diagnostics/health} <b>常态返回 DOWN</b>（看板长期误报）
     *   <li>{@code recent-window-ms = 0}：窗口秒数为 0 使 {@code produceRate/consumeRate/growthRate} 变成
     *       {@code Infinity} 并随响应体返回（下游仪表盘解析异常）；负值使窗口倒挂 → 查询恒为空
     *   <li>{@code dlq-max-retry-count <= 0}：任何一次失败消费都被判为死信，{@code totalDlqCount} 虚高
     * </ul>
     *
     * @throws IllegalArgumentException 任一取值非法
     */
    public void validate() {
        requirePositive("streammq.diagnostics.recent-window-ms", recentWindowMs);
        requirePositive("streammq.diagnostics.dlq-window-ms", dlqWindowMs);
        requirePositive("streammq.diagnostics.max-profile-query-size", maxProfileQuerySize);
        if (slowConsumeThresholdMs < 0) {
            throw new IllegalArgumentException(
                    "streammq.diagnostics.slow-consume-threshold-ms must be >= 0 (0 = every message"
                            + " counts as slow), got: "
                            + slowConsumeThresholdMs);
        }
        if (backlogWarningThreshold < 0) {
            throw new IllegalArgumentException(
                    "streammq.diagnostics.backlog-warning-threshold must be >= 0, got: "
                            + backlogWarningThreshold);
        }
        if (backlogCriticalThreshold <= backlogWarningThreshold) {
            throw new IllegalArgumentException(
                    "streammq.diagnostics.backlog-critical-threshold ("
                            + backlogCriticalThreshold
                            + ") must be > backlog-warning-threshold ("
                            + backlogWarningThreshold
                            + "), otherwise moderate backlog is reported as CRITICAL and the"
                            + " diagnostics health endpoint stays DOWN.");
        }
        if (dlqMaxRetryCount < 1) {
            throw new IllegalArgumentException(
                    "streammq.diagnostics.dlq-max-retry-count must be >= 1, got: "
                            + dlqMaxRetryCount);
        }
        // namespace 会直接拼进积压探针的 Redis Key 前缀（streammq:{ns}:msg:{topic}），与全局命名校验同口径
        io.github.streammq.core.util.StringUtils.requireValidNamespace(namespace);
    }

    private static void requirePositive(String key, long value) {
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be > 0, got: " + value);
        }
    }
}
