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
    private long backlogCriticalThreshold =
            StreamMQDiagnosticsDefaults.BACKLOG_CRITICAL_THRESHOLD;

    /** DLQ 主题标识关键字（小写匹配） */
    private String dlqTopicMarker = StreamMQDiagnosticsDefaults.DLQ_TOPIC_MARKER;

    /** DLQ 最大重试次数阈值，超过此值判定为死信 */
    private int dlqMaxRetryCount = StreamMQDiagnosticsDefaults.DLQ_MAX_RETRY_COUNT;

    /** 单次画像查询最大消息数，防止大范围查询导致 OOM */
    private int maxProfileQuerySize = StreamMQDiagnosticsDefaults.MAX_PROFILE_QUERY_SIZE;
}
