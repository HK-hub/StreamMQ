package io.github.streammq.diagnostics;

import io.github.streammq.core.StreamMQConstants;

/**
 * StreamMQ 诊断模块默认值常量。
 *
 * <p>作为 {@link StreamMQDiagnosticsProperties} 各配置项的默认值来源， 集中管理便于调整与复用。
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public final class StreamMQDiagnosticsDefaults {

    /** 配置属性前缀：streammq.diagnostics */
    public static final String PROP_PREFIX = "streammq.diagnostics";

    /** 开关属性名：enabled */
    public static final String PROP_NAME_ENABLED = "enabled";

    /** 开关属性值：true */
    public static final String PROP_VALUE_TRUE = "true";

    /** 近期诊断时间窗口（毫秒），5 分钟 */
    public static final long RECENT_WINDOW_MS = StreamMQConstants.DEFAULT_DIAGNOSTIC_WINDOW_MS;

    /** DLQ 诊断时间窗口（毫秒），1 小时 */
    public static final long DLQ_WINDOW_MS = 60L * 60 * 1000;

    /** 慢消费耗时阈值（毫秒） */
    public static final long SLOW_CONSUME_THRESHOLD_MS = 5_000L;

    /** 积压警告阈值 */
    public static final long BACKLOG_WARNING_THRESHOLD = 1_000L;

    /** 积压严重阈值 */
    public static final long BACKLOG_CRITICAL_THRESHOLD = 10_000L;

    /** DLQ 主题标识关键字（小写匹配） */
    public static final String DLQ_TOPIC_MARKER = "dlq";

    /** DLQ 最大重试次数阈值 */
    public static final int DLQ_MAX_RETRY_COUNT =
            StreamMQConstants.DEFAULT_DLQ_MAX_RETRY_ATTEMPTS;

    /** 单次画像查询最大消息数 */
    public static final int MAX_PROFILE_QUERY_SIZE = 1_000;

    /** P99 百分位基准 */
    public static final double P99_PERCENTILE = 0.99;

    /** Top-N 失败原因 / Topic 统计上限 */
    public static final int TOP_N_LIMIT = 10;

    /** topic:group 组合 key 分隔符 */
    public static final String KEY_SEPARATOR = ":";

    /**
     * 装配顺序依赖的自动配置类全限定名（starter 模块编译期不可见，故以 FQCN 声明）。
     *
     * <p>对应 {@code streammq-spring-boot-starter} 的追踪与 Listener 容器自动装配。
     */
    public static final String AUTO_CONFIGURE_AFTER_TRACE =
            "io.github.streammq.spring.boot.autoconfigure.StreamMQTraceAutoConfiguration";

    public static final String AUTO_CONFIGURE_AFTER_LISTENER_CONTAINER =
            "io.github.streammq.spring.boot.autoconfigure.StreamMQListenerContainerAutoConfiguration";

    /** XPENDING 单次拉取上限（与 starter 管理端点保持一致） */
    public static final int MAX_PENDING_QUERY_SIZE = 1_000;

    private StreamMQDiagnosticsDefaults() {}
}
