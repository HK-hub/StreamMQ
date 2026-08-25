/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics;

/**
 * 诊断报告的 locale-neutral 稳定标识码。客户端（仪表盘/告警系统）应基于 code 做逻辑分支， 而非解析人类可读的 message 文本；message
 * 文本仅用于展示，措辞可能随版本调整。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class DiagnosticsCodes {

    private DiagnosticsCodes() {}

    /** 慢消费：无追踪数据 */
    public static final String NO_TRACE_DATA = "NO_TRACE_DATA";

    /** 慢消费：平均消费耗时超过阈值 */
    public static final String SLOW_CONSUME = "SLOW_CONSUME";

    /** 慢消费：消费速率低于生产速率 */
    public static final String CONSUME_RATE_BEHIND = "CONSUME_RATE_BEHIND";

    /** 慢消费：性能正常 */
    public static final String HEALTHY = "HEALTHY";

    /** 积压：正常范围 */
    public static final String BACKLOG_NORMAL = "BACKLOG_NORMAL";

    /** 积压：警告级别 */
    public static final String BACKLOG_WARNING = "BACKLOG_WARNING";

    /** 积压：严重级别 */
    public static final String BACKLOG_CRITICAL = "BACKLOG_CRITICAL";

    /** 死信：正常范围 */
    public static final String DLQ_NORMAL = "DLQ_NORMAL";

    /** 死信：警告级别 */
    public static final String DLQ_WARNING = "DLQ_WARNING";

    /** 死信：严重级别 */
    public static final String DLQ_CRITICAL = "DLQ_CRITICAL";

    /** 慢消费报告码（别名） */
    public interface SlowConsumeCodes {
        String NO_TRACE_DATA = DiagnosticsCodes.NO_TRACE_DATA;
        String SLOW_CONSUME = DiagnosticsCodes.SLOW_CONSUME;
        String CONSUME_RATE_BEHIND = DiagnosticsCodes.CONSUME_RATE_BEHIND;
        String HEALTHY = DiagnosticsCodes.HEALTHY;
    }

    /** 积压报告码（别名） */
    public interface BacklogCodes {
        String NO_TRACE_DATA = DiagnosticsCodes.NO_TRACE_DATA;
        String NORMAL = BACKLOG_NORMAL;
        String WARNING = BACKLOG_WARNING;
        String CRITICAL = BACKLOG_CRITICAL;
    }

    /** 死信报告码（别名） */
    public interface DlqCodes {
        String NO_TRACE_DATA = DiagnosticsCodes.NO_TRACE_DATA;
        String NORMAL = DLQ_NORMAL;
        String WARNING = DLQ_WARNING;
        String CRITICAL = DLQ_CRITICAL;
    }
}
