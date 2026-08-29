/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics;

import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.diagnostics.DiagnosticsCodes.BacklogCodes;
import io.github.streammq.diagnostics.model.BacklogReport;
import io.github.streammq.diagnostics.model.Severity;
import io.github.streammq.diagnostics.spi.BacklogProbe;
import io.github.streammq.diagnostics.support.TraceRecordFilters;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

/**
 * 积压诊断器，分析指定主题+消费者组的积压状况与清积压预估。
 *
 * <p>积压量优先来自 {@link BacklogProbe}（真实 Redis XPENDING 未确认消息数）； 探针不可用或返回 null 时，回退到 追踪窗口内「生产数 -
 * 消费数」的估算（取非负）。
 *
 * <p>分析维度：
 *
 * <ul>
 *   <li>当前积压量
 *   <li>积压增长率（正数增加，负数减少）
 *   <li>预计清空时间
 *   <li>严重级别（INFO / WARNING / CRITICAL）
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class BacklogAnalyzer {

    private static final Logger log = LoggerFactory.getLogger(BacklogAnalyzer.class);

    private final StreamMQTraceService traceService;
    private final StreamMQDiagnosticsProperties properties;

    /** 积压探针（可空）：存在时基于真实 Redis XLEN/XPENDING 计算积压，否则回退到追踪窗口估算 */
    private final Optional<BacklogProbe> backlogProbe;

    /**
     * Creates an analyzer with an optional Redis-backed backlog probe. The probe is optional so
     * diagnostics can be enabled in applications that do not expose a Redisson client.
     *
     * @param traceService tracing service
     * @param properties diagnostics properties
     * @param backlogProbe optional backlog probe
     */
    @Autowired
    public BacklogAnalyzer(
            StreamMQTraceService traceService,
            StreamMQDiagnosticsProperties properties,
            @Nullable BacklogProbe backlogProbe) {
        this.traceService = traceService;
        this.properties = properties;
        this.backlogProbe = Optional.ofNullable(backlogProbe);
    }

    /**
     * 诊断消息积压，分析指定主题+消费者组的积压状况。
     *
     * @param topic 主题
     * @param group 消费者组
     * @return 积压诊断报告
     */
    public BacklogReport diagnose(String topic, String group) {
        long now = System.currentTimeMillis();
        long start = now - properties.getRecentWindowMs();

        List<TraceRecord> topicRecords = traceService.queryByTopic(topic, start, now);
        List<TraceRecord> consumeRecords =
                TraceRecordFilters.filterConsumeByGroup(topicRecords, group);
        List<TraceRecord> sendRecords = TraceRecordFilters.filterSend(topicRecords);

        long produceCount = sendRecords.size();
        long consumeCount = consumeRecords.size();

        // 积压量：优先真实 Redis 数据（XPENDING 未确认数），否则追踪窗口差值估算
        Long realBacklog = probeBacklog(topic, group);
        long currentBacklog =
                realBacklog != null ? realBacklog : Math.max(0, produceCount - consumeCount);
        if (CollectionUtils.isEmpty(topicRecords) && realBacklog == null) {
            return buildEmptyReport(topic, group);
        }

        double windowSeconds = properties.getRecentWindowMs() / 1000.0;
        double produceRate = produceCount / windowSeconds;
        double consumeRate = consumeCount / windowSeconds;
        double growthRate = (produceCount - consumeCount) / windowSeconds;

        long estimatedClearTimeMinutes =
                calculateEstimatedClearTime(currentBacklog, consumeRate, produceRate);
        Severity severity = determineSeverity(currentBacklog);
        String recommendation = buildRecommendation(severity, consumeRate, produceRate);

        return new BacklogReport(
                topic,
                group,
                currentBacklog,
                growthRate,
                estimatedClearTimeMinutes,
                produceRate,
                consumeRate,
                recommendation,
                severity,
                mapSeverityToCode(severity));
    }

    /**
     * 通过 {@link BacklogProbe} 获取真实积压（XPENDING 未确认消息数）。
     *
     * @param topic 主题
     * @param group 消费者组
     * @return 积压消息数；探针不可用或探测失败时为 null
     */
    private Long probeBacklog(String topic, String group) {
        if (backlogProbe.isEmpty()) {
            return null;
        }
        try {
            BacklogProbe.Result result = backlogProbe.get().probe(topic, group);
            return result != null ? result.pendingCount() : null;
        } catch (RuntimeException ex) {
            log.warn(
                    "Backlog probe failed for topic={}, group={}: {}",
                    topic,
                    group,
                    ex.getMessage());
            return null;
        }
    }

    /** 将 {@link Severity} 映射到 {@link BacklogCodes} 中的稳定码。 */
    private String mapSeverityToCode(Severity severity) {
        if (severity == Severity.CRITICAL) {
            return DiagnosticsCodes.BACKLOG_CRITICAL;
        }
        if (severity == Severity.WARNING) {
            return DiagnosticsCodes.BACKLOG_WARNING;
        }
        return DiagnosticsCodes.BACKLOG_NORMAL;
    }

    /** 构建空的积压报告（追踪数据不存在时使用）。 */
    private BacklogReport buildEmptyReport(String topic, String group) {
        return new BacklogReport(
                topic,
                group,
                0L,
                0.0,
                -1L,
                0.0,
                0.0,
                "No trace data; verify tracing is enabled",
                Severity.INFO,
                BacklogCodes.NO_TRACE_DATA);
    }

    /**
     * 计算预计清空积压时间（分钟）。
     *
     * @param currentBacklog 当前积压量
     * @param consumeRate 消费速率
     * @param produceRate 生产速率
     * @return 预计清空时间（分钟），-1 表示无法估算
     */
    private long calculateEstimatedClearTime(
            long currentBacklog, double consumeRate, double produceRate) {
        if (currentBacklog <= 0) {
            return 0L;
        }
        double netConsumeRate = consumeRate - produceRate;
        if (netConsumeRate <= 0) {
            return -1L;
        }
        return (long) (currentBacklog / netConsumeRate / 60.0);
    }

    /** 根据积压量判定严重级别，阈值由 {@link StreamMQDiagnosticsProperties} 配置。 */
    private Severity determineSeverity(long currentBacklog) {
        if (currentBacklog < properties.getBacklogWarningThreshold()) {
            return Severity.INFO;
        }
        if (currentBacklog < properties.getBacklogCriticalThreshold()) {
            return Severity.WARNING;
        }
        return Severity.CRITICAL;
    }

    /** 构建积压优化建议。 */
    private String buildRecommendation(Severity severity, double consumeRate, double produceRate) {
        switch (severity) {
            case INFO:
                return "Backlog within normal range; no action needed";
            case WARNING:
                if (consumeRate < produceRate) {
                    return "Backlog is high and growing; add consumer instances";
                }
                return "Backlog is elevated but draining; keep monitoring";
            case CRITICAL:
                if (consumeRate < produceRate) {
                    return "CRITICAL: backlog severe and growing; scale out consumers immediately"
                            + " and inspect consume logic";
                }
                return "Backlog severe but draining; keep monitoring drain progress";
            default:
                return "Keep monitoring backlog status";
        }
    }
}
