/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.trace.TraceType;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import io.github.streammq.diagnostics.DiagnosticsCodes.DlqCodes;
import io.github.streammq.diagnostics.model.DlqReport;
import io.github.streammq.diagnostics.model.FailureReason;
import io.github.streammq.diagnostics.model.TopicFailureCount;
import io.github.streammq.diagnostics.support.TraceRecordFilters;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * 死信队列诊断器，分析指定消费者组的死信状况与失败原因分布。
 *
 * <p>分析维度：
 *
 * <ul>
 *   <li>死信消息总数
 *   <li>Top 失败原因（按出现次数降序）
 *   <li>Top 失败主题（按失败次数降序）
 *   <li>最早死信消息时间戳
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
@RequiredArgsConstructor
public class DlqAnalyzer {

    /** 追踪记录扩展属性键：错误信息 */
    private static final String ATTR_ERROR_MESSAGE = StreamMQConstants.TRACE_ATTR_ERROR_MESSAGE;

    private final StreamMQTraceService traceService;
    private final StreamMQDiagnosticsProperties properties;

    /**
     * 诊断死信队列，分析指定消费者组的死信状况与失败原因分布。
     *
     * @param group 消费者组
     * @return 死信队列诊断报告
     */
    public DlqReport diagnose(String group) {
        long now = System.currentTimeMillis();
        long start = now - properties.getDlqWindowMs();

        List<TraceRecord> groupRecords = traceService.queryByGroup(group, start, now);
        if (CollectionUtils.isEmpty(groupRecords)) {
            return buildEmptyReport(group);
        }

        List<TraceRecord> failedRecords = TraceRecordFilters.filterFailedConsume(groupRecords);
        Set<String> dlqMessageIds = collectDlqMessageIds(groupRecords);

        long totalDlqCount = dlqMessageIds.size();
        List<FailureReason> topFailureReasons =
                aggregateFailureReasons(failedRecords, totalDlqCount);
        List<TopicFailureCount> topFailedTopics = aggregateFailedTopics(failedRecords);
        long oldestTimestamp = findOldestTimestamp(groupRecords);

        String recommendation = buildRecommendation(totalDlqCount, topFailureReasons);

        return new DlqReport(
                group,
                totalDlqCount,
                topFailureReasons,
                topFailedTopics,
                oldestTimestamp,
                recommendation,
                classifyCount(totalDlqCount));
    }

    /** 根据死信总数判定严重等级码。 */
    private String classifyCount(long totalDlqCount) {
        if (totalDlqCount > properties.getBacklogCriticalThreshold()) {
            return DiagnosticsCodes.DLQ_CRITICAL;
        }
        if (totalDlqCount > properties.getBacklogWarningThreshold()) {
            return DiagnosticsCodes.DLQ_WARNING;
        }
        return DiagnosticsCodes.DLQ_NORMAL;
    }

    /** 构建空的死信队列报告（追踪数据不存在时使用）。 */
    private DlqReport buildEmptyReport(String group) {
        return new DlqReport(
                group,
                0L,
                Collections.emptyList(),
                Collections.emptyList(),
                0L,
                "No trace data; verify tracing is enabled",
                DlqCodes.NO_TRACE_DATA);
    }

    /**
     * 收集死信消息 ID 集合。
     *
     * <p>判定规则：消息的消费记录中存在 DLQ 主题，或存在多次失败消费记录。
     */
    private Set<String> collectDlqMessageIds(List<TraceRecord> records) {
        Map<String, List<TraceRecord>> byMessageId = new LinkedHashMap<>();
        for (TraceRecord record : records) {
            if (StringUtils.isEmpty(record.messageId())) {
                continue;
            }
            byMessageId.computeIfAbsent(record.messageId(), k -> new ArrayList<>()).add(record);
        }

        Set<String> dlqIds = new HashSet<>();
        for (Map.Entry<String, List<TraceRecord>> entry : byMessageId.entrySet()) {
            if (isDlqMessage(entry.getValue())) {
                dlqIds.add(entry.getKey());
            }
        }
        return dlqIds;
    }

    /**
     * 判断消息是否为死信消息。
     *
     * <p>判定规则（可通过 {@link StreamMQDiagnosticsProperties} 配置）：
     *
     * <ul>
     *   <li>消费记录的主题包含 DLQ 标识关键字（默认 "dlq"）
     *   <li>存在多次失败消费记录（重试次数超过配置的阈值，默认 3）
     * </ul>
     */
    private boolean isDlqMessage(List<TraceRecord> records) {
        String marker = properties.getDlqTopicMarker().toLowerCase();
        int maxRetry = properties.getDlqMaxRetryCount();
        int failCount = 0;
        for (TraceRecord record : records) {
            if (Objects.nonNull(record.type()) && record.type() == TraceType.CONSUME) {
                if (StringUtils.isNotEmpty(record.topic())
                        && record.topic().toLowerCase().contains(marker)) {
                    return true;
                }
                if (!record.success()) {
                    failCount++;
                }
            }
        }
        return failCount >= maxRetry;
    }

    /**
     * 聚合失败原因。
     *
     * @param failedRecords 失败的消费记录列表
     * @param totalDlqCount 死信消息总数
     * @return Top 失败原因列表（按出现次数降序，最多 10 条）
     */
    private List<FailureReason> aggregateFailureReasons(
            List<TraceRecord> failedRecords, long totalDlqCount) {
        if (CollectionUtils.isEmpty(failedRecords)) {
            return Collections.emptyList();
        }
        Map<String, Long> reasonCounts = new HashMap<>();
        for (TraceRecord record : failedRecords) {
            String reason = extractErrorMessage(record);
            if (StringUtils.isEmpty(reason)) {
                reason = "unknown error";
            }
            reasonCounts.merge(reason, 1L, Long::sum);
        }

        List<FailureReason> reasons = new ArrayList<>();
        for (Map.Entry<String, Long> entry : sortByValueDesc(reasonCounts)) {
            double percentage = totalDlqCount > 0 ? entry.getValue() * 100.0 / totalDlqCount : 0.0;
            reasons.add(new FailureReason(entry.getKey(), entry.getValue(), percentage));
            if (reasons.size() >= StreamMQDiagnosticsDefaults.TOP_N_LIMIT) {
                break;
            }
        }
        return reasons;
    }

    /** 聚合失败主题。 */
    private List<TopicFailureCount> aggregateFailedTopics(List<TraceRecord> failedRecords) {
        if (CollectionUtils.isEmpty(failedRecords)) {
            return Collections.emptyList();
        }
        Map<String, long[]> topicStats = new HashMap<>();
        for (TraceRecord record : failedRecords) {
            String topic =
                    StringUtils.isNotEmpty(record.topic()) ? record.topic() : "unknown topic";
            long[] stats = topicStats.computeIfAbsent(topic, k -> new long[] {0L, 0L});
            stats[0]++;
            if (record.timestamp() > stats[1]) {
                stats[1] = record.timestamp();
            }
        }

        List<TopicFailureCount> topics = new ArrayList<>();
        List<Map.Entry<String, long[]>> sorted = new ArrayList<>(topicStats.entrySet());
        sorted.sort((a, b) -> Long.compare(b.getValue()[0], a.getValue()[0]));

        for (Map.Entry<String, long[]> entry : sorted) {
            topics.add(
                    new TopicFailureCount(
                            entry.getKey(), entry.getValue()[0], entry.getValue()[1]));
            if (topics.size() >= StreamMQDiagnosticsDefaults.TOP_N_LIMIT) {
                break;
            }
        }
        return topics;
    }

    /** 查找最早的追踪记录时间戳。 */
    private long findOldestTimestamp(List<TraceRecord> records) {
        long oldest = Long.MAX_VALUE;
        for (TraceRecord record : records) {
            if (record.timestamp() < oldest) {
                oldest = record.timestamp();
            }
        }
        return oldest == Long.MAX_VALUE ? 0L : oldest;
    }

    /** 构建死信队列优化建议。 */
    private String buildRecommendation(long totalDlqCount, List<FailureReason> topFailureReasons) {
        if (totalDlqCount <= 0) {
            return "No dead messages; consume pipeline healthy";
        }
        if (totalDlqCount > properties.getBacklogCriticalThreshold()) {
            if (CollectionUtils.isNotEmpty(topFailureReasons)) {
                return "Too many dead messages ("
                        + totalDlqCount
                        + "); inspect top failure cause immediately: "
                        + topFailureReasons.get(0).reason();
            }
            return "Too many dead messages ("
                    + totalDlqCount
                    + "); investigate consume failures now";
        }
        if (totalDlqCount > properties.getBacklogWarningThreshold()) {
            return "Elevated dead-message count ("
                    + totalDlqCount
                    + "); review failure causes periodically and improve consume logic";
        }
        return "Dead-message count within acceptable range; keep monitoring";
    }

    /** 从追踪记录中提取错误信息。 */
    private String extractErrorMessage(TraceRecord record) {
        Map<String, String> attrs = record.attributes();
        if (CollectionUtils.isEmpty(attrs)) {
            return null;
        }
        return attrs.get(ATTR_ERROR_MESSAGE);
    }

    /** 将 Map 按值降序排序。 */
    private static <K, V extends Comparable<V>> List<Map.Entry<K, V>> sortByValueDesc(
            Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new ArrayList<>(map.entrySet());
        entries.sort((a, b) -> b.getValue().compareTo(a.getValue()));
        return entries;
    }
}
