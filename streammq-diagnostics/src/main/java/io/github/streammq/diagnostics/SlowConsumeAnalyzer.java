/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.diagnostics.DiagnosticsCodes.SlowConsumeCodes;
import io.github.streammq.diagnostics.model.SlowConsumeReport;
import io.github.streammq.diagnostics.support.TraceRecordFilters;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * 慢消费诊断器，分析指定主题+消费者组的消费性能状况。
 *
 * <p>分析维度：
 *
 * <ul>
 *   <li>消费速率 vs 生产速率
 *   <li>平均 / 最大 / P99 消费耗时
 *   <li>消费者实例数
 *   <li>瓶颈定位与优化建议
 * </ul>
 *
 * <p>本类由 {@link StreamMQDiagnosticsService} 持有，对外提供慢消费诊断能力。 追踪数据不可用时返回无数据空报告，不抛异常。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
@RequiredArgsConstructor
public class SlowConsumeAnalyzer {

    /** 建议线程数上限 */
    private static final int RECOMMENDED_MAX_THREADS = 128;

    private final StreamMQTraceService traceService;
    private final StreamMQListenerContainer listenerContainer;
    private final StreamMQDiagnosticsProperties properties;

    /**
     * 诊断慢消费，分析指定主题+消费者组的消费性能状况。
     *
     * @param topic 主题
     * @param group 消费者组
     * @return 慢消费诊断报告
     */
    public SlowConsumeReport diagnose(String topic, String group) {
        long now = System.currentTimeMillis();
        long start = now - properties.getRecentWindowMs();

        List<TraceRecord> topicRecords = traceService.queryByTopic(topic, start, now);
        if (CollectionUtils.isEmpty(topicRecords)) {
            return buildEmptyReport(topic, group);
        }

        List<TraceRecord> consumeRecords = TraceRecordFilters.filterConsumeByGroup(topicRecords, group);
        List<TraceRecord> sendRecords = TraceRecordFilters.filterSend(topicRecords);

        double windowSeconds = properties.getRecentWindowMs() / 1000.0;
        double consumeRate = consumeRecords.size() / windowSeconds;
        double produceRate = sendRecords.size() / windowSeconds;

        List<Long> durations = extractDurations(consumeRecords);
        double avgConsumeTime = durations.stream().mapToLong(Long::longValue).average().orElse(0.0);
        long maxConsumeTime = durations.stream().mapToLong(Long::longValue).max().orElse(0L);
        long p99ConsumeTime = calculateP99(durations);

        int consumerCount = countConsumers(topic, group);
        // 不再伪造线程池活跃度：真实 executor 指标未接入前，报告消费实例数这一可观测事实
        String code;
        String bottleneck =
                analyzeBottleneck(avgConsumeTime, consumeRate, produceRate, consumerCount);
        if (bottleneck == null) {
            bottleneck = "no trace data";
            code = SlowConsumeCodes.NO_TRACE_DATA;
        } else {
            code = classify(avgConsumeTime, consumeRate, produceRate);
        }
        String recommendation =
                buildRecommendation(avgConsumeTime, consumeRate, produceRate, consumerCount);

        return new SlowConsumeReport(
                topic,
                group,
                consumeRate,
                produceRate,
                avgConsumeTime,
                maxConsumeTime,
                p99ConsumeTime,
                consumerCount,
                consumerCount,
                bottleneck,
                recommendation,
                code);
    }

    /**
     * 构建空的慢消费报告（追踪数据不存在时使用）。
     *
     * @param topic 主题
     * @param group 消费者组
     * @return 空报告
     */
    private SlowConsumeReport buildEmptyReport(String topic, String group) {
        int consumerCount = countConsumers(topic, group);
        return new SlowConsumeReport(
                topic,
                group,
                0.0,
                0.0,
                0.0,
                0L,
                0L,
                0,
                Math.max(consumerCount, 1),
                "No trace data available for bottleneck analysis",
                "Verify that message tracing is enabled and the trace service is healthy",
                SlowConsumeCodes.NO_TRACE_DATA);
    }

    /**
     * 提取消费记录的耗时列表。
     *
     * @param records 消费记录列表
     * @return 耗时列表
     */
    private List<Long> extractDurations(List<TraceRecord> records) {
        List<Long> durations = new ArrayList<>(records.size());
        for (TraceRecord record : records) {
            durations.add(record.durationMillis());
        }
        return durations;
    }

    /**
     * 计算 P99 耗时。
     *
     * @param durations 耗时列表
     * @return P99 耗时，若列表为空则返回 0
     */
    private long calculateP99(List<Long> durations) {
        if (CollectionUtils.isEmpty(durations)) {
            return 0L;
        }
        List<Long> sorted = new ArrayList<>(durations);
        sorted.sort(Long::compare);
        int index = (int) Math.ceil(sorted.size() * StreamMQDiagnosticsDefaults.P99_PERCENTILE) - 1;
        return sorted.get(Math.max(0, index));
    }

    /**
     * 统计指定主题+消费者组的消费者实例数。
     *
     * @param topic 主题
     * @param group 消费者组
     * @return 消费者实例数
     */
    private int countConsumers(String topic, String group) {
        Collection<StreamMQListenerContainer.ConsumerMetadata> consumers =
                listenerContainer.getConsumers();
        if (CollectionUtils.isEmpty(consumers)) {
            return 0;
        }
        int count = 0;
        for (StreamMQListenerContainer.ConsumerMetadata metadata : consumers) {
            if (Objects.nonNull(metadata)
                    && Objects.equals(metadata.topic(), topic)
                    && Objects.equals(metadata.consumerGroup(), group)) {
                count++;
            }
        }
        return count;
    }

    /**
     * 判定慢消费等级 code。
     *
     * @param avgConsumeTime 平均消费耗时
     * @param consumeRate 消费速率
     * @param produceRate 生产速率
     * @return 慢消费码
     */
    private String classify(double avgConsumeTime, double consumeRate, double produceRate) {
        if (avgConsumeTime > properties.getSlowConsumeThresholdMs()) {
            return SlowConsumeCodes.SLOW_CONSUME;
        }
        if (consumeRate < produceRate) {
            return SlowConsumeCodes.CONSUME_RATE_BEHIND;
        }
        return SlowConsumeCodes.HEALTHY;
    }

    /**
     * 瓶颈分析（locale-neutral code + English message）。不再伪造线程池利用率：真实 executor 指标尚未接入。
     */
    private String analyzeBottleneck(
            double avgConsumeTime, double consumeRate, double produceRate, int consumerCount) {
        if (avgConsumeTime > properties.getSlowConsumeThresholdMs()) {
            return String.format(
                    "Average consume time %.2fms exceeds threshold (%dms); slow logic or"
                            + " downstream dependency suspected",
                    avgConsumeTime, properties.getSlowConsumeThresholdMs());
        }
        if (consumeRate < produceRate) {
            return String.format(
                    "Consume rate %.2f msg/s is below produce rate %.2f msg/s; backlog growing",
                    consumeRate, produceRate);
        }
        return "Consume performance normal";
    }

    /**
     * 构建慢消费优化建议。
     *
     * @param avgConsumeTime 平均消费耗时
     * @param consumeRate 消费速率
     * @param produceRate 生产速率
     * @param consumerCount 当前消费者实例数
     * @return 优化建议
     */
    private String buildRecommendation(
            double avgConsumeTime, double consumeRate, double produceRate, int consumerCount) {
        if (avgConsumeTime > properties.getSlowConsumeThresholdMs()) {
            return "Optimize consume logic and check downstream dependency timeouts; consider"
                    + " increasing consumer threads or instances (current instances="
                    + consumerCount
                    + ", recommended max threads per instance="
                    + RECOMMENDED_MAX_THREADS
                    + ")";
        }
        if (consumeRate < produceRate) {
            return "Scale out consumers: add instances first, then raise"
                    + " streammq.consumer.consume-thread-max if CPU allows";
        }
        return "Consume rate is healthy; no action needed";
    }
}
