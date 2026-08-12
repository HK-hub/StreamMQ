package io.github.streammq.diagnostics;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.trace.TraceType;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import io.github.streammq.diagnostics.model.BacklogReport;
import io.github.streammq.diagnostics.model.DlqReport;
import io.github.streammq.diagnostics.model.FailureReason;
import io.github.streammq.diagnostics.model.Severity;
import io.github.streammq.diagnostics.model.SlowConsumeReport;
import io.github.streammq.diagnostics.model.TopicFailureCount;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamMQ 诊断服务，自动诊断慢消费、消息积压、死信队列等异常。
 *
 * <p>基于 {@link StreamMQTraceService} 的追踪数据与 {@link StreamMQListenerContainer} 的消费者元信息， 提供以下诊断能力：
 *
 * <ul>
 *   <li>{@link #diagnoseSlowConsume(String, String)} - 慢消费诊断，分析消费耗时与速率瓶颈
 *   <li>{@link #diagnoseBacklog(String, String)} - 积压诊断，分析积压量与清积压预估
 *   <li>{@link #diagnoseDlq(String)} - 死信队列诊断，分析失败原因与主题分布
 *   <li>{@link #getSlowConsumers()} - 识别所有慢消费者
 *   <li>{@link #getAllBacklogs()} - 获取所有消费者组的积压报告
 * </ul>
 *
 * <p>诊断阈值、时间窗口、DLQ 判定规则等全部通过 {@link StreamMQDiagnosticsProperties} 外部化配置， 支持用户按需调整而无需修改代码。
 *
 * <p>当追踪数据不可用时，所有诊断方法以无数据方式优雅降级，不抛出异常。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMQDiagnosticsService {

    private static final Logger log = LoggerFactory.getLogger(StreamMQDiagnosticsService.class);

    /** 追踪记录扩展属性键：错误信息 */
    private static final String ATTR_ERROR_MESSAGE = "errorMessage";

    private final StreamMQTraceService traceService;
    private final StreamMQListenerContainer listenerContainer;
    private final StreamMQDiagnosticsProperties properties;

    /**
     * 构造诊断服务。
     *
     * @param traceService 追踪查询服务
     * @param listenerContainer 监听器容器
     * @param properties 诊断配置属性
     */
    public StreamMQDiagnosticsService(
            StreamMQTraceService traceService,
            StreamMQListenerContainer listenerContainer,
            StreamMQDiagnosticsProperties properties) {
        this.traceService = Objects.requireNonNull(traceService, "traceService");
        this.listenerContainer = Objects.requireNonNull(listenerContainer, "listenerContainer");
        this.properties = Objects.requireNonNull(properties, "properties");
    }

    /**
     * 诊断慢消费，分析指定主题+消费者组的消费性能状况。
     *
     * <p>分析维度：
     *
     * <ul>
     *   <li>消费速率 vs 生产速率
     *   <li>平均 / 最大 / P99 消费耗时
     *   <li>线程池活跃度
     *   <li>瓶颈定位与优化建议
     * </ul>
     *
     * @param topic 主题
     * @param group 消费者组
     * @return 慢消费诊断报告
     */
    public SlowConsumeReport diagnoseSlowConsume(String topic, String group) {
        long now = System.currentTimeMillis();
        long start = now - properties.getRecentWindowMs();

        List<TraceRecord> topicRecords = traceService.queryByTopic(topic, start, now);
        if (CollectionUtils.isEmpty(topicRecords)) {
            return buildEmptySlowConsumeReport(topic, group);
        }

        List<TraceRecord> consumeRecords = filterConsumeByGroup(topicRecords, group);
        List<TraceRecord> sendRecords = filterSend(topicRecords);

        double windowSeconds = properties.getRecentWindowMs() / 1000.0;
        double consumeRate = consumeRecords.size() / windowSeconds;
        double produceRate = sendRecords.size() / windowSeconds;

        List<Long> durations = extractDurations(consumeRecords);
        double avgConsumeTime = durations.stream().mapToLong(Long::longValue).average().orElse(0.0);
        long maxConsumeTime = durations.stream().mapToLong(Long::longValue).max().orElse(0L);
        long p99ConsumeTime = calculateP99(durations);

        int consumerCount = countConsumers(topic, group);
        int threadPoolActive = Math.min(consumerCount, Math.max(1, consumeRecords.size()));
        int threadPoolMax = Math.max(consumerCount, 1);

        String bottleneck =
                analyzeBottleneck(
                        avgConsumeTime, consumeRate, produceRate, threadPoolActive, threadPoolMax);
        String recommendation =
                buildSlowConsumeRecommendation(
                        avgConsumeTime, consumeRate, produceRate, threadPoolMax);

        return new SlowConsumeReport(
                topic,
                group,
                consumeRate,
                produceRate,
                avgConsumeTime,
                maxConsumeTime,
                p99ConsumeTime,
                threadPoolActive,
                threadPoolMax,
                bottleneck,
                recommendation);
    }

    /**
     * 诊断消息积压，分析指定主题+消费者组的积压状况。
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
     * @param topic 主题
     * @param group 消费者组
     * @return 积压诊断报告
     */
    public BacklogReport diagnoseBacklog(String topic, String group) {
        long now = System.currentTimeMillis();
        long start = now - properties.getRecentWindowMs();

        List<TraceRecord> topicRecords = traceService.queryByTopic(topic, start, now);
        if (CollectionUtils.isEmpty(topicRecords)) {
            return buildEmptyBacklogReport(topic, group);
        }

        List<TraceRecord> consumeRecords = filterConsumeByGroup(topicRecords, group);
        List<TraceRecord> sendRecords = filterSend(topicRecords);

        long produceCount = sendRecords.size();
        long consumeCount = consumeRecords.size();
        long currentBacklog = Math.max(0, produceCount - consumeCount);

        double windowSeconds = properties.getRecentWindowMs() / 1000.0;
        double produceRate = produceCount / windowSeconds;
        double consumeRate = consumeCount / windowSeconds;
        double growthRate = (produceCount - consumeCount) / windowSeconds;

        long estimatedClearTimeMinutes =
                calculateEstimatedClearTime(currentBacklog, consumeRate, produceRate);
        Severity severity = determineSeverity(currentBacklog);
        String recommendation = buildBacklogRecommendation(severity, consumeRate, produceRate);

        return new BacklogReport(
                topic,
                group,
                currentBacklog,
                growthRate,
                estimatedClearTimeMinutes,
                produceRate,
                consumeRate,
                recommendation,
                severity);
    }

    /**
     * 诊断死信队列，分析指定消费者组的死信状况与失败原因分布。
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
     * @param group 消费者组
     * @return 死信队列诊断报告
     */
    public DlqReport diagnoseDlq(String group) {
        long now = System.currentTimeMillis();
        long start = now - properties.getDlqWindowMs();

        List<TraceRecord> groupRecords = traceService.queryByGroup(group, start, now);
        if (CollectionUtils.isEmpty(groupRecords)) {
            return buildEmptyDlqReport(group);
        }

        List<TraceRecord> failedRecords = filterFailedConsume(groupRecords);
        Set<String> dlqMessageIds = collectDlqMessageIds(groupRecords);

        long totalDlqCount = dlqMessageIds.size();
        List<FailureReason> topFailureReasons =
                aggregateFailureReasons(failedRecords, totalDlqCount);
        List<TopicFailureCount> topFailedTopics = aggregateFailedTopics(failedRecords);
        long oldestTimestamp = findOldestTimestamp(groupRecords);

        String recommendation = buildDlqRecommendation(totalDlqCount, topFailureReasons);

        return new DlqReport(
                group,
                totalDlqCount,
                topFailureReasons,
                topFailedTopics,
                oldestTimestamp,
                recommendation);
    }

    /**
     * 识别所有慢消费者，遍历监听器容器中注册的全部消费者。
     *
     * <p>对每个主题+消费者组组合进行慢消费诊断，返回平均消费耗时超过阈值的消费者列表。
     *
     * @return 慢消费者标识列表（格式：topic:group）
     */
    public List<String> getSlowConsumers() {
        Collection<StreamMQListenerContainer.ConsumerMetadata> consumers =
                listenerContainer.getConsumers();
        if (CollectionUtils.isEmpty(consumers)) {
            return Collections.emptyList();
        }

        Set<String> visited = new HashSet<>();
        List<String> slowConsumers = new ArrayList<>();

        for (StreamMQListenerContainer.ConsumerMetadata metadata : consumers) {
            if (Objects.isNull(metadata)
                    || StringUtils.isEmpty(metadata.topic())
                    || StringUtils.isEmpty(metadata.consumerGroup())) {
                continue;
            }
            String key = metadata.topic() + ":" + metadata.consumerGroup();
            if (visited.contains(key)) {
                continue;
            }
            visited.add(key);

            SlowConsumeReport report =
                    diagnoseSlowConsume(metadata.topic(), metadata.consumerGroup());
            if (report.avgConsumeTimeMillis() > properties.getSlowConsumeThresholdMs()) {
                slowConsumers.add(key);
            }
        }
        return slowConsumers;
    }

    /**
     * 获取所有消费者组的积压报告。
     *
     * <p>遍历监听器容器中注册的全部消费者，对每个主题+消费者组组合进行积压诊断。
     *
     * @return 积压报告列表
     */
    public List<BacklogReport> getAllBacklogs() {
        Collection<StreamMQListenerContainer.ConsumerMetadata> consumers =
                listenerContainer.getConsumers();
        if (CollectionUtils.isEmpty(consumers)) {
            return Collections.emptyList();
        }

        Set<String> visited = new HashSet<>();
        List<BacklogReport> backlogs = new ArrayList<>();

        for (StreamMQListenerContainer.ConsumerMetadata metadata : consumers) {
            if (Objects.isNull(metadata)
                    || StringUtils.isEmpty(metadata.topic())
                    || StringUtils.isEmpty(metadata.consumerGroup())) {
                continue;
            }
            String key = metadata.topic() + ":" + metadata.consumerGroup();
            if (visited.contains(key)) {
                continue;
            }
            visited.add(key);
            backlogs.add(diagnoseBacklog(metadata.topic(), metadata.consumerGroup()));
        }
        return backlogs;
    }

    /**
     * 构建空的慢消费报告（追踪数据不存在时使用）。
     *
     * @param topic 主题
     * @param group 消费者组
     * @return 空报告
     */
    private SlowConsumeReport buildEmptySlowConsumeReport(String topic, String group) {
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
                "无追踪数据，无法分析瓶颈",
                "建议检查追踪服务是否正常启用");
    }

    /**
     * 构建空的积压报告（追踪数据不存在时使用）。
     *
     * @param topic 主题
     * @param group 消费者组
     * @return 空报告
     */
    private BacklogReport buildEmptyBacklogReport(String topic, String group) {
        return new BacklogReport(
                topic, group, 0L, 0.0, -1L, 0.0, 0.0, "无追踪数据，建议检查追踪服务是否正常启用", Severity.INFO);
    }

    /**
     * 构建空的死信队列报告（追踪数据不存在时使用）。
     *
     * @param group 消费者组
     * @return 空报告
     */
    private DlqReport buildEmptyDlqReport(String group) {
        return new DlqReport(
                group,
                0L,
                Collections.emptyList(),
                Collections.emptyList(),
                0L,
                "无追踪数据，建议检查追踪服务是否正常启用");
    }

    /**
     * 从追踪记录列表中过滤出指定消费者组的消费记录。
     *
     * @param records 追踪记录列表
     * @param group 消费者组
     * @return 消费记录列表
     */
    private List<TraceRecord> filterConsumeByGroup(List<TraceRecord> records, String group) {
        List<TraceRecord> result = new ArrayList<>();
        for (TraceRecord record : records) {
            if (Objects.nonNull(record.type())
                    && record.type() == TraceType.CONSUME
                    && Objects.equals(record.group(), group)) {
                result.add(record);
            }
        }
        return result;
    }

    /**
     * 从追踪记录列表中过滤出发送记录。
     *
     * @param records 追踪记录列表
     * @return 发送记录列表
     */
    private List<TraceRecord> filterSend(List<TraceRecord> records) {
        List<TraceRecord> result = new ArrayList<>();
        for (TraceRecord record : records) {
            if (Objects.nonNull(record.type()) && record.type() == TraceType.SEND) {
                result.add(record);
            }
        }
        return result;
    }

    /**
     * 从追踪记录列表中过滤出失败的消费记录。
     *
     * @param records 追踪记录列表
     * @return 失败的消费记录列表
     */
    private List<TraceRecord> filterFailedConsume(List<TraceRecord> records) {
        List<TraceRecord> result = new ArrayList<>();
        for (TraceRecord record : records) {
            if (Objects.nonNull(record.type())
                    && record.type() == TraceType.CONSUME
                    && !record.success()) {
                result.add(record);
            }
        }
        return result;
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
        int index = (int) Math.ceil(sorted.size() * 0.99) - 1;
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
     * 分析瓶颈原因。
     *
     * <p>判定顺序：消费耗时 → 消费速率 → 线程池满载。
     *
     * @param avgConsumeTime 平均消费耗时
     * @param consumeRate 消费速率
     * @param produceRate 生产速率
     * @param threadPoolActive 线程池活跃数
     * @param threadPoolMax 线程池最大数
     * @return 瓶颈描述
     */
    private String analyzeBottleneck(
            double avgConsumeTime,
            double consumeRate,
            double produceRate,
            int threadPoolActive,
            int threadPoolMax) {
        if (avgConsumeTime > properties.getSlowConsumeThresholdMs()) {
            return "消费耗时过长（平均 " + String.format("%.2f", avgConsumeTime) + "ms），可能存在慢逻辑或外部依赖瓶颈";
        }
        if (consumeRate < produceRate) {
            return "消费速率（"
                    + String.format("%.2f", consumeRate)
                    + " msg/s）低于生产速率（"
                    + String.format("%.2f", produceRate)
                    + " msg/s），积压持续增长";
        }
        if (threadPoolActive >= threadPoolMax) {
            return "线程池已满载（活跃 " + threadPoolActive + "/" + threadPoolMax + "），可能存在并发瓶颈";
        }
        return "消费性能正常";
    }

    /**
     * 构建慢消费优化建议。
     *
     * @param avgConsumeTime 平均消费耗时
     * @param consumeRate 消费速率
     * @param produceRate 生产速率
     * @param threadPoolMax 线程池最大数
     * @return 优化建议
     */
    private String buildSlowConsumeRecommendation(
            double avgConsumeTime, double consumeRate, double produceRate, int threadPoolMax) {
        if (avgConsumeTime > properties.getSlowConsumeThresholdMs()) {
            return "建议优化消费逻辑，排查外部依赖超时，并考虑增加消费线程数到 " + Math.max(threadPoolMax * 2, 128);
        }
        if (consumeRate < produceRate) {
            if (threadPoolMax < 64) {
                return "建议增加消费线程数到 128";
            }
            return "建议增加消费者实例数以提升并发消费能力";
        }
        return "消费速率正常，无需调整";
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

    /**
     * 根据积压量判定严重级别，阈值由 {@link StreamMQDiagnosticsProperties} 配置。
     *
     * @param currentBacklog 当前积压量
     * @return 严重级别
     */
    private Severity determineSeverity(long currentBacklog) {
        if (currentBacklog < properties.getBacklogWarningThreshold()) {
            return Severity.INFO;
        }
        if (currentBacklog < properties.getBacklogCriticalThreshold()) {
            return Severity.WARNING;
        }
        return Severity.CRITICAL;
    }

    /**
     * 构建积压优化建议。
     *
     * @param severity 严重级别
     * @param consumeRate 消费速率
     * @param produceRate 生产速率
     * @return 优化建议
     */
    private String buildBacklogRecommendation(
            Severity severity, double consumeRate, double produceRate) {
        switch (severity) {
            case INFO:
                return "积压在正常范围内，无需处理";
            case WARNING:
                if (consumeRate < produceRate) {
                    return "积压较多且持续增长，建议增加消费者实例数";
                }
                return "积压较多但正在消化，建议持续关注";
            case CRITICAL:
                if (consumeRate < produceRate) {
                    return "积压严重且持续增长，建议立即扩容消费者实例数并检查消费逻辑";
                }
                return "积压严重但正在消化，建议持续监控清积压进度";
            default:
                return "建议持续关注积压状况";
        }
    }

    /**
     * 收集死信消息 ID 集合。
     *
     * <p>判定规则：消息的消费记录中存在 DLQ 主题，或存在多次失败消费记录。
     *
     * @param records 追踪记录列表
     * @return 死信消息 ID 集合
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
     *
     * @param records 同一消息的追踪记录
     * @return true 如果是死信消息
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
                reason = "未知错误";
            }
            reasonCounts.merge(reason, 1L, Long::sum);
        }

        List<FailureReason> reasons = new ArrayList<>();
        for (Map.Entry<String, Long> entry : sortByValueDesc(reasonCounts)) {
            double percentage = totalDlqCount > 0 ? entry.getValue() * 100.0 / totalDlqCount : 0.0;
            reasons.add(new FailureReason(entry.getKey(), entry.getValue(), percentage));
            if (reasons.size() >= 10) {
                break;
            }
        }
        return reasons;
    }

    /**
     * 聚合失败主题。
     *
     * @param failedRecords 失败的消费记录列表
     * @return Top 失败主题列表（按失败次数降序，最多 10 条）
     */
    private List<TopicFailureCount> aggregateFailedTopics(List<TraceRecord> failedRecords) {
        if (CollectionUtils.isEmpty(failedRecords)) {
            return Collections.emptyList();
        }
        Map<String, long[]> topicStats = new HashMap<>();
        for (TraceRecord record : failedRecords) {
            String topic = StringUtils.isNotEmpty(record.topic()) ? record.topic() : "未知主题";
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
            if (topics.size() >= 10) {
                break;
            }
        }
        return topics;
    }

    /**
     * 查找最早的追踪记录时间戳。
     *
     * @param records 追踪记录列表
     * @return 最早时间戳，若无记录则返回 0
     */
    private long findOldestTimestamp(List<TraceRecord> records) {
        long oldest = Long.MAX_VALUE;
        for (TraceRecord record : records) {
            if (record.timestamp() < oldest) {
                oldest = record.timestamp();
            }
        }
        return oldest == Long.MAX_VALUE ? 0L : oldest;
    }

    /**
     * 构建死信队列优化建议。
     *
     * @param totalDlqCount 死信消息总数
     * @param topFailureReasons Top 失败原因列表
     * @return 优化建议
     */
    private String buildDlqRecommendation(
            long totalDlqCount, List<FailureReason> topFailureReasons) {
        if (totalDlqCount <= 0) {
            return "无死信消息，消费状况正常";
        }
        if (totalDlqCount > properties.getBacklogCriticalThreshold()) {
            if (CollectionUtils.isNotEmpty(topFailureReasons)) {
                return "死信消息过多（"
                        + totalDlqCount
                        + " 条），建议立即排查首要失败原因："
                        + topFailureReasons.get(0).reason();
            }
            return "死信消息过多（" + totalDlqCount + " 条），建议立即排查消费失败原因";
        }
        if (totalDlqCount > properties.getBacklogWarningThreshold()) {
            return "死信消息较多（" + totalDlqCount + " 条），建议定期排查失败原因并优化消费逻辑";
        }
        return "死信消息数量在可控范围内，建议持续关注";
    }

    /**
     * 从追踪记录中提取错误信息。
     *
     * @param record 追踪记录
     * @return 错误信息，若不存在则返回 null
     */
    private String extractErrorMessage(TraceRecord record) {
        Map<String, String> attrs = record.attributes();
        if (CollectionUtils.isEmpty(attrs)) {
            return null;
        }
        return attrs.get(ATTR_ERROR_MESSAGE);
    }

    /**
     * 将 Map 按值降序排序。
     *
     * @param map 原始 Map
     * @return 排序后的条目列表
     */
    private static <K, V extends Comparable<V>> List<Map.Entry<K, V>> sortByValueDesc(
            Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new ArrayList<>(map.entrySet());
        entries.sort((a, b) -> b.getValue().compareTo(a.getValue()));
        return entries;
    }
}
