package io.github.streammq.tracing;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.listener.StreamMQListenerContainer.ConsumerMetadata;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.trace.TraceType;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import io.github.streammq.tracing.model.MessageTrace;
import io.github.streammq.tracing.model.TopologyGraph;
import io.github.streammq.tracing.model.TopologyNode;
import io.github.streammq.tracing.model.TopologyRoute;
import io.github.streammq.tracing.model.TraceEvent;
import io.github.streammq.tracing.model.TraceEventType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * StreamMQ 消息拓扑服务，基于 {@link StreamMQTraceService} 与 {@link StreamMQListenerContainer}
 * 构建并查询消息流转拓扑与完整链路。
 *
 * <p>核心能力：
 * <ul>
 *   <li>{@link #getTopicTopology(String)}：构建指定 Topic 的生产-消费拓扑图</li>
 *   <li>{@link #getMessageTrace(String)}：构建单条消息的完整生命周期链路</li>
 *   <li>{@link #getTopicTraces(String, long, long)}：查询时间范围内 Topic 下所有消息的链路</li>
 * </ul>
 *
 * <p>拓扑构建策略：
 * <ul>
 *   <li>生产者节点：从最近时间窗口内的 SEND 追踪记录中提取生产者组</li>
 *   <li>消费者节点：从监听器容器已注册的 Consumer 元信息中提取（按 Topic 过滤）</li>
 *   <li>路由速率：基于时间窗口内 SEND 记录数估算（条/秒）</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
@Slf4j
@RequiredArgsConstructor
public class StreamMQTopologyService {

    /** 拓扑构建回溯时间窗口（毫秒，最近 5 分钟） */
    private static final long TOPOLOGY_WINDOW_MS = 5 * 60 * 1000L;

    /** 默认生产者节点名称 */
    private static final String DEFAULT_PRODUCER_NAME = "Producer";

    private final StreamMQTraceService traceService;
    private final StreamMQListenerContainer listenerContainer;

    /**
     * 构建指定 Topic 的生产-消费拓扑图。
     *
     * @param topic Topic 名称
     * @return 拓扑图，无数据时返回空节点拓扑
     */
    public TopologyGraph getTopicTopology(String topic) {
        long now = System.currentTimeMillis();
        long start = now - TOPOLOGY_WINDOW_MS;
        List<TraceRecord> records = safeQuery(() -> traceService.queryByTopic(topic, start, now));

        List<TopologyNode> producers = buildProducerNodes(topic, records);
        List<TopologyNode> consumers = buildConsumerNodes(topic);
        List<TopologyRoute> routes = buildRoutes(topic, producers, consumers, records);
        return new TopologyGraph(topic, producers, consumers, routes, now);
    }

    /**
     * 构建单条消息的完整生命周期链路。
     *
     * @param messageId 消息 ID
     * @return 消息链路，无追踪记录时返回空事件链路
     */
    public MessageTrace getMessageTrace(String messageId) {
        List<TraceRecord> records = safeQuery(() -> traceService.queryByMessageId(messageId));
        return buildMessageTrace(messageId, records);
    }

    /**
     * 查询时间范围内 Topic 下所有消息的链路。
     *
     * @param topic   Topic 名称
     * @param startMs 起始时间戳（毫秒，包含）
     * @param endMs   结束时间戳（毫秒，包含）
     * @return 消息链路列表，按消息 ID聚合
     */
    public List<MessageTrace> getTopicTraces(String topic, long startMs, long endMs) {
        List<TraceRecord> records = safeQuery(() -> traceService.queryByTopic(topic, startMs, endMs));
        if (CollectionUtils.isEmpty(records)) {
            return Collections.emptyList();
        }
        Map<String, List<TraceRecord>> grouped = new LinkedHashMap<>();
        for (TraceRecord record : records) {
            String mid = StringUtils.isNotEmpty(record.messageId()) ? record.messageId() : "unknown";
            grouped.computeIfAbsent(mid, k -> new ArrayList<>()).add(record);
        }
        List<MessageTrace> traces = new ArrayList<>(grouped.size());
        for (Map.Entry<String, List<TraceRecord>> entry : grouped.entrySet()) {
            traces.add(buildMessageTrace(entry.getKey(), entry.getValue()));
        }
        return traces;
    }

    // ===================== 拓扑构建 =====================

    /**
     * 从 SEND 追踪记录构建生产者节点（按生产者组去重）。
     */
    private List<TopologyNode> buildProducerNodes(String topic, List<TraceRecord> records) {
        Set<String> producerGroups = new LinkedHashSet<>();
        if (CollectionUtils.isNotEmpty(records)) {
            for (TraceRecord record : records) {
                if (record.type() == TraceType.SEND) {
                    producerGroups.add(orDefault(record.group(), DEFAULT_PRODUCER_NAME));
                }
            }
        }
        if (producerGroups.isEmpty()) {
            producerGroups.add(DEFAULT_PRODUCER_NAME);
        }
        List<TopologyNode> nodes = new ArrayList<>(producerGroups.size());
        for (String group : producerGroups) {
            nodes.add(new TopologyNode(group, "PRODUCER", topic, group, true));
        }
        return nodes;
    }

    /**
     * 从监听器容器构建消费者节点（按 Topic 过滤）。
     */
    private List<TopologyNode> buildConsumerNodes(String topic) {
        Collection<ConsumerMetadata> consumers = Objects.isNull(listenerContainer)
            ? Collections.emptyList() : listenerContainer.getConsumers();
        if (CollectionUtils.isEmpty(consumers)) {
            return Collections.emptyList();
        }
        List<TopologyNode> nodes = new ArrayList<>();
        for (ConsumerMetadata metadata : consumers) {
            if (Objects.nonNull(metadata) && Objects.equals(metadata.topic(), topic)) {
                String name = StringUtils.isNotEmpty(metadata.consumerGroup())
                    ? metadata.consumerGroup()
                    : (Objects.nonNull(metadata.consumerType()) ? metadata.consumerType().getSimpleName() : "Consumer");
                nodes.add(new TopologyNode(name, "CONSUMER", topic, metadata.consumerGroup(), true));
            }
        }
        return nodes;
    }

    /**
     * 构建生产者到消费者的路由，速率基于时间窗口内 SEND 记录数估算。
     */
    private List<TopologyRoute> buildRoutes(String topic, List<TopologyNode> producers,
                                            List<TopologyNode> consumers, List<TraceRecord> records) {
        long sendCount = 0;
        if (CollectionUtils.isNotEmpty(records)) {
            for (TraceRecord record : records) {
                if (record.type() == TraceType.SEND) {
                    sendCount++;
                }
            }
        }
        double rate = sendCount > 0 ? (double) sendCount / (TOPOLOGY_WINDOW_MS / 1000.0) : 0.0;
        List<TopologyRoute> routes = new ArrayList<>();
        for (TopologyNode producer : producers) {
            for (TopologyNode consumer : consumers) {
                routes.add(new TopologyRoute(producer.name(), consumer.name(), topic, rate));
            }
        }
        return routes;
    }

    // ===================== 链路构建 =====================

    /**
     * 由追踪记录列表构建单条消息链路。
     */
    private MessageTrace buildMessageTrace(String messageId, List<TraceRecord> records) {
        List<TraceEvent> events = new ArrayList<>();
        String topic = "";
        if (CollectionUtils.isNotEmpty(records)) {
            for (TraceRecord record : records) {
                events.add(toTraceEvent(record));
                if (StringUtils.isEmpty(topic)) {
                    topic = orDefault(record.topic(), "");
                }
            }
        }
        long totalDuration = computeTotalDuration(events);
        List<String> routePath = buildRoutePath(topic, records);
        String finalStatus = computeFinalStatus(events);
        return new MessageTrace(messageId, topic, Collections.unmodifiableList(events),
            totalDuration, Collections.unmodifiableList(routePath), finalStatus);
    }

    /**
     * 将 {@link TraceRecord} 转换为 {@link TraceEvent}。
     */
    private TraceEvent toTraceEvent(TraceRecord record) {
        TraceEventType type = mapEventType(record);
        String detail = buildDetail(record);
        Map<String, String> attrs = Objects.nonNull(record.attributes())
            ? Collections.unmodifiableMap(new LinkedHashMap<>(record.attributes()))
            : Collections.emptyMap();
        return new TraceEvent(type, record.timestamp(), record.durationMillis(),
            record.success(), detail, attrs);
    }

    /**
     * 映射追踪记录类型到事件类型。
     */
    private TraceEventType mapEventType(TraceRecord record) {
        if (record.type() == TraceType.SEND) {
            return TraceEventType.SEND;
        }
        Map<String, String> attrs = record.attributes();
        if (CollectionUtils.isNotEmpty(attrs)) {
            if (StringUtils.isNotEmpty(attrs.get("dlqReason"))) {
                return TraceEventType.DLQ;
            }
            if (StringUtils.isNotEmpty(attrs.get("delayLevel"))) {
                return TraceEventType.DELAY;
            }
        }
        if (parseReconsumeTimes(attrs) > 0) {
            return TraceEventType.RETRY;
        }
        return TraceEventType.CONSUME;
    }

    /**
     * 构建事件详情描述。
     */
    private String buildDetail(TraceRecord record) {
        if (record.type() == TraceType.SEND) {
            return "发送至 Topic=" + orDefault(record.topic(), "unknown");
        }
        return "消费 by Group=" + orDefault(record.group(), "unknown");
    }

    /**
     * 计算链路总耗时（首个事件到末尾事件）。
     */
    private long computeTotalDuration(List<TraceEvent> events) {
        if (events.size() < 2) {
            return 0L;
        }
        long first = events.get(0).timestamp();
        long last = events.get(events.size() - 1).timestamp();
        return Math.max(0L, last - first);
    }

    /**
     * 构建消息流转路径。
     */
    private List<String> buildRoutePath(String topic, List<TraceRecord> records) {
        List<String> path = new ArrayList<>();
        path.add("Producer");
        path.add("Topic:" + orDefault(topic, "unknown"));
        Set<String> seenGroups = new LinkedHashSet<>();
        if (CollectionUtils.isNotEmpty(records)) {
            for (TraceRecord record : records) {
                if (record.type() == TraceType.CONSUME) {
                    String group = orDefault(record.group(), "unknown");
                    if (seenGroups.add(group)) {
                        path.add("Group:" + group);
                        path.add("Consumer:" + group);
                    }
                }
            }
        }
        return path;
    }

    /**
     * 计算链路最终状态。
     */
    private String computeFinalStatus(List<TraceEvent> events) {
        if (events.isEmpty()) {
            return "PROCESSING";
        }
        boolean hasDlq = false;
        boolean hasFailed = false;
        boolean hasConsumeSuccess = false;
        for (TraceEvent event : events) {
            if (event.type() == TraceEventType.DLQ) {
                hasDlq = true;
            }
            if (!event.success()) {
                hasFailed = true;
            }
            if (event.type() == TraceEventType.CONSUME && event.success()) {
                hasConsumeSuccess = true;
            }
        }
        if (hasDlq) {
            return "DLQ";
        }
        if (hasConsumeSuccess && !hasFailed) {
            return "SUCCESS";
        }
        if (hasFailed) {
            return "FAILED";
        }
        return "PROCESSING";
    }

    // ===================== 内部工具 =====================

    /**
     * 安全执行追踪查询，异常时返回空列表。
     */
    private List<TraceRecord> safeQuery(java.util.function.Supplier<List<TraceRecord>> supplier) {
        try {
            List<TraceRecord> records = supplier.get();
            return Objects.isNull(records) ? Collections.emptyList() : records;
        } catch (Exception ex) {
            log.warn("追踪查询失败，返回空列表: {}", ex.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * 从属性中解析重试次数。
     */
    private int parseReconsumeTimes(Map<String, String> attrs) {
        if (CollectionUtils.isEmpty(attrs)) {
            return 0;
        }
        String value = attrs.get("reconsumeTimes");
        if (StringUtils.isEmpty(value)) {
            return 0;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            return 0;
        }
    }

    /**
     * 字符串为空时返回默认值。
     */
    private static String orDefault(String str, String defaultStr) {
        return StringUtils.isNotEmpty(str) ? str : defaultStr;
    }
}
