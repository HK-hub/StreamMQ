package io.github.streammq.adapter.redisson.trace;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.trace.TraceType;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 基于 Redis Stream 的 {@link StreamMQTraceService} 实现。
 *
 * <p>追踪记录按日期存储在 {@code streammq:{ns}:trace:{date}} Stream 中，
 * 查询时遍历对应日期范围内的 trace Stream，在内存中过滤匹配。
 *
 * <p>适用于中小规模追踪数据查询。对于大规模数据场景，建议对接专业 APM 系统
 * （如 Elasticsearch / Zipkin / SkyWalking）。
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
public class RedisStreamMQTraceService implements StreamMQTraceService {

    private static final Logger LOG = LoggerFactory.getLogger(RedisStreamMQTraceService.class);

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.BASIC_ISO_DATE;
    private static final int MAX_READ_COUNT = 10000;

    private final RedissonClient redisson;
    private final String namespace;
    private final ObjectMapper objectMapper;

    /**
     * 构造 Redis 追踪查询服务。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间（可为 null）
     */
    public RedisStreamMQTraceService(RedissonClient redisson, String namespace) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = namespace == null ? "" : namespace;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public List<TraceRecord> queryByMessageId(String messageId) {
        Objects.requireNonNull(messageId, "messageId");
        if (messageId.isEmpty()) {
            return Collections.emptyList();
        }
        // 查询今天和昨天的 trace Stream
        List<String> dates = new ArrayList<>(2);
        dates.add(LocalDate.now().format(DATE_FMT));
        dates.add(LocalDate.now().minusDays(1).format(DATE_FMT));

        List<TraceRecord> results = new ArrayList<>();
        for (String date : dates) {
            List<TraceRecord> records = readTraceStream(date);
            for (TraceRecord record : records) {
                if (messageId.equals(record.messageId())) {
                    results.add(record);
                }
            }
        }
        results.sort(Comparator.comparingLong(TraceRecord::timestamp));
        return results;
    }

    @Override
    public List<TraceRecord> queryByTopic(String topic, long startTimeMs, long endTimeMs) {
        Objects.requireNonNull(topic, "topic");
        if (topic.isEmpty()) {
            return Collections.emptyList();
        }
        return queryByFilter(startTimeMs, endTimeMs,
            record -> topic.equals(record.topic()));
    }

    @Override
    public List<TraceRecord> queryByGroup(String group, long startTimeMs, long endTimeMs) {
        Objects.requireNonNull(group, "group");
        if (group.isEmpty()) {
            return Collections.emptyList();
        }
        return queryByFilter(startTimeMs, endTimeMs,
            record -> group.equals(record.group()));
    }

    /**
     * 按时间范围和过滤条件查询追踪记录。
     *
     * @param startTimeMs 起始时间戳（毫秒）
     * @param endTimeMs 结束时间戳（毫秒）
     * @param filter 过滤条件
     * @return 匹配的追踪记录列表，按时间升序排列
     */
    private List<TraceRecord> queryByFilter(long startTimeMs, long endTimeMs,
                                            java.util.function.Predicate<TraceRecord> filter) {
        List<String> dates = datesBetween(startTimeMs, endTimeMs);
        List<TraceRecord> results = new ArrayList<>();
        for (String date : dates) {
            List<TraceRecord> records = readTraceStream(date);
            for (TraceRecord record : records) {
                if (record.timestamp() >= startTimeMs && record.timestamp() <= endTimeMs && filter.test(record)) {
                    results.add(record);
                }
            }
        }
        results.sort(Comparator.comparingLong(TraceRecord::timestamp));
        return results;
    }

    /**
     * 读取指定日期的 trace Stream 全部记录。
     *
     * @param date 日期字符串（yyyyMMdd）
     * @return 追踪记录列表
     */
    private List<TraceRecord> readTraceStream(String date) {
        String traceKey = StreamMQKeys.traceStream(namespace, date);
        List<TraceRecord> records = new ArrayList<>();
        try {
            RStream<String, String> stream = redisson.getStream(traceKey);
            var entries = stream.range(MAX_READ_COUNT, StreamMessageId.MIN, StreamMessageId.MAX);
            if (entries != null) {
                for (var entry : entries.entrySet()) {
                    TraceRecord record = parseRecord(entry.getValue());
                    if (record != null) {
                        records.add(record);
                    }
                }
            }
        } catch (RuntimeException ex) {
            LOG.debug("Failed to read trace stream for date {}: {}", date, ex.getMessage());
        }
        return records;
    }

    /**
     * 从 Stream Entry 字段解析追踪记录。
     *
     * @param fields Stream Entry 字段
     * @return 追踪记录，解析失败返回 null
     */
    private TraceRecord parseRecord(Map<String, String> fields) {
        if (CollectionUtils.isEmpty(fields)) {
            return null;
        }
        try {
            String messageId = fields.getOrDefault(RedisTraceCollector.FIELD_MESSAGE_ID, "");
            String topic = fields.getOrDefault(RedisTraceCollector.FIELD_TOPIC, "");
            String group = fields.getOrDefault(RedisTraceCollector.FIELD_GROUP, "");
            TraceType type = TraceType.valueOf(
                fields.getOrDefault(RedisTraceCollector.FIELD_TYPE, TraceType.SEND.name()));
            boolean success = Boolean.parseBoolean(
                fields.getOrDefault(RedisTraceCollector.FIELD_SUCCESS, "false"));
            long timestamp = Long.parseLong(
                fields.getOrDefault(RedisTraceCollector.FIELD_TIMESTAMP, "0"));
            long durationMillis = Long.parseLong(
                fields.getOrDefault(RedisTraceCollector.FIELD_DURATION_MILLIS, "0"));
            String traceId = fields.getOrDefault(RedisTraceCollector.FIELD_TRACE_ID, "");
            Map<String, String> attributes = deserializeAttributes(
                fields.get(RedisTraceCollector.FIELD_ATTRIBUTES));
            return new TraceRecord(messageId, topic, group, type, success, timestamp,
                durationMillis, traceId, attributes);
        } catch (RuntimeException ex) {
            LOG.debug("Failed to parse trace record: {}", ex.getMessage());
            return null;
        }
    }

    /**
     * 反序列化扩展属性 JSON 字符串。
     *
     * @param json JSON 字符串
     * @return 属性 Map，null 或空时返回空 Map
     */
    private Map<String, String> deserializeAttributes(String json) {
        if (StringUtils.isEmpty(json)) {
            return Collections.emptyMap();
        }
        try {
            return objectMapper.readValue(json, new TypeReference<Map<String, String>>() {
            });
        } catch (JsonProcessingException ex) {
            return Collections.emptyMap();
        }
    }

    /**
     * 计算时间范围内的日期列表（yyyyMMdd）。
     *
     * @param startTimeMs 起始时间戳
     * @param endTimeMs 结束时间戳
     * @return 日期列表
     */
    private List<String> datesBetween(long startTimeMs, long endTimeMs) {
        List<String> dates = new ArrayList<>();
        ZoneId zone = ZoneId.systemDefault();
        LocalDate start = LocalDate.ofInstant(Instant.ofEpochMilli(startTimeMs), zone);
        LocalDate end = LocalDate.ofInstant(Instant.ofEpochMilli(endTimeMs), zone);
        for (LocalDate d = start; !d.isAfter(end); d = d.plusDays(1)) {
            dates.add(d.format(DATE_FMT));
        }
        return dates;
    }
}
