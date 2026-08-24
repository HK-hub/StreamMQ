package io.github.streammq.adapter.redisson.trace;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.trace.TraceType;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
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
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 Redis Stream 的 {@link StreamMQTraceService} 实现。
 *
 * <p>追踪记录按日期存储在 {@code streammq:{ns}:trace:{date}} Stream 中， 查询时遍历对应日期范围内的 trace Stream，在内存中过滤匹配。
 *
 * <p>限制（有意为之，适用于中小规模追踪数据）：
 *
 * <ul>
 *   <li>{@link #queryByMessageId} 仅查询<b>今天与昨天</b>两个 Stream，更早数据需直接查询对应日期 Stream
 *   <li>单日单次查询最多读取 {@code streammq.trace.max-read-count}（默认 10000）条记录，超出部分静默截断
 *   <li>时间范围查询遍历范围内的每一天，全量内存过滤
 * </ul>
 *
 * <p>对于大规模数据场景，建议对接专业 APM 系统 （如 Elasticsearch / Zipkin / SkyWalking）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class RedisStreamMQTraceService implements StreamMQTraceService {

    private static final Logger LOG = LoggerFactory.getLogger(RedisStreamMQTraceService.class);

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.BASIC_ISO_DATE;

    /** 单日单次查询默认最大读取条数 */
    private static final int DEFAULT_MAX_READ_COUNT =
            StreamMQConstants.DEFAULT_TRACE_MAX_READ_COUNT;

    private final RedissonClient redisson;
    private final String namespace;
    private final ObjectMapper objectMapper;

    /** 单日单次查询最大读取条数，可通过 {@link #setMaxReadCount(int)} 覆盖 */
    private volatile int maxReadCount = DEFAULT_MAX_READ_COUNT;

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

    /**
     * 设置单日单次查询最大读取条数。
     *
     * @param maxReadCount 最大读取条数，必须 &gt; 0
     */
    public void setMaxReadCount(int maxReadCount) {
        if (maxReadCount > 0) {
            this.maxReadCount = maxReadCount;
        }
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
        return queryByFilter(startTimeMs, endTimeMs, record -> topic.equals(record.topic()));
    }

    @Override
    public List<TraceRecord> queryByGroup(String group, long startTimeMs, long endTimeMs) {
        Objects.requireNonNull(group, "group");
        if (group.isEmpty()) {
            return Collections.emptyList();
        }
        return queryByFilter(startTimeMs, endTimeMs, record -> group.equals(record.group()));
    }

    /**
     * 按时间范围和过滤条件查询追踪记录。
     *
     * @param startTimeMs 起始时间戳（毫秒）
     * @param endTimeMs 结束时间戳（毫秒）
     * @param filter 过滤条件
     * @return 匹配的追踪记录列表，按时间升序排列
     */
    private List<TraceRecord> queryByFilter(
            long startTimeMs, long endTimeMs, java.util.function.Predicate<TraceRecord> filter) {
        List<String> dates = datesBetween(startTimeMs, endTimeMs);
        List<TraceRecord> results = new ArrayList<>();
        for (String date : dates) {
            List<TraceRecord> records = readTraceStream(date);
            for (TraceRecord record : records) {
                if (record.timestamp() >= startTimeMs
                        && record.timestamp() <= endTimeMs
                        && filter.test(record)) {
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
            var entries = stream.range(maxReadCount, StreamMessageId.MIN, StreamMessageId.MAX);
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
            TraceType type =
                    TraceType.valueOf(
                            fields.getOrDefault(
                                    RedisTraceCollector.FIELD_TYPE, TraceType.SEND.name()));
            boolean success =
                    Boolean.parseBoolean(
                            fields.getOrDefault(RedisTraceCollector.FIELD_SUCCESS, "false"));
            long timestamp =
                    Long.parseLong(fields.getOrDefault(RedisTraceCollector.FIELD_TIMESTAMP, "0"));
            long durationMillis =
                    Long.parseLong(
                            fields.getOrDefault(RedisTraceCollector.FIELD_DURATION_MILLIS, "0"));
            String traceId = fields.getOrDefault(RedisTraceCollector.FIELD_TRACE_ID, "");
            Map<String, String> attributes =
                    deserializeAttributes(fields.get(RedisTraceCollector.FIELD_ATTRIBUTES));
            return new TraceRecord(
                    messageId,
                    topic,
                    group,
                    type,
                    success,
                    timestamp,
                    durationMillis,
                    traceId,
                    attributes);
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
            return objectMapper.readValue(json, new TypeReference<Map<String, String>>() {});
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
