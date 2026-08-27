/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.trace;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.interceptor.TraceCollector;
import io.github.streammq.core.trace.TraceType;
import io.github.streammq.core.util.CollectionUtils;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamAddArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 Redis Stream 的追踪收集器，将追踪记录写入 {@code streammq:{ns}:trace:{date}} Stream。
 *
 * <p>每条追踪记录作为 Redis Stream 的一个 Entry 存储，字段包括：
 * messageId、topic、group、type、success、timestamp、durationMillis、traceId、attributes。
 *
 * <p>按日期分片存储（date 格式 yyyyMMdd），便于按天查询与过期清理。 当 {@code streammq.trace.enabled=true} 且 {@code
 * streammq.trace.storage=redis} 时启用。
 *
 * <p>追踪写入失败不影响主流程，仅记录 WARN 日志。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class RedisTraceCollector implements TraceCollector {

    private static final Logger LOG = LoggerFactory.getLogger(RedisTraceCollector.class);

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.BASIC_ISO_DATE;

    /** Stream Entry 字段名常量 */
    public static final String FIELD_MESSAGE_ID = "messageId";

    public static final String FIELD_TOPIC = "topic";
    public static final String FIELD_GROUP = "group";
    public static final String FIELD_TYPE = "type";
    public static final String FIELD_SUCCESS = "success";
    public static final String FIELD_TIMESTAMP = "timestamp";
    public static final String FIELD_DURATION_MILLIS = "durationMillis";
    public static final String FIELD_TRACE_ID = "traceId";
    public static final String FIELD_ATTRIBUTES = "attributes";

    /** 追踪 Stream 默认近似 MAXLEN：防止高频追踪写入无界增长拖垮 Redis 内存 */
    public static final int DEFAULT_MAX_STREAM_LEN = 100_000;

    /** 追踪日期 Key 的保留时长（7 天）：过期自动清理，无需外部运维 */
    static final java.time.Duration TRACE_KEY_TTL = java.time.Duration.ofDays(7);

    private final RedissonClient redisson;
    private final String namespace;
    private final ObjectMapper objectMapper;

    /** 追踪 Stream 近似 MAXLEN（可通过 {@link #setMaxStreamLen(int)} 覆盖；&lt;=0 表示不限制） */
    private volatile int maxStreamLen = DEFAULT_MAX_STREAM_LEN;

    /**
     * 设置追踪 Stream 的近似 MAXLEN（XADD MAXLEN ~）。
     *
     * @param maxStreamLen 最大长度（&lt;=0 表示不限制）
     */
    public void setMaxStreamLen(int maxStreamLen) {
        this.maxStreamLen = maxStreamLen;
    }

    /**
     * 构造 Redis 追踪收集器。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间（可为 null）
     */
    public RedisTraceCollector(RedissonClient redisson, String namespace) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = namespace == null ? "" : namespace;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void recordSend(SendTraceContext context) {
        if (context == null) {
            return;
        }
        try {
            Map<String, String> fields = new HashMap<>(9);
            fields.put(
                    FIELD_MESSAGE_ID,
                    context.messageId() != null ? context.messageId().getStreamEntryId() : "");
            fields.put(FIELD_TOPIC, nullToEmpty(context.topic()));
            fields.put(FIELD_GROUP, nullToEmpty(context.producerGroup()));
            fields.put(FIELD_TYPE, TraceType.SEND.name());
            fields.put(FIELD_SUCCESS, String.valueOf(context.success()));
            fields.put(FIELD_TIMESTAMP, Long.toString(System.currentTimeMillis()));
            fields.put(FIELD_DURATION_MILLIS, Long.toString(context.durationMillis()));
            fields.put(FIELD_TRACE_ID, nullToEmpty(context.traceId()));
            fields.put(
                    FIELD_ATTRIBUTES,
                    serializeAttributes(withTag(context.attributes(), context.tag())));
            writeTrace(fields);
        } catch (Exception ex) {
            LOG.warn(
                    "Failed to record send trace: topic={}, messageId={}: {}",
                    context.topic(),
                    context.messageId(),
                    ex.getMessage());
        }
    }

    @Override
    public void recordConsume(ConsumeTraceContext context) {
        if (context == null) {
            return;
        }
        try {
            Map<String, String> fields = new HashMap<>(9);
            fields.put(
                    FIELD_MESSAGE_ID,
                    context.messageId() != null ? context.messageId().getStreamEntryId() : "");
            fields.put(FIELD_TOPIC, nullToEmpty(context.topic()));
            fields.put(FIELD_GROUP, nullToEmpty(context.consumerGroup()));
            fields.put(FIELD_TYPE, TraceType.CONSUME.name());
            fields.put(FIELD_SUCCESS, String.valueOf(context.success()));
            fields.put(FIELD_TIMESTAMP, Long.toString(System.currentTimeMillis()));
            fields.put(FIELD_DURATION_MILLIS, Long.toString(context.durationMillis()));
            fields.put(FIELD_TRACE_ID, nullToEmpty(context.traceId()));
            fields.put(FIELD_ATTRIBUTES, serializeAttributes(withConsumeMetadata(context)));
            writeTrace(fields);
        } catch (Exception ex) {
            LOG.warn(
                    "Failed to record consume trace: topic={}, messageId={}: {}",
                    context.topic(),
                    context.messageId(),
                    ex.getMessage());
        }
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public String name() {
        return "redis";
    }

    /**
     * 将追踪记录写入当天的 trace Stream。
     *
     * <p>每次 XADD 附加近似 MAXLEN 截断（默认 {@link #DEFAULT_MAX_STREAM_LEN}，可经 setter 覆盖）， 并对日期 Key 施加 7 天
     * EXPIRE——双重兜底防止追踪流无界增长。保持同步写入语义。
     *
     * @param fields Stream Entry 字段
     */
    private void writeTrace(Map<String, String> fields) {
        String date = LocalDate.now().format(DATE_FMT);
        String traceKey = StreamMQKeys.traceStream(namespace, date);
        RStream<String, String> stream = redisson.getStream(traceKey);
        int limit = maxStreamLen;
        if (limit > 0) {
            stream.add(StreamAddArgs.entries(fields).trimNonStrict().maxLen(limit).noLimit());
        } else {
            stream.add(StreamAddArgs.entries(fields));
        }
        try {
            stream.expire(TRACE_KEY_TTL);
        } catch (RuntimeException ex) {
            // TTL 设置失败不影响追踪写入主流程（MAXLEN 已兜底内存增长）
            LOG.debug("Trace key expire failed: {}: {}", traceKey, ex.getMessage());
        }
    }

    /**
     * 序列化扩展属性为 JSON 字符串。
     *
     * @param attributes 扩展属性
     * @return JSON 字符串，null 或空时返回空字符串
     */
    private String serializeAttributes(Map<String, String> attributes) {
        if (CollectionUtils.isEmpty(attributes)) {
            return "";
        }
        try {
            return objectMapper.writeValueAsString(attributes);
        } catch (JsonProcessingException ex) {
            LOG.warn("Failed to serialize trace attributes: {}", ex.getMessage());
            return "";
        }
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    /** 将 tag 添加到属性 Map，若 tag 为 null 或空则原样返回。 */
    private Map<String, String> withTag(Map<String, String> attributes, String tag) {
        if (tag == null || tag.isEmpty()) {
            return attributes != null ? attributes : new HashMap<>();
        }
        Map<String, String> result =
                attributes != null ? new HashMap<>(attributes) : new HashMap<>();
        result.put(StreamMQConstants.TRACE_ATTR_TAG, tag);
        return result;
    }

    /** 为消费记录添加消费者名称与重试次数等元数据。 */
    private Map<String, String> withConsumeMetadata(ConsumeTraceContext context) {
        Map<String, String> result =
                context.attributes() != null
                        ? new HashMap<>(context.attributes())
                        : new HashMap<>();
        if (context.consumerName() != null && !context.consumerName().isEmpty()) {
            result.put(StreamMQConstants.TRACE_ATTR_CONSUMER_NAME, context.consumerName());
        }
        if (context.reconsumeTimes() > 0) {
            result.put(
                    StreamMQConstants.TRACE_ATTR_RECONSUME_TIMES,
                    String.valueOf(context.reconsumeTimes()));
        }
        if (context.tag() != null && !context.tag().isEmpty()) {
            result.put(StreamMQConstants.TRACE_ATTR_TAG, context.tag());
        }
        return result;
    }
}
