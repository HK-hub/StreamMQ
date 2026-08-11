package io.github.streammq.adapter.redisson.trace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.trace.TraceType;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;

/**
 * {@link RedisStreamMQTraceService} 单元测试，覆盖按消息 ID / Topic / 消费组查询、 时间范围过滤、结果排序、空入参与异常容忍场景。
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
@DisplayName("RedisStreamMQTraceService Redis 追踪查询服务测试")
class RedisStreamMQTraceServiceTest {

    private RedissonClient redisson;
    private RStream<String, String> stream;
    private RedisStreamMQTraceService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisson = mock(RedissonClient.class);
        stream = mock(RStream.class);
        doReturn(stream).when(redisson).getStream(anyString());
        service = new RedisStreamMQTraceService(redisson, "ns");
    }

    /** 构造一条追踪记录的 Stream Entry 字段。 */
    private Map<String, String> buildFields(
            String messageId,
            String topic,
            String group,
            TraceType type,
            boolean success,
            long timestamp,
            long duration,
            String traceId,
            String attributesJson) {
        Map<String, String> fields = new HashMap<>(9);
        fields.put(RedisTraceCollector.FIELD_MESSAGE_ID, messageId);
        fields.put(RedisTraceCollector.FIELD_TOPIC, topic);
        fields.put(RedisTraceCollector.FIELD_GROUP, group);
        fields.put(RedisTraceCollector.FIELD_TYPE, type.name());
        fields.put(RedisTraceCollector.FIELD_SUCCESS, String.valueOf(success));
        fields.put(RedisTraceCollector.FIELD_TIMESTAMP, Long.toString(timestamp));
        fields.put(RedisTraceCollector.FIELD_DURATION_MILLIS, Long.toString(duration));
        fields.put(RedisTraceCollector.FIELD_TRACE_ID, traceId);
        fields.put(RedisTraceCollector.FIELD_ATTRIBUTES, attributesJson);
        return fields;
    }

    /** 构造包含一条记录的 entries Map。 */
    private Map<StreamMessageId, Map<String, String>> singleEntry(Map<String, String> fields) {
        Map<StreamMessageId, Map<String, String>> entries = new HashMap<>(1);
        entries.put(StreamMessageId.MIN, fields);
        return entries;
    }

    @Test
    @DisplayName("queryByMessageId 返回匹配的追踪记录")
    void queryByMessageIdFound() {
        Map<String, String> fields =
                buildFields(
                        "msg-1",
                        "topic-1",
                        "cg-1",
                        TraceType.SEND,
                        true,
                        1000L,
                        10L,
                        "trace-1",
                        "{}");

        // 第一次调用（今天）返回 entries，第二次（昨天）返回空
        when(stream.range(anyInt(), any(StreamMessageId.class), any(StreamMessageId.class)))
                .thenReturn(singleEntry(fields))
                .thenReturn(Collections.emptyMap());

        List<TraceRecord> results = service.queryByMessageId("msg-1");

        assertThat(results).hasSize(1);
        TraceRecord record = results.get(0);
        assertThat(record.messageId()).isEqualTo("msg-1");
        assertThat(record.topic()).isEqualTo("topic-1");
        assertThat(record.group()).isEqualTo("cg-1");
        assertThat(record.type()).isEqualTo(TraceType.SEND);
        assertThat(record.success()).isTrue();
        assertThat(record.timestamp()).isEqualTo(1000L);
        assertThat(record.durationMillis()).isEqualTo(10L);
        assertThat(record.traceId()).isEqualTo("trace-1");
        assertThat(record.attributes()).isEmpty();
    }

    @Test
    @DisplayName("queryByMessageId 不匹配时返回空列表")
    void queryByMessageIdNotFound() {
        Map<String, String> fields =
                buildFields(
                        "msg-1",
                        "topic-1",
                        "cg-1",
                        TraceType.SEND,
                        true,
                        1000L,
                        10L,
                        "trace-1",
                        "{}");

        when(stream.range(anyInt(), any(StreamMessageId.class), any(StreamMessageId.class)))
                .thenReturn(singleEntry(fields))
                .thenReturn(Collections.emptyMap());

        List<TraceRecord> results = service.queryByMessageId("msg-other");

        assertThat(results).isEmpty();
    }

    @Test
    @DisplayName("queryByMessageId 空 messageId 返回空列表")
    void queryByMessageIdEmpty() {
        List<TraceRecord> results = service.queryByMessageId("");

        assertThat(results).isEmpty();
    }

    @Test
    @DisplayName("queryByMessageId null 抛出 NullPointerException")
    void queryByMessageIdNull() {
        assertThatThrownBy(() -> service.queryByMessageId(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("messageId");
    }

    @Test
    @DisplayName("queryByMessageId Stream 读取异常时返回空列表")
    void queryByMessageIdStreamException() {
        when(stream.range(anyInt(), any(StreamMessageId.class), any(StreamMessageId.class)))
                .thenThrow(new RuntimeException("redis down"));

        List<TraceRecord> results = service.queryByMessageId("msg-1");

        assertThat(results).isEmpty();
    }

    @Test
    @DisplayName("queryByMessageId 多条匹配结果按时间戳升序排列")
    void queryByMessageIdSortedByTimestamp() {
        Map<String, String> fields1 =
                buildFields(
                        "msg-1",
                        "topic-1",
                        "cg-1",
                        TraceType.SEND,
                        true,
                        2000L,
                        10L,
                        "trace-1",
                        "{}");
        Map<String, String> fields2 =
                buildFields(
                        "msg-1",
                        "topic-1",
                        "cg-1",
                        TraceType.CONSUME,
                        true,
                        1000L,
                        5L,
                        "trace-1",
                        "{}");

        Map<StreamMessageId, Map<String, String>> todayEntries = new HashMap<>(2);
        todayEntries.put(StreamMessageId.MIN, fields1);
        todayEntries.put(new StreamMessageId(2, 0), fields2);

        when(stream.range(anyInt(), any(StreamMessageId.class), any(StreamMessageId.class)))
                .thenReturn(todayEntries)
                .thenReturn(Collections.emptyMap());

        List<TraceRecord> results = service.queryByMessageId("msg-1");

        assertThat(results).hasSize(2);
        assertThat(results.get(0).timestamp()).isEqualTo(1000L);
        assertThat(results.get(1).timestamp()).isEqualTo(2000L);
        assertThat(results.get(0).type()).isEqualTo(TraceType.CONSUME);
        assertThat(results.get(1).type()).isEqualTo(TraceType.SEND);
    }

    @Test
    @DisplayName("queryByTopic 返回匹配的追踪记录")
    void queryByTopicFound() {
        long now = System.currentTimeMillis();
        Map<String, String> fields =
                buildFields(
                        "msg-1",
                        "topic-1",
                        "cg-1",
                        TraceType.SEND,
                        true,
                        now,
                        10L,
                        "trace-1",
                        "{}");

        when(stream.range(anyInt(), any(StreamMessageId.class), any(StreamMessageId.class)))
                .thenReturn(singleEntry(fields));

        List<TraceRecord> results = service.queryByTopic("topic-1", now - 1000, now + 1000);

        assertThat(results).hasSize(1);
        assertThat(results.get(0).topic()).isEqualTo("topic-1");
    }

    @Test
    @DisplayName("queryByTopic 不匹配时返回空列表")
    void queryByTopicNotFound() {
        long now = System.currentTimeMillis();
        Map<String, String> fields =
                buildFields(
                        "msg-1",
                        "topic-1",
                        "cg-1",
                        TraceType.SEND,
                        true,
                        now,
                        10L,
                        "trace-1",
                        "{}");

        when(stream.range(anyInt(), any(StreamMessageId.class), any(StreamMessageId.class)))
                .thenReturn(singleEntry(fields));

        List<TraceRecord> results = service.queryByTopic("topic-other", now - 1000, now + 1000);

        assertThat(results).isEmpty();
    }

    @Test
    @DisplayName("queryByTopic 时间范围外的记录被过滤")
    void queryByTopicTimeRangeFilter() {
        long now = System.currentTimeMillis();
        Map<String, String> fields =
                buildFields(
                        "msg-1",
                        "topic-1",
                        "cg-1",
                        TraceType.SEND,
                        true,
                        now,
                        10L,
                        "trace-1",
                        "{}");

        when(stream.range(anyInt(), any(StreamMessageId.class), any(StreamMessageId.class)))
                .thenReturn(singleEntry(fields));

        // 查询时间范围在记录之前
        List<TraceRecord> results = service.queryByTopic("topic-1", now - 5000, now - 1000);

        assertThat(results).isEmpty();
    }

    @Test
    @DisplayName("queryByTopic 空 topic 返回空列表")
    void queryByTopicEmpty() {
        long now = System.currentTimeMillis();
        List<TraceRecord> results = service.queryByTopic("", now - 1000, now + 1000);

        assertThat(results).isEmpty();
    }

    @Test
    @DisplayName("queryByTopic null 抛出 NullPointerException")
    void queryByTopicNull() {
        long now = System.currentTimeMillis();
        assertThatThrownBy(() -> service.queryByTopic(null, now - 1000, now + 1000))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("topic");
    }

    @Test
    @DisplayName("queryByGroup 返回匹配的追踪记录")
    void queryByGroupFound() {
        long now = System.currentTimeMillis();
        Map<String, String> fields =
                buildFields(
                        "msg-1",
                        "topic-1",
                        "cg-1",
                        TraceType.CONSUME,
                        true,
                        now,
                        5L,
                        "trace-1",
                        "{}");

        when(stream.range(anyInt(), any(StreamMessageId.class), any(StreamMessageId.class)))
                .thenReturn(singleEntry(fields));

        List<TraceRecord> results = service.queryByGroup("cg-1", now - 1000, now + 1000);

        assertThat(results).hasSize(1);
        assertThat(results.get(0).group()).isEqualTo("cg-1");
        assertThat(results.get(0).type()).isEqualTo(TraceType.CONSUME);
    }

    @Test
    @DisplayName("queryByGroup 不匹配时返回空列表")
    void queryByGroupNotFound() {
        long now = System.currentTimeMillis();
        Map<String, String> fields =
                buildFields(
                        "msg-1",
                        "topic-1",
                        "cg-1",
                        TraceType.CONSUME,
                        true,
                        now,
                        5L,
                        "trace-1",
                        "{}");

        when(stream.range(anyInt(), any(StreamMessageId.class), any(StreamMessageId.class)))
                .thenReturn(singleEntry(fields));

        List<TraceRecord> results = service.queryByGroup("cg-other", now - 1000, now + 1000);

        assertThat(results).isEmpty();
    }

    @Test
    @DisplayName("queryByGroup 空 group 返回空列表")
    void queryByGroupEmpty() {
        long now = System.currentTimeMillis();
        List<TraceRecord> results = service.queryByGroup("", now - 1000, now + 1000);

        assertThat(results).isEmpty();
    }

    @Test
    @DisplayName("queryByGroup null 抛出 NullPointerException")
    void queryByGroupNull() {
        long now = System.currentTimeMillis();
        assertThatThrownBy(() -> service.queryByGroup(null, now - 1000, now + 1000))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("group");
    }

    @Test
    @DisplayName("attributes JSON 被正确反序列化")
    void attributesDeserialization() {
        long now = System.currentTimeMillis();
        Map<String, String> fields =
                buildFields(
                        "msg-1",
                        "topic-1",
                        "cg-1",
                        TraceType.SEND,
                        true,
                        now,
                        10L,
                        "trace-1",
                        "{\"region\":\"us-east-1\",\"node\":\"n1\"}");

        when(stream.range(anyInt(), any(StreamMessageId.class), any(StreamMessageId.class)))
                .thenReturn(singleEntry(fields))
                .thenReturn(Collections.emptyMap());

        List<TraceRecord> results = service.queryByMessageId("msg-1");

        assertThat(results).hasSize(1);
        assertThat(results.get(0).attributes())
                .containsEntry("region", "us-east-1")
                .containsEntry("node", "n1");
    }

    @Test
    @DisplayName("attributes 为空字符串时反序列化为空 Map")
    void emptyAttributesDeserialization() {
        long now = System.currentTimeMillis();
        Map<String, String> fields =
                buildFields(
                        "msg-1", "topic-1", "cg-1", TraceType.SEND, true, now, 10L, "trace-1", "");

        when(stream.range(anyInt(), any(StreamMessageId.class), any(StreamMessageId.class)))
                .thenReturn(singleEntry(fields))
                .thenReturn(Collections.emptyMap());

        List<TraceRecord> results = service.queryByMessageId("msg-1");

        assertThat(results).hasSize(1);
        assertThat(results.get(0).attributes()).isEmpty();
    }

    @Test
    @DisplayName("success=false 被正确解析")
    void failureTraceRecord() {
        long now = System.currentTimeMillis();
        Map<String, String> fields =
                buildFields(
                        "msg-1",
                        "topic-1",
                        "cg-1",
                        TraceType.CONSUME,
                        false,
                        now,
                        50L,
                        "trace-1",
                        "{}");

        when(stream.range(anyInt(), any(StreamMessageId.class), any(StreamMessageId.class)))
                .thenReturn(singleEntry(fields));

        List<TraceRecord> results = service.queryByTopic("topic-1", now - 1000, now + 1000);

        assertThat(results).hasSize(1);
        assertThat(results.get(0).success()).isFalse();
    }

    @Test
    @DisplayName("构造 redisson 为 null 抛出 NullPointerException")
    void constructNullRedisson() {
        assertThatThrownBy(() -> new RedisStreamMQTraceService(null, "ns"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("redisson");
    }

    @Test
    @DisplayName("namespace 为 null 时正常工作")
    void nullNamespace() {
        RedisStreamMQTraceService nullNsService = new RedisStreamMQTraceService(redisson, null);
        long now = System.currentTimeMillis();
        Map<String, String> fields =
                buildFields(
                        "msg-1",
                        "topic-1",
                        "cg-1",
                        TraceType.SEND,
                        true,
                        now,
                        10L,
                        "trace-1",
                        "{}");

        when(stream.range(anyInt(), any(StreamMessageId.class), any(StreamMessageId.class)))
                .thenReturn(singleEntry(fields))
                .thenReturn(Collections.emptyMap());

        List<TraceRecord> results = nullNsService.queryByMessageId("msg-1");

        assertThat(results).hasSize(1);
    }

    @Test
    @DisplayName("跨天时间范围查询覆盖多天 trace Stream")
    void queryByTopicCrossDayRange() {
        LocalDate today = LocalDate.now();
        long startMs =
                today.minusDays(1).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long endMs = today.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli() + 1000;

        Map<String, String> fields =
                buildFields(
                        "msg-1",
                        "topic-1",
                        "cg-1",
                        TraceType.SEND,
                        true,
                        startMs + 1000,
                        10L,
                        "trace-1",
                        "{}");

        // 跨天查询会读取 2 个 trace Stream
        when(stream.range(anyInt(), any(StreamMessageId.class), any(StreamMessageId.class)))
                .thenReturn(singleEntry(fields))
                .thenReturn(Collections.emptyMap());

        List<TraceRecord> results = service.queryByTopic("topic-1", startMs, endMs);

        assertThat(results).hasSize(1);
        assertThat(results.get(0).topic()).isEqualTo("topic-1");
    }
}
