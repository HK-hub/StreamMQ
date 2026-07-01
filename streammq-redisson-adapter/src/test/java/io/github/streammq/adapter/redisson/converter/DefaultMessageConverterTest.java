package io.github.streammq.adapter.redisson.converter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.message.Message;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link DefaultMessageConverter} 单元测试，覆盖 Stream Entry 字段映射、可选字段省略、
 * 属性合并、往返一致性、非法字段解析与静态回填方法。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("DefaultMessageConverter 默认消息转换器测试")
class DefaultMessageConverterTest {

    private final JacksonJsonSerializer<Object> serializer = new JacksonJsonSerializer<>();
    private final DefaultMessageConverter converter = new DefaultMessageConverter(serializer);
    private final ObjectMapper propsMapper = new ObjectMapper();

    @Test
    @DisplayName("toStreamFields 完整字段映射（body Base64、bodyType 类名、tag/keys/shardingKey/bornTs/bornHost）")
    void toStreamFieldsFullMapping() {
        Message<String> msg = new Message<>();
        msg.setBody("hello");
        msg.setTag("vip");
        msg.setKeys("k1");
        msg.setShardingKey("shard-1");
        msg.setBornTimestamp(123456789L);
        msg.setBornHost("host:8080");

        Map<String, String> fields = converter.toStreamFields(msg);

        assertThat(fields).containsEntry("bodyType", "java.lang.String");
        assertThat(fields).containsEntry("tag", "vip");
        assertThat(fields).containsEntry("keys", "k1");
        assertThat(fields).containsEntry("shardingKey", "shard-1");
        assertThat(fields).containsEntry("bornTs", "123456789");
        assertThat(fields).containsEntry("bornHost", "host:8080");

        String expectedBody = Base64.getEncoder().encodeToString(serializer.serialize("hello", Object.class));
        assertThat(fields).containsEntry("body", expectedBody);
    }

    @Test
    @DisplayName("toStreamFields body 为 null 时不写入 body/bodyType")
    void toStreamFieldsNullBody() {
        Message<String> msg = new Message<>();
        msg.setBornTimestamp(1L);

        Map<String, String> fields = converter.toStreamFields(msg);

        assertThat(fields).doesNotContainKey("body");
        assertThat(fields).doesNotContainKey("bodyType");
        assertThat(fields).containsKey("bornTs");
    }

    @Test
    @DisplayName("toStreamFields 可选字段为 null 时不写入")
    void toStreamFieldsOptionalNull() {
        Message<String> msg = new Message<>();
        msg.setBody("hello");
        msg.setBornTimestamp(1L);

        Map<String, String> fields = converter.toStreamFields(msg);

        assertThat(fields).doesNotContainKey("tag");
        assertThat(fields).doesNotContainKey("keys");
        assertThat(fields).doesNotContainKey("shardingKey");
        assertThat(fields).doesNotContainKey("bornHost");
        assertThat(fields).doesNotContainKey("retryTimes");
        assertThat(fields).doesNotContainKey("txId");
        assertThat(fields).doesNotContainKey("props");
    }

    @Test
    @DisplayName("toStreamFields 包含 properties 与 userProperties 时合并为 JSON 字符串")
    void toStreamFieldsMergedProps() throws Exception {
        Message<String> msg = new Message<>();
        msg.setBody("hello");
        msg.setBornTimestamp(1L);
        msg.putProperty("traceId", "t1");
        msg.putUserProperty("u1", "v1");

        Map<String, String> fields = converter.toStreamFields(msg);

        assertThat(fields).containsKey("props");
        Map<String, String> parsed = propsMapper.readValue(
            fields.get("props"), new TypeReference<Map<String, String>>() {
            });
        assertThat(parsed)
            .containsEntry("traceId", "t1")
            .containsEntry("u1", "v1");
    }

    @Test
    @DisplayName("toStreamFields 包含 retryTimes > 0 时写入")
    void toStreamFieldsRetryTimesPositive() {
        Message<String> msg = new Message<>();
        msg.setBody("hello");
        msg.setBornTimestamp(1L);
        msg.setReconsumeTimes(3);

        Map<String, String> fields = converter.toStreamFields(msg);
        assertThat(fields).containsEntry("retryTimes", "3");
    }

    @Test
    @DisplayName("toStreamFields retryTimes == 0 时不写入")
    void toStreamFieldsRetryTimesZero() {
        Message<String> msg = new Message<>();
        msg.setBody("hello");
        msg.setBornTimestamp(1L);
        msg.setReconsumeTimes(0);

        Map<String, String> fields = converter.toStreamFields(msg);
        assertThat(fields).doesNotContainKey("retryTimes");
    }

    @Test
    @DisplayName("toStreamFields 包含 transactionId 时写入")
    void toStreamFieldsTransactionId() {
        Message<String> msg = new Message<>();
        msg.setBody("hello");
        msg.setBornTimestamp(1L);
        msg.setTransactionId("tx-001");

        Map<String, String> fields = converter.toStreamFields(msg);
        assertThat(fields).containsEntry("txId", "tx-001");
    }

    @Test
    @DisplayName("toStreamFields message 为 null 抛出 NullPointerException")
    void toStreamFieldsNullMessage() {
        assertThatThrownBy(() -> converter.toStreamFields(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("message");
    }

    @Test
    @DisplayName("fromStreamFields 往返：序列化后反序列化字段一致")
    void fromStreamFieldsRoundTrip() {
        Message<String> msg = new Message<>();
        msg.setBody("hello");
        msg.setTag("vip");
        msg.setKeys("k1");
        msg.setShardingKey("shard-1");
        msg.setBornTimestamp(123456789L);
        msg.setBornHost("host:8080");
        msg.setReconsumeTimes(2);
        msg.setTransactionId("tx-1");
        msg.putProperty("traceId", "t1");
        msg.putUserProperty("u1", "v1");

        Map<String, String> fields = converter.toStreamFields(msg);
        Message<String> restored = converter.fromStreamFields(fields, String.class);

        assertThat(restored.getBody()).isEqualTo("hello");
        assertThat(restored.getTag()).isEqualTo("vip");
        assertThat(restored.getKeys()).isEqualTo("k1");
        assertThat(restored.getShardingKey()).isEqualTo("shard-1");
        assertThat(restored.getBornTimestamp()).isEqualTo(123456789L);
        assertThat(restored.getBornHost()).isEqualTo("host:8080");
        assertThat(restored.getReconsumeTimes()).isEqualTo(2);
        assertThat(restored.getTransactionId()).isEqualTo("tx-1");
        assertThat(restored.getUserProperties())
            .containsEntry("traceId", "t1")
            .containsEntry("u1", "v1");
    }

    @Test
    @DisplayName("fromStreamFields 字段缺失场景（只有必填 body/bornTs）")
    void fromStreamFieldsMissingOptional() {
        Map<String, String> fields = new HashMap<>();
        fields.put("body", Base64.getEncoder().encodeToString(serializer.serialize("hi", Object.class)));
        fields.put("bornTs", "999");

        Message<String> msg = converter.fromStreamFields(fields, String.class);

        assertThat(msg.getBody()).isEqualTo("hi");
        assertThat(msg.getBornTimestamp()).isEqualTo(999L);
        assertThat(msg.getTag()).isNull();
        assertThat(msg.getKeys()).isNull();
        assertThat(msg.getShardingKey()).isNull();
        assertThat(msg.getBornHost()).isNull();
        assertThat(msg.getTransactionId()).isNull();
        assertThat(msg.getReconsumeTimes()).isZero();
    }

    @Test
    @DisplayName("fromStreamFields 非法 bornTs 抛出 SerializationException")
    void fromStreamFieldsInvalidBornTs() {
        Map<String, String> fields = new HashMap<>();
        fields.put("body", Base64.getEncoder().encodeToString(serializer.serialize("hi", Object.class)));
        fields.put("bornTs", "not-a-number");

        assertThatThrownBy(() -> converter.fromStreamFields(fields, String.class))
            .isInstanceOf(SerializationException.class)
            .hasMessageContaining("bornTs");
    }

    @Test
    @DisplayName("fromStreamFields 非法 retryTimes 抛出 SerializationException")
    void fromStreamFieldsInvalidRetryTimes() {
        Map<String, String> fields = new HashMap<>();
        fields.put("body", Base64.getEncoder().encodeToString(serializer.serialize("hi", Object.class)));
        fields.put("bornTs", "1");
        fields.put("retryTimes", "abc");

        assertThatThrownBy(() -> converter.fromStreamFields(fields, String.class))
            .isInstanceOf(SerializationException.class)
            .hasMessageContaining("retryTimes");
    }

    @Test
    @DisplayName("fromStreamFields fields 为 null 抛出 NullPointerException")
    void fromStreamFieldsNullFields() {
        assertThatThrownBy(() -> converter.fromStreamFields(null, String.class))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("fields");
    }

    @Test
    @DisplayName("fromStreamFields targetType 为 null 抛出 NullPointerException")
    void fromStreamFieldsNullTargetType() {
        assertThatThrownBy(() -> converter.fromStreamFields(new HashMap<>(), null))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("targetType");
    }

    @Test
    @DisplayName("name 返回 default")
    void name() {
        assertThat(converter.name()).isEqualTo("default");
    }

    @Test
    @DisplayName("applyTopic 为消息回填 topic")
    void applyTopic() {
        Message<String> msg = new Message<>();
        DefaultMessageConverter.applyTopic(msg, "topic-1");
        assertThat(msg.getTopic()).isEqualTo("topic-1");
    }

    @Test
    @DisplayName("applyMessageId 为消息回填 MessageId")
    void applyMessageId() {
        Message<String> msg = new Message<>();
        DefaultMessageConverter.applyMessageId(msg, "123-0");
        assertThat(msg.getMessageId()).isNotNull();
        assertThat(msg.getMessageId().getStreamEntryId()).isEqualTo("123-0");
    }
}
