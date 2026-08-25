/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.streammq.adapter.redisson.compression.GzipCompressionCodec;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import io.github.streammq.core.compression.CompressionCodec;
import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.message.Message;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link DefaultMessageConverter} 单元测试，覆盖 Stream Entry 字段映射、可选字段省略、属性合并、 往返一致性、非法字段解析与
 * Topic/MessageId 派生方法。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("DefaultMessageConverter 默认消息转换器测试")
class DefaultMessageConverterTest {

    private final JacksonJsonSerializer<Object> serializer = new JacksonJsonSerializer<>();
    private final DefaultMessageConverter converter = new DefaultMessageConverter(serializer);
    private final ObjectMapper propsMapper = new ObjectMapper();

    /** 构造测试消息（不可变工厂）。 */
    private Message<String> msg(
            String body,
            String tag,
            String keys,
            String shardingKey,
            long bornTs,
            String bornHost,
            int retryTimes,
            String txId) {
        return new Message<>(
                "test-topic",
                tag,
                keys,
                shardingKey,
                null,
                null,
                body,
                (DelayLevel) null,
                null,
                bornTs,
                bornHost,
                txId,
                retryTimes);
    }

    /** 最小消息：仅 body + bornTs。 */
    private Message<String> msg(String body, long bornTs) {
        return msg(body, null, null, null, bornTs, null, 0, null);
    }

    @Test
    @DisplayName(
            "toStreamFields 完整字段映射（body Base64、bodyType 类名、tag/keys/shardingKey/bornTs/bornHost）")
    void toStreamFieldsFullMapping() {
        Message<String> m = msg("hello", "vip", "k1", "shard-1", 123456789L, "host:8080", 0, null);

        Map<String, String> fields = converter.toStreamFields(m);

        assertThat(fields).containsEntry("bodyType", "java.lang.String");
        assertThat(fields).containsEntry("tag", "vip");
        assertThat(fields).containsEntry("keys", "k1");
        assertThat(fields).containsEntry("shardingKey", "shard-1");
        assertThat(fields).containsEntry("bornTs", "123456789");
        assertThat(fields).containsEntry("bornHost", "host:8080");

        String expectedBody =
                Base64.getEncoder().encodeToString(serializer.serialize("hello", Object.class));
        assertThat(fields).containsEntry("body", expectedBody);
    }

    @Test
    @DisplayName("toStreamFields body 为 null 时不写入 body/bodyType")
    void toStreamFieldsNullBody() {
        Map<String, String> fields = converter.toStreamFields(msg(null, 1L));

        assertThat(fields).doesNotContainKey("body");
        assertThat(fields).doesNotContainKey("bodyType");
        assertThat(fields).containsKey("bornTs");
    }

    @Test
    @DisplayName("toStreamFields 可选字段为 null 时不写入")
    void toStreamFieldsOptionalNull() {
        Map<String, String> fields = converter.toStreamFields(msg("hello", 1L));

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
        Message<String> m =
                msg("hello", 1L).addProperty("traceId", "t1").addUserProperty("u1", "v1");

        Map<String, String> fields = converter.toStreamFields(m);

        assertThat(fields).containsKey("props");
        Map<String, String> parsed =
                propsMapper.readValue(
                        fields.get("props"), new TypeReference<Map<String, String>>() {});
        assertThat(parsed).containsEntry("traceId", "t1").containsEntry("u1", "v1");
    }

    @Test
    @DisplayName("toStreamFields 包含 retryTimes > 0 时写入")
    void toStreamFieldsRetryTimesPositive() {
        Map<String, String> fields =
                converter.toStreamFields(msg("hello", 1L).withReconsumeTimes(3));
        assertThat(fields).containsEntry("retryTimes", "3");
    }

    @Test
    @DisplayName("toStreamFields retryTimes == 0 时不写入")
    void toStreamFieldsRetryTimesZero() {
        Map<String, String> fields = converter.toStreamFields(msg("hello", 1L));
        assertThat(fields).doesNotContainKey("retryTimes");
    }

    @Test
    @DisplayName("toStreamFields 包含 transactionId 时写入")
    void toStreamFieldsTransactionId() {
        Map<String, String> fields =
                converter.toStreamFields(msg("hello", 1L).withTransactionId("tx-001"));
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
        Message<String> source =
                msg("hello", "vip", "k1", "shard-1", 123456789L, "host:8080", 2, "tx-1")
                        .addProperty("traceId", "t1")
                        .addUserProperty("u1", "v1");

        Map<String, String> fields = converter.toStreamFields(source);
        Message<String> restored = converter.fromStreamFields(fields, String.class, "order-topic");

        assertThat(restored.getTopic()).isEqualTo("order-topic");
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
    @DisplayName("fromStreamFields 字段缺失场景（只有必填 body/bodyType/bornTs）")
    void fromStreamFieldsMissingOptional() {
        Map<String, String> fields = new HashMap<>();
        fields.put(
                "body",
                Base64.getEncoder().encodeToString(serializer.serialize("hi", Object.class)));
        fields.put("bodyType", String.class.getName());
        fields.put("bornTs", "999");

        Message<String> restored =
                converter.fromStreamFields(fields, String.class, "fallback-topic");

        assertThat(restored.getTopic()).isEqualTo("fallback-topic");
        assertThat(restored.getBody()).isEqualTo("hi");
        assertThat(restored.getBornTimestamp()).isEqualTo(999L);
        assertThat(restored.getTag()).isNull();
        assertThat(restored.getKeys()).isNull();
        assertThat(restored.getShardingKey()).isNull();
        assertThat(restored.getBornHost()).isNull();
        assertThat(restored.getTransactionId()).isNull();
        assertThat(restored.getReconsumeTimes()).isZero();
    }

    @Test
    @DisplayName("fromStreamFields 非法 bornTs 抛出 SerializationException")
    void fromStreamFieldsInvalidBornTs() {
        Map<String, String> fields = new HashMap<>();
        fields.put(
                "body",
                Base64.getEncoder().encodeToString(serializer.serialize("hi", Object.class)));
        fields.put("bodyType", String.class.getName());
        fields.put("bornTs", "not-a-number");

        assertThatThrownBy(() -> converter.fromStreamFields(fields, String.class))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("bornTs");
    }

    @Test
    @DisplayName("fromStreamFields 非法 retryTimes 抛出 SerializationException")
    void fromStreamFieldsInvalidRetryTimes() {
        Map<String, String> fields = new HashMap<>();
        fields.put(
                "body",
                Base64.getEncoder().encodeToString(serializer.serialize("hi", Object.class)));
        fields.put("bodyType", String.class.getName());
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
    @DisplayName("fromStreamFields 缺失 topic 且无 fallbackTopic 抛出 SerializationException")
    void fromStreamFieldsMissingTopicWithoutFallback() {
        Map<String, String> fields = new HashMap<>();
        fields.put("body", "raw");
        fields.put("bornTs", "1");

        assertThatThrownBy(() -> converter.fromStreamFields(fields, String.class))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("topic");
    }

    @Test
    @DisplayName("fromStreamFields 缺失 topic 时使用 fallbackTopic 回填")
    void fromStreamFieldsFallbackTopic() {
        Map<String, String> fields = new HashMap<>();
        fields.put("body", "raw");
        fields.put("bornTs", "1");

        Message<String> restored =
                converter.fromStreamFields(fields, String.class, "fallback-topic");

        assertThat(restored.getTopic()).isEqualTo("fallback-topic");
        assertThat(restored.getBody()).isEqualTo("raw");
    }

    @Test
    @DisplayName("name 返回 default")
    void name() {
        assertThat(converter.name()).isEqualTo("default");
    }

    @Test
    @DisplayName("applyTopic 返回携带指定 topic 的派生实例")
    void applyTopic() {
        Message<String> derived = DefaultMessageConverter.applyTopic(msg("b", 1L), "topic-1");
        assertThat(derived.getTopic()).isEqualTo("topic-1");
        // 原实例不变
        assertThat(msg("b", 1L).getTopic()).isEqualTo("test-topic");
    }

    @Test
    @DisplayName("applyMessageId 返回携带 MessageId 的派生实例")
    void applyMessageId() {
        Message<String> derived = DefaultMessageConverter.applyMessageId(msg("b", 1L), "123-0");
        assertThat(derived.getMessageId()).isNotNull();
        assertThat(derived.getMessageId().getStreamEntryId()).isEqualTo("123-0");
    }

    // ===================== 跨平台反序列化测试 =====================

    /** 自定义 POJO，用于验证跨平台 JSON 反序列化。 */
    public static class UserDto {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    @Test
    @DisplayName("跨平台：bodyType 缺失 + targetType=String → 返回原始字符串（Go JSON 场景）")
    void crossPlatformRawStringBody() {
        // 模拟 Go 发送的原始 JSON 字符串（未经 Base64 编码，无 bodyType 字段）
        String rawJson = "{\"name\":\"Alice\",\"age\":30}";
        Map<String, String> fields = new HashMap<>();
        fields.put("body", rawJson);
        fields.put("bornTs", "1");

        Message<String> restored = converter.fromStreamFields(fields, String.class, "t");

        assertThat(restored.getBody()).isEqualTo(rawJson);
    }

    @Test
    @DisplayName("跨平台：bodyType 缺失 + targetType=POJO → JSON 反序列化为 POJO")
    void crossPlatformJsonToPojo() {
        // 模拟 Go 发送的 JSON 字符串，consumer 声明 POJO 类型
        String rawJson = "{\"name\":\"Bob\",\"age\":25}";
        Map<String, String> fields = new HashMap<>();
        fields.put("body", rawJson);
        fields.put("bornTs", "1");

        Message<UserDto> restored = converter.fromStreamFields(fields, UserDto.class, "t");

        assertThat(restored.getBody()).isNotNull();
        assertThat(restored.getBody().getName()).isEqualTo("Bob");
        assertThat(restored.getBody().getAge()).isEqualTo(25);
    }

    @Test
    @DisplayName("跨平台：bodyType 存在 → 走 SDK 路径（Base64 + serializer）")
    void sdkPathWithBodyType() {
        // SDK 发送方：body 为 Base64 编码，bodyType 字段存在
        Map<String, String> fields = new HashMap<>();
        fields.put(
                "body",
                Base64.getEncoder().encodeToString(serializer.serialize("hi", Object.class)));
        fields.put("bodyType", String.class.getName());
        fields.put("bornTs", "1");

        Message<String> restored = converter.fromStreamFields(fields, String.class, "t");

        assertThat(restored.getBody()).isEqualTo("hi");
    }

    // ===================== 压缩/解压测试 =====================

    @Test
    @DisplayName("压缩消息往返：序列化 → 手动压缩 → 解压 → 反序列化（SDK 路径）")
    void compressedBodyRoundTripSdkPath() {
        CompressionCodec codec = new GzipCompressionCodec();
        DefaultMessageConverter compressedConverter = new DefaultMessageConverter(serializer);
        compressedConverter.setCompressionCodec(codec);

        Message<String> source =
                msg("hello compression world", "vip", null, null, 123456789L, null, 0, null);

        // 序列化为 Stream Fields
        Map<String, String> fields = compressedConverter.toStreamFields(source);

        // 手动压缩 body（模拟 Producer 的 applyCompression 逻辑）
        String bodyStr = fields.get(DefaultMessageConverter.FIELD_BODY);
        byte[] bodyBytes = Base64.getDecoder().decode(bodyStr);
        byte[] compressed = codec.compress(bodyBytes);
        fields.put(
                DefaultMessageConverter.FIELD_BODY, Base64.getEncoder().encodeToString(compressed));
        fields.put(DefaultMessageConverter.FIELD_COMPRESSED, "true");

        // 反序列化（应自动解压）
        Message<String> restored =
                compressedConverter.fromStreamFields(fields, String.class, "test-topic");

        assertThat(restored.getBody()).isEqualTo("hello compression world");
        assertThat(restored.getTag()).isEqualTo("vip");
        assertThat(restored.getBornTimestamp()).isEqualTo(123456789L);
    }

    @Test
    @DisplayName("compressed=true 但未配置 CompressionCodec 抛出 SerializationException")
    void compressedWithoutCodec() {
        Map<String, String> fields = converter.toStreamFields(msg("hello", 1L));
        // 标记为压缩但不实际压缩，验证未配置 codec 时的异常
        fields.put(DefaultMessageConverter.FIELD_COMPRESSED, "true");

        assertThatThrownBy(() -> converter.fromStreamFields(fields, String.class))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("CompressionCodec");
    }

    @Test
    @DisplayName("未压缩消息不受 CompressionCodec 影响")
    void uncompressedMessageWithCodec() {
        CompressionCodec codec = new GzipCompressionCodec();
        DefaultMessageConverter compressedConverter = new DefaultMessageConverter(serializer);
        compressedConverter.setCompressionCodec(codec);

        Map<String, String> fields =
                compressedConverter.toStreamFields(msg("no compression here", 1L));
        // 不设置 compressed=true，即使配置了 codec 也不应解压
        Message<String> restored =
                compressedConverter.fromStreamFields(fields, String.class, "test-topic");

        assertThat(restored.getBody()).isEqualTo("no compression here");
    }

    @Test
    @DisplayName("压缩消息往返：跨平台路径（bodyType 缺失 + String body）")
    void compressedBodyCrossPlatformStringPath() {
        CompressionCodec codec = new GzipCompressionCodec();
        DefaultMessageConverter compressedConverter = new DefaultMessageConverter(serializer);
        compressedConverter.setCompressionCodec(codec);

        // 模拟跨平台场景：原始 body 为字符串，压缩后 Base64 编码
        String rawBody = "{\"name\":\"Alice\",\"age\":30}";
        byte[] rawBytes = rawBody.getBytes(StandardCharsets.UTF_8);
        byte[] compressed = codec.compress(rawBytes);

        Map<String, String> fields = new HashMap<>();
        fields.put(
                DefaultMessageConverter.FIELD_BODY, Base64.getEncoder().encodeToString(compressed));
        fields.put(DefaultMessageConverter.FIELD_COMPRESSED, "true");
        fields.put("bornTs", "1");
        // 不设置 bodyType → 走跨平台路径

        Message<String> restored = compressedConverter.fromStreamFields(fields, String.class, "t");

        assertThat(restored.getBody()).isEqualTo(rawBody);
    }

    @Test
    @DisplayName("压缩消息往返：跨平台路径（bodyType 缺失 + POJO body）")
    void compressedBodyCrossPlatformPojoPath() {
        CompressionCodec codec = new GzipCompressionCodec();
        DefaultMessageConverter compressedConverter = new DefaultMessageConverter(serializer);
        compressedConverter.setCompressionCodec(codec);

        // 模拟跨平台场景：JSON body 压缩后 Base64 编码
        String rawJson = "{\"name\":\"Bob\",\"age\":25}";
        byte[] rawBytes = rawJson.getBytes(StandardCharsets.UTF_8);
        byte[] compressed = codec.compress(rawBytes);

        Map<String, String> fields = new HashMap<>();
        fields.put(
                DefaultMessageConverter.FIELD_BODY, Base64.getEncoder().encodeToString(compressed));
        fields.put(DefaultMessageConverter.FIELD_COMPRESSED, "true");
        fields.put("bornTs", "1");
        // 不设置 bodyType → 走跨平台路径

        Message<UserDto> restored =
                compressedConverter.fromStreamFields(fields, UserDto.class, "t");

        assertThat(restored.getBody()).isNotNull();
        assertThat(restored.getBody().getName()).isEqualTo("Bob");
        assertThat(restored.getBody().getAge()).isEqualTo(25);
    }

    @Test
    @DisplayName("FIELD_COMPRESSED 常量值为 compressed")
    void fieldCompressedConstant() {
        assertThat(DefaultMessageConverter.FIELD_COMPRESSED).isEqualTo("compressed");
    }
}
