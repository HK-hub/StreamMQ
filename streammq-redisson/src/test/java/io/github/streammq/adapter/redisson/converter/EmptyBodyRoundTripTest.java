/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.streammq.adapter.redisson.serializer.ByteArraySerializer;
import io.github.streammq.adapter.redisson.serializer.StringSerializer;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 空 body 端到端往返 + 畸形 topic 异常契约测试（第六轮红队 R3-1 / R3-8）。
 *
 * <p>R3-1：{@code send(topic, "")} 必须端到端还原为 {@code ""}（而非 null）。三处协同：
 *
 * <ol>
 *   <li>直通型序列化器 {@code deserialize(byte[0])} 返回空值本身
 *   <li>转换器按"body 字段是否存在"判定是否解码（存在即使空串也解码）
 *   <li>缺失字段（无载荷）仍为 null
 * </ol>
 *
 * <p>R3-8：Entry 字段中的 topic 违反命名校验时，{@link Message} 构造期的 {@link IllegalArgumentException} 必须被包装为
 * {@link SerializationException}（保留 cause），消费路径不得看到裸 IAE。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("空 body 往返与畸形 topic 异常契约")
class EmptyBodyRoundTripTest {

    private static final String TOPIC = "roundtrip-topic";

    @Test
    @DisplayName("DefaultMessageConverter + StringSerializer：send(topic,\"\") → 消费端 \"\"（非 null）")
    void defaultConverter_emptyStringBody_roundTripsAsEmptyString() {
        DefaultMessageConverter converter = new DefaultMessageConverter(new StringSerializer());
        Message<String> sent = MessageBuilder.<String>withTopic(TOPIC).body("").build();

        Map<String, String> fields = converter.toStreamFields(sent);
        assertThat(fields)
                .as("空串 body 序列化后是 byte[0] → Base64 为空串，但 body 字段必须存在")
                .containsEntry(DefaultMessageConverter.FIELD_BODY, "")
                .containsEntry(DefaultMessageConverter.FIELD_BODY_TYPE, "java.lang.String");

        Message<String> received = converter.fromStreamFields(fields, String.class, TOPIC);
        assertThat(received.getBody())
                .as("消费端必须还原为 \"\"，业务 body.isEmpty() 不得 NPE")
                .isNotNull()
                .isEqualTo("");
    }

    @Test
    @DisplayName("DefaultMessageConverter + ByteArraySerializer：空 byte[] → 消费端空 byte[]（非 null）")
    void defaultConverter_emptyByteArrayBody_roundTripsAsEmptyArray() {
        DefaultMessageConverter converter = new DefaultMessageConverter(new ByteArraySerializer());
        Message<byte[]> sent = MessageBuilder.<byte[]>withTopic(TOPIC).body(new byte[0]).build();

        Map<String, String> fields = converter.toStreamFields(sent);
        Message<byte[]> received = converter.fromStreamFields(fields, byte[].class, TOPIC);

        assertThat(received.getBody()).as("空数组必须还原为空数组").isNotNull().isEmpty();
    }

    @Test
    @DisplayName("PassThroughMessageConverter：空串 body 往返仍为空串（非 null）")
    void passThroughConverter_emptyStringBody_roundTrips() {
        PassThroughMessageConverter converter = new PassThroughMessageConverter();
        Message<String> sent = MessageBuilder.<String>withTopic(TOPIC).body("").build();

        Map<String, String> fields = converter.toStreamFields(sent);
        Message<String> received = converter.fromStreamFields(fields, String.class, TOPIC);

        assertThat(received.getBody()).isNotNull().isEqualTo("");
    }

    @Test
    @DisplayName("body 字段不存在 → body 为 null（无载荷与空载荷语义不同）")
    void missingBodyField_decodesAsNull() {
        DefaultMessageConverter converter = new DefaultMessageConverter(new StringSerializer());
        Message<String> sent = MessageBuilder.<String>withTopic(TOPIC).body("x").build();

        Map<String, String> fields = new HashMap<>(converter.toStreamFields(sent));
        fields.remove(DefaultMessageConverter.FIELD_BODY);
        fields.remove(DefaultMessageConverter.FIELD_BODY_TYPE);

        Message<String> received = converter.fromStreamFields(fields, String.class, TOPIC);
        assertThat(received.getBody()).as("字段不存在 = 无载荷 → null").isNull();
    }

    @Test
    @DisplayName("originTopic 字段含非法字符：抛 SerializationException（cause = IAE），不得泄漏裸 IAE")
    void malformedOriginTopic_wrappedAsSerializationException() {
        DefaultMessageConverter converter = new DefaultMessageConverter(new StringSerializer());
        Map<String, String> fields =
                new HashMap<>(
                        converter.toStreamFields(
                                MessageBuilder.<String>withTopic(TOPIC).body("x").build()));
        fields.put(DefaultMessageConverter.FIELD_ORIGIN_TOPIC, "bad:topic");

        assertThatThrownBy(() -> converter.fromStreamFields(fields, String.class, null))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("bad:topic")
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("保留前缀 __ 的 topic（哨兵冲突名）：抛 SerializationException（cause = IAE）")
    void reservedSentinelTopic_wrappedAsSerializationException() {
        DefaultMessageConverter converter = new DefaultMessageConverter(new StringSerializer());
        Map<String, String> fields =
                new HashMap<>(
                        converter.toStreamFields(
                                MessageBuilder.<String>withTopic(TOPIC).body("x").build()));
        fields.put(DefaultMessageConverter.FIELD_ORIGIN_TOPIC, "__dlq__");

        assertThatThrownBy(() -> converter.fromStreamFields(fields, String.class, null))
                .isInstanceOf(SerializationException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("fallbackTopic 非法（含空白）：抛 SerializationException（cause = IAE）")
    void malformedFallbackTopic_wrappedAsSerializationException() {
        DefaultMessageConverter converter = new DefaultMessageConverter(new StringSerializer());

        assertThatThrownBy(
                        () ->
                                converter.fromStreamFields(
                                        Map.of(
                                                DefaultMessageConverter.FIELD_BODY,
                                                "eA==",
                                                DefaultMessageConverter.FIELD_BODY_TYPE,
                                                "java.lang.String"),
                                        String.class,
                                        "bad topic"))
                .isInstanceOf(SerializationException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }
}
