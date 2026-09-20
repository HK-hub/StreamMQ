/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.serializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.message.MessageBuilder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * {@link MessageSerializer} 契约测试（表驱动）。
 *
 * <p>core 模块不含内置序列化器实现，本测试用最小直通型/结构化型测试替身把 0.1.2 定稿的 null / 空语义钉死为可执行契约 （真实内置实现位于 {@code
 * streammq-redisson}，须满足同一契约）：
 *
 * <ul>
 *   <li>{@code serialize(null)} → {@code null}（全部实现，直通型也不例外）
 *   <li>{@code deserialize(null)} → {@code null}
 *   <li>{@code deserialize(byte[0])} → 直通型返回空值本身（{@code ""} / {@code new byte[0]}）；结构化型返回 {@code
 *       null}
 *   <li>端到端不变式：{@code ""} body 在"字段出现"时还原为 {@code ""}，字段缺失时才是 {@code null}
 * </ul>
 */
@DisplayName("MessageSerializer null/空语义契约测试")
class MessageSerializerContractTest {

    /** 最小直通型 String 序列化器（对齐内置 StringSerializer 契约）。 */
    private static final class PassThroughStringSerializer implements MessageSerializer<String> {

        @Override
        public byte[] serialize(String object, Class<String> type) {
            return Objects.isNull(object) ? null : object.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <R> R deserialize(byte[] bytes, Class<R> type) throws SerializationException {
            Objects.requireNonNull(type, "type");
            if (Objects.isNull(bytes)) {
                return null;
            }
            if (bytes.length == 0) {
                // 直通型：空数组就是合法的空字符串，原样还原（不是 null）
                return (R) "";
            }
            return (R) new String(bytes, StandardCharsets.UTF_8);
        }
    }

    /** 最小直通型 byte[] 序列化器（对齐内置 ByteArraySerializer 契约）。 */
    private static final class PassThroughByteArraySerializer implements MessageSerializer<byte[]> {

        @Override
        public byte[] serialize(byte[] object, Class<byte[]> type) {
            return Objects.isNull(object) ? null : object;
        }

        @Override
        public <R> R deserialize(byte[] bytes, Class<R> type) throws SerializationException {
            Objects.requireNonNull(type, "type");
            if (Objects.isNull(bytes)) {
                return null;
            }
            if (bytes.length == 0) {
                // 直通型：空数组原样返回（不是 null）
                return type.cast(new byte[0]);
            }
            return type.cast(bytes);
        }
    }

    /** 最小结构化型序列化器（载荷形如 {@code <len>:<text>}，编码后永不为空数组）。 */
    private static final class StructuredSerializer implements MessageSerializer<String> {

        @Override
        public byte[] serialize(String object, Class<String> type) {
            if (Objects.isNull(object)) {
                return null;
            }
            byte[] raw = object.getBytes(StandardCharsets.UTF_8);
            byte[] out = new byte[raw.length + 1];
            System.arraycopy(raw, 0, out, 1, raw.length);
            out[0] = 1; // 结构化标记：即使空字符串编码后也至少 1 字节
            return out;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <R> R deserialize(byte[] bytes, Class<R> type) throws SerializationException {
            Objects.requireNonNull(type, "type");
            if (Objects.isNull(bytes) || bytes.length == 0) {
                // 结构化型：空数组不是合法载荷（合法编码至少含标记字节），返回 null
                return null;
            }
            return (R) new String(bytes, 1, bytes.length - 1, StandardCharsets.UTF_8);
        }
    }

    @Nested
    @DisplayName("serialize(null) → null（全部实现）")
    class SerializeNull {

        @Test
        @DisplayName("直通型 String 序列化器 serialize(null) 返回 null（不返回 byte[0]）")
        void passThroughStringSerializeNull() {
            assertThat(new PassThroughStringSerializer().serialize(null, String.class)).isNull();
        }

        @Test
        @DisplayName("直通型 byte[] 序列化器 serialize(null) 返回 null")
        void passThroughBytesSerializeNull() {
            assertThat(new PassThroughByteArraySerializer().serialize(null, byte[].class)).isNull();
        }

        @Test
        @DisplayName("结构化型序列化器 serialize(null) 返回 null")
        void structuredSerializeNull() {
            assertThat(new StructuredSerializer().serialize(null, String.class)).isNull();
        }
    }

    @Nested
    @DisplayName("deserialize(null) → null（全部实现）")
    class DeserializeNull {

        @Test
        @DisplayName("直通型 / 结构化型 deserialize(null) 均返回 null 且不抛异常")
        void allImplementationsReturnNull() {
            assertThat(new PassThroughStringSerializer().deserialize(null, String.class)).isNull();
            assertThat(new PassThroughByteArraySerializer().deserialize(null, byte[].class))
                    .isNull();
            assertThat(new StructuredSerializer().deserialize(null, String.class)).isNull();
        }

        @Test
        @DisplayName("deserialize(type=null) 抛 NullPointerException")
        void nullTypeRejected() {
            assertThatThrownBy(() -> new PassThroughStringSerializer().deserialize(null, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    @DisplayName("deserialize(byte[0]) 直通 vs 结构化")
    class EmptyPayload {

        @Test
        @DisplayName("直通型 String：空数组 → \"\"（不是 null）")
        void passThroughStringEmpty() {
            assertThat(new PassThroughStringSerializer().deserialize(new byte[0], String.class))
                    .isEqualTo("");
        }

        @Test
        @DisplayName("直通型 byte[]：空数组 → new byte[0]（不是 null）")
        void passThroughBytesEmpty() {
            byte[] result =
                    new PassThroughByteArraySerializer().deserialize(new byte[0], byte[].class);
            assertThat(result).isNotNull().isEmpty();
        }

        @Test
        @DisplayName("结构化型：空数组 → null（合法载荷永不为空数组）")
        void structuredEmpty() {
            assertThat(new StructuredSerializer().deserialize(new byte[0], String.class)).isNull();
        }
    }

    @Nested
    @DisplayName("端到端不变式：send(topic, \"\") 消费端还原为 \"\"")
    class EndToEndEmptyBody {

        /** 用"字段是否存在"判定 body 语义的最小转换器（对齐适配层字段存在性口径）。 */
        private String decodeBody(
                Map<String, String> fields, MessageSerializer<String> serializer) {
            if (!fields.containsKey("body")) {
                return null; // 字段缺失 = 无载荷消息
            }
            return serializer.deserialize(
                    fields.get("body").getBytes(StandardCharsets.ISO_8859_1), String.class);
        }

        @ParameterizedTest(name = "body=\"{0}\" 往返后仍为空字符串")
        @ValueSource(strings = {"", "payload"})
        @DisplayName("空字符串 body 经 serialize → 字段写入 → deserialize 后还原为 \"\"（而非 null）")
        void emptyStringRoundTrip(String body) {
            MessageSerializer<String> serializer = new PassThroughStringSerializer();
            byte[] encoded = serializer.serialize(body, String.class);

            Map<String, String> fields = new HashMap<>();
            fields.put("body", new String(encoded, StandardCharsets.ISO_8859_1));

            String decoded = decodeBody(fields, serializer);
            assertThat(decoded).isEqualTo(body).isNotNull();
        }

        @Test
        @DisplayName("body 字段缺失（而非为空）时才还原为 null：存在性以字段是否出现为准")
        void absentFieldMeansNullBody() {
            MessageSerializer<String> serializer = new PassThroughStringSerializer();
            assertThat(decodeBody(new HashMap<>(), serializer)).isNull();
            assertThat(decodeBody(Map.of("body", ""), serializer)).isEqualTo("");
        }

        @Test
        @DisplayName("MessageBuilder 允许空字符串 body（与 null body 是两种语义）")
        void builderKeepsEmptyStringBody() {
            assertThat(MessageBuilder.<String>withTopic("t").body("").build().getBody())
                    .isEqualTo("");
        }
    }
}
