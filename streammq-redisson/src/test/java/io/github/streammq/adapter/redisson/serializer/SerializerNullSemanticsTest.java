/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.serializer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.core.serializer.MessageSerializer;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * 内置序列化器 null / 空输入语义的一致性测试（{@link MessageSerializer} 契约固化）。
 *
 * <p>统一契约（0.1.2 起）：
 *
 * <ul>
 *   <li>{@code serialize(null, type)} → {@code null}（<b>全部</b>内置实现，直通型也不例外）
 *   <li>{@code deserialize(null, type)} → {@code null}
 *   <li><b>结构化型</b>：{@code deserialize(new byte[0], type)} → {@code null}
 *   <li><b>直通型</b>（{@link StringSerializer} / {@link ByteArraySerializer}）：{@code deserialize(new
 *       byte[0], type)} → 空值本身（{@code ""} / {@code new byte[0]}）——这是 {@code send(topic, "")}
 *       端到端还原为空值（而非 null）的必要条件
 * </ul>
 *
 * <p>表驱动覆盖 6 个结构化实现：Fury（安全模式，仅覆盖 null/空路径，不做真实序列化）、JDK、Jackson、
 * Protostuff、FlatBuffers、SBE；直通型单独断言（泛型参数不同，且空数组语义相反）。调用方（消息转换器）在 body 为 null 时不写入 body
 * 字段，因此这些返回值不会进入 Base64 编码路径；统一语义避免"同一份业务代码换序列化器即 NPE / 空数组歧义"。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("内置序列化器 null/空输入语义一致性")
class SerializerNullSemanticsTest {

    static Stream<Arguments> structuredSerializers() {
        return Stream.of(
                Arguments.of("fury", new FurySerializer<String>()),
                Arguments.of("jdk", new JdkSerializer<String>()),
                Arguments.of("jackson-json", new JacksonJsonSerializer<String>()),
                Arguments.of("protostuff", new ProtostuffSerializer<String>()),
                Arguments.of("flatbuffers", new FlatBuffersSerializer<String>()),
                Arguments.of("sbe", new SbeSerializer<String>()));
    }

    @ParameterizedTest(name = "{0}: serialize(null) 返回 null")
    @MethodSource("structuredSerializers")
    void serializeNullReturnsNull(String name, MessageSerializer<String> serializer) {
        assertThat(serializer.name()).isEqualTo(name);
        assertThat(serializer.serialize(null, String.class)).isNull();
    }

    @ParameterizedTest(name = "{0}: deserialize(null) 返回 null")
    @MethodSource("structuredSerializers")
    void deserializeNullReturnsNull(String name, MessageSerializer<String> serializer) {
        assertThat(serializer.name()).isEqualTo(name);
        assertThat(serializer.deserialize(null, String.class)).isNull();
    }

    @ParameterizedTest(name = "{0}: deserialize(byte[0]) 返回 null（结构化型）")
    @MethodSource("structuredSerializers")
    void deserializeEmptyReturnsNull(String name, MessageSerializer<String> serializer) {
        assertThat(serializer.name()).isEqualTo(name);
        assertThat(serializer.deserialize(new byte[0], String.class)).isNull();
    }

    // ===================== 直通型：空数组返回空值本身 =====================

    @Test
    @DisplayName(
            "string: serialize(null)=null / serialize(\"\")=byte[0] / deserialize(byte[0])=\"\"")
    void stringSerializerPassthroughSemantics() {
        StringSerializer serializer = new StringSerializer();
        assertThat(serializer.name()).isEqualTo("string");

        assertThat(serializer.serialize(null, String.class))
                .as("直通型 serialize(null) 必须为 null（不再退化为 byte[0]，否则与'空 body'不可区分）")
                .isNull();
        assertThat(serializer.deserialize(null, String.class)).isNull();

        byte[] emptyBytes = serializer.serialize("", String.class);
        assertThat(emptyBytes).as("\"\".getBytes(UTF_8) 为空数组").isEmpty();
        assertThat(serializer.deserialize(new byte[0], String.class))
                .as("deserialize(byte[0]) 必须返回空串本身")
                .isEqualTo("");
        assertThat(serializer.deserialize(emptyBytes, String.class)).isEqualTo("");

        byte[] bytes = serializer.serialize("hello", String.class);
        assertThat(bytes).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
        assertThat(serializer.deserialize(bytes, String.class)).isEqualTo("hello");
    }

    @Test
    @DisplayName("byte-array: serialize(null)=null / deserialize(byte[0])=new byte[0]")
    void byteArraySerializerPassthroughSemantics() {
        ByteArraySerializer serializer = new ByteArraySerializer();
        assertThat(serializer.name()).isEqualTo("byte-array");

        assertThat(serializer.serialize(null, byte[].class))
                .as("直通型 serialize(null) 必须为 null（不再退化为 byte[0]）")
                .isNull();
        assertThat(serializer.deserialize(null, byte[].class)).isNull();

        assertThat(serializer.deserialize(new byte[0], byte[].class))
                .as("deserialize(byte[0]) 必须返回空数组本身（非 null）")
                .isNotNull()
                .isEmpty();

        byte[] payload = {1, 2, 3};
        assertThat(serializer.serialize(payload, byte[].class)).isSameAs(payload);
        assertThat(serializer.deserialize(payload, byte[].class)).isSameAs(payload);
    }
}
