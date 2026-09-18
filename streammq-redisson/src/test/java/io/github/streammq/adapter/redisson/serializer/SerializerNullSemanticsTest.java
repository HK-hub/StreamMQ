/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.serializer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.core.serializer.MessageSerializer;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * 内置序列化器 null / 空输入语义的一致性测试（{@link MessageSerializer} 契约固化）。
 *
 * <p>统一契约（0.1.2 起）：
 *
 * <ul>
 *   <li>{@code serialize(null, type)} → {@code null}
 *   <li>{@code deserialize(null, type)} → {@code null}
 *   <li>{@code deserialize(new byte[0], type)} → {@code null}
 * </ul>
 *
 * <p>表驱动覆盖 6 个内置实现：Fury（安全模式，仅覆盖 null/空路径，不做真实序列化）、JDK、Jackson、
 * Protostuff、FlatBuffers、SBE。调用方（消息转换器）在 body 为 null 时不写入 body 字段， 因此这些返回值不会进入 Base64
 * 编码路径；统一语义避免“同一份业务代码换序列化器即 NPE / 空数组歧义”。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("内置序列化器 null/空输入语义一致性")
class SerializerNullSemanticsTest {

    static Stream<Arguments> builtInSerializers() {
        return Stream.of(
                Arguments.of("fury", new FurySerializer<String>()),
                Arguments.of("jdk", new JdkSerializer<String>()),
                Arguments.of("jackson-json", new JacksonJsonSerializer<String>()),
                Arguments.of("protostuff", new ProtostuffSerializer<String>()),
                Arguments.of("flatbuffers", new FlatBuffersSerializer<String>()),
                Arguments.of("sbe", new SbeSerializer<String>()));
    }

    @ParameterizedTest(name = "{0}: serialize(null) 返回 null")
    @MethodSource("builtInSerializers")
    void serializeNullReturnsNull(String name, MessageSerializer<String> serializer) {
        assertThat(serializer.name()).isEqualTo(name);
        assertThat(serializer.serialize(null, String.class)).isNull();
    }

    @ParameterizedTest(name = "{0}: deserialize(null) 返回 null")
    @MethodSource("builtInSerializers")
    void deserializeNullReturnsNull(String name, MessageSerializer<String> serializer) {
        assertThat(serializer.name()).isEqualTo(name);
        assertThat(serializer.deserialize(null, String.class)).isNull();
    }

    @ParameterizedTest(name = "{0}: deserialize(byte[0]) 返回 null")
    @MethodSource("builtInSerializers")
    void deserializeEmptyReturnsNull(String name, MessageSerializer<String> serializer) {
        assertThat(serializer.name()).isEqualTo(name);
        assertThat(serializer.deserialize(new byte[0], String.class)).isNull();
    }
}
