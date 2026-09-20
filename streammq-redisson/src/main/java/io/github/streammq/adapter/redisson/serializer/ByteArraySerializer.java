/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.serializer;

import io.github.streammq.core.serializer.MessageSerializer;
import java.util.Objects;

/**
 * byte[] 类型 body 的序列化器（直通型）。
 *
 * <p>序列化与反序列化均直接返回原 byte[]（零拷贝），不进行任何编码转换， 适用于 body 类型为 {@code byte[]} 的消息（如二进制数据、已序列化的字节流、图片/音频等）。
 *
 * <p>注意：byte[] 的 Class 类型通过 {@code byte[].class} 表示， 反序列化时仅接受目标类型为 {@code byte[]} 的请求，其他类型将抛出
 * {@link IllegalArgumentException}。
 *
 * <p><b>null / 空输入契约（0.1.2 起，直通型语义）：</b>
 *
 * <ul>
 *   <li>{@code serialize(null, type)} → {@code null}（与其余内置序列化器统一；此前返回 {@code byte[0]}， 会让"null
 *       body"与"空 body"在 Redis 中不可区分）
 *   <li>{@code deserialize(null, type)} → {@code null}
 *   <li>{@code deserialize(byte[0], type)} → {@code new byte[0]}（返回空值本身，而非 null）
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class ByteArraySerializer implements MessageSerializer<byte[]> {

    @Override
    public byte[] serialize(byte[] object, Class<byte[]> type) {
        if (Objects.isNull(object)) {
            return null;
        }
        // 零拷贝：直接返回原 byte[]
        return object;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R deserialize(byte[] bytes, Class<R> type) {
        Objects.requireNonNull(type, "type");
        if (type != byte[].class) {
            throw new IllegalArgumentException(
                    "ByteArraySerializer only supports byte[] target type, got: " + type.getName());
        }
        if (Objects.isNull(bytes)) {
            return null;
        }
        // 零拷贝：直接返回原 byte[]（空数组时返回空数组本身，而非 null）
        return (R) bytes;
    }

    @Override
    public String name() {
        return "byte-array";
    }
}
