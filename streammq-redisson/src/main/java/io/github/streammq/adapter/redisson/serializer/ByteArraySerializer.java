package io.github.streammq.adapter.redisson.serializer;

import io.github.streammq.core.serializer.MessageSerializer;

import java.util.Objects;

/**
 * byte[] 类型 body 的序列化器。
 *
 * <p>序列化与反序列化均直接返回原 byte[]（零拷贝），不进行任何编码转换，
 * 适用于 body 类型为 {@code byte[]} 的消息（如二进制数据、已序列化的字节流、图片/音频等）。
 *
 * <p>注意：byte[] 的 Class 类型通过 {@code byte[].class} 表示，
 * 反序列化时仅接受目标类型为 {@code byte[]} 的请求，其他类型将抛出
 * {@link IllegalArgumentException}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class ByteArraySerializer implements MessageSerializer<byte[]> {

    @Override
    public byte[] serialize(byte[] object, Class<byte[]> type) {
        if (Objects.isNull(object)) {
            return new byte[0];
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
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        // 零拷贝：直接返回原 byte[]
        return (R) bytes;
    }

    @Override
    public String name() {
        return "byte-array";
    }
}
