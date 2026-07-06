package io.github.streammq.adapter.redisson.serializer;

import io.github.streammq.core.serializer.MessageSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * String 类型 body 的序列化器。
 *
 * <p>使用 UTF-8 编码直接在 String 与 byte[] 之间转换，无任何外部依赖，
 * 适用于 body 类型为 {@link String} 的消息（如纯文本、JSON 字符串、跨语言透传场景）。
 *
 * <p>序列化为 byte[] 时使用 {@link StandardCharsets#UTF_8} 编码；
 * 反序列化时按 UTF-8 解码为 String。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StringSerializer implements MessageSerializer<String> {

    @Override
    public byte[] serialize(String object, Class<String> type) {
        if (object == null) {
            return new byte[0];
        }
        return object.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R deserialize(byte[] bytes, Class<R> type) {
        Objects.requireNonNull(type, "type");
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        String str = new String(bytes, StandardCharsets.UTF_8);
        return (R) str;
    }

    @Override
    public String name() {
        return "string";
    }
}
