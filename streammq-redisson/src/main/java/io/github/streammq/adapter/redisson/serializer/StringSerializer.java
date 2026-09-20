/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.serializer;

import io.github.streammq.core.serializer.MessageSerializer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * String 类型 body 的序列化器（直通型）。
 *
 * <p>使用 UTF-8 编码直接在 String 与 byte[] 之间转换，无任何外部依赖， 适用于 body 类型为 {@link String} 的消息（如纯文本、JSON
 * 字符串、跨语言透传场景）。
 *
 * <p>序列化为 byte[] 时使用 {@link StandardCharsets#UTF_8} 编码； 反序列化时按 UTF-8 解码为 String。
 *
 * <p><b>null / 空输入契约（0.1.2 起，直通型语义）：</b>
 *
 * <ul>
 *   <li>{@code serialize(null, type)} → {@code null}（与其余内置序列化器统一；此前返回 {@code byte[0]}， 会让"null
 *       body"与"空 body"在 Redis 中不可区分）
 *   <li>{@code deserialize(null, type)} → {@code null}
 *   <li>{@code deserialize(byte[0], type)} → {@code ""}（返回空值本身，而非 null）——这是 {@code send(topic, "")}
 *       端到端还原为空串的必要条件（转换器按"body 字段是否存在"判定是否解码）
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StringSerializer implements MessageSerializer<String> {

    @Override
    public byte[] serialize(String object, Class<String> type) {
        if (Objects.isNull(object)) {
            return null;
        }
        return object.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R deserialize(byte[] bytes, Class<R> type) {
        Objects.requireNonNull(type, "type");
        if (Objects.isNull(bytes)) {
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
