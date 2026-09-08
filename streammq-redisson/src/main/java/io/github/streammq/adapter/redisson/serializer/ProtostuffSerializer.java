/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.serializer;

import io.github.streammq.core.serializer.MessageSerializer;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 基于 Protostuff 的高性能 Protocol Buffer 序列化器（<b>可选实现</b>：框架默认序列化器为 {@link FurySerializer}）。
 *
 * <p>Protostuff 提供了无需 .proto 文件的运行时 Protocol Buffer 序列化， 性能显著优于 JSON/JDK 序列化，且向前/向后兼容性更好。
 *
 * <p><b>安全性（无 gadget RCE 面）：</b>schema 由调用方显式传入的 {@code type} 决定（{@code RuntimeSchema}），反序列化
 * <b>不会</b>依据报文内容解析并实例化任意类名，因此不存在 Fury 宽松模式 / Jackson 默认多态类型（{@code activateDefaultTyping}）那类反序列化
 * RCE 攻击面。它是「不能接受任何 RCE 面」场景下的推荐替代：在<b>共享/多租户 Redis</b>上比默认 Fury 更安全，且吞吐仍显著优于 JSON。
 *
 * <p><b>使用约束：</b>Protostuff 要求被序列化的类有默认构造器（无参构造）。{@code String} 与 {@code byte[]} 不是 protostuff
 * 消息（{@code RuntimeSchema} 会为其生成 0 字段 schema，导致静默丢数据），故本实现对其原生支持： {@code String} 按 UTF-8 编解码，{@code
 * byte[]} 直通。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class ProtostuffSerializer<T> implements MessageSerializer<T> {

    private final ConcurrentMap<Class<?>, Schema<?>> schemaCache = new ConcurrentHashMap<>();
    private final ThreadLocal<LinkedBuffer> bufferPool =
            ThreadLocal.withInitial(() -> LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE));

    @Override
    public byte[] serialize(T object, Class<T> type) {
        if (Objects.isNull(object)) {
            return new byte[0];
        }
        // String / byte[] 不是 protostuff 消息：RuntimeSchema 会为其生成 0 字段 schema，
        // 序列化结果为空字节、反序列化返回 null，造成静默丢数据。作为默认序列化器必须原生支持。
        if (object instanceof String str) {
            return str.getBytes(StandardCharsets.UTF_8);
        }
        if (object instanceof byte[] raw) {
            return raw;
        }
        @SuppressWarnings("unchecked")
        Schema<T> schema = (Schema<T>) getSchema(object.getClass());
        LinkedBuffer buffer = bufferPool.get();
        try {
            return ProtostuffIOUtil.toByteArray(object, schema, buffer);
        } finally {
            buffer.clear();
        }
    }

    @Override
    public <R> R deserialize(byte[] bytes, Class<R> type) {
        if (Objects.isNull(bytes) || bytes.length == 0) {
            return null;
        }
        if (String.class.equals(type)) {
            return type.cast(new String(bytes, StandardCharsets.UTF_8));
        }
        if (byte[].class.equals(type)) {
            return type.cast(bytes);
        }
        @SuppressWarnings("unchecked")
        Schema<R> schema = (Schema<R>) getSchema(type);
        try {
            R instance = type.getDeclaredConstructor().newInstance();
            ProtostuffIOUtil.mergeFrom(bytes, instance, schema);
            return instance;
        } catch (Exception ex) {
            throw new io.github.streammq.core.exception.SerializationException(
                    "Protostuff deserialize failed for type: " + type.getName(), ex);
        }
    }

    @Override
    public String name() {
        return "protostuff";
    }

    @SuppressWarnings("unchecked")
    private Schema<?> getSchema(Class<?> clazz) {
        return schemaCache.computeIfAbsent(clazz, RuntimeSchema::getSchema);
    }
}
