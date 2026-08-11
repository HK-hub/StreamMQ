package io.github.streammq.adapter.redisson.serializer;

import io.github.streammq.core.serializer.MessageSerializer;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 基于 Protostuff 的高性能 Protocol Buffer 序列化器。
 *
 * <p>Protostuff 提供了无需 .proto 文件的运行时 Protocol Buffer 序列化， 性能优于 JDK 序列化，且向前/向后兼容性更好。
 *
 * <p>注意：Protostuff 要求被序列化的类有默认构造器（无参构造）。
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
