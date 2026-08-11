package io.github.streammq.adapter.redisson.serializer;

import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.serializer.MessageSerializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;

/**
 * 基于 JDK 原生序列化机制的备选实现。
 *
 * <p>要求 body 类型实现 {@link Serializable}。 优点：无需引入第三方库；缺点：跨语言不兼容、性能较差、序列化体积较大。
 *
 * <p>适用于内部系统、对性能不敏感的场景，或对依赖大小有严格限制的部署。
 *
 * @param <T> body 类型，需实现 {@link Serializable}
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class JdkSerializer<T extends Serializable> implements MessageSerializer<T> {

    @Override
    public byte[] serialize(T object, Class<T> type) throws SerializationException {
        if (object == null) {
            return new byte[0];
        }
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(object);
            oos.flush();
            return bos.toByteArray();
        } catch (IOException ex) {
            throw new SerializationException(
                    "JDK serialize failed for type " + object.getClass().getName(), ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R deserialize(byte[] bytes, Class<R> type) throws SerializationException {
        Objects.requireNonNull(type, "type");
        if (Objects.isNull(bytes) || bytes.length == 0) {
            return null;
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            Object obj = ois.readObject();
            if (Objects.nonNull(obj) && !type.isInstance(obj)) {
                throw new SerializationException(
                        "JDK deserialize type mismatch: expected "
                                + type.getName()
                                + ", got "
                                + obj.getClass().getName());
            }
            return (R) obj;
        } catch (IOException | ClassNotFoundException ex) {
            throw new SerializationException(
                    "JDK deserialize failed for type " + type.getName(), ex);
        }
    }

    @Override
    public String name() {
        return "jdk";
    }
}
