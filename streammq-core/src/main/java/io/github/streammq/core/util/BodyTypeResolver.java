package io.github.streammq.core.util;

import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;

/**
 * 解析 Consumer 实现类的泛型 body 类型 T。
 *
 * <p>StreamMQ 的 Consumer 接口均声明了泛型 T：
 * <ul>
 *   <li>{@link StreamMessageConcurrentlyConsumer}&lt;T&gt;</li>
 *   <li>{@link StreamMessageOrderlyConsumer}&lt;T&gt;</li>
 * </ul>
 *
 * <p>本工具通过反射提取实现类上声明的具体泛型类型，用于跨平台消息反序列化。
 * 当消息发送方不是通过 StreamMQ SDK 发送（如 Go、Python 直接写 Redis Stream），
 * Stream Entry 中不包含 {@code bodyType} 字段，此时使用 Consumer 声明的 T 作为反序列化目标类型。
 *
 * <p>例如：Go 发送了一条 body 为 JSON string 的消息，
 * 消费者声明 {@code StreamMessageConcurrentlyConsumer<String>} 即可正确接收为 String，由消费者自行反序列化为目标类。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class BodyTypeResolver {

    private BodyTypeResolver() {
    }

    /**
     * 从 Consumer 实例解析其泛型 body 类型 T。
     *
     * @param consumer Consumer 实例（实现了 {@link StreamMessageConcurrentlyConsumer} / {@link StreamMessageOrderlyConsumer}）
     * @return 泛型 T 对应的 Class，解析失败返回 {@code null}
     */
    public static Class<?> resolve(Object consumer) {
        if (Objects.isNull(consumer)) {
            return null;
        }
        Class<?> clazz = consumer.getClass();
        // 遍历类层次结构（包括父类），查找实现了 StreamMQ Consumer 接口的泛型声明
        for (Class<?> c = clazz; Objects.nonNull(c) && c != Object.class; c = c.getSuperclass()) {
            Class<?> resolved = resolveFromInterfaces(c);
            if (Objects.nonNull(resolved)) {
                return resolved;
            }
        }
        return null;
    }

    /**
     * 从指定类直接实现的接口中查找 StreamMQ Consumer 泛型。
     */
    private static Class<?> resolveFromInterfaces(Class<?> clazz) {
        // getGenericInterfaces() 返回带泛型信息的 Type[]
        Type[] interfaces = clazz.getGenericInterfaces();
        for (Type iface : interfaces) {
            if (iface instanceof ParameterizedType pt) {
                Class<?> rawType = (Class<?>) pt.getRawType();
                if (isStreamMQConsumer(rawType)) {
                    Type[] typeArgs = pt.getActualTypeArguments();
                    if (typeArgs.length > 0) {
                        return resolveType(typeArgs[0]);
                    }
                }
            }
        }
        return null;
    }

    /**
     * 判断类型是否为 StreamMQ Consumer 接口。
     */
    private static boolean isStreamMQConsumer(Class<?> rawType) {
        return rawType == StreamMessageConcurrentlyConsumer.class
            || rawType == StreamMessageOrderlyConsumer.class;
    }

    /**
     * 将 Type 解析为 Class。
     *
     * <p>支持：
     * <ul>
     *   <li>普通 Class → 直接返回</li>
     *   <li>ParameterizedType（如 {@code List<Order>}）→ 返回原始类型</li>
     *   <li>TypeVariable（未指定具体类型，如裸泛型 T）→ 返回 null</li>
     * </ul>
     */
    private static Class<?> resolveType(Type type) {
        if (type instanceof Class<?> classType) {
            return classType;
        }
        if (type instanceof ParameterizedType pt) {
            Type raw = pt.getRawType();
            if (raw instanceof Class<?> rawClass) {
                return rawClass;
            }
        }
        // TypeVariable（裸泛型）或其他类型，无法解析
        return null;
    }
}
