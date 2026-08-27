/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.util;

import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 解析 Consumer 实现类的泛型 body 类型 T。
 *
 * <p>StreamMQ 的 Consumer 接口均声明了泛型 T：
 *
 * <ul>
 *   <li>{@link StreamMessageConcurrentlyConsumer}&lt;T&gt;
 *   <li>{@link StreamMessageOrderlyConsumer}&lt;T&gt;
 * </ul>
 *
 * <p>本工具通过反射提取实现类上声明的具体泛型类型，用于跨平台消息反序列化。 当消息发送方不是通过 StreamMQ SDK 发送（如 Go、Python 直接写 Redis Stream），
 * Stream Entry 中不包含 {@code bodyType} 字段，此时使用 Consumer 声明的 T 作为反序列化目标类型。
 *
 * <p>解析算法沿类层次结构自下而上行走，并在每层维护「类型变量 → 实际类型」绑定表（type-variable substitution），因此支持任意深度的中间泛型基类链，例如：
 *
 * <pre>{@code
 * interface StreamMessageConsumer<T> { ... }
 * abstract class Base<T> implements StreamMessageConcurrentlyConsumer<T> {}
 * class Child extends Base<BigDecimal> {}   // 解析结果：BigDecimal
 * }</pre>
 *
 * <p>例如：Go 发送了一条 body 为 JSON string 的消息， 消费者声明 {@code StreamMessageConcurrentlyConsumer<String>}
 * 即可正确接收为 String，由消费者自行反序列化为目标类。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class BodyTypeResolver {

    /** 层次结构遍历深度上限，防御异常深的继承链与潜在的循环引用 */
    private static final int MAX_HIERARCHY_DEPTH = 32;

    private BodyTypeResolver() {}

    /**
     * 从 Consumer 实例解析其泛型 body 类型 T。
     *
     * @param consumer Consumer 实例（实现了 {@link StreamMessageConcurrentlyConsumer} / {@link
     *     StreamMessageOrderlyConsumer}）
     * @return 泛型 T 对应的 Class，解析失败返回 {@code null}
     */
    public static Class<?> resolve(Object consumer) {
        if (Objects.isNull(consumer)) {
            return null;
        }
        return resolveHierarchy(consumer.getClass(), new HashMap<>(), 0);
    }

    /**
     * 沿类层次结构向上查找 Consumer 泛型实参，携带父类形参变量的实际类型绑定表。
     *
     * @param clazz 当前类
     * @param bindings 已知的「类型变量 → 实际类型」绑定（来自更具体层次的参数化父类声明）
     * @param depth 当前深度
     * @return 解析到的 Class，未解析到返回 {@code null}
     */
    private static Class<?> resolveHierarchy(
            Class<?> clazz, Map<TypeVariable<?>, Type> bindings, int depth) {
        if (depth > MAX_HIERARCHY_DEPTH || Objects.isNull(clazz) || clazz == Object.class) {
            return null;
        }
        // 先检查当前类直接声明的接口（含接口继承链）
        for (Type iface : clazz.getGenericInterfaces()) {
            Class<?> resolved = resolveFromInterface(iface, bindings, depth);
            if (Objects.nonNull(resolved)) {
                return resolved;
            }
        }
        // 再沿父类继续向上，参数化父类同时产生新的类型变量绑定
        Type superclass = clazz.getGenericSuperclass();
        if (superclass instanceof ParameterizedType pt) {
            Class<?> rawSuper = (Class<?>) pt.getRawType();
            Map<TypeVariable<?>, Type> childBindings = new HashMap<>(bindings);
            bindTypeVariables(rawSuper, pt.getActualTypeArguments(), childBindings);
            return resolveHierarchy(rawSuper, childBindings, depth + 1);
        }
        // 裸（非参数化）父类：绑定关系不变，仅前进一层
        return resolveHierarchy(clazz.getSuperclass(), new HashMap<>(bindings), depth + 1);
    }

    /**
     * 在接口（及其继承链）中查找 Consumer 泛型实参。
     *
     * @param type 接口 Type（可能为参数化形式）
     * @param bindings 类型变量绑定表
     * @param depth 当前深度
     * @return 解析到的 Class，未解析到返回 {@code null}
     */
    private static Class<?> resolveFromInterface(
            Type type, Map<TypeVariable<?>, Type> bindings, int depth) {
        if (depth > MAX_HIERARCHY_DEPTH || !(type instanceof ParameterizedType pt)) {
            return null;
        }
        Class<?> rawInterface = (Class<?>) pt.getRawType();
        if (isStreamMQConsumer(rawInterface)) {
            Type[] typeArgs = pt.getActualTypeArguments();
            return typeArgs.length > 0 ? resolveToClass(typeArgs[0], bindings) : null;
        }
        // 非目标接口：把该接口自身的形参变量代入后，沿其声明的父接口继续查找
        Map<TypeVariable<?>, Type> ifaceBindings = new HashMap<>(bindings);
        bindTypeVariables(rawInterface, pt.getActualTypeArguments(), ifaceBindings);
        for (Type superInterface : rawInterface.getGenericInterfaces()) {
            Class<?> resolved = resolveFromInterface(superInterface, ifaceBindings, depth + 1);
            if (Objects.nonNull(resolved)) {
                return resolved;
            }
        }
        return null;
    }

    /**
     * 将 {@code owner} 声明的形参变量按 {@code actualArgs} 绑定到实际类型，写入绑定表。 实参中的类型变量先经当前绑定表代入， 以处理多层泛型传递（如
     * {@code Mid<T> extends Base<List<T>>}）。
     */
    private static void bindTypeVariables(
            Class<?> owner, Type[] actualArgs, Map<TypeVariable<?>, Type> bindings) {
        TypeVariable<?>[] typeParameters = owner.getTypeParameters();
        for (int i = 0; i < typeParameters.length && i < actualArgs.length; i++) {
            bindings.put(typeParameters[i], substitute(actualArgs[i], bindings));
        }
    }

    /** 将 Type 中出现的已绑定类型变量替换为其实际类型。 */
    private static Type substitute(Type type, Map<TypeVariable<?>, Type> bindings) {
        if (type instanceof TypeVariable<?> variable) {
            Type bound = bindings.get(variable);
            return Objects.nonNull(bound) ? bound : variable;
        }
        return type;
    }

    /**
     * 将（可能为类型变量的）Type 代入绑定后解析为 Class。
     *
     * <p>支持普通 Class 与 ParameterizedType（取原始类型）；仍未绑定的裸类型变量返回 {@code null}。
     */
    private static Class<?> resolveToClass(Type type, Map<TypeVariable<?>, Type> bindings) {
        Type substituted = substitute(type, bindings);
        if (substituted instanceof Class<?> classType) {
            return classType;
        }
        if (substituted instanceof ParameterizedType pt
                && pt.getRawType() instanceof Class<?> raw) {
            return raw;
        }
        return null;
    }

    /** 判断类型是否为 StreamMQ Consumer 接口。 */
    private static boolean isStreamMQConsumer(Class<?> rawType) {
        return rawType == StreamMessageConcurrentlyConsumer.class
                || rawType == StreamMessageOrderlyConsumer.class;
    }
}
