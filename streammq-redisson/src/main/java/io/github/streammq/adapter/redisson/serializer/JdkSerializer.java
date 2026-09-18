/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.serializer;

import io.github.streammq.adapter.redisson.support.PayloadTypeSafety;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.serializer.MessageSerializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDK 原生序列化器，内置 JEP 290 反序列化过滤器。
 *
 * <p><b>安全模型：</b>JDK 反序列化在 {@code readObject()} 期间即可执行类上的回调逻辑， 经典 Commons-Collections 等 gadget
 * 链可在类型检查之前完成攻击。因此本实现默认启用 类名白名单过滤器：仅允许目标类型、JDK 基础类型（原始包装/字符串/集合/时间/大数）以及
 * 用户显式追加的类名通过；任何其它类（包括数组元素与动态代理）在反序列化前即被拒绝。
 *
 * <p>若业务 body 引用了第三方库中的自定义类型，请通过 {@link #JdkSerializer(Collection)} 或 {@link
 * #addAllowedClasses(Collection)} 显式放行—— 只放行业务确实需要的类，不要使用 {@link #unrestricted()} 关闭过滤。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class JdkSerializer<T extends java.io.Serializable> implements MessageSerializer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(JdkSerializer.class);

    /** 默认放行的 JDK 集合/容器实现（前缀规则之外需要逐一列出的部分） */
    private static final Set<String> DEFAULT_ALLOWED_CLASSES =
            Set.of(
                    // java.lang 语言标量（刻意不再整包放行 java.lang，避免 java.lang.reflect /
                    // java.lang.invoke 等 gadget 使能类被放行）
                    "java.lang.String",
                    "java.lang.Integer",
                    "java.lang.Long",
                    "java.lang.Short",
                    "java.lang.Byte",
                    "java.lang.Boolean",
                    "java.lang.Character",
                    "java.lang.Double",
                    "java.lang.Float",
                    "java.lang.Number",
                    "java.lang.StringBuilder",
                    // java.lang.Object：集合/数组的**结构性**元素类型（ArrayList 的 Object[] 后备数组、
                    // 异构数组元素等）。JDK 在 checkArray 时会以组件类型回调过滤器，拒绝它会让任何
                    // List/数组载荷无法反序列化；Object 本身无 readObject/readResolve 钩子，放行不引入 gadget 面。
                    "java.lang.Object",
                    // 集合/容器
                    "java.util.ArrayList",
                    "java.util.LinkedList",
                    "java.util.ArrayDeque",
                    "java.util.HashMap",
                    "java.util.LinkedHashMap",
                    "java.util.TreeMap",
                    "java.util.HashSet",
                    "java.util.LinkedHashSet",
                    "java.util.TreeSet",
                    "java.util.Date",
                    "java.util.UUID",
                    "java.util.BitSet");

    /** 允许的前缀：时间 / 大数（语言标量改为逐类放行，见 {@link #DEFAULT_ALLOWED_CLASSES}） */
    private static final Set<String> ALLOWED_PREFIXES = Set.of("java.time.", "java.math.");

    private final Set<String> allowedClasses = new HashSet<>(DEFAULT_ALLOWED_CLASSES);

    /** 创建默认（白名单加固）实例。 */
    public JdkSerializer() {}

    /**
     * 创建带自定义类名白名单的实例。
     *
     * @param allowedClasses 额外放行的类全限定名集合
     */
    public JdkSerializer(Collection<String> allowedClasses) {
        addAllowedClasses(allowedClasses);
    }

    /**
     * 追加放行的类全限定名。
     *
     * @param classNames 类全限定名集合
     */
    public synchronized void addAllowedClasses(Collection<String> classNames) {
        if (Objects.nonNull(classNames)) {
            classNames.stream().filter(Objects::nonNull).forEach(allowedClasses::add);
        }
    }

    /**
     * 创建完全不过滤的实例（仅供兼容迁移使用）。
     *
     * <p><b>警告：</b>等同于开放任意 classpath 类的反序列化，存在已知 gadget 利用面；仅当 Redis 完全可信时使用，并尽快迁移到白名单模式。
     *
     * <p><b>门禁：</b>必须显式设置 JVM 系统属性 {@code -Dstreammq.security.allowUnrestrictedSerializer=true}
     * 才能创建无过滤器实例。缺省情况下抛 {@link SecurityException}——这是反序列化 RCE 的最后一道防线。
     *
     * @return 无过滤器实例
     * @throws SecurityException 当未设置上述系统属性时
     * @deprecated 该方法在共享/多租户 Redis 场景下是远程代码执行向量。请改用 {@link #JdkSerializer(Collection)} 显式放行业务类型；并设置
     *     {@code -Djdk.serialFilter=...} 兜底。
     */
    @Deprecated
    public static <T extends java.io.Serializable> JdkSerializer<T> unrestricted() {
        if (!Boolean.getBoolean("streammq.security.allowUnrestrictedSerializer")) {
            throw new SecurityException(
                    "JdkSerializer.unrestricted() is gated by"
                        + " -Dstreammq.security.allowUnrestrictedSerializer=true. This method is a"
                        + " known RCE vector on shared Redis. Use JdkSerializer(Set<String>) to"
                        + " explicitly allow only the classes your business code needs.");
        }
        JdkSerializer<T> s = new JdkSerializer<>();
        s.allowedClasses.clear();
        return s;
    }

    /**
     * {@inheritDoc}
     *
     * <p>{@code object == null} 时返回 {@code null}（与 {@code MessageSerializer} 的统一 null 契约一致）。
     */
    @Override
    public byte[] serialize(T object, Class<T> type) throws SerializationException {
        if (object == null) {
            return null;
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
            installFilter(ois, type.getName());
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

    /** 组装本次调用的过滤器：目标类型 + 全局白名单 + 前缀规则 + 数组展开 + 深度/引用上限。 */
    private void installFilter(ObjectInputStream ois, String targetTypeName) {
        Set<String> effective = new HashSet<>(allowedClasses);
        // 目标类型可能是载荷驱动解析出来的（bodyType），不可无条件信任：
        // 仅在其通过统一载荷类型护栏时才加入本次放行集，避免“载荷自扩白名单”。
        if (Objects.nonNull(targetTypeName) && !PayloadTypeSafety.isBlocked(targetTypeName)) {
            effective.add(PayloadTypeSafety.normalize(targetTypeName));
        } else if (Objects.nonNull(targetTypeName)) {
            LOG.warn(
                    "JDK deserialization target type rejected by payload type guard (dangerous"
                            + " namespace or malformed name): {} — the type is not added to the"
                            + " allow-list for this read",
                    targetTypeName);
        }
        ObjectInputFilter filter =
                info -> {
                    Class<?> serialClass = info.serialClass();
                    String className = Objects.isNull(serialClass) ? null : serialClass.getName();
                    if (Objects.isNull(className)) {
                        // serialClass == null 表示 JDK 正在做「深度 / 引用计数」检查，而不是类检查：
                        // 反序列化含对象回引用（同一实例被写入多次，线格式 TC_REFERENCE）的合法载荷时，
                        // JDK 21 会以 class=null、arrayLength=-1 调用过滤器；此处返回 REJECTED 会让
                        // 合法载荷抛 InvalidClassException: filter status: REJECTED。
                        // 返回 UNDECIDED 交由 merge 进来的 maxdepth/maxrefs/maxbytes 兜底过滤器裁决。
                        return ObjectInputFilter.Status.UNDECIDED;
                    }
                    // 数组类型：校验组件类名
                    while (className.startsWith("[")) {
                        if (className.startsWith("[L") && className.endsWith(";")) {
                            className = className.substring(2, className.length() - 1);
                        } else {
                            // 原生类型数组（[I、[J 等）
                            return ObjectInputFilter.Status.ALLOWED;
                        }
                    }
                    if (effective.contains(className)) {
                        return ObjectInputFilter.Status.ALLOWED;
                    }
                    // 枚举类型本身不携带 readObject 钩子，放行以保证枚举 body 可用
                    if (serialClass.isEnum()) {
                        return ObjectInputFilter.Status.ALLOWED;
                    }
                    for (String prefix : ALLOWED_PREFIXES) {
                        if (className.startsWith(prefix)) {
                            return ObjectInputFilter.Status.ALLOWED;
                        }
                    }
                    return ObjectInputFilter.Status.REJECTED;
                };
        ois.setObjectInputFilter(
                ObjectInputFilter.merge(
                        filter,
                        ObjectInputFilter.Config.createFilter(
                                "maxdepth=64;maxrefs=4096;maxbytes=104857600")));
    }

    @Override
    public String name() {
        return "jdk";
    }

    /** 供测试/诊断使用：返回当前生效的白名单快照。 */
    synchronized Collection<String> snapshotAllowedClasses() {
        return new ArrayList<>(allowedClasses);
    }
}
