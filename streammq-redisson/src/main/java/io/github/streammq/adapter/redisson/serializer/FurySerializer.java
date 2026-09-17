/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.serializer;

import io.github.streammq.core.serializer.MessageSerializer;
import java.util.Arrays;
import java.util.Objects;
import org.apache.fory.Fory;
import org.apache.fory.ThreadSafeFory;
import org.apache.fory.config.Language;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 Apache Fory（原 Apache Fury）的高性能跨语言序列化器，是 StreamMQ 的<b>可选</b>高吞吐序列化器。
 *
 * <p>Fory 支持 Java 对象的高性能序列化，性能显著优于 JDK 序列化， 且支持跨语言场景（通过 {@link Language#XLANG} 模式）。 其吞吐约为 Jackson 的
 * 7~13 倍、JDK 的约 10 倍，且无需 {@code .proto} 文件、任意 POJO 开箱即用—— 这是项目在「吞吐优先」与「零 RCE 面」之间做的明确权衡（默认序列化器为
 * {@code JacksonJsonSerializer}，见 {@link
 * io.github.streammq.core.StreamMQConstants#DEFAULT_SERIALIZER} 与 SECURITY.md）。
 *
 * <p><b>依赖坐标与版本下限：</b>{@code org.apache.fory:fory-core}。1.1.0 之前的版本（原 {@code
 * org.apache.fury:fury-core} 与 fory 早期版本）存在 <b>CVE-2026-50076</b>（CVSS 9.1：反序列化时可绕过类注册校验， 触发
 * classpath 上的 resolve/readExternal 钩子），因此本项目要求 {@code >= 1.1.0}。
 *
 * <p>注意：Fory 序列化要求被序列化的类与反序列化端的类版本一致， 适合 StreamMQ 内部消息体（body）的序列化。
 *
 * <p><b>⚠️ 安全姿态（默认强制类注册白名单）：</b>构造器默认采用<b>强制类注册白名单</b>（{@code requireClassRegistration=true}）——
 * 只有显式注册过的类型才能被反序列化，未注册类型直接拒绝。仅当显式调用 {@code new FurySerializer(false)} 关闭白名单（宽松模式）时， Redis
 * 中被写入的字节流才可被反序列化为 classpath 上的<b>任意类</b>；在<b>共享/多租户 Redis</b>场景下，攻击者写入消息字节即可构造 gadget 链触发
 * <b>远程代码执行（RCE）</b>。
 *
 * <p><b>校验时机提示：</b>{@code isInstance} 类型校验发生在反序列化<b>之后</b>，只能发现结果类型不匹配，无法阻止 gadget 在反序列化过程中
 * 执行——真正的防线是默认开启的类注册白名单。
 *
 * <p><b>风险缓解（按场景选择）：</b>
 *
 * <ul>
 *   <li><b>受信单租户 Redis</b>（最常见内部部署）：保持默认白名单模式，并预注册业务消息体类型；
 *   <li><b>共享/多租户 Redis</b>：保持默认白名单（{@code new FurySerializer()} / {@code new
 *       FurySerializer<>(Xxx.class)} 均可），并通过 {@link #register(Class)} / {@link
 *       #registerAll(Class...)} 预注册业务消息体类型，或在 Spring 配置中设置 {@code
 *       streammq.producer.fury-registered-classes}；
 *   <li><b>不能接受任何 RCE 面</b>：切换为 {@code JacksonJsonSerializer}（严格类型、无多态反序列化）或 {@code
 *       io.github.streammq.adapter.redisson.serializer.ProtostuffSerializer}（schema 由目标类型决定，无
 *       gadget 面）。
 * </ul>
 *
 * <p>宽松模式构造受系统属性门禁保护：必须显式设置 {@code -Dstreammq.security.allowUnrestrictedSerializer=true} 才能创建（否则抛
 * {@link SecurityException}）——这是反序列化 RCE 的最后一道防线，与 {@link JdkSerializer#unrestricted()} 的
 * 门禁保持一致。类名与 {@link #name()} 保持不变（{@code FurySerializer} / {@code "fury"}），以兼容 0.1.x 已发布的配置写法。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class FurySerializer<T> implements MessageSerializer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(FurySerializer.class);

    /** 关闭类注册白名单（宽松模式）的显式确认属性：未设置时构造宽松实例将抛 {@link SecurityException}。 */
    static final String ALLOW_UNRESTRICTED_SERIALIZER_PROPERTY =
            "streammq.security.allowUnrestrictedSerializer";

    private final ThreadSafeFory fory;

    private final boolean requireClassRegistration;

    /** 创建默认实例：强制类注册白名单模式（{@code requireClassRegistration=true}），仅允许注册过的类型。 */
    public FurySerializer() {
        this(true);
    }

    /**
     * Creates a secure serializer and registers the supplied application types. Registration is
     * performed once during construction and is safe to use from all producer and consumer threads
     * afterwards.
     *
     * @param registeredTypes message types allowed by Fory's class-registration whitelist
     */
    public FurySerializer(Class<?>... registeredTypes) {
        this(true, registeredTypes);
    }

    /**
     * 构造序列化器。
     *
     * @param requireClassRegistration 是否强制类注册白名单（{@code true} 时仅允许显式注册过的类反序列化， 建议共享/多租户 Redis
     *     场景开启；{@code false} 为宽松模式，任意 POJO 开箱即用，受系统属性门禁保护）
     * @throws SecurityException 当 {@code requireClassRegistration=false} 且未设置 {@code
     *     -Dstreammq.security.allowUnrestrictedSerializer=true} 时
     */
    public FurySerializer(boolean requireClassRegistration) {
        this(requireClassRegistration, new Class<?>[0]);
    }

    /**
     * Creates a serializer with an explicit security mode and optional initial registrations.
     *
     * @param requireClassRegistration whether Fory must use its class whitelist
     * @param registeredTypes classes to register when the serializer is created
     * @throws SecurityException when the unrestricted mode is requested without the explicit
     *     system-property confirmation
     */
    public FurySerializer(boolean requireClassRegistration, Class<?>... registeredTypes) {
        this.requireClassRegistration = requireClassRegistration;
        guardUnrestricted(requireClassRegistration);
        this.fory =
                Fory.builder()
                        .withLanguage(Language.JAVA)
                        .withRefTracking(true)
                        .requireClassRegistration(requireClassRegistration)
                        .buildThreadSafeFory();
        registerAll(registeredTypes);
    }

    /**
     * 宽松模式门禁：未显式确认时拒绝构造（与 {@link JdkSerializer#unrestricted()} 同语义）。
     *
     * <p>宽松模式读取的字节流可实例化 classpath 上的任意类，在共享/多租户 Redis 下是远程代码执行向量； 因此不提供"仅告警"的降级路径——必须由运维显式声明"该
     * Redis 完全可信"。
     */
    private static void guardUnrestricted(boolean requireClassRegistration) {
        if (requireClassRegistration) {
            return;
        }
        if (!Boolean.getBoolean(ALLOW_UNRESTRICTED_SERIALIZER_PROPERTY)) {
            throw new SecurityException(
                    "Unrestricted FurySerializer (requireClassRegistration=false) is gated by"
                            + " -Dstreammq.security.allowUnrestrictedSerializer=true. Unregistered"
                            + " bytes read from Redis may deserialize to arbitrary classes on the"
                            + " classpath - an RCE vector on shared/multi-tenant Redis. Prefer new"
                            + " FurySerializer(true) (or new FurySerializer<>(YourType.class)) and"
                            + " register your message types; or switch to JacksonJsonSerializer.");
        }
        LOG.warn(
                "Unrestricted FurySerializer created (requireClassRegistration=false) with"
                        + " explicit {} confirmation. Bytes read from Redis may deserialize to"
                        + " arbitrary classes on the classpath - never use this mode on"
                        + " shared/multi-tenant Redis.",
                ALLOW_UNRESTRICTED_SERIALIZER_PROPERTY);
    }

    /**
     * 返回当前是否强制类注册白名单。
     *
     * @return {@code true} 表示仅允许显式注册过的类反序列化（secure-by-default）
     */
    public boolean isRequireClassRegistration() {
        return requireClassRegistration;
    }

    /**
     * Registers a message type in Fory's whitelist.
     *
     * @param type application message class
     * @return this serializer for fluent configuration
     */
    public FurySerializer<T> register(Class<?> type) {
        fory.register(Objects.requireNonNull(type, "type"));
        return this;
    }

    /**
     * Registers multiple message types in Fory's whitelist.
     *
     * @param types application message classes; null elements are rejected
     * @return this serializer for fluent configuration
     */
    public FurySerializer<T> registerAll(Class<?>... types) {
        Objects.requireNonNull(types, "types");
        Arrays.stream(types).forEach(this::register);
        return this;
    }

    @Override
    public byte[] serialize(T object, Class<T> type) {
        if (Objects.isNull(object)) {
            return new byte[0];
        }
        try {
            return fory.serialize(object);
        } catch (RuntimeException ex) {
            throw new io.github.streammq.core.exception.SerializationException(
                    "Fury serialize failed for "
                            + type.getName()
                            + ". If the cause mentions 'class ... is not registered', call"
                            + " serializer.register("
                            + type.getName()
                            + ".class) once at startup (or set"
                            + " streammq.producer.fury-registered-classes), or switch to Jackson"
                            + " (JacksonJsonSerializer) which does not require pre-registration."
                            + " Underlying: "
                            + ex.getMessage(),
                    ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R deserialize(byte[] bytes, Class<R> type) {
        if (Objects.isNull(bytes) || bytes.length == 0) {
            return null;
        }
        try {
            Object result = fory.deserialize(bytes);
            if (Objects.nonNull(result) && !type.isInstance(result)) {
                throw new io.github.streammq.core.exception.SerializationException(
                        "Fury deserialized type "
                                + result.getClass().getName()
                                + " is not assignable to expected type "
                                + type.getName()
                                + "; class mismatch may indicate tampered payload."
                                + " Use requireClassRegistration=true or switch to"
                                + " JacksonJsonSerializer which validates type safety.");
            }
            return (R) result;
        } catch (RuntimeException ex) {
            throw new io.github.streammq.core.exception.SerializationException(
                    "Fury deserialize failed for "
                            + type.getName()
                            + ". If the cause mentions 'class ... is not registered', call"
                            + " serializer.register("
                            + type.getName()
                            + ".class) once at startup (or set"
                            + " streammq.producer.fury-registered-classes), or switch to Jackson"
                            + " (JacksonJsonSerializer) which does not require pre-registration."
                            + " Underlying: "
                            + ex.getMessage(),
                    ex);
        }
    }

    @Override
    public String name() {
        return "fury";
    }
}
