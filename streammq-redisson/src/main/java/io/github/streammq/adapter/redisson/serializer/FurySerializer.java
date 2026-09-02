/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.serializer;

import io.github.streammq.core.serializer.MessageSerializer;
import java.util.Arrays;
import java.util.Objects;
import org.apache.fury.Fury;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.config.Language;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 Apache Fury 的高性能跨语言序列化器。
 *
 * <p>Fury 支持 Java 对象的高性能序列化，性能显著优于 JDK 序列化， 且支持跨语言场景（通过 {@link Language#XLANG} 模式）。
 *
 * <p>注意：Fury 序列化要求被序列化的类与反序列化端的类版本一致， 适合 StreamMQ 内部消息体（body）的序列化。
 *
 * <p><b>安全提示：</b>序列化器默认采用<b>宽松模式</b>（{@code requireClassRegistration=false}），任意 POJO 均可开箱即用， 但
 * Redis 中被写入的字节流可被反序列化为 classpath 上的任意类，共享/多租户 Redis 场景下是反序列化攻击面（RCE 向量）。 生产环境建议开启类注册白名单：{@code new
 * FurySerializer(true)} 或 {@code new FurySerializer<>(Xxx.class)}， 并通过 {@link #register(Class)} /
 * {@link #registerAll(Class...)} 注册业务消息体类型。 宽松模式下构造会输出一条 WARN 提醒；如已评估并接受风险，可设置系统属性 {@code
 * -Dstreammq.security.allowUnrestrictedSerializer=true} 抑制该提醒。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class FurySerializer<T> implements MessageSerializer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(FurySerializer.class);

    /** 关闭类注册白名单（宽松模式）的显式确认属性：设置后抑制构造时的 WARN 提醒。 */
    static final String ALLOW_UNRESTRICTED_SERIALIZER_PROPERTY =
            "streammq.security.allowUnrestrictedSerializer";

    private final ThreadSafeFury fury;

    private final boolean requireClassRegistration;

    /** 创建默认实例：宽松模式（{@code requireClassRegistration=false}），任意 POJO 开箱即用。 */
    public FurySerializer() {
        this(false);
    }

    /**
     * Creates a secure serializer and registers the supplied application types. Registration is
     * performed once during construction and is safe to use from all producer and consumer threads
     * afterwards.
     *
     * @param registeredTypes message types allowed by Fury's class-registration whitelist
     */
    public FurySerializer(Class<?>... registeredTypes) {
        this(true, registeredTypes);
    }

    /**
     * 构造序列化器。
     *
     * @param requireClassRegistration 是否强制类注册白名单（{@code true} 时仅允许显式注册过的类反序列化， 建议共享/多租户 Redis
     *     场景开启；{@code false} 为宽松模式，任意 POJO 开箱即用）
     */
    public FurySerializer(boolean requireClassRegistration) {
        this(requireClassRegistration, new Class<?>[0]);
    }

    /**
     * Creates a serializer with an explicit security mode and optional initial registrations.
     *
     * @param requireClassRegistration whether Fury must use its class whitelist
     * @param registeredTypes classes to register when the serializer is created
     */
    public FurySerializer(boolean requireClassRegistration, Class<?>... registeredTypes) {
        this.requireClassRegistration = requireClassRegistration;
        warnIfUnrestricted(requireClassRegistration);
        this.fury =
                Fury.builder()
                        .withLanguage(Language.JAVA)
                        .withRefTracking(true)
                        .requireClassRegistration(requireClassRegistration)
                        .buildThreadSafeFury();
        registerAll(registeredTypes);
    }

    private static void warnIfUnrestricted(boolean requireClassRegistration) {
        if (!requireClassRegistration
                && !Boolean.getBoolean(ALLOW_UNRESTRICTED_SERIALIZER_PROPERTY)) {
            LOG.warn(
                    "FurySerializer created without class registration whitelist"
                        + " (requireClassRegistration=false). Bytes read from Redis may deserialize"
                        + " to arbitrary classes on the classpath - an RCE vector on"
                        + " shared/multi-tenant Redis. Prefer new FurySerializer(true) (or new"
                        + " FurySerializer<>(YourType.class)) and register your message types; or"
                        + " set -Dstreammq.security.allowUnrestrictedSerializer=true after"
                        + " confirming Redis is fully trusted to suppress this warning.");
        }
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
     * Registers a message type in Fury's whitelist.
     *
     * @param type application message class
     * @return this serializer for fluent configuration
     */
    public FurySerializer<T> register(Class<?> type) {
        fury.register(Objects.requireNonNull(type, "type"));
        return this;
    }

    /**
     * Registers multiple message types in Fury's whitelist.
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
            return fury.serialize(object);
        } catch (RuntimeException ex) {
            throw new io.github.streammq.core.exception.SerializationException(
                    "Fury serialize failed for "
                            + type.getName()
                            + ". If the cause mentions 'class ... is not registered', call"
                            + " serializer.register("
                            + type.getName()
                            + ".class) once at startup, or switch to Jackson"
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
            return (R) fury.deserialize(bytes);
        } catch (RuntimeException ex) {
            throw new io.github.streammq.core.exception.SerializationException(
                    "Fury deserialize failed for "
                            + type.getName()
                            + ". If the cause mentions 'class ... is not registered', call"
                            + " serializer.register("
                            + type.getName()
                            + ".class) once at startup, or switch to Jackson"
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
