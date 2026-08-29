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

/**
 * 基于 Apache Fury 的高性能跨语言序列化器。
 *
 * <p>Fury 支持 Java 对象的高性能序列化，性能显著优于 JDK 序列化， 且支持跨语言场景（通过 {@link Language#XLANG} 模式）。
 *
 * <p>注意：Fury 序列化要求被序列化的类与反序列化端的类版本一致， 适合 StreamMQ 内部消息体（body）的序列化。
 *
 * <p><b>安全提示：</b>默认 {@code requireClassRegistration=true}（secure-by-default）：仅允许显式注册过的类反序列化。
 * 首次使用前需调用 {@link #register(Class)} 注册业务消息体类型， 或在 Redis 完全可信的场景下通过 {@link #FurySerializer(boolean)}
 * 传入 {@code false} 关闭白名单以获得任意 POJO 开箱即用能力 （关闭后 Redis 中被写入的字节流可反序列化为 classpath 上的任意类，共享/多租户 Redis
 * 场景请勿关闭）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class FurySerializer<T> implements MessageSerializer<T> {

    private final ThreadSafeFury fury;

    /** 创建默认实例：强制类注册白名单（secure-by-default）。 */
    public FurySerializer() {
        this(true);
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
     * @param requireClassRegistration 是否强制类注册白名单（生产环境建议开启以收窄反序列化攻击面）
     * @throws SecurityException 当 {@code requireClassRegistration=false} 且未设置系统属性 {@code
     *     -Dstreammq.security.allowUnrestrictedSerializer=true} 时。 关闭白名单意味着 Redis 中被写入的字节流可反序列化 为
     *     classpath 上的任意类，共享/多租户 Redis 场景下是远程代码执行向量。 用户必须显式声明"我已知悉风险"才能使用。
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
        if (!requireClassRegistration
                && !Boolean.getBoolean("streammq.security.allowUnrestrictedSerializer")) {
            throw new SecurityException(
                    "FurySerializer(false) is gated by"
                        + " -Dstreammq.security.allowUnrestrictedSerializer=true. Disabling Fury's"
                        + " class registration whitelist is a known RCE vector on shared Redis."
                        + " Either keep the whitelist (default) and register your message types via"
                        + " Fury.register(Class), or set the system property after confirming Redis"
                        + " is fully trusted.");
        }
        this.fury =
                Fury.builder()
                        .withLanguage(Language.JAVA)
                        .withRefTracking(true)
                        .requireClassRegistration(requireClassRegistration)
                        .buildThreadSafeFury();
        registerAll(registeredTypes);
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
                            + ". If the cause mentions 'class ... is not registered', register"
                            + " register it with FurySerializer.register(Class) or switch to"
                            + " Jackson. Underlying: "
                            + ex.getMessage(),
                    ex);
        }
    }

    @Override
    public String name() {
        return "fury";
    }
}
