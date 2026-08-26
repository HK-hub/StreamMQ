/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.serializer;

import io.github.streammq.core.serializer.MessageSerializer;
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
 * 首次使用前需调用 {@link org.apache.fury.Fury#register(Class)} 注册业务消息体类型， 或在 Redis 完全可信的场景下通过 {@link
 * #FurySerializer(boolean)} 传入 {@code false} 关闭白名单以获得任意 POJO 开箱即用能力 （关闭后 Redis 中被写入的字节流可反序列化为
 * classpath 上的任意类，共享/多租户 Redis 场景请勿关闭）。
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
     * 构造序列化器。
     *
     * @param requireClassRegistration 是否强制类注册白名单（生产环境建议开启以收窄反序列化攻击面）
     */
    public FurySerializer(boolean requireClassRegistration) {
        this.fury =
                Fury.builder()
                        .withLanguage(Language.JAVA)
                        .withRefTracking(true)
                        .requireClassRegistration(requireClassRegistration)
                        .buildThreadSafeFury();
    }

    @Override
    public byte[] serialize(T object, Class<T> type) {
        if (Objects.isNull(object)) {
            return new byte[0];
        }
        return fury.serialize(object);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R deserialize(byte[] bytes, Class<R> type) {
        if (Objects.isNull(bytes) || bytes.length == 0) {
            return null;
        }
        return (R) fury.deserialize(bytes);
    }

    @Override
    public String name() {
        return "fury";
    }
}
