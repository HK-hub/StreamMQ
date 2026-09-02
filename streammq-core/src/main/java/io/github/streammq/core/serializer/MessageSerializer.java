/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.serializer;

import io.github.streammq.core.exception.SerializationException;

/**
 * 消息序列化器 SPI，负责 Message body 与 byte[] 的双向转换。
 *
 * <p>元信息（topic/tag/keys/shardingKey/properties）始终为 String，不参与序列化。 仅 {@code body} 字段经由此接口序列化。
 *
 * <p>内置实现（默认使用 Fury）：
 *
 * <ul>
 *   <li>{@code FurySerializer} - 基于 Apache Fury 的二进制序列化（<b>默认</b>，见 {@link
 *       io.github.streammq.core.StreamMQConstants#DEFAULT_SERIALIZER}）
 *   <li>{@code JacksonJsonSerializer} - 基于 Jackson 的 JSON 序列化（跨语言/可读性优先）
 *   <li>{@code JdkSerializer} - 基于 JDK 原生序列化（备选）
 *   <li>{@code ProtostuffSerializer} - 基于 Protostuff 的二进制序列化
 *   <li>{@code StringSerializer} / {@code ByteArraySerializer} - 直通序列化
 * </ul>
 *
 * <p><b>注意：</b>{@code FurySerializer}（默认序列化器）默认不强制类注册，任意 POJO 开箱即用；共享/多租户 Redis 建议开启类注册白名单（{@code
 * new FurySerializer(true)} 或 Spring 配置 {@code
 * streammq.producer.fury-require-class-registration=true}）。{@code JdkSerializer} 内置 JEP 290
 * 白名单，自定义业务 body 类型需显式加白，详见各实现类 Javadoc。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface MessageSerializer<T> {

    /**
     * 序列化对象为 byte[]。
     *
     * @param object 待序列化对象
     * @param type 目标类型（用于多态场景）
     * @return 字节数组
     * @throws SerializationException 序列化失败
     */
    byte[] serialize(T object, Class<T> type) throws SerializationException;

    /**
     * 反序列化 byte[] 为对象。
     *
     * @param bytes 字节数组
     * @param type 目标类型
     * @param <R> 反序列化目标类型
     * @return 反序列化对象
     * @throws SerializationException 反序列化失败
     */
    <R> R deserialize(byte[] bytes, Class<R> type) throws SerializationException;

    /**
     * 返回序列化器名称（用于 SPI 选择与监控）。
     *
     * @return 名称
     */
    default String name() {
        return getClass().getSimpleName();
    }
}
