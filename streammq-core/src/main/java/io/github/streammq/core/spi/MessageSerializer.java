package io.github.streammq.core.spi;

import io.github.streammq.core.exception.SerializationException;

/**
 * 消息序列化器 SPI，负责 Message body 与 byte[] 的双向转换。
 *
 * <p>元信息（topic/tag/keys/shardingKey/properties）始终为 String，不参与序列化。
 * 仅 {@code body} 字段经由此接口序列化。
 *
 * <p>默认实现：
 * <ul>
 *   <li>{@code JacksonJsonSerializer} - 基于 Jackson 的 JSON 序列化（默认）</li>
 *   <li>{@code JdkSerializer} - 基于 JDK 原生序列化（备选）</li>
 * </ul>
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
