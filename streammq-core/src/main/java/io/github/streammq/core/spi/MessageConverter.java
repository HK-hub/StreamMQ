package io.github.streammq.core.spi;

import io.github.streammq.core.message.Message;

import java.util.Map;

/**
 * 消息转换器：连接 {@link Message} 与 Redis Stream Entry 字段的双向转换。
 *
 * <p>负责：
 * <ul>
 *   <li>将 {@code Message} 转换为 Stream Entry 字段 Map（用于 XADD）</li>
 *   <li>将 Stream Entry 字段 Map 还原为 {@code Message}（用于 XREADGROUP）</li>
 * </ul>
 *
 * <p>不负责 body 的序列化（由 {@link MessageSerializer} 处理），仅做字段映射。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface MessageConverter {

    /**
     * 将 Message 转换为 Stream Entry 字段 Map。
     *
     * @param message 消息
     * @return 不可修改的 Stream Entry 字段 Map（key 为字段名，value 为字符串/byte[]）
     * @throws IllegalArgumentException 如果 message 缺失必填字段
     */
    Map<String, String> toStreamFields(Message<?> message);

    /**
     * 将 Stream Entry 字段 Map 还原为 Message。
     *
     * @param fields Stream Entry 字段 Map
     * @param targetType body 目标类型（用于反序列化）
     * @param <T> body 类型
     * @return Message 实例
     * @throws io.github.streammq.core.exception.SerializationException 反序列化失败
     */
    <T> Message<T> fromStreamFields(Map<String, String> fields, Class<T> targetType);

    /**
     * 返回转换器名称。
     *
     * @return 名称
     */
    default String name() {
        return getClass().getSimpleName();
    }
}
