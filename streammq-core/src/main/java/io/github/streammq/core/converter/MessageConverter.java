/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.converter;

import io.github.streammq.core.message.Message;
import java.util.Map;

/**
 * 消息转换器：连接 {@link Message} 与 Redis Stream Entry 字段的双向转换。
 *
 * <p>负责：
 *
 * <ul>
 *   <li>将 {@code Message} 转换为 Stream Entry 字段 Map（用于 XADD）
 *   <li>将 Stream Entry 字段 Map 还原为 {@code Message}（用于 XREADGROUP）
 * </ul>
 *
 * <p>不负责 body 的序列化（由 {@link io.github.streammq.core.serializer.MessageSerializer} 处理），仅做字段映射。
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
     * 将 Stream Entry 字段 Map 还原为 Message（不允许字段缺失 Topic，除非实现另有语义）。
     *
     * <p>默认委托到 {@link #fromStreamFields(Map, Class, String)} 并传入 {@code null} 回填值。
     *
     * @param fields Stream Entry 字段 Map
     * @param targetType body 目标类型（用于反序列化）
     * @param <T> body 类型
     * @return Message 实例
     * @throws io.github.streammq.core.exception.SerializationException 反序列化失败或 Topic 缺失
     */
    default <T> Message<T> fromStreamFields(Map<String, String> fields, Class<T> targetType) {
        return fromStreamFields(fields, targetType, null);
    }

    /**
     * 将 Stream Entry 字段 Map 还原为 Message，并指定回填 Topic。
     *
     * <p>{@link Message} 为不可变对象；当 Entry 字段中不携带 Topic（跨平台生产者场景）时， 使用调用方已知的 {@code fallbackTopic}
     * 补全。两个字段来源均缺失 Topic 时抛出异常。
     *
     * @param fields Stream Entry 字段 Map
     * @param targetType body 目标类型（用于反序列化）
     * @param fallbackTopic 字段缺失 Topic 时的回填值（可为 null，表示不允许缺失）
     * @param <T> body 类型
     * @return 完整的不可变 Message 实例
     * @throws io.github.streammq.core.exception.SerializationException 反序列化失败或 Topic 缺失
     */
    default <T> Message<T> fromStreamFields(
            Map<String, String> fields, Class<T> targetType, String fallbackTopic) {
        return fromStreamFields(fields, targetType);
    }

    /**
     * 返回转换器名称。
     *
     * @return 名称
     */
    default String name() {
        return getClass().getSimpleName();
    }
}
