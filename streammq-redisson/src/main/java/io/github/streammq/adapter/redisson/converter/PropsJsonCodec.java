/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.util.StringUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 属性 JSON 编解码工具（包内共享）。
 *
 * <p>属性 JSON 序列化是三个具体转换器（{@link DefaultMessageConverter} / {@link CompactMessageConverter} / {@link
 * PassThroughMessageConverter}）共用的能力——Default 与 PassThrough 将 sys + user 合并为单个字段，Compact
 * 则按字段分开存储——但都不属于模板方法契约本身。此前该方法 挂在 {@link AbstractMessageConverter} 上，扩大了抽象类的受保护面——提取为本工具类后，抽象类只保留
 * 模板契约。
 *
 * <p><b>键冲突规则：</b>系统属性优先（后写入覆盖）——SDK 内部元数据（如 trace 上下文）不可被业务 同名用户属性静默篡改。
 *
 * @author StreamMQ Contributors
 * @since 0.1.1
 */
final class PropsJsonCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private PropsJsonCodec() {}

    /**
     * 将系统属性和用户属性合并序列化为单个 JSON 字段。
     *
     * <p>两个 Map 均可能为空，合并后若为空则不做任何写入。
     *
     * @param fields 目标 Map
     * @param fieldName JSON 字段名（如 {@code "props"}）
     * @param sysProps 系统属性 Map（可空，不可变视图）
     * @param userProps 用户属性 Map（可空，防御性拷贝）
     * @throws SerializationException 当 JSON 序列化失败时
     */
    static void write(
            Map<String, String> fields,
            String fieldName,
            Map<String, String> sysProps,
            Map<String, String> userProps) {
        Map<String, String> merged = new HashMap<>(sysProps.size() + userProps.size());
        merged.putAll(userProps);
        merged.putAll(sysProps);
        if (merged.isEmpty()) {
            return;
        }
        try {
            fields.put(fieldName, MAPPER.writeValueAsString(merged));
        } catch (JsonProcessingException ex) {
            throw new SerializationException("Failed to serialize message properties", ex);
        }
    }

    /**
     * 从单个 JSON 字段反序列化属性。
     *
     * <p>字段不存在或为空时不做任何操作。
     *
     * @param fields Stream Entry 全部字段
     * @param fieldName JSON 字段名（如 {@code "props"}）
     * @param consumer 接收反序列化后的属性 Map 的消费者（如 {@code draft.userProperties::putAll}）
     * @throws SerializationException 当 JSON 反序列化失败时
     */
    @SuppressWarnings("unchecked")
    static void read(
            Map<String, String> fields, String fieldName, Consumer<Map<String, String>> consumer) {
        String json = fields.get(fieldName);
        if (StringUtils.isEmpty(json)) {
            return;
        }
        try {
            consumer.accept(MAPPER.readValue(json, new TypeReference<Map<String, String>>() {}));
        } catch (JsonProcessingException ex) {
            throw new SerializationException("Failed to deserialize message properties", ex);
        }
    }
}
