/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.serializer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.streammq.adapter.redisson.serializer.sbe.MessageHeaderDecoder;
import io.github.streammq.adapter.redisson.serializer.sbe.MessageHeaderEncoder;
import io.github.streammq.adapter.redisson.serializer.sbe.StreamMessageDecoder;
import io.github.streammq.adapter.redisson.serializer.sbe.StreamMessageEncoder;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.serializer.MessageSerializer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * 基于 SBE（Simple Binary Encoding，FIX 社区标准）的内置序列化器——信封（envelope）模式。
 *
 * <p>SBE 是严格 schema + 代码生成格式（{@code uk.co.real-logic:sbe-tool} 在构建期根据 {@code
 * src/main/resources/sbe/streammq-message.xml} 生成 {@code StreamMessageEncoder/Decoder}）， 无法像
 * Fury/Protostuff 那样反射序列化任意 POJO。因此本实现采用<b>信封</b>模式：
 *
 * <ul>
 *   <li>业务消息体先由内嵌（严格类型、无多态）序列化器编码为 opaque 字节流，整体放入 SBE 消息的单个 {@code varData(payload)} 字段；
 *   <li>SBE 仅负责定长 8 字节消息头 + 零解析拷贝的二进制容器——读 {@code payloadLength()}/{@code getPayload()} 直接基于偏移量，不解释
 *       body 内部结构。
 * </ul>
 *
 * <p>这样 {@code SbeSerializer} 对任意 POJO / String / byte[] 均透明可用，同时对外暴露 SBE 的金融级确定性
 * 时延与极小体积优势。若需<b>全零拷贝 body</b>，可在 schema 中新增面向具体消息类型的 {@code message} 定义， 并让本类直接操作生成类型（去掉内层 JSON
 * 编码）。
 *
 * <h3>安全</h3>
 *
 * <p>内层编码器关闭了默认类型 / 多态（{@code FAIL_ON_UNKNOWN_PROPERTIES} 关闭仅为兼容字段增删）， 反序列化 body 不会实例化 classpath
 * 上任意类，无 gadget RCE 面。选用本序列化器时，用户需在自身依赖中加入 {@code org.agrona:agrona}（本模块以 optional 声明，不强制传递）。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class SbeSerializer<T> implements MessageSerializer<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        // 严格类型、无多态默认类型（避免 Jackson 反序列化 gadget 面）。关闭未知字段仅用于兼容 body 演进。
        MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    @Override
    public byte[] serialize(T object, Class<T> type) throws SerializationException {
        if (object == null) {
            return null;
        }
        byte[] payload;
        if (object instanceof byte[]) {
            payload = (byte[]) object;
        } else {
            try {
                payload = MAPPER.writeValueAsBytes(object);
            } catch (Exception ex) {
                throw new SerializationException("SBE serialize failed: " + ex.getMessage(), ex);
            }
        }
        int maxLen = MessageHeaderEncoder.ENCODED_LENGTH + 4 + payload.length;
        byte[] buf = new byte[maxLen];
        MutableDirectBuffer buffer = new UnsafeBuffer(buf);
        MessageHeaderEncoder header = new MessageHeaderEncoder();
        StreamMessageEncoder encoder = new StreamMessageEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, header);
        encoder.putPayload(payload, 0, payload.length);
        // 注意：encodedLength() 仅含消息体（不含 8 字节定长头），完整线长需加上头。
        int total = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
        byte[] out = new byte[total];
        buffer.getBytes(0, out, 0, total);
        return out;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R deserialize(byte[] bytes, Class<R> type) throws SerializationException {
        if (bytes == null) {
            return null;
        }
        MutableDirectBuffer buffer = new UnsafeBuffer(bytes);
        MessageHeaderDecoder header = new MessageHeaderDecoder();
        header.wrap(buffer, 0);
        StreamMessageDecoder decoder = new StreamMessageDecoder();
        decoder.wrap(buffer, header.encodedLength(), header.blockLength(), header.version());
        int len = decoder.payloadLength();
        byte[] payload = new byte[len];
        decoder.getPayload(payload, 0, len);
        if (byte[].class.equals(type)) {
            return (R) payload;
        }
        try {
            return MAPPER.readValue(payload, type);
        } catch (Exception ex) {
            throw new SerializationException("SBE deserialize failed: " + ex.getMessage(), ex);
        }
    }

    @Override
    public String name() {
        return "sbe";
    }

    /**
     * 仅用于单测断言：确认 body 确实走了 SBE 信封。
     *
     * <p>SBE 没有魔数前缀；这里通过解析定长 8 字节消息头中的 {@code templateId}（=1）与 {@code
     * schemaId}（=12345）来识别信封，非正式协议的一部分。
     */
    static boolean looksLikeSbeEnvelope(byte[] bytes) {
        if (bytes == null || bytes.length < 8) {
            return false;
        }
        // SBE 头布局（小端）：blockLength(uint16) @0 + templateId(uint16) @2 + schemaId(uint16) @4
        // + version(uint16) @6
        int templateId = ((bytes[2] & 0xFF)) | ((bytes[3] & 0xFF) << 8);
        int schemaId = ((bytes[4] & 0xFF)) | ((bytes[5] & 0xFF) << 8);
        return templateId == StreamMessageEncoder.TEMPLATE_ID
                && schemaId == StreamMessageEncoder.SCHEMA_ID;
    }
}
