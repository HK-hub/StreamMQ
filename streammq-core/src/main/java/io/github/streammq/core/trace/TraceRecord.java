/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.trace;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 追踪记录，表示一次消息发送或消费事件的完整追踪信息。
 *
 * <p>由 {@link StreamMQTraceService} 存储和查询，用于消息链路分析与问题排查。
 *
 * <p><b>不可变性（0.1.2）：</b>{@code attributes} 在紧凑构造器内做防御性拷贝（不可修改视图）， 与 {@link
 * io.github.streammq.core.message.Message} / {@code TransactionContext} 一致——外部在构造后修改传入 Map 不再影响本记录。
 *
 * @param messageId 消息 ID
 * @param topic 主题
 * @param group 消费者组或生产者组
 * @param type 事件类型（SEND / CONSUME）
 * @param success 是否成功
 * @param timestamp 事件时间戳（毫秒）
 * @param durationMillis 耗时（毫秒）
 * @param traceId 追踪 ID
 * @param attributes 扩展属性（防御性拷贝为不可修改视图；null 视为空 Map）
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public record TraceRecord(
        String messageId,
        String topic,
        String group,
        TraceType type,
        boolean success,
        long timestamp,
        long durationMillis,
        String traceId,
        Map<String, String> attributes) {

    /** 紧凑构造器：{@code attributes} 防御性拷贝，避免调用方在构造后修改外部 Map 影响已构造记录。 */
    public TraceRecord {
        attributes =
                attributes == null
                        ? Map.of()
                        : Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
    }
}
