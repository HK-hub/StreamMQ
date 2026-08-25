/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.trace;

import java.util.Map;

/**
 * 追踪记录，表示一次消息发送或消费事件的完整追踪信息。
 *
 * <p>由 {@link StreamMQTraceService} 存储和查询，用于消息链路分析与问题排查。
 *
 * @param messageId 消息 ID
 * @param topic 主题
 * @param group 消费者组或生产者组
 * @param type 事件类型（SEND / CONSUME）
 * @param success 是否成功
 * @param timestamp 事件时间戳（毫秒）
 * @param durationMillis 耗时（毫秒）
 * @param traceId 追踪 ID
 * @param attributes 扩展属性
 * @author StreamMQ Contributors
 * @since 1.0.0
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
        Map<String, String> attributes) {}
