/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.interceptor;

import io.github.streammq.core.message.MessageId;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 追踪收集器 SPI，用于消息发送/消费的追踪埋点。
 *
 * <p>对接 OpenTelemetry、Zipkin、SkyWalking 等 APM 系统时，业务方实现此接口 将追踪数据上报至 APM 后端。
 *
 * <p>框架在 {@code beforeSend} / {@code afterSend} / {@code beforeConsume} / {@code afterConsume}
 * 调用对应方法。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface TraceCollector {

    /**
     * 记录发送事件。
     *
     * @param context 发送追踪上下文
     */
    void recordSend(SendTraceContext context);

    /**
     * 记录消费事件。
     *
     * @param context 消费追踪上下文
     */
    void recordConsume(ConsumeTraceContext context);

    /**
     * 是否启用。返回 false 时框架将跳过记录以节省开销。
     *
     * @return true 启用
     */
    default boolean isEnabled() {
        return true;
    }

    /**
     * 收集器名称。
     *
     * @return 名称
     */
    default String name() {
        return getClass().getSimpleName();
    }

    /**
     * 发送追踪上下文。
     *
     * <p>{@code attributes} 在紧凑构造器内做防御性拷贝（不可修改视图），外部修改传入 Map 不影响本记录（0.1.2）。
     *
     * @param topic 主题
     * @param tag 标签
     * @param messageId 消息 ID（发送前可能为 null）
     * @param producerGroup 生产者组
     * @param bornTimestamp 出生时间戳
     * @param success 是否成功
     * @param durationMillis 发送耗时（毫秒）
     * @param traceId 追踪 ID
     * @param attributes 扩展属性（防御性拷贝为不可修改视图；null 视为空 Map）
     */
    record SendTraceContext(
            String topic,
            String tag,
            MessageId messageId,
            String producerGroup,
            long bornTimestamp,
            boolean success,
            long durationMillis,
            String traceId,
            Map<String, String> attributes) {

        /** 紧凑构造器：{@code attributes} 防御性拷贝，避免调用方在构造后修改外部 Map 影响已构造上下文。 */
        public SendTraceContext {
            attributes =
                    attributes == null
                            ? Map.of()
                            : Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
        }
    }

    /**
     * 消费追踪上下文。
     *
     * <p>{@code attributes} 在紧凑构造器内做防御性拷贝（不可修改视图），外部修改传入 Map 不影响本记录（0.1.2）。
     *
     * @param topic 主题
     * @param tag 标签
     * @param messageId 消息 ID
     * @param consumerGroup 消费者组
     * @param consumerName 消费者实例名
     * @param reconsumeTimes 重试次数
     * @param success 是否成功
     * @param durationMillis 消费耗时（毫秒）
     * @param traceId 追踪 ID
     * @param attributes 扩展属性（防御性拷贝为不可修改视图；null 视为空 Map）
     */
    record ConsumeTraceContext(
            String topic,
            String tag,
            MessageId messageId,
            String consumerGroup,
            String consumerName,
            int reconsumeTimes,
            boolean success,
            long durationMillis,
            String traceId,
            Map<String, String> attributes) {

        /** 紧凑构造器：{@code attributes} 防御性拷贝，避免调用方在构造后修改外部 Map 影响已构造上下文。 */
        public ConsumeTraceContext {
            attributes =
                    attributes == null
                            ? Map.of()
                            : Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
        }
    }
}
