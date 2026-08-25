/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics.model;

/**
 * 主题失败次数统计，表示某一主题在死信队列中的消息数量。
 *
 * <p>由 {@link io.github.streammq.diagnostics.StreamMQDiagnosticsService#diagnoseDlq(String)} 聚合生成，
 * 用于识别失败率最高的主题。
 *
 * @param topic 主题名
 * @param count 失败消息数
 * @param lastFailureTimestamp 最近一次失败时间戳（毫秒）
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public record TopicFailureCount(String topic, long count, long lastFailureTimestamp) {}
