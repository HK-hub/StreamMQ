/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics.model;

/**
 * 单次消费尝试记录，表示消息被某一消费者组消费一次的完整信息。
 *
 * <p>一条消息可能被多次消费（重试场景），每次消费生成一条 {@link ConsumeAttempt}， 按时间顺序组成 {@link
 * MessageProfile#consumeHistory()}。
 *
 * @param consumerGroup 消费者组名
 * @param consumerName 消费者实例名
 * @param timestamp 消费时间戳（毫秒）
 * @param durationMillis 消费耗时（毫秒）
 * @param success 是否消费成功
 * @param reconsumeTimes 第几次重试（0 表示首次消费）
 * @param errorMessage 失败时的错误信息，成功时为 null
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public record ConsumeAttempt(
        String consumerGroup,
        String consumerName,
        long timestamp,
        long durationMillis,
        boolean success,
        int reconsumeTimes,
        String errorMessage) {}
