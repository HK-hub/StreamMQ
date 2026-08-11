package io.github.streammq.core.event;

import io.github.streammq.core.message.MessageId;
import java.time.Instant;
import java.util.Objects;

/**
 * 消息消费完成事件。
 *
 * @param topic 主题
 * @param group 消费组
 * @param messageId 消息 ID
 * @param success 是否成功
 * @param reconsumeTimes 重试次数
 * @param durationMillis 消费耗时（毫秒）
 * @param timestamp 事件时间
 */
public record MessageConsumedEvent(
        String topic,
        String group,
        MessageId messageId,
        boolean success,
        int reconsumeTimes,
        long durationMillis,
        Instant timestamp) {
    public MessageConsumedEvent {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(timestamp, "timestamp");
    }
}
