package io.github.streammq.core.event;

import io.github.streammq.core.message.MessageId;
import java.time.Instant;
import java.util.Objects;

/**
 * 消息发送完成事件。
 *
 * @param topic 主题
 * @param tag 标签
 * @param messageId 消息 ID
 * @param success 是否成功
 * @param durationMillis 发送耗时（毫秒）
 * @param timestamp 事件时间
 */
public record MessageSentEvent(
    String topic,
    String tag,
    MessageId messageId,
    boolean success,
    long durationMillis,
    Instant timestamp) {
  public MessageSentEvent {
    Objects.requireNonNull(topic, "topic");
    Objects.requireNonNull(timestamp, "timestamp");
  }
}
