package io.github.streammq.core.message;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 批量消息包装类。
 *
 * <p>底层通过 Redisson RBatch（Pipeline）一次性 XADD 多条消息，减少 RTT。 使用示例：
 *
 * <pre>{@code
 * BatchMessage<String> batch = BatchMessage.<String>withTopic("order-topic")
 *     .add(msg1)
 *     .add(msg2)
 *     .add(msg3)
 *     .build();
 * List<SendResult> results = template.syncSendBatch(batch);
 * }</pre>
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class BatchMessage<T> {

  /** 共享 Topic（所有消息必须同 Topic） */
  private final String topic;

  /** 不可修改的消息列表 */
  private final List<Message<T>> messages;

  private BatchMessage(String topic, List<Message<T>> messages) {
    this.topic = Objects.requireNonNull(topic, "topic");
    this.messages = List.copyOf(messages);
  }

  /**
   * 创建指定 Topic 的 Builder。
   *
   * @param topic 主题
   * @param <T> body 类型
   * @return Builder 实例
   */
  public static <T> Builder<T> withTopic(String topic) {
    return new Builder<>(topic);
  }

  /**
   * 返回 Topic。
   *
   * @return Topic
   */
  public String getTopic() {
    return topic;
  }

  /**
   * 返回不可修改的消息列表。
   *
   * @return 消息列表
   */
  public List<Message<T>> getMessages() {
    return messages;
  }

  /**
   * 返回消息数量。
   *
   * @return 消息数量
   */
  public int size() {
    return messages.size();
  }

  /**
   * 是否为空。
   *
   * @return true 如果没有消息
   */
  public boolean isEmpty() {
    return messages.isEmpty();
  }

  /**
   * 批量消息构造器。
   *
   * @param <T> body 类型
   */
  public static final class Builder<T> {

    private final String topic;
    private final List<Message<T>> messages = new ArrayList<>();

    private Builder(String topic) {
      this.topic = Objects.requireNonNull(topic, "topic").trim();
      if (this.topic.isEmpty()) {
        throw new IllegalArgumentException("topic must not be empty");
      }
    }

    /**
     * 添加一条消息（必须与 batch 同 Topic）。
     *
     * @param message 消息
     * @return this
     * @throws IllegalArgumentException 如果消息 Topic 与 batch Topic 不一致
     */
    public Builder<T> add(Message<T> message) {
      Objects.requireNonNull(message, "message");
      if (!topic.equals(message.getTopic())) {
        throw new IllegalArgumentException(
            "message topic '"
                + message.getTopic()
                + "' does not match batch topic '"
                + topic
                + '\'');
      }
      this.messages.add(message);
      return this;
    }

    /**
     * 批量添加消息。
     *
     * @param messages 消息列表
     * @return this
     */
    public Builder<T> addAll(List<Message<T>> messages) {
      if (Objects.nonNull(messages)) {
        for (Message<T> m : messages) {
          add(m);
        }
      }
      return this;
    }

    /**
     * 构造批量消息。
     *
     * @return 批量消息
     * @throws IllegalStateException 如果消息列表为空
     */
    public BatchMessage<T> build() {
      if (messages.isEmpty()) {
        throw new IllegalStateException("batch messages is empty");
      }
      return new BatchMessage<>(topic, messages);
    }
  }
}
