/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.message;

import io.github.streammq.core.util.StringUtils;
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
 * <p><b>条数上限（0.1.2 明确）：</b>本类型<b>不施加</b>最大条数限制——Pipeline 的关键约束是单次请求体积（见 {@code
 * StreamMQConstants#MAX_MESSAGE_SIZE_BYTES} / {@code RECOMMENDED_MAX_BODY_SIZE_BYTES}）与 Redis 服务端
 * {@code proto-max-bulk-len}，由调用方按消息体大小与 Broker 侧上限共同约束。 空集合在 {@link Builder#build()} 处立即失败（{@link
 * IllegalArgumentException}），避免空 Pipeline 造成无意义往返。
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
            this.topic = StringUtils.requireValidTopic(topic);
        }

        /**
         * 添加一条消息（必须与 batch 同 Topic）。
         *
         * @param message 消息
         * @return this
         * @throws IllegalArgumentException 如果 message 为 null 或消息 Topic 与 batch Topic 不一致 （异常口径与
         *     {@link #addAll(List)} 统一为 IllegalArgumentException）
         */
        public Builder<T> add(Message<T> message) {
            if (Objects.isNull(message)) {
                throw new IllegalArgumentException("message must not be null");
            }
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
         * @throws IllegalArgumentException 如果 {@code messages} 为 null，或列表中含 null 元素（与 {@link
         *     #add(Message)} 的校验对称，避免静默忽略）
         */
        public Builder<T> addAll(List<Message<T>> messages) {
            if (Objects.isNull(messages)) {
                throw new IllegalArgumentException("messages list must not be null");
            }
            for (Message<T> m : messages) {
                add(m);
            }
            return this;
        }

        /**
         * 构造批量消息。
         *
         * @return 批量消息
         * @throws IllegalArgumentException 如果消息列表为空（0.1.2 起由 IllegalStateException 改为此类型，与 {@code
         *     StreamMessageTemplate#syncSendBatch} 的 javadoc 及本类 add/addAll 的异常口径统一）
         */
        public BatchMessage<T> build() {
            if (messages.isEmpty()) {
                throw new IllegalArgumentException("batch messages is empty");
            }
            return new BatchMessage<>(topic, messages);
        }
    }
}
