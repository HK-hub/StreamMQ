package io.github.streammq.adapter.redisson.consumer;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.support.StreamMqKeys;
import io.github.streammq.core.consumer.StreamMqConsumer;
import io.github.streammq.core.exception.StreamMqBrokerException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.spi.MessageConverter;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于 Redisson 的 {@link StreamMqConsumer} 默认实现。
 *
 * <p>底层调用 {@link RStream#readGroup} 拉取消息，{@link RStream#ack} 确认消息。
 * 每个实例绑定一个 Topic + ConsumerGroup + ConsumerName。
 *
 * <p>支持：
 * <ul>
 *   <li>非阻塞拉取 {@link #pull}（基于 {@code XREADGROUP > COUNT n}）</li>
 *   <li>阻塞拉取 {@link #pullBlock}（基于 {@code XREADGROUP > COUNT n BLOCK ms}）</li>
 *   <li>单条/批量 ACK {@link #ack} / {@link #ackBatch}</li>
 *   <li>消费者组自动创建（首次拉取时 lazy init，{@code MKSTREAM}）</li>
 * </ul>
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型，可在多线程间共享。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class RedissonStreamConsumer implements StreamMqConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(RedissonStreamConsumer.class);

    private final RedissonClient redisson;
    private final String namespace;
    private final String topic;
    private final String group;
    private final String consumerName;
    private final MessageConverter converter;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean groupCreated = new AtomicBoolean(false);

    /**
     * 构造 Consumer。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param topic 主题
     * @param group 消费者组名
     * @param consumerName 消费者实例名
     * @param converter 消息转换器
     */
    public RedissonStreamConsumer(RedissonClient redisson, String namespace, String topic,
                                   String group, String consumerName, MessageConverter converter) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = namespace == null ? "" : namespace;
        this.topic = Objects.requireNonNull(topic, "topic");
        this.group = Objects.requireNonNull(group, "group");
        this.consumerName = Objects.requireNonNull(consumerName, "consumerName");
        this.converter = Objects.requireNonNull(converter, "converter");
    }

    @Override
    public List<Message<?>> pull(int batchSize) {
        ensureOpen();
        validateBatchSize(batchSize);
        ensureGroup();
        return doRead(batchSize, null);
    }

    @Override
    public List<Message<?>> pullBlock(int batchSize, Duration timeout) {
        ensureOpen();
        validateBatchSize(batchSize);
        Objects.requireNonNull(timeout, "timeout");
        ensureGroup();
        return doRead(batchSize, timeout);
    }

    @Override
    public void ack(MessageId messageId) {
        ensureOpen();
        Objects.requireNonNull(messageId, "messageId");
        RStream<String, String> stream = getStream();
        try {
            stream.ack(group, toStreamId(messageId));
        } catch (Exception ex) {
            throw new StreamMqBrokerException(
                "ack failed for topic " + topic + ", messageId=" + messageId, null, ex);
        }
    }

    @Override
    public void ackBatch(List<MessageId> messageIds) {
        ensureOpen();
        Objects.requireNonNull(messageIds, "messageIds");
        if (messageIds.isEmpty()) {
            return;
        }
        RStream<String, String> stream = getStream();
        StreamMessageId[] streamIds = new StreamMessageId[messageIds.size()];
        for (int i = 0; i < messageIds.size(); i++) {
            streamIds[i] = toStreamId(messageIds.get(i));
        }
        try {
            stream.ack(group, streamIds);
        } catch (Exception ex) {
            throw new StreamMqBrokerException(
                "ackBatch failed for topic " + topic + ", size=" + messageIds.size(), null, ex);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            LOG.info("RedissonStreamConsumer closed: topic={}, group={}, consumer={}",
                topic, group, consumerName);
        }
    }

    /**
     * 返回消费者是否正在运行。
     *
     * @return true 如果未关闭
     */
    public boolean isRunning() {
        return !closed.get();
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
     * 返回消费者组名。
     *
     * @return 消费者组名
     */
    public String getGroup() {
        return group;
    }

    /**
     * 返回消费者实例名。
     *
     * @return 消费者实例名
     */
    public String getConsumerName() {
        return consumerName;
    }

    // ===================== 内部方法 =====================

    private List<Message<?>> doRead(int batchSize, Duration timeout) {
        RStream<String, String> stream = getStream();
        StreamReadGroupArgs args;
        if (timeout != null && !timeout.isZero() && !timeout.isNegative()) {
            args = StreamReadGroupArgs.neverDelivered().count(batchSize).timeout(timeout);
        } else {
            args = StreamReadGroupArgs.neverDelivered().count(batchSize);
        }
        try {
            Map<StreamMessageId, Map<String, String>> result = stream.readGroup(group, consumerName, args);
            if (result == null || result.isEmpty()) {
                return List.of();
            }
            List<Message<?>> messages = new ArrayList<>(result.size());
            for (Map.Entry<StreamMessageId, Map<String, String>> entry : result.entrySet()) {
                Message<?> message = toMessage(entry.getKey(), entry.getValue());
                messages.add(message);
            }
            return messages;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new StreamMqBrokerException(
                "readGroup interrupted for topic " + topic, null, ex);
        } catch (Exception ex) {
            throw new StreamMqBrokerException(
                "readGroup failed for topic " + topic, null, ex);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Message<?> toMessage(StreamMessageId streamId, Map<String, String> fields) {
        String bodyTypeName = fields.get(DefaultMessageConverter.FIELD_BODY_TYPE);
        Class<?> bodyType = Object.class;
        if (bodyTypeName != null && !bodyTypeName.isEmpty()) {
            try {
                bodyType = Class.forName(bodyTypeName, false, Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException ex) {
                LOG.warn("Body type class not found, fallback to Object: {}", bodyTypeName);
                bodyType = Object.class;
            }
        }
        Message<?> message = converter.fromStreamFields(fields, (Class) bodyType);
        // 回填 topic 与 messageId（Stream Entry 字段中不含 topic）
        DefaultMessageConverter.applyTopic(message, topic);
        DefaultMessageConverter.applyMessageId(message, streamId.toString());
        return message;
    }

    private void ensureGroup() {
        if (groupCreated.get()) {
            return;
        }
        if (groupCreated.compareAndSet(false, true)) {
            RStream<String, String> stream = getStream();
            try {
                // makeStream：如果 Stream 不存在则创建
                // id(MIN)：从头开始消费
                stream.createGroup(StreamCreateGroupArgs.name(group).makeStream().id(StreamMessageId.MIN));
                LOG.info("Consumer group created: topic={}, group={}", topic, group);
            } catch (Exception ex) {
                // BUSYGROUP 表示 group 已存在，属于正常情况
                String msg = ex.getMessage();
                if (msg != null && msg.contains("BUSYGROUP")) {
                    LOG.debug("Consumer group already exists: topic={}, group={}", topic, group);
                } else {
                    // 其他错误重置标志位，允许下次重试
                    groupCreated.set(false);
                    throw new StreamMqBrokerException(
                        "createGroup failed for topic " + topic + ", group " + group, null, ex);
                }
            }
        }
    }

    private RStream<String, String> getStream() {
        String streamKey = StreamMqKeys.topicStream(namespace, topic);
        return redisson.getStream(streamKey);
    }

    private static StreamMessageId toStreamId(MessageId messageId) {
        return new StreamMessageId(messageId.getTimestamp(), messageId.getSequence());
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Consumer is closed: topic=" + topic + ", group=" + group);
        }
    }

    private static void validateBatchSize(int batchSize) {
        if (batchSize <= 0 || batchSize > 1000) {
            throw new IllegalArgumentException("batchSize must be between 1 and 1000, got " + batchSize);
        }
    }
}
