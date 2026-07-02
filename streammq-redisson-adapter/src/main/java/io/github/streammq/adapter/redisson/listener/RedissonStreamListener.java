package io.github.streammq.adapter.redisson.listener;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMqConstants;
import io.github.streammq.core.exception.StreamMqBrokerException;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.spi.MessageConverter;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
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
 * 基于 Redisson 的 {@link StreamMQListener} 默认实现。
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
@Getter
public class RedissonStreamListener implements StreamMQListener {

    private static final Logger LOG = LoggerFactory.getLogger(RedissonStreamListener.class);

    private final @NonNull RedissonClient redisson;
    private final String namespace;
    private final @NonNull String topic;
    private final @NonNull String group;
    private final @NonNull String consumerName;
    private final @NonNull MessageConverter converter;
    /** DLQ 模式标志：true=从 DLQ Stream 消费死信消息 */
    private final boolean dlqMode;
    /** DLQ 原始消费者组（仅 dlqMode=true 时使用，用于构造 DLQ Stream Key） */
    private final String dlqOriginalGroup;
    /**
     * 目标 body 类型（跨平台反序列化回退类型）。
     *
     * <p>当 Stream Entry 缺失 {@code bodyType} 字段（发送方非 StreamMQ SDK），
     * 或 {@code bodyType} 类不可加载时，回退到此类型。若仍为 null，则回退到 {@link String}。
     */
    private final Class<?> targetBodyType;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean groupCreated = new AtomicBoolean(false);

    /** batchSize 校验上界，对应 Redis Stream 单次 XREADGROUP 的合理上限 */
    private static final int MAX_BATCH_SIZE = StreamMqConstants.MAX_BATCH_SIZE_LIMIT;
    /** BUSYGROUP 错误标识，用于判断消费者组已存在 */
    private static final String BUSYGROUP_MARKER = "BUSYGROUP";

    /**
     * 兼容构造器：不启用 DLQ 模式（等价于 {@code dlqMode=false, dlqOriginalGroup=null, targetBodyType=null}）。
     *
     * @param redisson Redisson 客户端（必填）
     * @param namespace 命名空间（可为 null，默认空字符串）
     * @param topic 主题（必填）
     * @param group 消费者组名（必填）
     * @param consumerName 消费者实例名（必填）
     * @param converter 消息转换器（必填）
     */
    public RedissonStreamListener(@NonNull RedissonClient redisson, String namespace, @NonNull String topic,
                                   @NonNull String group, @NonNull String consumerName, @NonNull MessageConverter converter) {
        this(redisson, namespace, topic, group, consumerName, converter, false, null, null);
    }

    /**
     * 构造 Listener，支持 Builder 模式。
     *
     * <p>使用示例：
     * <pre>{@code
     * RedissonStreamListener listener = RedissonStreamListener.builder()
     *     .redisson(redissonClient)
     *     .topic("my-topic")
     *     .group("my-group")
     *     .consumerName("consumer-1")
     *     .converter(converter)
     *     .build();
     * }</pre>
     *
     * <p>DLQ 模式示例：
     * <pre>{@code
     * RedissonStreamListener dlqListener = RedissonStreamListener.builder()
     *     .redisson(redissonClient)
     *     .topic("my-topic")              // 原始 topic
     *     .group("dlq-consumer-my-group") // DLQ 消费者组
     *     .consumerName("dlq-consumer-1")
     *     .converter(converter)
     *     .dlqMode(true)
     *     .dlqOriginalGroup("my-group")   // 原始消费者组
     *     .build();
     * }</pre>
     *
     * <p>跨平台 body 类型示例：
     * <pre>{@code
     * RedissonStreamListener listener = RedissonStreamListener.builder()
     *     .redisson(redissonClient)
     *     .topic("cross-lang-topic")
     *     .group("my-group")
     *     .consumerName("consumer-1")
     *     .converter(converter)
     *     .targetBodyType(String.class)   // Go 发送 JSON string，接收为 String 自行解析
     *     .build();
     * }</pre>
     *
     * @param redisson Redisson 客户端（必填）
     * @param namespace 命名空间（可为 null，默认空字符串）
     * @param topic 主题（必填；DLQ 模式下为原始 topic）
     * @param group 消费者组名（必填；DLQ 模式下为 DLQ 消费者组名）
     * @param consumerName 消费者实例名（必填）
     * @param converter 消息转换器（必填）
     * @param dlqMode DLQ 模式标志（true=从 DLQ Stream 消费）
     * @param dlqOriginalGroup DLQ 原始消费者组（仅 dlqMode=true 时使用）
     * @param targetBodyType 目标 body 类型（跨平台回退类型，null=最终回退到 String）
     */
    @Builder
    public RedissonStreamListener(@NonNull RedissonClient redisson, String namespace, @NonNull String topic,
                                   @NonNull String group, @NonNull String consumerName, @NonNull MessageConverter converter,
                                   boolean dlqMode, String dlqOriginalGroup, Class<?> targetBodyType) {
        this.redisson = redisson;
        this.namespace = namespace == null ? "" : namespace;
        this.topic = topic;
        this.group = group;
        this.consumerName = consumerName;
        this.converter = converter;
        this.dlqMode = dlqMode;
        this.dlqOriginalGroup = dlqOriginalGroup;
        this.targetBodyType = targetBodyType;
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
        } catch (RuntimeException ex) {
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
        } catch (RuntimeException ex) {
            throw new StreamMqBrokerException(
                "ackBatch failed for topic " + topic + ", size=" + messageIds.size(), null, ex);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            LOG.info("RedissonStreamListener closed: topic={}, group={}, consumer={}",
                topic, group, consumerName);
        }
    }

    /**
     * 返回 Listener 是否正在运行。
     *
     * @return true 如果未关闭
     */
    public boolean isRunning() {
        return !closed.get();
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
        } catch (RuntimeException ex) {
            throw new StreamMqBrokerException(
                "readGroup failed for topic " + topic, null, ex);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Message<?> toMessage(StreamMessageId streamId, Map<String, String> fields) {
        // 反序列化目标类型回退链：
        //   1. Stream Entry 中的 bodyType 字段（StreamMQ SDK 发送方写入）
        //   2. targetBodyType（容器解析自 Listener 泛型 T，跨平台场景使用）
        //   3. String.class（最终回退，由消费者自行反序列化）
        String bodyTypeName = fields.get(DefaultMessageConverter.FIELD_BODY_TYPE);
        Class<?> bodyType = null;
        if (bodyTypeName != null && !bodyTypeName.isEmpty()) {
            try {
                bodyType = Class.forName(bodyTypeName, false, Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException ex) {
                LOG.warn("Body type class not found, fallback to targetBodyType/String: {}", bodyTypeName);
                bodyType = null;
            }
        }
        if (bodyType == null) {
            bodyType = targetBodyType != null ? targetBodyType : String.class;
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
                // id(0-0)：从头开始消费
                stream.createGroup(StreamCreateGroupArgs.name(group).makeStream().id(new StreamMessageId(0, 0)));
                LOG.info("Consumer group created: topic={}, group={}", topic, group);
            } catch (RuntimeException ex) {
                // BUSYGROUP 表示 group 已存在，属于正常情况
                String msg = ex.getMessage();
                if (msg != null && msg.contains(BUSYGROUP_MARKER)) {
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
        String streamKey = dlqMode
            ? StreamMQKeys.dlqStream(namespace, topic, dlqOriginalGroup)
            : StreamMQKeys.topicStream(namespace, topic);
        return redisson.getStream(streamKey);
    }

    private static StreamMessageId toStreamId(MessageId messageId) {
        return new StreamMessageId(messageId.getTimestamp(), messageId.getSequence());
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Listener is closed: topic=" + topic + ", group=" + group);
        }
    }

    private static void validateBatchSize(int batchSize) {
        if (batchSize <= 0 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                "batchSize must be between 1 and " + MAX_BATCH_SIZE + ", got " + batchSize);
        }
    }
}
