package io.github.streammq.kafka;

import io.github.streammq.core.listener.ListenerConfig;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * StreamMQ 兼容 Kafka 风格的 Consumer。
 *
 * <p>对齐 Kafka {@code KafkaConsumer<K, V>} API 风格，提供 {@code subscribe} / {@code poll} /
 * {@code commitSync} / {@code close} 方法。
 * 底层通过 {@link StreamMQListenerFactory} 为每个订阅的 topic 创建 {@link StreamMQListener}，
 * 通过 {@code pullBlock} 拉取消息并转换为 {@link ConsumerRecord}。
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建 ListenerFactory（通常由 RedissonStreamListenerFactory 实现）
 * StreamMQListenerFactory listenerFactory = new RedissonStreamListenerFactory(redisson, converter);
 * KafkaConsumer<String, String> consumer = new KafkaConsumer<>(listenerFactory, "my-group");
 * consumer.subscribe(List.of("my-topic"));
 * List<ConsumerRecord<String, String>> records = consumer.poll(Duration.ofSeconds(5));
 * for (ConsumerRecord<String, String> r : records) {
 *     System.out.println(r.value());
 * }
 * consumer.close();
 * }</pre>
 *
 * <p><b>语义说明</b>：
 * <ul>
 *   <li>{@code poll} 自动 ACK 拉取到的消息，无需手动提交</li>
 *   <li>{@code commitSync} 为 no-op（消息已在 poll 时 ACK）</li>
 *   <li>Redis Stream 无原生 partition，{@link ConsumerRecord#partition} 固定为 0</li>
 *   <li>不支持 rebalance / seek / pause 等 Kafka 高级特性</li>
 * </ul>
 *
 * @param <K> key 类型（内部为 String）
 * @param <V> value 类型（消息体类型）
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class KafkaConsumer<K, V> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

    /** 默认 poll 批量大小 */
    private static final int DEFAULT_POLL_BATCH_SIZE = 32;

    /** 默认拉取阻塞超时 */
    private static final long DEFAULT_POLL_BLOCK_TIMEOUT_MS = 1000L;

    /** 消费者组名 */
    private final String group;

    /** 命名空间 */
    private final String namespace;

    /** 监听器工厂 */
    private final StreamMQListenerFactory listenerFactory;

    /** topic -> listener 映射（保持插入顺序） */
    private final Map<String, StreamMQListener> listeners = new LinkedHashMap<>();

    /** 是否已订阅 */
    private volatile boolean subscribed = false;

    /** 是否已关闭 */
    private volatile boolean closed = false;

    /**
     * 构造 KafkaConsumer。
     *
     * @param listenerFactory 监听器工厂（必填），例如 {@code RedissonStreamListenerFactory}
     * @param group           消费者组名（必填）
     * @throws NullPointerException 如果任一参数为 null
     * @throws IllegalArgumentException 如果 group 为空
     */
    public KafkaConsumer(StreamMQListenerFactory listenerFactory, String group) {
        this(listenerFactory, group, "");
    }

    /**
     * 全参构造，指定命名空间。
     *
     * @param listenerFactory 监听器工厂（必填）
     * @param group           消费者组名（必填）
     * @param namespace       命名空间（可选，默认空字符串）
     * @throws NullPointerException 如果 listenerFactory 或 group 为 null
     * @throws IllegalArgumentException 如果 group 为空
     */
    public KafkaConsumer(StreamMQListenerFactory listenerFactory, String group, String namespace) {
        this.listenerFactory = Objects.requireNonNull(listenerFactory, "listenerFactory");
        this.group = validateNotEmpty(group, "group");
        this.namespace = namespace != null ? namespace : "";
    }

    /**
     * 订阅一组 topic。
     *
     * <p>为每个 topic 通过 {@link StreamMQListenerFactory} 创建一个底层 {@link StreamMQListener}。
     * 只允许调用一次，重复调用将抛出异常。
     *
     * @param topics topic 集合（必填，不能为空）
     * @throws NullPointerException 如果 topics 为 null
     * @throws IllegalArgumentException 如果 topics 为空
     * @throws IllegalStateException 如果已订阅或工厂已关闭
     */
    public void subscribe(Collection<String> topics) {
        ensureOpen();
        Objects.requireNonNull(topics, "topics");
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("topics must not be empty");
        }
        if (subscribed) {
            throw new IllegalStateException("Already subscribed, create a new KafkaConsumer to change subscription");
        }

        for (String topic : topics) {
            String t = validateNotEmpty(topic, "topic");
            if (listeners.containsKey(t)) {
                LOG.warn("Topic already subscribed: {}", t);
                continue;
            }
            ListenerConfig config = ListenerConfig.builder()
                .topic(t)
                .consumerGroup(group)
                .namespace(namespace)
                .build();
            StreamMQListener listener = listenerFactory.createListener(config);
            listeners.put(t, listener);
            LOG.info("Subscribed to topic: {}, group: {}", t, group);
        }
        subscribed = true;
    }

    /**
     * 拉取消息。
     *
     * <p>从所有已订阅 topic 对应的 {@link StreamMQListener} 拉取消息。
     * 当前实现为串行遍历各 topic 拉取（简单但非最高性能），
     * 拉取到消息后自动 ACK。
     *
     * @param timeout 阻塞超时时长（每个 topic 独立超时）
     * @return ConsumerRecord 列表，可能为空
     * @throws IllegalStateException 如果未订阅
     * @throws io.github.streammq.core.exception.StreamMQBrokerException 如果拉取失败
     */
    @SuppressWarnings("unchecked")
    public List<ConsumerRecord<K, V>> poll(Duration timeout) {
        ensureOpen();
        if (!subscribed) {
            throw new IllegalStateException("Not subscribed, call subscribe() first");
        }

        List<ConsumerRecord<K, V>> result = new ArrayList<>();
        for (Map.Entry<String, StreamMQListener> entry : listeners.entrySet()) {
            String topic = entry.getKey();
            StreamMQListener listener = entry.getValue();

            try {
                List<Message<?>> messages = listener.pullBlock(DEFAULT_POLL_BATCH_SIZE, timeout);
                if (messages.isEmpty()) {
                    continue;
                }

                // 批量 ACK
                List<MessageId> messageIds = new ArrayList<>(messages.size());
                for (Message<?> message : messages) {
                    if (message.getMessageId() != null) {
                        messageIds.add(message.getMessageId());
                    }
                    ConsumerRecord<K, V> record = toConsumerRecord(topic, (Message<V>) message);
                    result.add(record);
                }
                if (!messageIds.isEmpty()) {
                    listener.ackBatch(messageIds);
                }
            } catch (Exception ex) {
                LOG.warn("Poll failed for topic {}: {}", topic, ex.getMessage(), ex);
                // 继续处理下一个 topic，不因单个 topic 失败而中断
            }
        }

        LOG.debug("Poll returned {} records from {} topics", result.size(), listeners.size());
        return result;
    }

    /**
     * 同步提交偏移量（no-op）。
     *
     * <p>StreamMQ 在 {@link #poll} 时已自动 ACK 消息，无需手动提交。
     * 本方法为空操作，仅为 API 兼容保留。
     */
    public void commitSync() {
        ensureOpen();
        // 消息已在 poll 时自动 ACK，无需额外提交
        LOG.debug("commitSync called (no-op for StreamMQ, messages auto-acked on poll)");
    }

    /**
     * 关闭 Consumer，释放所有底层 Listener 资源。
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        for (Map.Entry<String, StreamMQListener> entry : listeners.entrySet()) {
            try {
                entry.getValue().close();
                LOG.debug("Listener closed for topic: {}", entry.getKey());
            } catch (Exception ex) {
                LOG.warn("Error closing listener for topic {}: {}",
                    entry.getKey(), ex.getMessage(), ex);
            }
        }
        listeners.clear();
        LOG.info("KafkaConsumer closed, group: {}", group);
    }

    /**
     * 返回消费者组名。
     *
     * @return group
     */
    public String group() {
        return group;
    }

    /**
     * 返回已订阅的 topic 集合（不可变）。
     *
     * @return topic 集合
     */
    public Collection<String> subscription() {
        return Collections.unmodifiableCollection(listeners.keySet());
    }

    // ===================== 内部方法 =====================

    /**
     * 将 StreamMQ {@link Message} 转换为 {@link ConsumerRecord}。
     *
     * @param topic   主题
     * @param message StreamMQ Message
     * @return ConsumerRecord
     */
    private ConsumerRecord<K, V> toConsumerRecord(String topic, Message<V> message) {
        MessageId messageId = message.getMessageId();
        long offset = messageId != null ? messageId.getTimestamp() : 0L;
        long timestamp = message.getBornTimestamp();

        @SuppressWarnings("unchecked")
        K key = (K) message.getKeys();

        Map<String, String> headers = new LinkedHashMap<>(message.getUserProperties());

        return new ConsumerRecord<>(topic, 0, offset, key, message.getBody(), headers, timestamp);
    }

    /**
     * 确保 Consumer 未关闭。
     *
     * @throws IllegalStateException 如果已关闭
     */
    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("KafkaConsumer is closed");
        }
    }

    /**
     * 验证字符串参数非空。
     *
     * @param value 参数值
     * @param name  参数名
     * @return value（去除首尾空白后）
     * @throws NullPointerException 如果 value 为 null
     * @throws IllegalArgumentException 如果 value 为空字符串
     */
    private static String validateNotEmpty(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value.trim();
    }
}
