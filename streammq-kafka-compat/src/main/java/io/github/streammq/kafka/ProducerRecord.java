package io.github.streammq.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * StreamMQ 兼容 Kafka 风格的 ProducerRecord。
 *
 * <p>对齐 Kafka {@code ProducerRecord<K, V>} API 风格，封装一条待发送消息的 topic、key、value 及 headers。
 * 通过构造函数设置所有字段，不可变设计。
 *
 * <p>底层由 {@link KafkaProducer} 转换为 StreamMQ 的 {@link io.github.streammq.core.message.Message}
 * 后委托 {@code StreamMessageTemplate} 发送。
 *
 * @param <K> key 类型
 * @param <V> value 类型（消息体类型）
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class ProducerRecord<K, V> {

    /** 主题（必填） */
    private final String topic;

    /** 消息键（可选） */
    private final K key;

    /** 消息体（必填） */
    private final V value;

    /** 消息头（可选，不可变） */
    private final Map<String, String> headers;

    /**
     * 构造 ProducerRecord，仅指定 topic 和 value。
     *
     * @param topic 主题
     * @param value 消息体
     * @throws NullPointerException 如果 topic 或 value 为 null
     */
    public ProducerRecord(String topic, V value) {
        this(topic, null, value, null);
    }

    /**
     * 构造 ProducerRecord，指定 topic、key 和 value。
     *
     * @param topic 主题
     * @param key 消息键，可为 null
     * @param value 消息体
     * @throws NullPointerException 如果 topic 或 value 为 null
     */
    public ProducerRecord(String topic, K key, V value) {
        this(topic, key, value, null);
    }

    /**
     * 全参构造，指定 topic、key、value 和 headers。
     *
     * @param topic  主题
     * @param key    消息键，可为 null
     * @param value  消息体
     * @param headers 消息头，可为 null（表示空 headers）
     * @throws NullPointerException 如果 topic 或 value 为 null
     */
    public ProducerRecord(String topic, K key, V value, Map<String, String> headers) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.value = Objects.requireNonNull(value, "value");
        this.key = key;
        this.headers = headers == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new HashMap<>(headers));
    }

    /**
     * 返回主题。
     *
     * @return topic
     */
    public String topic() {
        return topic;
    }

    /**
     * 返回消息键。
     *
     * @return key，可能为 null
     */
    public K key() {
        return key;
    }

    /**
     * 返回消息体。
     *
     * @return value
     */
    public V value() {
        return value;
    }

    /**
     * 返回消息头（不可变）。
     *
     * @return headers，不可为 null
     */
    public Map<String, String> headers() {
        return headers;
    }

    @Override
    public String toString() {
        return "ProducerRecord{"
            + "topic='" + topic + '\''
            + ", key=" + key
            + ", headers.size=" + headers.size()
            + '}';
    }
}
