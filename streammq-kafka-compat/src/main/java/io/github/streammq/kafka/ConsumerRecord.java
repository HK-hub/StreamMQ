package io.github.streammq.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * StreamMQ 兼容 Kafka 风格的 ConsumerRecord。
 *
 * <p>对齐 Kafka {@code ConsumerRecord<K, V>} API 风格，封装一条消费到的消息。
 * 通过构造函数设置所有字段，不可变设计。
 *
 * <p>由 {@link KafkaConsumer#poll} 从底层 {@code RedissonStreamListener} 拉取消息后构造。
 * Redis Stream 没有原生 partition 概念，partition 固定为 0；offset 使用消息 ID 的 timestamp 部分。
 *
 * @param <K> key 类型
 * @param <V> value 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class ConsumerRecord<K, V> {

    /** 主题 */
    private final String topic;

    /** 分区（Redis Stream 无原生分区，固定为 0） */
    private final int partition;

    /** 偏移量（使用消息 ID 的 timestamp 部分） */
    private final long offset;

    /** 消息键 */
    private final K key;

    /** 消息体 */
    private final V value;

    /** 消息头（不可变） */
    private final Map<String, String> headers;

    /** 消息时间戳（毫秒） */
    private final long timestamp;

    /**
     * 全参构造。
     *
     * @param topic     主题
     * @param partition 分区
     * @param offset    偏移量
     * @param key      消息键，可为 null
     * @param value    消息体
     * @param headers  消息头，可为 null
     * @param timestamp 时间戳
     */
    public ConsumerRecord(String topic, int partition, long offset,
                          K key, V value, Map<String, String> headers, long timestamp) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = value;
        this.headers = headers == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new HashMap<>(headers));
        this.timestamp = timestamp;
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
     * 返回分区（Redis Stream 无原生分区，固定为 0）。
     *
     * @return 0
     */
    public int partition() {
        return partition;
    }

    /**
     * 返回偏移量。
     *
     * @return offset
     */
    public long offset() {
        return offset;
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
     * @return headers
     */
    public Map<String, String> headers() {
        return headers;
    }

    /**
     * 返回消息时间戳（毫秒）。
     *
     * @return timestamp
     */
    public long timestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "ConsumerRecord{"
            + "topic='" + topic + '\''
            + ", partition=" + partition
            + ", offset=" + offset
            + ", key=" + key
            + ", timestamp=" + timestamp
            + ", headers.size=" + headers.size()
            + '}';
    }
}
