package io.github.streammq.adapter.redisson.consumer;

import io.github.streammq.core.consumer.StreamMqConsumer;
import io.github.streammq.core.consumer.StreamMqConsumerFactory;
import io.github.streammq.core.spi.MessageConverter;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 基于 Redisson 的 {@link StreamMqConsumerFactory} 默认实现。
 *
 * <p>持有共享的 {@link RedissonClient} 与 {@link MessageConverter}，按属性创建
 * {@link RedissonStreamConsumer} 实例。
 *
 * <p>属性 key（对齐 {@link StreamMqConsumerFactory#createConsumer}）：
 * <ul>
 *   <li>{@code topic} - 主题（必填）</li>
 *   <li>{@code consumer-group} - 消费者组名（必填）</li>
 *   <li>{@code consumer-name} - 消费者实例名（可选，默认自动生成 UUID 后缀）</li>
 *   <li>{@code namespace} - 命名空间（可选，默认全局配置）</li>
 * </ul>
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class RedissonStreamConsumerFactory implements StreamMqConsumerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(RedissonStreamConsumerFactory.class);

    /** 属性 key：主题 */
    public static final String PROP_TOPIC = "topic";
    /** 属性 key：消费者组名 */
    public static final String PROP_CONSUMER_GROUP = "consumer-group";
    /** 属性 key：消费者实例名 */
    public static final String PROP_CONSUMER_NAME = "consumer-name";
    /** 属性 key：命名空间 */
    public static final String PROP_NAMESPACE = "namespace";

    private final RedissonClient redisson;
    private final MessageConverter converter;
    private final ConcurrentMap<RedissonStreamConsumer, Boolean> consumers = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    /**
     * 构造工厂。
     *
     * @param redisson Redisson 客户端
     * @param converter 消息转换器
     */
    public RedissonStreamConsumerFactory(RedissonClient redisson, MessageConverter converter) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.converter = Objects.requireNonNull(converter, "converter");
    }

    @Override
    public StreamMqConsumer createConsumer(Map<String, Object> properties) {
        if (closed) {
            throw new IllegalStateException("ConsumerFactory is closed");
        }
        Objects.requireNonNull(properties, "properties");
        String topic = requireString(properties, PROP_TOPIC);
        String group = requireString(properties, PROP_CONSUMER_GROUP);
        String consumerName = optionalString(properties, PROP_CONSUMER_NAME);
        if (consumerName == null || consumerName.isEmpty()) {
            consumerName = group + "-" + UUID.randomUUID().toString().substring(0, 8);
        }
        String namespace = optionalString(properties, PROP_NAMESPACE);
        if (namespace == null) {
            namespace = "";
        }

        RedissonStreamConsumer consumer = new RedissonStreamConsumer(
            redisson, namespace, topic, group, consumerName, converter);
        consumers.put(consumer, Boolean.TRUE);
        LOG.debug("Consumer created: topic={}, group={}, consumer={}", topic, group, consumerName);
        return consumer;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (RedissonStreamConsumer consumer : consumers.keySet()) {
            try {
                consumer.close();
            } catch (RuntimeException ex) {
                LOG.warn("Failed to close consumer: {}", ex.getMessage());
            }
        }
        consumers.clear();
        LOG.info("RedissonStreamConsumerFactory closed, total consumers: {}", consumers.size());
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    private static String requireString(Map<String, Object> props, String key) {
        Object value = props.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing required property: " + key);
        }
        String str = value.toString();
        if (str.isEmpty()) {
            throw new IllegalArgumentException("Property " + key + " must not be empty");
        }
        return str;
    }

    private static String optionalString(Map<String, Object> props, String key) {
        Object value = props.get(key);
        return value == null ? null : value.toString();
    }
}
