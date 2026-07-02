package io.github.streammq.adapter.redisson.consumer;

import io.github.streammq.core.consumer.ConsumerConfig;
import io.github.streammq.core.consumer.StreamMqConsumer;
import io.github.streammq.core.consumer.StreamMqConsumerFactory;
import io.github.streammq.core.spi.MessageConverter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 基于 Redisson 的 {@link StreamMqConsumerFactory} 默认实现。
 *
 * <p>持有共享的 {@link RedissonClient} 与 {@link MessageConverter}，按 {@link ConsumerConfig}
 * 创建 {@link RedissonStreamConsumer} 实例。
 *
 * <p>配置项（参见 {@link ConsumerConfig}）：
 * <ul>
 *   <li>{@code topic} - 主题（必填）</li>
 *   <li>{@code consumerGroup} - 消费者组名（必填）</li>
 *   <li>{@code consumerName} - 消费者实例名（可选，默认自动生成 UUID 后缀）</li>
 *   <li>{@code namespace} - 命名空间（可选，默认空字符串）</li>
 * </ul>
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@RequiredArgsConstructor
public class RedissonStreamConsumerFactory implements StreamMqConsumerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(RedissonStreamConsumerFactory.class);

    @NonNull
    private final RedissonClient redisson;
    @NonNull
    private final MessageConverter converter;
    private final ConcurrentMap<RedissonStreamConsumer, Boolean> consumers = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    @Override
    public StreamMqConsumer createConsumer(ConsumerConfig config) {
        if (closed) {
            throw new IllegalStateException("ConsumerFactory is closed");
        }
        Objects.requireNonNull(config, "config");
        String topic = config.getTopic();
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("Missing required property: topic");
        }
        String group = config.getConsumerGroup();
        if (group == null || group.isEmpty()) {
            throw new IllegalArgumentException("Missing required property: consumerGroup");
        }
        String consumerName = config.getConsumerName();
        if (consumerName == null || consumerName.isEmpty()) {
            consumerName = group + "-" + UUID.randomUUID().toString().substring(0, 8);
        }
        String namespace = config.getNamespace();
        if (namespace == null) {
            namespace = "";
        }

        RedissonStreamConsumer consumer = RedissonStreamConsumer.builder()
            .redisson(redisson)
            .namespace(namespace)
            .topic(topic)
            .group(group)
            .consumerName(consumerName)
            .converter(converter)
            .dlqMode(config.isDlqMode())
            .dlqOriginalGroup(config.getDlqOriginalGroup())
            .targetBodyType(config.getTargetBodyType())
            .build();
        consumers.put(consumer, Boolean.TRUE);
        LOG.debug("Consumer created: topic={}, group={}, consumer={}, dlqMode={}",
            topic, group, consumerName, config.isDlqMode());
        return consumer;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        int total = consumers.size();
        for (RedissonStreamConsumer consumer : consumers.keySet()) {
            try {
                consumer.close();
            } catch (RuntimeException ex) {
                LOG.warn("Failed to close consumer: {}", ex.getMessage(), ex);
            }
        }
        consumers.clear();
        LOG.info("RedissonStreamConsumerFactory closed, total consumers: {}", total);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
