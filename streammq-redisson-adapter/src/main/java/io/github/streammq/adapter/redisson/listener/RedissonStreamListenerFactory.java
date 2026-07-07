package io.github.streammq.adapter.redisson.listener;

import io.github.streammq.core.listener.ListenerConfig;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.converter.MessageConverter;
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
 * 基于 Redisson 的 {@link StreamMQListenerFactory} 默认实现。
 *
 * <p>持有共享的 {@link RedissonClient} 与 {@link MessageConverter}，按 {@link ListenerConfig}
 * 创建 {@link RedissonStreamListener} 实例。
 *
 * <p>配置项（参见 {@link ListenerConfig}）：
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
public class RedissonStreamListenerFactory implements StreamMQListenerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(RedissonStreamListenerFactory.class);

    @NonNull
    private final RedissonClient redisson;
    @NonNull
    private final MessageConverter converter;
    private final ConcurrentMap<RedissonStreamListener, Boolean> listeners = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    @Override
    public StreamMQListener createListener(ListenerConfig config) {
        if (closed) {
            throw new IllegalStateException("ListenerFactory is closed");
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

        RedissonStreamListener listener = RedissonStreamListener.builder()
            .redisson(redisson)
            .namespace(namespace)
            .topic(topic)
            .group(group)
            .consumerName(consumerName)
            .converter(config.getConverter() != null ? config.getConverter() : converter)
            .dlqMode(config.isDlqMode())
            .retryMode(config.isRetryMode())
            .targetBodyType(config.getTargetBodyType())
            .build();
        listeners.put(listener, Boolean.TRUE);
        LOG.debug("Listener created: topic={}, group={}, consumer={}, dlqMode={}, retryMode={}",
            topic, group, consumerName, config.isDlqMode(), config.isRetryMode());
        return listener;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        int total = listeners.size();
        for (RedissonStreamListener listener : listeners.keySet()) {
            try {
                listener.close();
            } catch (RuntimeException ex) {
                LOG.warn("Failed to close listener: {}", ex.getMessage(), ex);
            }
        }
        listeners.clear();
        LOG.info("RedissonStreamListenerFactory closed, total listeners: {}", total);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
