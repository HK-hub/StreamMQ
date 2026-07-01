package io.github.streammq.adapter.redisson.producer;

import io.github.streammq.core.producer.StreamMqProducer;
import io.github.streammq.core.producer.StreamMqProducerFactory;
import io.github.streammq.core.spi.MessageConverter;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link StreamMqProducerFactory} 的 Redisson 默认实现。
 *
 * <p>按 {@code group} 缓存 {@link RedissonStreamProducer} 实例，避免重复创建。
 * 同一 {@code group} 的所有 Producer 共享底层 {@link RedissonClient}。
 *
 * <p>属性 key 约定（取自 {@link StreamMqProducerFactory#createProducer(Map)}）：
 * <ul>
 *   <li>{@code group} - 生产者组名（必填）</li>
 *   <li>{@code namespace} - 命名空间（默认空字符串）</li>
 *   <li>{@code send-message-timeout} - 默认发送超时毫秒（默认 3000）</li>
 *   <li>{@code stream.max-len} - Stream 最大长度（默认 0，不限制）</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class RedissonStreamProducerFactory implements StreamMqProducerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(RedissonStreamProducerFactory.class);

    /** 默认发送超时（毫秒） */
    public static final long DEFAULT_SEND_TIMEOUT_MILLIS = 3000L;

    /** 默认 Stream 最大长度（0 = 不限制） */
    public static final int DEFAULT_MAX_LEN = 0;

    private final RedissonClient redisson;
    private final MessageConverter converter;
    private final ConcurrentMap<String, RedissonStreamProducer> producers = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * 构造工厂。
     *
     * @param redisson Redisson 客户端
     * @param converter 消息转换器
     */
    public RedissonStreamProducerFactory(RedissonClient redisson, MessageConverter converter) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.converter = Objects.requireNonNull(converter, "converter");
    }

    @Override
    public StreamMqProducer createProducer(Map<String, Object> properties) {
        ensureOpen();
        Objects.requireNonNull(properties, "properties");
        String group = requireString(properties, "group");
        return producers.computeIfAbsent(group, g -> {
            String namespace = optionalString(properties, "namespace", "");
            long timeout = optionalLong(properties, "send-message-timeout", DEFAULT_SEND_TIMEOUT_MILLIS);
            int maxLen = optionalInt(properties, "stream.max-len", DEFAULT_MAX_LEN);
            LOG.info("Create RedissonStreamProducer: group={}, namespace={}, timeout={}ms, maxLen={}",
                g, namespace, timeout, maxLen);
            return new RedissonStreamProducer(redisson, namespace, g, converter, timeout, maxLen);
        });
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (RedissonStreamProducer producer : producers.values()) {
                producer.close();
            }
            producers.clear();
            LOG.info("RedissonStreamProducerFactory closed");
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("ProducerFactory is closed");
        }
    }

    private static String requireString(Map<String, Object> props, String key) {
        Object value = props.get(key);
        if (value == null || value.toString().isEmpty()) {
            throw new IllegalArgumentException("Missing required property: " + key);
        }
        return value.toString();
    }

    private static String optionalString(Map<String, Object> props, String key, String defaultValue) {
        Object value = props.get(key);
        return value == null ? defaultValue : value.toString();
    }

    private static long optionalLong(Map<String, Object> props, String key, long defaultValue) {
        Object value = props.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number n) {
            return n.longValue();
        }
        return Long.parseLong(value.toString());
    }

    private static int optionalInt(Map<String, Object> props, String key, int defaultValue) {
        Object value = props.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number n) {
            return n.intValue();
        }
        return Integer.parseInt(value.toString());
    }
}
