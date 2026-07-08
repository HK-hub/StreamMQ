package io.github.streammq.adapter.redisson.producer;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.producer.ProducerConfig;
import io.github.streammq.core.producer.StreamMessageProducer;
import io.github.streammq.core.producer.StreamMessageProducerFactory;
import io.github.streammq.core.converter.MessageConverter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link StreamMessageProducerFactory} 的 Redisson 默认实现。
 *
 * <p>按 {@code group} 缓存 {@link RedissonStreamProducer} 实例，避免重复创建。
 * 同一 {@code group} 的所有 Producer 共享底层 {@link RedissonClient}。
 *
 * <p>配置项（参见 {@link ProducerConfig}）：
 * <ul>
 *   <li>{@code group} - 生产者组名（必填）</li>
 *   <li>{@code namespace} - 命名空间（默认空字符串）</li>
 *   <li>{@code sendMessageTimeout} - 默认发送超时毫秒（默认 3000）</li>
 *   <li>{@code streamMaxLen} - Stream 最大长度（默认 0，不限制）</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@RequiredArgsConstructor
public class RedissonStreamProducerFactory implements StreamMessageProducerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(RedissonStreamProducerFactory.class);

    /** 默认发送超时（毫秒） */
    public static final long DEFAULT_SEND_TIMEOUT_MILLIS = StreamMQConstants.DEFAULT_SEND_TIMEOUT_MS;

    /** 默认 Stream 最大长度（0 = 不限制） */
    public static final int DEFAULT_MAX_LEN = StreamMQConstants.DEFAULT_STREAM_MAX_LEN;

    @NonNull
    private final RedissonClient redisson;
    @NonNull
    private final MessageConverter converter;
    private final ConcurrentMap<String, RedissonStreamProducer> producers = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Override
    public StreamMessageProducer createProducer(ProducerConfig config) {
        ensureOpen();
        Objects.requireNonNull(config, "config");
        String group = config.getGroup();
        if (group == null || group.isEmpty()) {
            throw new IllegalArgumentException("Missing required property: group");
        }
        return producers.computeIfAbsent(group, g -> {
            String namespace = config.getNamespace();
            if (namespace == null) {
                namespace = "";
            }
            long timeout = config.getSendMessageTimeout();
            if (timeout <= 0) {
                timeout = DEFAULT_SEND_TIMEOUT_MILLIS;
            }
            int maxLen = config.getStreamMaxLen();
            if (maxLen < 0) {
                maxLen = DEFAULT_MAX_LEN;
            }
            LOG.info("Create RedissonStreamProducer: group={}, namespace={}, timeout={}ms, maxLen={}",
                g, namespace, timeout, maxLen);
            return RedissonStreamProducer.builder()
                .redisson(redisson)
                .namespace(namespace)
                .group(g)
                .converter(converter)
                .defaultTimeoutMillis(timeout)
                .maxLen(maxLen)
                .build();
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
}
