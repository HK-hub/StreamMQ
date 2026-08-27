/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.producer;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.compression.CompressionCodec;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.producer.ProducerConfig;
import io.github.streammq.core.producer.StreamMessageProducer;
import io.github.streammq.core.producer.StreamMessageProducerFactory;
import io.github.streammq.core.util.StringUtils;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link StreamMessageProducerFactory} 的 Redisson 默认实现。
 *
 * <p>按「全部影响行为的配置字段」组合键缓存 {@link RedissonStreamProducer} 实例，避免重复创建。 键包含 namespace、group、发送超时、Stream
 * 最大长度、压缩阈值与单条消息上限——此前仅按 {@code group} 缓存，同组第二次以不同配置调用会静默拿到旧配置的实例。 同一键的所有 Producer 共享底层 {@link
 * RedissonClient}。
 *
 * <p>配置项（参见 {@link ProducerConfig}）：
 *
 * <ul>
 *   <li>{@code group} - 生产者组名（必填）
 *   <li>{@code namespace} - 命名空间（默认空字符串）
 *   <li>{@code sendMessageTimeout} - 默认发送超时毫秒（默认 3000）
 *   <li>{@code streamMaxLen} - Stream 最大长度（默认 0，不限制）
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@RequiredArgsConstructor
public class RedissonStreamProducerFactory implements StreamMessageProducerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(RedissonStreamProducerFactory.class);

    /** 默认发送超时（毫秒） */
    public static final long DEFAULT_SEND_TIMEOUT_MILLIS =
            StreamMQConstants.DEFAULT_SEND_TIMEOUT_MS;

    /** 默认 Stream 最大长度（0 = 不限制） */
    public static final int DEFAULT_MAX_LEN = StreamMQConstants.DEFAULT_STREAM_MAX_LEN;

    /**
     * Producer 缓存键：覆盖 {@link #createProducer} 实际消费的全部影响行为的配置字段 （归一化后参与比较，等效配置共享实例）。
     *
     * @param namespace 命名空间（null 归一化为空串）
     * @param group 生产组名
     * @param sendMessageTimeoutMillis 发送超时毫秒
     * @param maxLen Stream 最大长度
     * @param compressThreshold 压缩阈值
     * @param maxMessageSize 单条消息最大字节数
     */
    private record ProducerCacheKey(
            String namespace,
            String group,
            long sendMessageTimeoutMillis,
            int maxLen,
            int compressThreshold,
            long maxMessageSize) {}

    @NonNull private final RedissonClient redisson;
    @NonNull private final MessageConverter converter;
    private final ConcurrentMap<ProducerCacheKey, RedissonStreamProducer> producers =
            new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** 压缩编解码器（可选注入，配合 ProducerConfig.compressThreshold 使用） */
    @Setter private CompressionCodec compressionCodec;

    @Override
    public StreamMessageProducer createProducer(ProducerConfig config) {
        ensureOpen();
        Objects.requireNonNull(config, "config");
        String group = StringUtils.requireValidGroup(config.getGroup());
        // 与创建逻辑保持一致的归一化：等效配置必须命中同一缓存键
        String namespace = Objects.isNull(config.getNamespace()) ? "" : config.getNamespace();
        long timeout =
                config.getSendMessageTimeout() > 0
                        ? config.getSendMessageTimeout()
                        : DEFAULT_SEND_TIMEOUT_MILLIS;
        int maxLen = Math.max(config.getStreamMaxLen(), DEFAULT_MAX_LEN);
        int compressThreshold = Math.max(config.getCompressThreshold(), 0);
        long maxMessageSize =
                config.getMaxMessageSize() > 0
                        ? config.getMaxMessageSize()
                        : StreamMQConstants.MAX_MESSAGE_SIZE_BYTES;
        ProducerCacheKey key =
                new ProducerCacheKey(
                        namespace, group, timeout, maxLen, compressThreshold, maxMessageSize);
        return producers.computeIfAbsent(
                key,
                k -> {
                    LOG.info(
                            "Create RedissonStreamProducer: group={}, namespace={}, timeout={}ms,"
                                    + " maxLen={}, compressThreshold={}, maxMessageSize={}",
                            k.group(),
                            k.namespace(),
                            k.sendMessageTimeoutMillis(),
                            k.maxLen(),
                            k.compressThreshold(),
                            k.maxMessageSize());
                    RedissonStreamProducer producer =
                            RedissonStreamProducer.builder()
                                    .redisson(redisson)
                                    .namespace(k.namespace())
                                    .group(k.group())
                                    .converter(converter)
                                    .defaultTimeoutMillis(k.sendMessageTimeoutMillis())
                                    .maxLen(k.maxLen())
                                    .compressThreshold(k.compressThreshold())
                                    .maxMessageSize(k.maxMessageSize())
                                    .build();
                    if (Objects.nonNull(compressionCodec)) {
                        producer.setCompressionCodec(compressionCodec);
                    }
                    return producer;
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
