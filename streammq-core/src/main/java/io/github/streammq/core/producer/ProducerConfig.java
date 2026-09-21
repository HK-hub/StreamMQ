/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.producer;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.serializer.MessageSerializer;
import java.util.Objects;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/**
 * 生产者配置，替代弱类型 {@code Map<String, Object>} 的强类型值对象。
 *
 * <p>使用 Builder 模式构造：
 *
 * <pre>{@code
 * ProducerConfig config = ProducerConfig.builder()
 *     .group("producer-group")
 *     .namespace("ns")
 *     .sendMessageTimeout(3000)
 *     .streamMaxLen(0)
 *     .build();
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
@Builder
public class ProducerConfig {

    /** 生产者组名（必填） */
    @NonNull private final String group;

    /** 命名空间（可选，默认空字符串） */
    @Builder.Default private final String namespace = "";

    /** 发送超时毫秒（可选，默认 {@link StreamMQConstants#DEFAULT_SEND_TIMEOUT_MS}） */
    @Builder.Default
    private final long sendMessageTimeout = StreamMQConstants.DEFAULT_SEND_TIMEOUT_MS;

    /** Stream 最大长度，0 表示不限制（可选，默认 {@link StreamMQConstants#DEFAULT_STREAM_MAX_LEN}） */
    @Builder.Default private final int streamMaxLen = StreamMQConstants.DEFAULT_STREAM_MAX_LEN;

    /** 压缩阈值（字节），body 字节数超过此值时触发压缩，0 = 禁用（默认禁用） */
    @Builder.Default private final int compressThreshold = 0;

    /** 单条消息最大大小（字节），发送时校验（默认 512MB = Redis Stream 上限） */
    @Builder.Default private final long maxMessageSize = StreamMQConstants.MAX_MESSAGE_SIZE_BYTES;

    /** 同步发送失败时的最大重试次数（来自 {@code streammq.producer.retry-times}） */
    @Builder.Default private final int retryTimes = StreamMQConstants.DEFAULT_SYNC_RETRY_TIMES;

    /** 序列化器类（可选，为 null 表示使用全局配置） */
    private final Class<? extends MessageSerializer<?>> serializer;

    /**
     * 全参构造器（由 Lombok {@code @Builder} 调用），统一执行构造期校验。
     *
     * <p><b>为什么需要显式构造器：</b>{@code PublisherConfig} 的 {@code namespace} 此前完全没有校验， 而 namespace
     * 会被直接拼进所有 Redis Key（{@code streammq:{ns}:msg:{topic}}）——含 {@code ':'} 会让 Key 结构错位，含 {@code
     * '{'}/{@code '}'} 会在 Redis Cluster 下把整个 Key 家族钉到同一 slot。 消费侧（{@link
     * io.github.streammq.core.listener.ListenerConfig}）早已 fail-fast，生产侧却静默接受， 同一份 namespace
     * 配置在两侧行为不一致。
     *
     * @param group 生产者组名
     * @param namespace 命名空间
     * @param sendMessageTimeout 发送超时（毫秒）
     * @param streamMaxLen Stream 最大长度（0 = 不限）
     * @param compressThreshold 压缩阈值（字节，0 = 禁用）
     * @param maxMessageSize 单条消息最大字节数
     * @param retryTimes 同步发送重试次数
     * @param serializer 序列化器类
     */
    @SuppressWarnings("java:S107")
    ProducerConfig(
            String group,
            String namespace,
            long sendMessageTimeout,
            int streamMaxLen,
            int compressThreshold,
            long maxMessageSize,
            int retryTimes,
            Class<? extends MessageSerializer<?>> serializer) {
        this.group = Objects.requireNonNull(group, "group");
        // 与 ListenerConfig 同一校验入口：null/空归一为空串，非法字符快速失败
        this.namespace = io.github.streammq.core.util.StringUtils.requireValidNamespace(namespace);
        // 与消费侧（ListenerConfig / DefaultListenerRegistration）同口径 fail-fast：非法数值此前会被静默接受，
        // 直到运行期才以 Redis 错误或"消息全部被拒"暴露（maxMessageSize < 0 会拒绝所有消息，
        // sendMessageTimeout <= 0 会让每次发送立即超时）。
        if (sendMessageTimeout <= 0) {
            throw new IllegalArgumentException(
                    "sendMessageTimeout must be > 0, got: " + sendMessageTimeout);
        }
        if (streamMaxLen < 0) {
            throw new IllegalArgumentException(
                    "streamMaxLen must be >= 0 (0 = unlimited), got: " + streamMaxLen);
        }
        if (compressThreshold < 0) {
            throw new IllegalArgumentException(
                    "compressThreshold must be >= 0 (0 = disabled), got: " + compressThreshold);
        }
        if (maxMessageSize <= 0) {
            throw new IllegalArgumentException(
                    "maxMessageSize must be > 0, got: " + maxMessageSize);
        }
        if (retryTimes < 0) {
            throw new IllegalArgumentException("retryTimes must be >= 0, got: " + retryTimes);
        }
        this.sendMessageTimeout = sendMessageTimeout;
        this.streamMaxLen = streamMaxLen;
        this.compressThreshold = compressThreshold;
        this.maxMessageSize = maxMessageSize;
        this.retryTimes = retryTimes;
        this.serializer = serializer;
    }
}
