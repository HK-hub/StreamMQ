package io.github.streammq.core.producer;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.serializer.MessageSerializer;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/**
 * 生产者配置，替代弱类型 {@code Map<String, Object>} 的强类型值对象。
 *
 * <p>使用 Builder 模式构造：
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
    @NonNull
    private final String group;

    /** 命名空间（可选，默认空字符串） */
    @Builder.Default
    private final String namespace = "";

    /** 发送超时毫秒（可选，默认 {@link StreamMQConstants#DEFAULT_SEND_TIMEOUT_MS}） */
    @Builder.Default
    private final long sendMessageTimeout = StreamMQConstants.DEFAULT_SEND_TIMEOUT_MS;

    /** Stream 最大长度，0 表示不限制（可选，默认 {@link StreamMQConstants#DEFAULT_STREAM_MAX_LEN}） */
    @Builder.Default
    private final int streamMaxLen = StreamMQConstants.DEFAULT_STREAM_MAX_LEN;

    /** 序列化器类（可选，为 null 表示使用全局配置） */
    private final Class<? extends MessageSerializer<?>> serializer;
}
