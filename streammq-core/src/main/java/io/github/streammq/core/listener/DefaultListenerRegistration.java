package io.github.streammq.core.listener;

import io.github.streammq.core.consumer.StreamMessageConsumer;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.filter.ConsumerFilter;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.util.StringUtils;
import java.util.List;
import java.util.concurrent.locks.Lock;
import lombok.Getter;
import lombok.Setter;

@Getter
public class DefaultListenerRegistration<T> implements ListenerRegistration<T> {

    /** DLQ 监听器注册 key 前缀 */
    private static final String DLQ_KEY_PREFIX = "dlq:";

    /** 注册 key 中 topic 与 group 的分隔符 */
    private static final String KEY_SEPARATOR = ":";

    private final ListenerType type;
    private final StreamMessageConsumer<T> consumer;
    private final String topic;
    private final String group;
    private final ConsumeMode consumeMode;
    private final int maxReconsumeTimes;
    private final int shardCount;
    private final long consumeTimeoutMillis;
    private final List<Lock> shardLocks;
    private final int pullBatchSize;
    private final long pullBlockTimeoutMillis;
    private final long pullIntervalMillis;
    private final String selectorExpression;
    private final Class<? extends MessageSerializer> serializer;
    private final Class<? extends RetryPolicy> retryPolicy;
    private final Class<? extends MessageConverter> messageConverter;
    private final Class<? extends RebalanceStrategy> rebalanceStrategy;
    private final long suspendCurrentQueueTimeMillis;
    private final int streamMaxLen;
    private final boolean enableMsgTrace;
    private final boolean dlqMode;
    private final Class<?> targetBodyType;
    private final Class<? extends DlqFailureStrategy> dlqFailureStrategy;
    private final Class<? extends ConsumerFilter>[] consumerFilter;
    private final SelectorType selectorType;

    @Setter private String namespace;

    public DefaultListenerRegistration(
            ListenerType type,
            StreamMessageConsumer<T> consumer,
            String topic,
            String group,
            ConsumeMode consumeMode,
            int maxReconsumeTimes,
            int shardCount,
            long consumeTimeoutMillis,
            List<Lock> shardLocks,
            int pullBatchSize,
            long pullBlockTimeoutMillis,
            long pullIntervalMillis,
            String selectorExpression,
            Class<? extends MessageSerializer> serializer,
            Class<? extends RetryPolicy> retryPolicy,
            Class<? extends MessageConverter> messageConverter,
            Class<? extends RebalanceStrategy> rebalanceStrategy,
            long suspendCurrentQueueTimeMillis,
            int streamMaxLen,
            boolean enableMsgTrace,
            boolean dlqMode,
            Class<?> targetBodyType,
            Class<? extends DlqFailureStrategy> dlqFailureStrategy,
            Class<? extends ConsumerFilter>[] consumerFilter,
            SelectorType selectorType,
            String namespace) {
        this.type = type;
        this.consumer = consumer;
        this.topic = topic;
        this.group = group;
        this.consumeMode = consumeMode;
        this.maxReconsumeTimes = maxReconsumeTimes;
        this.shardCount = shardCount;
        this.consumeTimeoutMillis = consumeTimeoutMillis;
        this.shardLocks = shardLocks;
        this.pullBatchSize = pullBatchSize;
        this.pullBlockTimeoutMillis = pullBlockTimeoutMillis;
        this.pullIntervalMillis = pullIntervalMillis;
        this.selectorExpression = selectorExpression;
        this.serializer = serializer;
        this.retryPolicy = retryPolicy;
        this.messageConverter = messageConverter;
        this.rebalanceStrategy = rebalanceStrategy;
        this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
        this.streamMaxLen = streamMaxLen;
        this.enableMsgTrace = enableMsgTrace;
        this.dlqMode = dlqMode;
        this.targetBodyType = targetBodyType;
        this.dlqFailureStrategy = dlqFailureStrategy;
        this.consumerFilter = consumerFilter;
        this.selectorType = selectorType;
        this.namespace = namespace;
    }

    @Override
    public void resolveNamespace(String defaultNs) {
        if (StringUtils.isEmpty(namespace)) {
            namespace = defaultNs;
        }
    }

    @Override
    public String key() {
        return (dlqMode ? DLQ_KEY_PREFIX : "") + topic + KEY_SEPARATOR + group;
    }
}
