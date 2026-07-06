package io.github.streammq.core.listener;

import io.github.streammq.core.consumer.StreamMessageConsumer;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.policy.DlqFailureHandler;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;

import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * Listener 注册信息接口（值对象）。
 *
 * <p>封装容器在注册 Listener 时所需的全量配置，包括监听类型、消费参数、重试策略、
 * 顺序消费分片锁、DLQ 模式标志、跨平台 body 类型等。
 *
 * <p>使用 Builder 模式构造（参见 {@link ListenerRegistration.Builder}），避免多参数构造器的可读性问题。
 *
 * @param <T> Listener 处理的 body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ListenerRegistration<T> {

    ListenerType getType();

    StreamMessageConsumer<T> getConsumer();

    String getTopic();

    String getGroup();

    ConsumeMode getConsumeMode();

    int getMaxReconsumeTimes();

    int getShardCount();

    long getConsumeTimeoutMillis();

    List<Lock> getShardLocks();

    int getPullBatchSize();

    long getPullBlockTimeoutMillis();

    long getPullIntervalMillis();

    String getSelectorExpression();

    Class<? extends MessageSerializer> getSerializer();

    Class<? extends RetryPolicy> getRetryPolicy();

    Class<? extends MessageConverter> getMessageConverter();

    Class<? extends RebalanceStrategy> getRebalanceStrategy();

    long getSuspendCurrentQueueTimeMillis();

    int getStreamMaxLen();

    boolean isEnableMsgTrace();

    boolean isDlqMode();

    Class<?> getTargetBodyType();

    Class<? extends DlqFailureHandler> getDlqFailureHandler();

    String getNamespace();

    void setNamespace(String namespace);

    void resolveNamespace(String defaultNs);

    String key();

    class Builder<T> {
        private ListenerType type;
        private StreamMessageConsumer<T> consumer;
        private String topic;
        private String group;
        private ConsumeMode consumeMode;
        private int maxReconsumeTimes;
        private int shardCount;
        private long consumeTimeoutMillis;
        private List<Lock> shardLocks;
        private int pullBatchSize;
        private long pullBlockTimeoutMillis;
        private long pullIntervalMillis;
        private String selectorExpression;
        private Class<? extends MessageSerializer> serializer;
        private Class<? extends RetryPolicy> retryPolicy;
        private Class<? extends MessageConverter> messageConverter;
        private Class<? extends RebalanceStrategy> rebalanceStrategy;
        private long suspendCurrentQueueTimeMillis;
        private int streamMaxLen;
        private boolean enableMsgTrace;
        private boolean dlqMode;
        private Class<?> targetBodyType;
        private Class<? extends DlqFailureHandler> dlqFailureHandler;
        private String namespace;

        public Builder<T> type(ListenerType type) {
            this.type = type;
            return this;
        }

        public Builder<T> consumer(StreamMessageConsumer<T> consumer) {
            this.consumer = consumer;
            return this;
        }

        public Builder<T> topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder<T> group(String group) {
            this.group = group;
            return this;
        }

        public Builder<T> consumeMode(ConsumeMode consumeMode) {
            this.consumeMode = consumeMode;
            return this;
        }

        public Builder<T> maxReconsumeTimes(int maxReconsumeTimes) {
            this.maxReconsumeTimes = maxReconsumeTimes;
            return this;
        }

        public Builder<T> shardCount(int shardCount) {
            this.shardCount = shardCount;
            return this;
        }

        public Builder<T> consumeTimeoutMillis(long consumeTimeoutMillis) {
            this.consumeTimeoutMillis = consumeTimeoutMillis;
            return this;
        }

        public Builder<T> shardLocks(List<Lock> shardLocks) {
            this.shardLocks = shardLocks;
            return this;
        }

        public Builder<T> pullBatchSize(int pullBatchSize) {
            this.pullBatchSize = pullBatchSize;
            return this;
        }

        public Builder<T> pullBlockTimeoutMillis(long pullBlockTimeoutMillis) {
            this.pullBlockTimeoutMillis = pullBlockTimeoutMillis;
            return this;
        }

        public Builder<T> pullIntervalMillis(long pullIntervalMillis) {
            this.pullIntervalMillis = pullIntervalMillis;
            return this;
        }

        public Builder<T> selectorExpression(String selectorExpression) {
            this.selectorExpression = selectorExpression;
            return this;
        }

        public Builder<T> serializer(Class<? extends MessageSerializer> serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder<T> retryPolicy(Class<? extends RetryPolicy> retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        public Builder<T> messageConverter(Class<? extends MessageConverter> messageConverter) {
            this.messageConverter = messageConverter;
            return this;
        }

        public Builder<T> rebalanceStrategy(Class<? extends RebalanceStrategy> rebalanceStrategy) {
            this.rebalanceStrategy = rebalanceStrategy;
            return this;
        }

        public Builder<T> suspendCurrentQueueTimeMillis(long suspendCurrentQueueTimeMillis) {
            this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
            return this;
        }

        public Builder<T> streamMaxLen(int streamMaxLen) {
            this.streamMaxLen = streamMaxLen;
            return this;
        }

        public Builder<T> enableMsgTrace(boolean enableMsgTrace) {
            this.enableMsgTrace = enableMsgTrace;
            return this;
        }

        public Builder<T> dlqMode(boolean dlqMode) {
            this.dlqMode = dlqMode;
            return this;
        }

        public Builder<T> targetBodyType(Class<?> targetBodyType) {
            this.targetBodyType = targetBodyType;
            return this;
        }

        public Builder<T> dlqFailureHandler(Class<? extends DlqFailureHandler> dlqFailureHandler) {
            this.dlqFailureHandler = dlqFailureHandler;
            return this;
        }

        public Builder<T> namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public ListenerRegistration<T> build() {
            return new DefaultListenerRegistration<>(type, consumer, topic, group, consumeMode, maxReconsumeTimes,
                shardCount, consumeTimeoutMillis, shardLocks, pullBatchSize,
                pullBlockTimeoutMillis, pullIntervalMillis, selectorExpression, serializer, retryPolicy,
                messageConverter, rebalanceStrategy, suspendCurrentQueueTimeMillis, streamMaxLen,
                enableMsgTrace, dlqMode, targetBodyType, dlqFailureHandler, namespace);
        }
    }

    static <T> Builder<T> builder() {
        return new Builder<>();
    }
}