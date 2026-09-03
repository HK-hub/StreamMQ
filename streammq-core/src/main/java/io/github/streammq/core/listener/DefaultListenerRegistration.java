/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.listener;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.consumer.StreamMessageConsumer;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeFromWhere;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.filter.ConsumerFilter;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.util.StringUtils;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import lombok.Getter;
import lombok.Setter;

/**
 * {@link ListenerRegistration} 默认实现（0.1.0 起为唯一注册模型）。
 *
 * <p>声明式字段与运行时字段统一在此建模；此前与之并行的 {@link ListenerConfig} 已降级为派生视图——由 {@link
 * ListenerConfig#from(ListenerRegistration)} 单点映射， 供底层 {@link StreamMQListenerFactory} SPI
 * 消费，二者不再各自维护可漂移的字段副本。
 *
 * <p>与 {@link ListenerConfig} 的校验差异（有意保留）：本类对 {@code consumeThreadMax}/{@code shardCount}
 * 等运行时参数采取「夹取（clamp）」策略以保证注册期弹性； 而 {@link ListenerConfig} 构造器对同类参数直接抛出 IllegalArgumentException
 * 以便配置错误尽早暴露。详见两者各自的字段注释。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
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

    /** 顺序消费分片数（夹取下界 0，非负；与 {@link ListenerConfig} 的「&lt;1 抛异常」策略不同） */
    private final int shardCount;

    private final long consumeTimeoutMillis;

    /** 顺序消费单条消息消费超时（毫秒），0 表示不启用（仅对 ORDERLY 生效，默认关闭） */
    private final long orderlyConsumeTimeoutMillis;

    /** 顺序消费分片锁（构造时防御性拷贝为不可变列表，null 表示未设置） */
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

    /** 新消费者组的起始消费位点（永不为 null，未声明时为 {@link ConsumeFromWhere#DEFAULT}）。 */
    private final ConsumeFromWhere consumeFromWhere;

    private final boolean enableMsgTrace;
    private final boolean dlqMode;
    private final Class<?> targetBodyType;
    private final Class<? extends DlqFailureStrategy> dlqFailureStrategy;

    @SuppressWarnings("unchecked")
    private final Class<? extends ConsumerFilter>[] consumerFilter;

    private final SelectorType selectorType;

    /** 底层 Redis 消费者名（null 时由适配层自动生成）。 */
    private final String consumerName;

    /** 是否为 retry Stream 监听（对齐 RocketMQ %RETRY%{group}%）。 */
    private final boolean retryMode;

    /** per-consumer 已解析转换器实例（null 表示使用全局），由容器在 SPI 解析后回填。 */
    @Getter @Setter private MessageConverter converterInstance;

    /** 并发消费循环数下限（夹取下界 1）与上限（夹取至 &gt;= min；与 {@link ListenerConfig} 的「非法即抛异常」策略不同）。 */
    private final int consumeThreadMin;

    private final int consumeThreadMax;

    @Setter private String namespace;

    private DefaultListenerRegistration(Builder<T> b) {
        this.type = b.type;
        this.consumer = b.consumer;
        this.topic = StringUtils.requireValidName(b.topic, "topic");
        this.group = StringUtils.requireValidName(b.group, "consumerGroup");
        this.consumeMode = b.consumeMode;
        this.maxReconsumeTimes = (int) requireMin("maxReconsumeTimes", b.maxReconsumeTimes, 0);
        this.shardCount = Math.max(0, b.shardCount);
        this.consumeTimeoutMillis = requireMin("consumeTimeoutMillis", b.consumeTimeoutMillis, 0);
        this.orderlyConsumeTimeoutMillis =
                requireMin("orderlyConsumeTimeoutMillis", b.orderlyConsumeTimeoutMillis, 0);
        // 防御性拷贝为不可变列表，避免外部持有原引用在注册后被修改（容器注册后视为不可变）
        this.shardLocks = Objects.isNull(b.shardLocks) ? null : List.copyOf(b.shardLocks);
        this.pullBatchSize = (int) requireMin("pullBatchSize", b.pullBatchSize, 1);
        this.pullBlockTimeoutMillis =
                requireMin("pullBlockTimeoutMillis", b.pullBlockTimeoutMillis, 0);
        this.pullIntervalMillis = requireMin("pullIntervalMillis", b.pullIntervalMillis, 0);
        this.selectorExpression = b.selectorExpression;
        this.serializer = b.serializer;
        this.retryPolicy = b.retryPolicy;
        this.messageConverter = b.messageConverter;
        this.rebalanceStrategy = b.rebalanceStrategy;
        this.suspendCurrentQueueTimeMillis =
                requireMin("suspendCurrentQueueTimeMillis", b.suspendCurrentQueueTimeMillis, 0);
        this.streamMaxLen = Math.max(0, b.streamMaxLen);
        this.consumeFromWhere =
                Objects.isNull(b.consumeFromWhere) ? ConsumeFromWhere.DEFAULT : b.consumeFromWhere;
        this.enableMsgTrace = b.enableMsgTrace;
        this.dlqMode = b.dlqMode;
        this.targetBodyType = b.targetBodyType;
        this.dlqFailureStrategy = b.dlqFailureStrategy;
        this.consumerFilter = b.consumerFilter;
        this.selectorType = b.selectorType;
        this.namespace = b.namespace;
        this.consumeThreadMin = Math.max(1, b.consumeThreadMin);
        this.consumeThreadMax = Math.max(this.consumeThreadMin, b.consumeThreadMax);
        this.consumerName = b.consumerName;
        this.retryMode = b.retryMode;
    }

    private static long requireMin(String name, long value, long min) {
        if (value < min) {
            throw new IllegalArgumentException(name + " must be >= " + min + ", got: " + value);
        }
        return value;
    }

    /** 唯一构造入口（Builder 模式），字段校验集中于此。 */
    public static final class Builder<T> {
        private ListenerType type;
        private StreamMessageConsumer<T> consumer;
        private String topic;
        private String group;
        private ConsumeMode consumeMode = ConsumeMode.CLUSTERING;
        private int maxReconsumeTimes = StreamMQConstants.DEFAULT_MAX_RECONSUME_TIMES;
        private int shardCount;
        private long consumeTimeoutMillis = StreamMQConstants.DEFAULT_CONSUME_TIMEOUT_MS;
        private long orderlyConsumeTimeoutMillis;
        private List<Lock> shardLocks;
        private int pullBatchSize = StreamMQConstants.DEFAULT_CONSUME_BATCH_SIZE;
        private long pullBlockTimeoutMillis = StreamMQConstants.DEFAULT_PULL_BLOCK_TIMEOUT_MS;
        private long pullIntervalMillis = StreamMQConstants.DEFAULT_PULL_INTERVAL_MS;
        private String selectorExpression;
        private Class<? extends MessageSerializer> serializer;
        private Class<? extends RetryPolicy> retryPolicy;
        private Class<? extends MessageConverter> messageConverter;
        private Class<? extends RebalanceStrategy> rebalanceStrategy;
        private long suspendCurrentQueueTimeMillis =
                StreamMQConstants.DEFAULT_SUSPEND_CURRENT_QUEUE_TIME_MS;
        private int streamMaxLen;
        private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.DEFAULT;
        private boolean enableMsgTrace;
        private boolean dlqMode;
        private Class<?> targetBodyType;
        private Class<? extends DlqFailureStrategy> dlqFailureStrategy;
        private Class<? extends ConsumerFilter>[] consumerFilter;
        private SelectorType selectorType;
        private String namespace;
        private String consumerName;
        private boolean retryMode;
        private MessageConverter converterInstance;
        private int consumeThreadMin = 1;
        private int consumeThreadMax = StreamMQConstants.DEFAULT_CONSUME_THREAD_MAX;

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

        public Builder<T> maxReconsumeTimes(int v) {
            this.maxReconsumeTimes = v;
            return this;
        }

        public Builder<T> shardCount(int v) {
            this.shardCount = v;
            return this;
        }

        public Builder<T> consumeTimeoutMillis(long v) {
            this.consumeTimeoutMillis = v;
            return this;
        }

        public Builder<T> orderlyConsumeTimeoutMillis(long v) {
            this.orderlyConsumeTimeoutMillis = v;
            return this;
        }

        public Builder<T> shardLocks(List<Lock> v) {
            this.shardLocks = v;
            return this;
        }

        public Builder<T> pullBatchSize(int v) {
            this.pullBatchSize = v;
            return this;
        }

        public Builder<T> pullBlockTimeoutMillis(long v) {
            this.pullBlockTimeoutMillis = v;
            return this;
        }

        public Builder<T> pullIntervalMillis(long v) {
            this.pullIntervalMillis = v;
            return this;
        }

        public Builder<T> selectorExpression(String v) {
            this.selectorExpression = v;
            return this;
        }

        public Builder<T> serializer(Class<? extends MessageSerializer> v) {
            this.serializer = v;
            return this;
        }

        public Builder<T> retryPolicy(Class<? extends RetryPolicy> v) {
            this.retryPolicy = v;
            return this;
        }

        public Builder<T> messageConverter(Class<? extends MessageConverter> v) {
            this.messageConverter = v;
            return this;
        }

        public Builder<T> rebalanceStrategy(Class<? extends RebalanceStrategy> v) {
            this.rebalanceStrategy = v;
            return this;
        }

        public Builder<T> suspendCurrentQueueTimeMillis(long v) {
            this.suspendCurrentQueueTimeMillis = v;
            return this;
        }

        /**
         * 设置新消费者组的起始消费位点；传 null 视为 {@link ConsumeFromWhere#DEFAULT}。
         *
         * @param v 起始消费位点策略
         * @return this
         */
        public Builder<T> consumeFromWhere(ConsumeFromWhere v) {
            this.consumeFromWhere = v;
            return this;
        }

        public Builder<T> streamMaxLen(int v) {
            this.streamMaxLen = v;
            return this;
        }

        public Builder<T> enableMsgTrace(boolean v) {
            this.enableMsgTrace = v;
            return this;
        }

        public Builder<T> dlqMode(boolean v) {
            this.dlqMode = v;
            return this;
        }

        public Builder<T> targetBodyType(Class<?> v) {
            this.targetBodyType = v;
            return this;
        }

        public Builder<T> dlqFailureStrategy(Class<? extends DlqFailureStrategy> v) {
            this.dlqFailureStrategy = v;
            return this;
        }

        @SuppressWarnings("unchecked")
        public Builder<T> consumerFilter(Class<? extends ConsumerFilter>[] v) {
            this.consumerFilter = v;
            return this;
        }

        public Builder<T> selectorType(SelectorType v) {
            this.selectorType = v;
            return this;
        }

        public Builder<T> namespace(String v) {
            this.namespace = v;
            return this;
        }

        public Builder<T> consumerName(String v) {
            this.consumerName = v;
            return this;
        }

        public Builder<T> retryMode(boolean v) {
            this.retryMode = v;
            return this;
        }

        public Builder<T> converterInstance(MessageConverter v) {
            this.converterInstance = v;
            return this;
        }

        public Builder<T> consumeThreadMin(int v) {
            this.consumeThreadMin = v;
            return this;
        }

        public Builder<T> consumeThreadMax(int v) {
            this.consumeThreadMax = v;
            return this;
        }

        public DefaultListenerRegistration<T> build() {
            return new DefaultListenerRegistration<>(this);
        }
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
