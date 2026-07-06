package io.github.streammq.core.listener;

import io.github.streammq.core.enums.AcknowledgeMode;
import io.github.streammq.core.enums.ConsumeMode;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.redisson.api.RLock;

/**
 * Listener 注册信息（值对象）。
 *
 * <p>封装容器在注册 Listener 时所需的全量配置，包括监听类型、消费参数、重试策略、
 * 顺序消费分片锁、DLQ 模式标志、跨平台 body 类型等。
 *
 * <p>使用 {@link Builder} 模式构造，避免多参数构造器的可读性问题。
 *
 * @param <T> Listener 处理的 body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
public class ListenerRegistration<T> {

    /** Listener 类型 */
    private final ListenerType type;
    /** Consumer 实例（StreamMessageConcurrentlyConsumer / StreamMessageOrderlyConsumer） */
    private final Object consumer;
    /** 主题 */
    private final String topic;
    /** 消费者组名 */
    private final String group;
    /** 消费模式 */
    private final ConsumeMode consumeMode;
    /** ACK 模式 */
    private final AcknowledgeMode ackMode;
    /** 最大重试次数 */
    private final int maxReconsumeTimes;
    /** 顺序消费分片数（仅 ORDERLY 类型有效，其他类型为 0） */
    private final int shardCount;
    /** 单条消息消费超时（毫秒），用于 shard 锁租约时间 */
    private final long consumeTimeoutMillis;
    /** 顺序消费 shard 级分布式锁数组（仅 ORDERLY 类型非 null） */
    private final RLock[] shardLocks;
    /** 单次拉取批量大小 */
    private final int pullBatchSize;
    /** 拉取阻塞超时（毫秒） */
    private final long pullBlockTimeoutMillis;
    /** 拉取间隔（毫秒，0=不间隔） */
    private final long pullIntervalMillis;
    /** Tag 过滤表达式 */
    private final String selectorExpression;
    /** 序列化器类（null=使用全局） */
    private final Class<?> serializer;
    /** 重试策略类（null=使用全局） */
    private final Class<?> retryPolicy;
    /** 消息转换器类（null=使用全局） */
    private final Class<?> messageConverter;
    /** 重平衡策略类（null=使用全局） */
    private final Class<?> rebalanceStrategy;
    /** 顺序消费挂起时长（毫秒） */
    private final long suspendCurrentQueueTimeMillis;
    /** Stream 最大长度（0=使用全局配置） */
    private final int streamMaxLen;
    /** 是否启用消息追踪 */
    private final boolean enableMsgTrace;
    /** DLQ 模式标志：true=从 DLQ Stream 消费死信消息 */
    private final boolean dlqMode;
    /** DLQ 原始消费者组（仅 dlqMode=true 时使用，用于构造 DLQ Stream Key） */
    private final String dlqOriginalGroup;
    /** 目标 body 类型（解析自 Listener 泛型 T，跨平台反序列化回退类型） */
    private final Class<?> targetBodyType;

    /** 命名空间（由 setter 注入，resolveNamespace 后确定最终值） */
    @Setter
    private String namespace;

    @Builder
    public ListenerRegistration(ListenerType type, Object consumer, String topic, String group,
                                ConsumeMode consumeMode, AcknowledgeMode ackMode, int maxReconsumeTimes,
                                int shardCount, long consumeTimeoutMillis, RLock[] shardLocks,
                                int pullBatchSize, long pullBlockTimeoutMillis, long pullIntervalMillis,
                                String selectorExpression, Class<?> serializer, Class<?> retryPolicy,
                                Class<?> messageConverter, Class<?> rebalanceStrategy,
                                long suspendCurrentQueueTimeMillis, int streamMaxLen, boolean enableMsgTrace,
                                boolean dlqMode, String dlqOriginalGroup, Class<?> targetBodyType,
                                String namespace) {
        this.type = type;
        this.consumer = consumer;
        this.topic = topic;
        this.group = group;
        this.consumeMode = consumeMode;
        this.ackMode = ackMode;
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
        this.dlqOriginalGroup = dlqOriginalGroup;
        this.targetBodyType = targetBodyType;
        this.namespace = namespace;
    }

    /**
     * 解析命名空间：若当前 namespace 为 null 或空，则使用默认命名空间。
     *
     * @param defaultNs 默认命名空间
     */
    public void resolveNamespace(String defaultNs) {
        if (namespace == null || namespace.isEmpty()) {
            namespace = defaultNs;
        }
    }

    /**
     * 返回注册项的唯一 key（topic:group，DLQ 模式加前缀）。
     *
     * @return key 字符串
     */
    public String key() {
        return (dlqMode ? "dlq:" : "") + topic + ":" + group;
    }
}
