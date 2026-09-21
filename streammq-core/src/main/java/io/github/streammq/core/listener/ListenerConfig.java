/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.listener;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeFromWhere;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.util.StringUtils;
import java.util.Objects;
import lombok.Builder;
import lombok.Getter;

/**
 * 监听器配置，替代弱类型 {@code Map<String, Object>} 的强类型值对象。
 *
 * <p>用于 {@link StreamMQListenerFactory#createListener(ListenerConfig)} 创建底层 {@link
 * StreamMQListener} 实例。Listener 负责从 Redis Stream 拉取消息， 然后交给业务层 {@link
 * StreamMessageConcurrentlyConsumer} 处理。
 *
 * <p>使用 Builder 模式构造：
 *
 * <pre>{@code
 * ListenerConfig config = ListenerConfig.builder()
 *     .topic("my-topic")
 *     .consumerGroup("my-group")
 *     .consumerName("consumer-1")
 *     .namespace("ns")
 *     .build();
 * }</pre>
 *
 * <p><b>与注册模型的校验策略一致（0.1.2 起）：</b>本类与 {@link
 * io.github.streammq.core.listener.DefaultListenerRegistration} 对非法数值一律 <b>fail-fast</b>（抛 {@link
 * IllegalArgumentException}），不再存在「夹取」路径——静默把非法值改成合法值会让配置错误一路带到线上。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
@Builder
public class ListenerConfig {

    /** 主题（必填） */
    private final String topic;

    /** 消费者组名（必填） */
    private final String consumerGroup;

    /** 消费者实例名（可选，默认自动生成 UUID 后缀） */
    private final String consumerName;

    /**
     * 命名空间（可选，默认空字符串表示使用全局命名空间）。
     *
     * <p>构造时由 {@link io.github.streammq.core.util.StringUtils#requireValidNamespace(String)} 校验：
     * 非空时不得含 {@code ':'}、{@code '*'}、{@code '{'}、{@code '}'}、{@code '|'}、{@code ','} 或空白字符（这些字符会破坏
     * Redis Key 结构）。
     */
    @Builder.Default private final String namespace = "";

    /** 每次拉取批量大小（可选，默认 {@link StreamMQConstants#DEFAULT_CONSUME_BATCH_SIZE}） */
    @Builder.Default private final int pullBatchSize = StreamMQConstants.DEFAULT_CONSUME_BATCH_SIZE;

    /** 阻塞拉取超时毫秒（可选，默认 {@link StreamMQConstants#DEFAULT_PULL_BLOCK_TIMEOUT_MS}） */
    @Builder.Default
    private final long pullBlockTimeoutMillis = StreamMQConstants.DEFAULT_PULL_BLOCK_TIMEOUT_MS;

    /** 拉取间隔毫秒（可选，默认 0 表示不等待） */
    @Builder.Default
    private final long pullIntervalMillis = StreamMQConstants.DEFAULT_PULL_INTERVAL_MS;

    /** 序列化器类（可选，为 null 表示使用全局配置） */
    private final Class<? extends MessageSerializer> serializer;

    /**
     * 每个消费者专属消息转换器实例（可选，为 null 表示使用工厂全局转换器）。
     *
     * <p>由容器在注册时按注解 {@code messageConverter} / {@code serializer} 实例化， 传入工厂创建的 Listener
     * 使用此转换器解码消息，实现 per-consumer 序列化策略。
     *
     * @return 消息转换器实例，可为 null
     */
    private final MessageConverter converter;

    /**
     * DLQ 模式标志（可选，默认 false）。 设置为 true 时，监听器从 DLQ Stream 消费死信消息， Stream Key 使用 {@code
     * streammq:{ns}:dlq:{consumerGroup}}（对齐 RocketMQ %DLQ%{group}）。
     *
     * @return true 表示 DLQ 模式
     */
    @Builder.Default private final boolean dlqMode = false;

    /**
     * Retry 模式标志（可选，默认 false）。 设置为 true 时，监听器从 retry Stream 消费重试消息， Stream Key 使用 {@code
     * streammq:{ns}:retry:msg:{topic}:{consumerGroup}}（对齐 RocketMQ %RETRY%{group}%）。
     *
     * @return true 表示 retry 模式
     */
    @Builder.Default private final boolean retryMode = false;

    /**
     * 目标 body 类型（跨平台反序列化回退类型）。
     *
     * <p>当 Stream Entry 中不含 {@code bodyType} 字段（发送方非 StreamMQ SDK，如 Go/Python 直接写 Redis Stream）， 或
     * {@code bodyType} 指向的类在消费端不可加载时，使用此类型作为反序列化目标类型。
     *
     * <p>通常由容器在注册 Consumer 时通过 {@code io.github.streammq.core.util.BodyTypeResolver} 解析 {@code
     * StreamMessageConcurrentlyConsumer<T>} 的泛型 T 自动填入，无需用户手动配置。
     *
     * <p>若此字段为 null 且 Stream Entry 缺失 {@code bodyType}，最终回退为 {@code String.class}，
     * 由消费者自行将字符串反序列化为目标类型（跨语言场景的推荐用法）。
     *
     * @return 目标 body 类型，可为 null
     */
    private final Class<?> targetBodyType;

    /**
     * 广播消费模式标志（可选，默认 false）。 设置为 true 时，每个消费者实例使用独立的消费者组名， 使得同一 Topic 的每条消息被所有消费者实例各自处理一次。
     *
     * @return true 表示广播模式
     */
    @Builder.Default private final boolean broadcast = false;

    @lombok.Builder.Default
    private final int maxReconsumeTimes = StreamMQConstants.DEFAULT_MAX_RECONSUME_TIMES;

    @lombok.Builder.Default
    private final long consumeTimeoutMillis = StreamMQConstants.DEFAULT_CONSUME_TIMEOUT_MS;

    /** 顺序消费单条消息消费超时（毫秒），0 表示不启用（仅对 ORDERLY 生效，默认关闭） */
    @lombok.Builder.Default private final long orderlyConsumeTimeoutMillis = 0L;

    /**
     * 并发消费循环数（0.1.2 起唯一并发度入口，默认 1）。
     *
     * <p>仅 CONCURRENT 集群消费生效；每个循环独立 XREADGROUP 拉取（共享同一 consumer name，分配互不相交）。 合法区间 {@code [1,
     * StreamMQConstants#DEFAULT_CONSUME_THREAD_MAX]}，越界抛 {@link IllegalArgumentException}。
     *
     * <p>该字段取代 0.1.0 的 {@code consumeThreadMin}/{@code consumeThreadMax} 双字段：旧命名语义与生态相反 （实际并发数 =
     * min，max 只是夹取上界），只配置 max 的用户会静默得到单循环消费。
     *
     * @since 0.1.2
     */
    @Builder.Default private final int consumeThreads = 1;

    @lombok.Builder.Default
    private final long suspendCurrentQueueTimeMillis =
            StreamMQConstants.DEFAULT_SUSPEND_CURRENT_QUEUE_TIME_MS;

    /**
     * 顺序消费分片数（默认 {@link StreamMQConstants#DEFAULT_SHARD_COUNT}）。
     *
     * <p><b>语义（0.1.2 明确）：</b>{@code > 0} 表示顺序消费的分片数（同 shardingKey 路由到同一分片串行消费）； {@code 0}
     * 表示<b>未分片</b>（不创建分片锁，单循环串行消费），消费端据此判定；{@code < 0} 非法。
     *
     * <p>此前 {@link #from(ListenerRegistration, boolean)} 会把 0 强制夹取为 1，导致注册模型的"未分片"状态在派生视图中丢失， 与消费端
     * {@code shardId() <= 0 → 0} 的判定矛盾。
     */
    @Builder.Default private final int shardCount = StreamMQConstants.DEFAULT_SHARD_COUNT;

    @lombok.Builder.Default
    private final int streamMaxLen = StreamMQConstants.DEFAULT_STREAM_MAX_LEN;

    /**
     * 新消费者组的起始消费位点（默认 {@link ConsumeFromWhere#DEFAULT}）。
     *
     * <p>仅在该 Redis 消费者组<b>首次创建</b>时生效，已存在的组不受影响。 由 {@code streammq.consumer.consume-from-where} 或
     * {@code io.github.streammq.core.annotation.StreamMQConsumer#consumeFromWhere()} 提供， 经 {@link
     * ListenerRegistration#getConsumeFromWhere()} 单点派生。
     */
    @lombok.Builder.Default
    private final ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.DEFAULT;

    @lombok.Builder.Default private final boolean enableMsgTrace = false;

    /**
     * 全参构造器：Builder（类级 {@code @Builder} 唯一定义）构建完成后经此构造器统一执行字段校验。
     *
     * <p>验证规则：
     *
     * <ul>
     *   <li>topic 不能为空且不含 {@code ':'}/{@code '*'}/{@code '{}'}/{@code '|'}/{@code ','}/空白
     *   <li>consumerGroup 同上
     *   <li>namespace 允许空串（= 使用全局命名空间），非空时同上校验
     *   <li>pullBatchSize 必须 > 0
     *   <li>consumeThreads 必须在 {@code [1, 64]} 区间内
     *   <li>maxReconsumeTimes 必须 >= 0
     *   <li>shardCount 必须 >= 0（{@code 0} 表示未分片）
     * </ul>
     *
     * <p><b>签名变更（0.1.2）：</b>原 {@code consumeThreadMin}/{@code consumeThreadMax} 两个参数已收敛为单个 {@code
     * consumeThreads}（并发消费循环数）。
     */
    public ListenerConfig(
            String topic,
            String consumerGroup,
            String consumerName,
            String namespace,
            int pullBatchSize,
            long pullBlockTimeoutMillis,
            long pullIntervalMillis,
            Class<? extends MessageSerializer> serializer,
            MessageConverter converter,
            boolean dlqMode,
            boolean retryMode,
            Class<?> targetBodyType,
            boolean broadcast,
            int maxReconsumeTimes,
            long consumeTimeoutMillis,
            long orderlyConsumeTimeoutMillis,
            int consumeThreads,
            long suspendCurrentQueueTimeMillis,
            int shardCount,
            int streamMaxLen,
            ConsumeFromWhere consumeFromWhere,
            boolean enableMsgTrace) {
        this.topic = StringUtils.requireValidTopic(topic);
        this.consumerGroup = StringUtils.requireValidGroup(consumerGroup);
        if (pullBatchSize <= 0) {
            throw new IllegalArgumentException("pullBatchSize must be > 0, got: " + pullBatchSize);
        }
        if (consumeThreads < 1 || consumeThreads > StreamMQConstants.DEFAULT_CONSUME_THREAD_MAX) {
            throw new IllegalArgumentException(
                    "consumeThreads must be in [1, "
                            + StreamMQConstants.DEFAULT_CONSUME_THREAD_MAX
                            + "], got: "
                            + consumeThreads);
        }
        if (maxReconsumeTimes < 0) {
            throw new IllegalArgumentException(
                    "maxReconsumeTimes must be >= 0, got: " + maxReconsumeTimes);
        }
        if (shardCount < 0) {
            throw new IllegalArgumentException(
                    "shardCount must be >= 0 (0 = unsharded), got: " + shardCount);
        }
        if (consumeTimeoutMillis < 0) {
            throw new IllegalArgumentException(
                    "consumeTimeoutMillis must be >= 0, got: " + consumeTimeoutMillis);
        }
        if (orderlyConsumeTimeoutMillis < 0) {
            throw new IllegalArgumentException(
                    "orderlyConsumeTimeoutMillis must be >= 0, got: "
                            + orderlyConsumeTimeoutMillis);
        }
        if (pullBlockTimeoutMillis < 0) {
            throw new IllegalArgumentException(
                    "pullBlockTimeoutMillis must be >= 0, got: " + pullBlockTimeoutMillis);
        }
        if (pullIntervalMillis < 0) {
            throw new IllegalArgumentException(
                    "pullIntervalMillis must be >= 0, got: " + pullIntervalMillis);
        }
        if (suspendCurrentQueueTimeMillis < 0) {
            throw new IllegalArgumentException(
                    "suspendCurrentQueueTimeMillis must be >= 0, got: "
                            + suspendCurrentQueueTimeMillis);
        }
        if (streamMaxLen < 0) {
            throw new IllegalArgumentException(
                    "streamMaxLen must be >= 0 (0 = unlimited), got: " + streamMaxLen);
        }

        this.consumerName = consumerName;
        this.namespace = StringUtils.requireValidNamespace(namespace);
        this.pullBatchSize = pullBatchSize;
        this.pullBlockTimeoutMillis = pullBlockTimeoutMillis;
        this.pullIntervalMillis = pullIntervalMillis;
        this.serializer = serializer;
        this.converter = converter;
        this.dlqMode = dlqMode;
        this.retryMode = retryMode;
        this.targetBodyType = targetBodyType;
        this.broadcast = broadcast;
        this.maxReconsumeTimes = maxReconsumeTimes;
        this.consumeTimeoutMillis = consumeTimeoutMillis;
        this.orderlyConsumeTimeoutMillis = orderlyConsumeTimeoutMillis;
        this.consumeThreads = consumeThreads;
        this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
        this.shardCount = shardCount;
        this.streamMaxLen = streamMaxLen;
        this.consumeFromWhere =
                Objects.isNull(consumeFromWhere) ? ConsumeFromWhere.DEFAULT : consumeFromWhere;
        this.enableMsgTrace = enableMsgTrace;
    }

    // ===================== 派生视图（0.1.0 起唯一推荐构造路径） =====================

    /**
     * 从注册模型派生监听器配置（单点映射，消除双模型字段漂移）。
     *
     * <p>0.1.0 起 {@link ListenerRegistration} 是唯一的注册持有模型；本类降级为底层 {@link StreamMQListenerFactory}
     * SPI 的派生视图——所有声明式字段一律从注册读取， 不再允许旁路构造导致两份模型各自漂移。
     *
     * <p><b>透传语义（0.1.2）：</b>{@code shardCount} 原样透传（{@code 0} 表示未分片，不再强制夹取为 1）； {@code
     * consumeThreads} 取注册模型解析后的并发消费循环数。命名空间在此路径上由构造器再次校验。
     *
     * @param reg 注册模型
     * @param retryMode 是否为 retry Stream 监听（同一注册的 original/retry 双监听复用此方法）
     * @return 派生的监听器配置
     */
    public static ListenerConfig from(ListenerRegistration<?> reg, boolean retryMode) {
        return ListenerConfig.builder()
                .topic(reg.getTopic())
                .consumerGroup(reg.getGroup())
                .consumerName(reg.getConsumerName())
                .namespace(reg.getNamespace())
                .pullBatchSize(reg.getPullBatchSize())
                .pullBlockTimeoutMillis(reg.getPullBlockTimeoutMillis())
                .pullIntervalMillis(reg.getPullIntervalMillis())
                .serializer(reg.getSerializer())
                .converter(reg.getConverterInstance())
                .dlqMode(reg.isDlqMode())
                .retryMode(retryMode)
                .targetBodyType(reg.getTargetBodyType())
                .broadcast(
                        reg.getConsumeMode()
                                == io.github.streammq.core.enums.ConsumeMode.BROADCASTING)
                .maxReconsumeTimes(reg.getMaxReconsumeTimes())
                .consumeTimeoutMillis(reg.getConsumeTimeoutMillis())
                .orderlyConsumeTimeoutMillis(reg.getOrderlyConsumeTimeoutMillis())
                .consumeThreads(reg.getConsumeThreads())
                .suspendCurrentQueueTimeMillis(reg.getSuspendCurrentQueueTimeMillis())
                .shardCount(reg.getShardCount())
                .streamMaxLen(reg.getStreamMaxLen())
                .consumeFromWhere(reg.getConsumeFromWhere())
                .enableMsgTrace(reg.isEnableMsgTrace())
                .build();
    }
}
