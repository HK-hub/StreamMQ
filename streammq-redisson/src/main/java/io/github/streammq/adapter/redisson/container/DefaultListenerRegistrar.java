/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.support.BroadcastGroupNaming;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.annotation.StreamMQDlqConsumer;
import io.github.streammq.core.broadcast.BroadcastInstanceIdResolver;
import io.github.streammq.core.consumer.DlqMessageConsumer;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.enums.ConsumeFromWhere;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.listener.DefaultListenerRegistration;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.ListenerType;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.util.BodyTypeResolver;
import io.github.streammq.core.util.StringUtils;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 监听器注册服务（God class 拆分第二轮）。
 *
 * <p><b>设计模式：</b>
 *
 * <ul>
 *   <li><b>Factory Method</b>——三类注册各自对应一个构建工厂方法 （{@link #concurrentOrOrderlyBuilder} / {@link
 *       #dlqBuilder}）， 并发与顺序共享同一装配方法（仅 type/shard 差异参数化）， 此前两段几乎相同的 30 行 Builder 链不再重复；
 *   <li><b>Template Method（轻量）</b>——统一收尾管线 {@code resolveNamespace → SPI 解析 → 入库 → 运行中动态接线} 由
 *       {@link #finalizeAndWire} 固化，三个入口只提供差异化的前半段。
 * </ul>
 *
 * <p>注册前置状态校验委托 {@link ContainerStateMachine#assertRegistrable()}； per-consumer SPI 解析委托 {@link
 * PerConsumerSpiResolver}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultListenerRegistrar implements ListenerRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(ListenerRegistrar.class);

    private final ContainerStateMachine stateMachine;
    private final RegistrationStore store;
    private final PerConsumerSpiResolver spiResolver;
    private final ConsumerTuning tuning;
    private final String defaultNamespace;
    private final String instanceToken;
    private final ConsumeFromWhere defaultConsumeFromWhere;
    private final ShardLocksFactory shardLocksFactory;
    private final java.util.function.Consumer<ListenerRegistration<?>> wireIfRunning;

    /**
     * 广播消费实例身份解析器（可为 null = 使用容器级 {@link #instanceToken}）。
     *
     * <p>非 null 时，每个 <b>广播模式</b>注册都会向它申请一个<b>跨重启稳定</b>的实例身份， 从而保证广播消费者组名 {@code
     * {group}:{group}-{instanceId}} 在重启后不变、PEL 与消费位点得以复用。
     */
    private final BroadcastInstanceIdResolver broadcastResolver;

    /** 显式配置的广播实例身份提供源（可为 null，表示交由解析器自行读取系统属性/环境变量）。 */
    private final java.util.function.Supplier<String> configuredBroadcastId;

    /** 分片锁创建抽象（容器侧委托 OrderlyShardLockManager）。 */
    interface ShardLocksFactory {
        List<Lock> create(String defaultNs, String topic, String group, String ns, int shardCount);
    }

    /**
     * 兼容构造：不启用持久化广播实例身份（沿用容器级 {@code instanceToken}）。
     *
     * @param stateMachine 生命周期状态机
     * @param store 注册表
     * @param spiResolver per-consumer SPI 解析器
     * @param tuning 拉取运行参数
     * @param defaultNamespace 默认命名空间
     * @param instanceToken 容器级实例标识
     * @param defaultConsumeFromWhere 新组起始位点
     * @param shardLocksFactory 分片锁工厂
     * @param wireIfRunning 运行中动态接线回调
     */
    public DefaultListenerRegistrar(
            ContainerStateMachine stateMachine,
            RegistrationStore store,
            PerConsumerSpiResolver spiResolver,
            ConsumerTuning tuning,
            String defaultNamespace,
            String instanceToken,
            ConsumeFromWhere defaultConsumeFromWhere,
            ShardLocksFactory shardLocksFactory,
            java.util.function.Consumer<ListenerRegistration<?>> wireIfRunning) {
        this(
                stateMachine,
                store,
                spiResolver,
                tuning,
                defaultNamespace,
                instanceToken,
                defaultConsumeFromWhere,
                shardLocksFactory,
                wireIfRunning,
                null,
                null);
    }

    /**
     * 全参构造（启用持久化广播实例身份）。
     *
     * @param stateMachine 生命周期状态机
     * @param store 注册表
     * @param spiResolver per-consumer SPI 解析器
     * @param tuning 拉取运行参数
     * @param defaultNamespace 默认命名空间
     * @param instanceToken 容器级实例标识（广播身份不可用时的回退值）
     * @param defaultConsumeFromWhere 新组起始位点
     * @param shardLocksFactory 分片锁工厂
     * @param wireIfRunning 运行中动态接线回调
     * @param broadcastResolver 广播实例身份解析器，可为 null
     * @param configuredBroadcastId 显式配置的广播实例身份提供源，可为 null
     */
    public DefaultListenerRegistrar(
            ContainerStateMachine stateMachine,
            RegistrationStore store,
            PerConsumerSpiResolver spiResolver,
            ConsumerTuning tuning,
            String defaultNamespace,
            String instanceToken,
            ConsumeFromWhere defaultConsumeFromWhere,
            ShardLocksFactory shardLocksFactory,
            java.util.function.Consumer<ListenerRegistration<?>> wireIfRunning,
            BroadcastInstanceIdResolver broadcastResolver,
            java.util.function.Supplier<String> configuredBroadcastId) {
        this.stateMachine = Objects.requireNonNull(stateMachine);
        this.store = Objects.requireNonNull(store);
        this.spiResolver = Objects.requireNonNull(spiResolver);
        this.tuning = Objects.requireNonNull(tuning);
        this.defaultNamespace = Objects.requireNonNull(defaultNamespace);
        this.instanceToken = Objects.requireNonNull(instanceToken);
        this.defaultConsumeFromWhere =
                Objects.isNull(defaultConsumeFromWhere)
                        ? ConsumeFromWhere.DEFAULT
                        : defaultConsumeFromWhere;
        this.shardLocksFactory = Objects.requireNonNull(shardLocksFactory);
        this.wireIfRunning = Objects.requireNonNull(wireIfRunning);
        this.broadcastResolver = broadcastResolver;
        this.configuredBroadcastId = configuredBroadcastId;
    }

    /**
     * 解析本注册应使用的实例标识。
     *
     * <p>仅<b>广播模式</b>走持久化身份解析器：广播语义依赖"每实例一个消费者组"， 组名不稳定会直接导致 PEL 泄漏与重启期间消息漏投。 集群模式（{@code
     * CLUSTERING}）的消费者组由 rebalance 统一分配，沿用容器级标识即可。
     */
    private String resolveInstanceToken(String namespace, String topic, String group) {
        if (broadcastResolver == null) {
            return instanceToken;
        }
        String ns = StringUtils.isEmpty(namespace) ? defaultNamespace : namespace;
        String configured = configuredBroadcastId == null ? null : configuredBroadcastId.get();
        try {
            BroadcastInstanceIdResolver.Resolution resolution =
                    broadcastResolver.resolve(ns, topic, group, configured);
            if (!resolution.isStable()) {
                LOG.warn(
                        "Broadcast instance identity for topic={}, group={} is NOT stable"
                                + " (source={}); the broadcast consumer group will change across"
                                + " restarts and restart-window messages may be missed",
                        topic,
                        group,
                        resolution.source());
            }
            return resolution.instanceId();
        } catch (RuntimeException ex) {
            LOG.warn(
                    "Failed to resolve persistent broadcast instance identity (topic={},"
                            + " group={}), falling back to container token: {}",
                    topic,
                    group,
                    ex.toString());
            return instanceToken;
        }
    }

    // ===================== 公共入口 =====================

    @Override
    public <T> void registerConcurrent(
            StreamMessageConcurrentlyConsumer<T> consumer, StreamMQConsumer annotation) {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(annotation, "annotation");
        stateMachine.assertRegistrable();
        StringUtils.requireValidTopic(annotation.topic());
        StringUtils.requireValidGroup(annotation.consumerGroup());
        StringUtils.requireValidNamespace(annotation.namespace());

        Class<?> bodyType = BodyTypeResolver.resolve(consumer);
        DefaultListenerRegistration.Builder<T> b =
                concurrentOrOrderlyBuilder(
                        ListenerType.AUTO_ACK,
                        annotation,
                        0,
                        null,
                        /* dlqMode */ annotation.dlqMode());
        finalizeAndWire(
                b.consumer(consumer).targetBodyType(bodyType).build(),
                "Registered StreamMQ Consumer: topic={}, group={}, dlqMode={}, bodyType={}",
                annotation.topic(),
                annotation.consumerGroup(),
                annotation.dlqMode(),
                bodyType);
    }

    @Override
    public <T> void registerOrderly(
            StreamMessageOrderlyConsumer<T> consumer, StreamMQConsumer annotation) {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(annotation, "annotation");
        stateMachine.assertRegistrable();
        StringUtils.requireValidTopic(annotation.topic());
        StringUtils.requireValidGroup(annotation.consumerGroup());
        StringUtils.requireValidNamespace(annotation.namespace());

        int shardCount = annotation.shardCount();
        List<Lock> shardLocks =
                shardLocksFactory.create(
                        defaultNamespace,
                        annotation.topic(),
                        annotation.consumerGroup(),
                        annotation.namespace(),
                        shardCount);
        Class<?> bodyType = BodyTypeResolver.resolve(consumer);
        DefaultListenerRegistration.Builder<T> b =
                concurrentOrOrderlyBuilder(
                        ListenerType.ORDERLY, annotation, shardCount, shardLocks, false);
        finalizeAndWire(
                b.consumer(consumer).targetBodyType(bodyType).build(),
                "Registered StreamMQ Orderly Consumer: topic={}, group={}, shardCount={},"
                        + " bodyType={}",
                annotation.topic(),
                annotation.consumerGroup(),
                shardCount,
                bodyType);
    }

    @Override
    public <T> void registerDlq(DlqMessageConsumer<T> consumer, StreamMQDlqConsumer annotation) {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(annotation, "annotation");
        stateMachine.assertRegistrable();
        StringUtils.requireValidGroup(annotation.consumerGroup());
        StringUtils.requireValidNamespace(annotation.namespace());

        String effectiveGroup = annotation.consumerGroup();
        Class<?> bodyType = BodyTypeResolver.resolve(consumer);
        DefaultListenerRegistration.Builder<T> b = dlqBuilder(annotation, effectiveGroup);
        finalizeAndWire(
                b.consumer(consumer).targetBodyType(bodyType).build(),
                "Registered StreamMQ DLQ Consumer: group={}, bodyType={}",
                effectiveGroup,
                bodyType);
    }

    // ===================== Factory Method：Builder 装配 =====================

    /** 并发 / 顺序注册的共享装配（此前两段重复的 30 行 Builder 链收敛于此）， 仅 type、分片数、分片锁与 DLQ 标志参数化。 */
    private <T> DefaultListenerRegistration.Builder<T> concurrentOrOrderlyBuilder(
            ListenerType type,
            StreamMQConsumer ann,
            int shardCount,
            List<Lock> shardLocks,
            boolean dlqMode) {
        return ListenerRegistration.<T>builder()
                .type(type)
                .topic(ann.topic())
                .group(ann.consumerGroup())
                .consumeMode(ann.consumeMode())
                .maxReconsumeTimes(tuning.effectiveMaxReconsumeTimes(ann.maxReconsumeTimes()))
                .shardCount(shardCount)
                .consumeTimeoutMillis(tuning.effectiveConsumeTimeoutMillis(ann.consumeTimeout()))
                .orderlyConsumeTimeoutMillis(
                        tuning.effectiveOrderlyConsumeTimeoutMillis(ann.orderlyConsumeTimeout()))
                .shardLocks(shardLocks)
                .pullBatchSize(tuning.effectivePullBatchSize(ann.pullBatchSize()))
                .pullBlockTimeoutMillis(tuning.defaultPullBlockTimeoutMillis())
                .pullIntervalMillis(tuning.effectivePullInterval(ann.pullInterval()))
                .selectorExpression(ann.selectorExpression())
                .serializer(ann.serializer())
                .retryPolicy(ann.retryPolicy())
                .messageConverter(ann.messageConverter())
                .rebalanceStrategy(ann.rebalanceStrategy())
                .suspendCurrentQueueTimeMillis(ann.suspendCurrentQueueTimeMillis())
                .streamMaxLen(ann.streamMaxLen())
                .consumeFromWhere(resolveConsumeFromWhere(ann.consumeFromWhere()))
                .enableMsgTrace(ann.enableMsgTrace())
                .dlqMode(dlqMode)
                .dlqFailureStrategy(DlqFailureStrategy.class)
                .consumerFilter(ann.consumerFilter())
                .selectorType(ann.selectorType())
                .namespace(ann.namespace())
                // 广播模式的实例标识必须跨重启稳定：组名 = {group}:{group}-{token}，
                // 组名漂移会导致旧组成为僵尸组（PEL 泄漏）且重启期间的消息不会被补投。
                .consumerName(
                        BroadcastGroupNaming.consumerName(
                                ann.consumerGroup(),
                                ann.consumeMode() == ConsumeMode.BROADCASTING
                                        ? resolveInstanceToken(
                                                ann.namespace(), ann.topic(), ann.consumerGroup())
                                        : instanceToken))
                .consumeThreadMin(ann.consumeThreadMin())
                .consumeThreadMax(ann.consumeThreadMax());
    }

    /** DLQ 注册装配：topic 即 group，重试/DLQ 参数固定为安全默认。 */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T> DefaultListenerRegistration.Builder<T> dlqBuilder(
            StreamMQDlqConsumer ann, String effectiveGroup) {
        return ListenerRegistration.<T>builder()
                .type(ListenerType.AUTO_ACK)
                .topic(effectiveGroup)
                .group(effectiveGroup)
                .consumeMode(ConsumeMode.CLUSTERING)
                .maxReconsumeTimes(0)
                .shardCount(0)
                .consumeTimeoutMillis(
                        tuning.effectiveConsumeTimeoutMillis(
                                StreamMQConstants.ANNOTATION_UNSET_LONG))
                .pullBatchSize(
                        tuning.effectivePullBatchSize(StreamMQConstants.ANNOTATION_UNSET_INT))
                .pullBlockTimeoutMillis(tuning.defaultPullBlockTimeoutMillis())
                .selectorExpression(StreamMQConstants.SELECTOR_WILDCARD)
                .serializer(MessageSerializer.class)
                .retryPolicy(RetryPolicy.class)
                .messageConverter(io.github.streammq.core.converter.MessageConverter.class)
                .rebalanceStrategy(RebalanceStrategy.class)
                .suspendCurrentQueueTimeMillis(
                        StreamMQConstants.DEFAULT_SUSPEND_CURRENT_QUEUE_TIME_MS)
                .streamMaxLen(StreamMQConstants.DEFAULT_STREAM_MAX_LEN)
                .enableMsgTrace(false)
                .dlqMode(true)
                .dlqFailureStrategy(ann.failureStrategy())
                .consumerFilter(new Class[0])
                .selectorType(SelectorType.TAG)
                .namespace(ann.namespace())
                .consumerName(effectiveGroup + "-" + instanceToken);
    }

    // ===================== Template Method：统一收尾 =====================

    /** 统一收尾管线（固化）：命名空间回填 → per-consumer SPI 解析 → 注册入库 → 容器运行中则动态接线 → 日志。 */
    private <T> void finalizeAndWire(
            ListenerRegistration<T> reg, String logFormat, Object... logArgs) {
        reg.resolveNamespace(defaultNamespace);
        spiResolver.resolveInto(reg, store);
        store.putRegistration(reg);
        wireIfRunning.accept(reg);
        LOG.info(logFormat, logArgs);
    }

    /**
     * 解析新消费者组起始消费位点。
     *
     * <p><b>为何仅 {@code CONSUME_FROM_FIRST} 视为显式覆盖：</b>注解枚举属性无法使用 {@code null} 哨兵，其默认值只能是某个枚举常量（这里取
     * {@code CONSUME_FROM_LAST}，与全局默认相同）， 因此「未声明」与「显式
     * CONSUME_FROM_LAST」在字节码层面无法区分。为保证「全局配置定义默认行为、用户可显式覆盖」的单一口径：
     *
     * <ul>
     *   <li>注解显式声明 {@code CONSUME_FROM_FIRST} → 用户意图明确，采用之（最高优先级）
     *   <li>其余（注解未声明 / 显式 CONSUME_FROM_LAST，二者不可区分）→ 一律采用全局配置 {@link
     *       #defaultConsumeFromWhere}（其默认同为 CONSUME_FROM_LAST）
     * </ul>
     *
     * <p>该策略下：仅当用户<a href="...">显式</a>把全局设为 {@code CONSUME_FROM_FIRST} 时，未声明注解的消费者即重放历史； 若个别消费者想强制
     * {@code CONSUME_FROM_LAST}，显式声明注解值（效果等同全局默认，无副作用）。
     */
    private ConsumeFromWhere resolveConsumeFromWhere(ConsumeFromWhere annotationValue) {
        return annotationValue == ConsumeFromWhere.CONSUME_FROM_FIRST
                ? ConsumeFromWhere.CONSUME_FROM_FIRST
                : defaultConsumeFromWhere;
    }
}
