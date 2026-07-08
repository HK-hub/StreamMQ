package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.scheduler.PelClaimScheduler;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.ConsumeOrderlyContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.enums.OrderlyAction;
import io.github.streammq.core.exception.StreamMQBrokerException;
import io.github.streammq.core.listener.ListenerConfig;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.interceptor.ConsumerInterceptor;
import io.github.streammq.core.interceptor.ConsumerInterceptorChain;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.ListenerType;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureHandler;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.OrderlyShardLockManager;
import io.github.streammq.core.policy.RetryAndDlqHandler;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.util.BodyTypeResolver;
import io.github.streammq.core.util.SpiResolver;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

/**
 * {@link StreamMQListenerContainer} 默认实现，编排 Listener 的生命周期与消费循环。
 *
 * <p>核心职责（单一职责：消费循环编排）：
 * <ul>
 *   <li>注册并发 / 顺序 / DLQ Listener（统一通过 {@link StreamMQConsumer} 注解驱动）</li>
 *   <li>管理容器生命周期（start / stop / pause / resume）</li>
 *   <li>为每个 Listener 启动虚拟线程消费循环</li>
 *   <li>按注解 per-consumer 实例化 {@link RetryPolicy} / {@link MessageConverter} /
 *       {@link DlqFailureHandler} / {@link MessageSerializer} / {@link RebalanceStrategy}
 *       并创建 per-consumer {@link RetryAndDlqHandler}，实现高度可配置</li>
 * </ul>
 *
 * <p>消费结果统一由 {@code onMessage} 返回值（{@link ConsumeAction} / {@link OrderlyAction}）表达，
 * 不再支持手动 ACK/nack/defer 调用，避免双模式冲突。
 *
 * <p>以下职责已委托给独立的策略类（组合模式）：
 * <ul>
 *   <li>{@link ConsumerInterceptorChain} - 拦截器链管理与执行</li>
 *   <li>{@link RetryAndDlqHandler} - ACK / 重试 / DLQ 路由（per-consumer 实例）</li>
 *   <li>{@link OrderlyShardLockManager} - 顺序消费分片锁管理</li>
 *   <li>{@link ConsumerMdcTrace} - MDC 结构化日志上下文</li>
 * </ul>
 *
 * <p>线程安全：注册方法与生命周期方法均线程安全；消费循环在独立虚拟线程执行。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultStreamMQListenerContainer implements StreamMQListenerContainer {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamMQListenerContainer.class);

    /** 单次 pull 批量大小 */
    private static final int DEFAULT_BATCH_SIZE = StreamMQConstants.DEFAULT_CONSUME_BATCH_SIZE;
    /** pullBlock 超时（秒），控制消费循环响应停止信号的延迟 */
    private static final Duration PULL_BLOCK_TIMEOUT = Duration.ofSeconds(1);
    /** 暂停状态下消费循环的休眠间隔（毫秒） */
    private static final long PAUSED_SLEEP_MILLIS = StreamMQConstants.DEFAULT_PAUSED_SLEEP_MS;
    /** Broker 异常后消费循环的退避休眠间隔（毫秒） */
    private static final long BROKER_ERROR_BACKOFF_MILLIS = StreamMQConstants.DEFAULT_BROKER_ERROR_BACKOFF_MS;
    /** 关闭消费线程池时的等待超时（秒） */
    private static final long AWAIT_TERMINATION_SECONDS = StreamMQConstants.DEFAULT_AWAIT_TERMINATION_SECONDS;

    private final RedissonClient redisson;
    private final StreamMQListenerFactory consumerFactory;
    /** 全局消息转换器（per-consumer 未指定时的回退） */
    private final MessageConverter messageConverter;
    /** 全局重试策略（per-consumer 未指定时的回退） */
    private final RetryPolicy retryPolicy;
    /** 全局死信消费失败处理器（per-consumer 未指定时的回退） */
    private final DlqFailureHandler dlqFailureHandler;
    private final String defaultNamespace;

    /** Listener 注册表 */
    private final ConcurrentMap<String, ListenerRegistration<?>> registrations = new ConcurrentHashMap<>();
    /** per-consumer ACK/重试/DLQ 路由处理器（按注解实例化的策略组合） */
    private final ConcurrentMap<String, RetryAndDlqHandler> perConsumerHandlers = new ConcurrentHashMap<>();
    /** per-consumer 消息转换器（传给 Listener 工厂用于解码） */
    private final ConcurrentMap<String, MessageConverter> perConsumerConverters = new ConcurrentHashMap<>();
    /** 消费线程池（虚拟线程） */
    private final ExecutorService consumeExecutor = Executors.newVirtualThreadPerTaskExecutor();
    /** 内部队列（背压控制：拉取与处理解耦） */
    private final int inflightCapacity;
    /** 容器状态 */
    private final AtomicReference<ContainerState> state = new AtomicReference<>(ContainerState.INIT);
    /** 消费任务 Future 表 */
    private final ConcurrentMap<String, Future<?>> consumeFutures = new ConcurrentHashMap<>();
    /** 暂停标志 */
    private volatile boolean paused = false;

    /** 策略类：拦截器链 */
    private final ConsumerInterceptorChain interceptorChain;
    /** 策略类：ACK/重试/DLQ 路由（per-consumer 关闭时的共享实例） */
    private final RetryAndDlqHandler sharedRetryDlqHandler;
    /** 策略类：顺序消费分片锁 */
    private final OrderlyShardLockManager shardLockManager;
    /** 是否启用 per-consumer 策略实例化（高级构造器注入自定义 handler 时关闭） */
    private final boolean perConsumerEnabled;
    /** 全局 DLQ 失败策略 */
    private final DlqFailureStrategy dlqFailureStrategy;
    /** 全局 DLQ 配置 */
    private final DlqConfig dlqConfig;

    /**
     * 构造容器（向后兼容：内部创建默认策略实现，per-consumer 启用）。
     */
    public DefaultStreamMQListenerContainer(RedissonClient redisson,
                                            StreamMQListenerFactory consumerFactory,
                                            MessageConverter messageConverter,
                                            RetryPolicy retryPolicy,
                                            String defaultNamespace) {
        this(redisson, consumerFactory, messageConverter, retryPolicy,
            new LogAndDropDlqFailureHandler(), new LogAndDropDlqFailureStrategy(),
            DlqConfig.builder().build(), defaultNamespace);
    }

    /**
     * 构造容器并注入全局死信消费失败处理器（per-consumer 启用）。
     */
    public DefaultStreamMQListenerContainer(RedissonClient redisson,
                                            StreamMQListenerFactory consumerFactory,
                                            MessageConverter messageConverter,
                                            RetryPolicy retryPolicy,
                                            DlqFailureHandler dlqFailureHandler,
                                            String defaultNamespace) {
        this(redisson, consumerFactory, messageConverter, retryPolicy, dlqFailureHandler,
            new LogAndDropDlqFailureStrategy(), DlqConfig.builder().build(), defaultNamespace);
    }

    /**
     * 构造容器并注入全局 DLQ 策略与配置（per-consumer 启用）。
     */
    public DefaultStreamMQListenerContainer(RedissonClient redisson,
                                            StreamMQListenerFactory consumerFactory,
                                            MessageConverter messageConverter,
                                            RetryPolicy retryPolicy,
                                            DlqFailureHandler dlqFailureHandler,
                                            DlqFailureStrategy dlqFailureStrategy,
                                            DlqConfig dlqConfig,
                                            String defaultNamespace) {
        this(redisson, consumerFactory, messageConverter, retryPolicy, dlqFailureHandler,
            dlqFailureStrategy, dlqConfig, defaultNamespace,
            StreamMQConstants.DEFAULT_INFLIGHT_CAPACITY);
    }

    /**
     * 全参构造（含背压队列容量）。
     */
    public DefaultStreamMQListenerContainer(RedissonClient redisson,
                                            StreamMQListenerFactory consumerFactory,
                                            MessageConverter messageConverter,
                                            RetryPolicy retryPolicy,
                                            DlqFailureHandler dlqFailureHandler,
                                            DlqFailureStrategy dlqFailureStrategy,
                                            DlqConfig dlqConfig,
                                            String defaultNamespace,
                                            int inflightCapacity) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.consumerFactory = Objects.requireNonNull(consumerFactory, "consumerFactory");
        this.messageConverter = Objects.requireNonNull(messageConverter, "messageConverter");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
        this.dlqFailureHandler = Objects.requireNonNull(dlqFailureHandler, "dlqFailureHandler");
        this.dlqFailureStrategy = Objects.requireNonNull(dlqFailureStrategy, "dlqFailureStrategy");
        this.dlqConfig = Objects.requireNonNull(dlqConfig, "dlqConfig");
        this.defaultNamespace = defaultNamespace == null ? "" : defaultNamespace;
        this.inflightCapacity = inflightCapacity > 0 ? inflightCapacity : StreamMQConstants.DEFAULT_INFLIGHT_CAPACITY;
        DefaultConsumerInterceptorChain chain = new DefaultConsumerInterceptorChain();
        this.interceptorChain = chain;
        this.shardLockManager = new RedissonOrderlyShardLockManager(redisson);
        this.sharedRetryDlqHandler = new DefaultRetryAndDlqHandler(
            redisson, messageConverter, retryPolicy, chain, dlqFailureHandler, dlqFailureStrategy, dlqConfig);
        this.perConsumerEnabled = true;
    }

    /**
     * 构造容器并注入自定义策略实现（依赖接口而非实现，per-consumer 关闭，使用传入的共享 handler）。
     *
     * <p>适用于需要完全自定义 ACK/重试/DLQ 路由的高级场景。此时 per-consumer 注解策略实例化关闭，
     * 所有消费者共用传入的 {@code retryDlqHandler}。
     *
     * @param redisson Redisson 客户端
     * @param consumerFactory 消费者工厂
     * @param messageConverter 全局消息转换器
     * @param retryPolicy 全局重试策略（回退）
     * @param dlqFailureHandler 全局死信失败处理器（回退）
     * @param dlqFailureStrategy 全局 DLQ 失败策略（回退）
     * @param dlqConfig 全局 DLQ 配置（回退）
     * @param defaultNamespace 默认命名空间
     * @param interceptorChain 消费者拦截器链
     * @param retryDlqHandler 共享 ACK/重试/DLQ 路由处理器
     * @param shardLockManager 顺序消费分片锁管理器
     */
    public DefaultStreamMQListenerContainer(RedissonClient redisson,
                                            StreamMQListenerFactory consumerFactory,
                                            MessageConverter messageConverter,
                                            RetryPolicy retryPolicy,
                                            DlqFailureHandler dlqFailureHandler,
                                            DlqFailureStrategy dlqFailureStrategy,
                                            DlqConfig dlqConfig,
                                            String defaultNamespace,
                                            ConsumerInterceptorChain interceptorChain,
                                            RetryAndDlqHandler retryDlqHandler,
                                            OrderlyShardLockManager shardLockManager) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.consumerFactory = Objects.requireNonNull(consumerFactory, "consumerFactory");
        this.messageConverter = Objects.requireNonNull(messageConverter, "messageConverter");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
        this.dlqFailureHandler = Objects.requireNonNull(dlqFailureHandler, "dlqFailureHandler");
        this.dlqFailureStrategy = Objects.requireNonNull(dlqFailureStrategy, "dlqFailureStrategy");
        this.dlqConfig = Objects.requireNonNull(dlqConfig, "dlqConfig");
        this.defaultNamespace = defaultNamespace == null ? "" : defaultNamespace;
        this.interceptorChain = Objects.requireNonNull(interceptorChain, "interceptorChain");
        this.sharedRetryDlqHandler = Objects.requireNonNull(retryDlqHandler, "retryDlqHandler");
        this.shardLockManager = Objects.requireNonNull(shardLockManager, "shardLockManager");
        this.perConsumerEnabled = false;
        this.inflightCapacity = StreamMQConstants.DEFAULT_INFLIGHT_CAPACITY;
    }

    // ===================== 消费者拦截器 =====================

    /**
     * 添加单个消费者拦截器。
     *
     * @param interceptor 拦截器实例
     */
    public void addConsumerInterceptor(ConsumerInterceptor interceptor) {
        interceptorChain.addInterceptor(interceptor);
    }

    /**
     * 批量添加消费者拦截器。
     *
     * @param interceptors 拦截器集合
     */
    public void addConsumerInterceptors(Collection<ConsumerInterceptor> interceptors) {
        interceptorChain.addInterceptors(interceptors);
    }

    // ===================== 注册方法 =====================

    @Override
    public <T> void registerConsumer(StreamMessageConcurrentlyConsumer<T> consumer, StreamMQConsumer annotation) {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(annotation, "annotation");
        checkBeforeStart();
        Class<?> bodyType = BodyTypeResolver.resolve(consumer);
        boolean dlqMode = annotation.dlqMode();
        String effectiveGroup = annotation.consumerGroup();
        ListenerRegistration<T> reg = ListenerRegistration.<T>builder()
            .type(ListenerType.AUTO_ACK)
            .consumer(consumer)
            .topic(annotation.topic())
            .group(effectiveGroup)
            .consumeMode(annotation.consumeMode())
            .maxReconsumeTimes(annotation.maxReconsumeTimes())
            .shardCount(0)
            .consumeTimeoutMillis(annotation.consumeTimeout())
            .shardLocks(null)
            .pullBatchSize(annotation.pullBatchSize())
            .pullBlockTimeoutMillis(annotation.consumeTimeout())
            .pullIntervalMillis(annotation.pullInterval())
            .selectorExpression(annotation.selectorExpression())
            .serializer(annotation.serializer())
            .retryPolicy(annotation.retryPolicy())
            .messageConverter(annotation.messageConverter())
            .rebalanceStrategy(annotation.rebalanceStrategy())
            .suspendCurrentQueueTimeMillis(annotation.suspendCurrentQueueTimeMillis())
            .streamMaxLen(annotation.streamMaxLen())
            .enableMsgTrace(annotation.enableMsgTrace())
            .dlqMode(dlqMode)
            .targetBodyType(bodyType)
            .dlqFailureHandler(annotation.dlqFailureHandler())
            .namespace(annotation.namespace())
            .build();
        reg.resolveNamespace(defaultNamespace);
        resolvePerConsumerSpi(reg);
        registrations.put(reg.key(), reg);
        if (dlqMode) {
            LOG.info("Registered StreamMQ DLQ Consumer: topic={}, group={}, bodyType={}",
                annotation.topic(), effectiveGroup, bodyType);
        } else {
            LOG.info("Registered StreamMQ Consumer: topic={}, group={}, bodyType={}",
                annotation.topic(), effectiveGroup, bodyType);
        }
    }

    @Override
    public <T> void registerOrderlyConsumer(StreamMessageOrderlyConsumer<T> consumer, StreamMQConsumer annotation) {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(annotation, "annotation");
        checkBeforeStart();
        int shardCount = annotation.shardCount();
        RLock[] shardLockArray = shardLockManager.createShardLocks(defaultNamespace, annotation.topic(),
            annotation.consumerGroup(), annotation.namespace(), shardCount);
        List<Lock> shardLocks = shardLockArray != null ? Arrays.asList(shardLockArray) : null;
        Class<?> bodyType = BodyTypeResolver.resolve(consumer);
        ListenerRegistration<T> reg = ListenerRegistration.<T>builder()
            .type(ListenerType.ORDERLY)
            .consumer(consumer)
            .topic(annotation.topic())
            .group(annotation.consumerGroup())
            .consumeMode(annotation.consumeMode())
            .maxReconsumeTimes(annotation.maxReconsumeTimes())
            .shardCount(shardCount)
            .consumeTimeoutMillis(annotation.consumeTimeout())
            .shardLocks(shardLocks)
            .pullBatchSize(annotation.pullBatchSize())
            .pullBlockTimeoutMillis(annotation.consumeTimeout())
            .pullIntervalMillis(annotation.pullInterval())
            .selectorExpression(annotation.selectorExpression())
            .serializer(annotation.serializer())
            .retryPolicy(annotation.retryPolicy())
            .messageConverter(annotation.messageConverter())
            .rebalanceStrategy(annotation.rebalanceStrategy())
            .suspendCurrentQueueTimeMillis(annotation.suspendCurrentQueueTimeMillis())
            .streamMaxLen(annotation.streamMaxLen())
            .enableMsgTrace(annotation.enableMsgTrace())
            .dlqMode(false)
            .targetBodyType(bodyType)
            .dlqFailureHandler(annotation.dlqFailureHandler())
            .namespace(annotation.namespace())
            .build();
        reg.resolveNamespace(defaultNamespace);
        resolvePerConsumerSpi(reg);
        registrations.put(reg.key(), reg);
        LOG.info("Registered StreamMQ Orderly Consumer: topic={}, group={}, shardCount={}, bodyType={}",
            annotation.topic(), annotation.consumerGroup(), annotation.shardCount(), bodyType);
    }

    /**
     * 按注解 per-consumer 实例化 {@link RetryPolicy} / {@link DlqFailureHandler} /
     * {@link MessageConverter} / {@link MessageSerializer}，并创建 per-consumer
     * {@link DefaultRetryAndDlqHandler}，缓存到 {@link #perConsumerHandlers} 与
     * {@link #perConsumerConverters}。
     *
     * <p>注解以 SPI 接口本身（如 {@code RetryPolicy.class}）作为"使用全局"的 marker；
     * marker 时回退到容器全局实例，否则以无参构造器实例化自定义实现。
     *
     * @param reg Listener 注册信息
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void resolvePerConsumerSpi(ListenerRegistration<?> reg) {
        if (!perConsumerEnabled) {
            return;
        }
        // 1. per-consumer 消息转换器（含 per-consumer 序列化器）
        MessageConverter converter = resolveConverter(reg);
        perConsumerConverters.put(reg.key(), converter);

        // 2. per-consumer 重试策略
        RetryPolicy policy = SpiResolver.resolveOrInstantiate(
            (Class) reg.getRetryPolicy(), RetryPolicy.class, this.retryPolicy);

        // 3. per-consumer 死信失败处理器
        DlqFailureHandler dlqHandler = SpiResolver.resolveOrInstantiate(
            reg.getDlqFailureHandler(), DlqFailureHandler.class, this.dlqFailureHandler);

        // 4. per-consumer 路由处理器
        RetryAndDlqHandler handler = new DefaultRetryAndDlqHandler(
            redisson, converter, policy, interceptorChain, dlqHandler,
            this.dlqFailureStrategy, this.dlqConfig);
        perConsumerHandlers.put(reg.key(), handler);

        // 5. per-consumer 重平衡策略（实例化校验，运行期 Rebalance 模块启用后使用）
        Class<? extends RebalanceStrategy> rebalanceClass = reg.getRebalanceStrategy();
        if (rebalanceClass != null && rebalanceClass != RebalanceStrategy.class) {
            try {
                SpiResolver.resolveOrInstantiate((Class) rebalanceClass, RebalanceStrategy.class, null);
            } catch (RuntimeException ex) {
                LOG.warn("Failed to pre-instantiate rebalanceStrategy for {} ({}): {}",
                    reg.key(), rebalanceClass.getName(), ex.getMessage());
            }
        }
        LOG.debug("Resolved per-consumer SPI: key={}, retryPolicy={}, converter={}, dlqFailureHandler={}",
            reg.key(), policy.name(), converter.getClass().getSimpleName(), dlqHandler.name());
    }

    /**
     * 解析 per-consumer 消息转换器：
     * <ul>
     *   <li>注解指定自定义 {@code messageConverter} 类 → 无参实例化</li>
     *   <li>注解指定自定义 {@code serializer} 类 → 实例化后包装为 {@link DefaultMessageConverter}</li>
     *   <li>均为 marker → 回退全局转换器</li>
     * </ul>
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private MessageConverter resolveConverter(ListenerRegistration<?> reg) {
        Class<? extends MessageConverter> converterClass = reg.getMessageConverter();
        Class<? extends MessageSerializer> serializerClass = reg.getSerializer();
        if (converterClass != null && converterClass != MessageConverter.class) {
            return SpiResolver.resolveOrInstantiate((Class) converterClass, MessageConverter.class, this.messageConverter);
        }
        if (serializerClass != null && serializerClass != MessageSerializer.class) {
            try {
                MessageSerializer<?> serializer = serializerClass.getDeclaredConstructor().newInstance();
                return new DefaultMessageConverter(serializer);
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException("Failed to instantiate serializer " + serializerClass.getName()
                    + " (requires public no-arg constructor)", e);
            }
        }
        return this.messageConverter;
    }

    @Override
    public Collection<ConsumerMetadata> getConsumers() {
        List<ConsumerMetadata> list = new ArrayList<>(registrations.size());
        for (ListenerRegistration<?> reg : registrations.values()) {
            list.add(new ConsumerMetadata(reg.getTopic(), reg.getGroup(), reg.getConsumer().getClass(),
                reg.getTargetBodyType() != null ? reg.getTargetBodyType() : Object.class));
        }
        return Collections.unmodifiableList(list);
    }

    /**
     * 将所有已注册 Listener 的 (topic, group, maxReconsumeTimes) 注册到 {@link RetryScheduler}。
     *
     * @param scheduler 重试调度器
     */
    public void registerRetryTargets(RetryScheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler");
        int count = 0;
        for (ListenerRegistration<?> reg : registrations.values()) {
            if (!reg.isDlqMode()) {
                scheduler.registerRetryTarget(reg.getTopic(), reg.getGroup(), reg.getMaxReconsumeTimes());
                count++;
            }
        }
        LOG.info("Registered {} retry targets to RetryScheduler ({} DLQ listeners skipped)",
            count, registrations.size() - count);
    }

    /**
     * 将所有已注册的顺序消费 Listener 的 (topic, group, maxReconsumeTimes) 注册到 {@link PelClaimScheduler}。
     *
     * <p>仅 ORDERLY 类型消费者需要 PEL 认领（并发消费者的重试走 retry stream）。
     *
     * @param scheduler PEL 认领调度器
     */
    public void registerPelClaimTargets(PelClaimScheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler");
        int count = 0;
        for (ListenerRegistration<?> reg : registrations.values()) {
            if (reg.getType() == ListenerType.ORDERLY && !reg.isDlqMode()) {
                scheduler.registerTarget(reg.getTopic(), reg.getGroup(), reg.getMaxReconsumeTimes());
                count++;
            }
        }
        LOG.info("Registered {} PelClaim targets ({} non-orderly skipped)",
            count, registrations.size() - count);
    }

    // ===================== 生命周期方法 =====================

    @Override
    public void start() {
        if (!state.compareAndSet(ContainerState.INIT, ContainerState.STARTING)) {
            throw new IllegalStateException("Container already started or in invalid state: " + state.get());
        }
        LOG.info("Starting ListenerContainer with {} registration(s)", registrations.size());
        state.set(ContainerState.RUNNING);
        doStartListeners();
        LOG.info("ListenerContainer started, state=RUNNING");
    }

    @Override
    public void stop() {
        ContainerState current = state.get();
        if (current == ContainerState.STOPPED || current == ContainerState.INIT) {
            return;
        }
        state.set(ContainerState.STOPPING);
        LOG.info("Stopping ListenerContainer...");
        for (Future<?> future : consumeFutures.values()) {
            future.cancel(true);
        }
        consumeFutures.clear();
        consumerFactory.close();
        consumeExecutor.shutdown();
        try {
            if (!consumeExecutor.awaitTermination(AWAIT_TERMINATION_SECONDS, TimeUnit.SECONDS)) {
                consumeExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            consumeExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        state.set(ContainerState.STOPPED);
        LOG.info("ListenerContainer stopped, state=STOPPED");
    }

    @Override
    public void pause() {
        paused = true;
        LOG.info("ListenerContainer paused");
    }

    @Override
    public void resume() {
        paused = false;
        LOG.info("ListenerContainer resumed");
    }

    @Override
    public boolean isRunning() {
        return state.get() == ContainerState.RUNNING;
    }

    /**
     * 返回容器当前状态。
     *
     * @return 状态
     */
    public ContainerState getState() {
        return state.get();
    }

    // ===================== 内部编排方法 =====================

    private void doStartListeners() {
        for (ListenerRegistration<?> reg : registrations.values()) {
            // 并发非 DLQ 消费者：双 listener（original + retry），对齐 RocketMQ 订阅 original + %RETRY%
            if (reg.getType() == ListenerType.AUTO_ACK && !reg.isDlqMode()) {
                Future<?> origFuture = consumeExecutor.submit(() -> consumeLoop(reg, false));
                consumeFutures.put(reg.key(), origFuture);
                Future<?> retryFuture = consumeExecutor.submit(() -> consumeLoop(reg, true));
                consumeFutures.put(reg.key() + ":retry", retryFuture);
            } else {
                // 顺序消费 / DLQ 消费者：单 listener
                Future<?> future = consumeExecutor.submit(() -> consumeLoop(reg, false));
                consumeFutures.put(reg.key(), future);
            }
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void consumeLoop(ListenerRegistration reg, boolean retryMode) {
        StreamMQListener listener;
        try {
            listener = createConsumerFor(reg, retryMode);
        } catch (RuntimeException ex) {
            LOG.error("Failed to create consumer for listener (topic={}, group={}, retryMode={}): {}, listener will not consume",
                reg.getTopic(), reg.getGroup(), retryMode, ex.getMessage(), ex);
            return;
        }
        LOG.info("Consume loop started: topic={}, group={}, retryMode={}, listener={}",
            reg.getTopic(), reg.getGroup(), retryMode, reg.getConsumer().getClass().getSimpleName());

        // 背压队列（inflightCapacity > 0 时启用）
        final BlockingQueue<Message<?>> inflightQueue;
        if (inflightCapacity > 0) {
            inflightQueue = new LinkedBlockingQueue<>(inflightCapacity);
            // 启动独立的处理线程
            Thread.ofVirtual()
                .name("streammq-process-" + reg.getTopic() + "-" + reg.getGroup())
                .start(() -> processFromInflightQueue(reg, listener, inflightQueue));
        } else {
            inflightQueue = null;
        }

        try {
            while (state.get() == ContainerState.RUNNING) {
                if (paused) {
                    sleepQuietly(PAUSED_SLEEP_MILLIS);
                    continue;
                }
                try {
                    List<Message<?>> messages = listener.pullBlock(reg.getPullBatchSize(),
                        Duration.ofMillis(reg.getPullBlockTimeoutMillis()));
                    if (messages == null || messages.isEmpty()) {
                        if (reg.getPullIntervalMillis() > 0) {
                            sleepQuietly(reg.getPullIntervalMillis());
                        }
                        continue;
                    }
                    for (Message<?> message : messages) {
                        if (state.get() != ContainerState.RUNNING) {
                            break;
                        }
                        if (inflightQueue != null) {
                            // 背压：队列满时阻塞等待
                            inflightQueue.put(message);
                        } else {
                            processMessage(message, reg, listener);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (StreamMQBrokerException ex) {
                    LOG.warn("Broker error in consume loop (topic={}, group={}): {}",
                        reg.getTopic(), reg.getGroup(), ex.getMessage());
                    sleepQuietly(BROKER_ERROR_BACKOFF_MILLIS);
                } catch (RuntimeException ex) {
                    LOG.warn("Unexpected error in consume loop (topic={}, group={}): {}",
                        reg.getTopic(), reg.getGroup(), ex.getMessage(), ex);
                    sleepQuietly(BROKER_ERROR_BACKOFF_MILLIS);
                }
            }
        } finally {
            LOG.info("Consume loop exited: topic={}, group={}, retryMode={}", reg.getTopic(), reg.getGroup(), retryMode);
        }
    }

    /**
     * 从背压队列中取出消息并处理（独立虚拟线程）。
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void processFromInflightQueue(ListenerRegistration reg, StreamMQListener listener,
                                           BlockingQueue<Message<?>> inflightQueue) {
        try {
            while (state.get() == ContainerState.RUNNING) {
                Message<?> message = inflightQueue.poll(1, TimeUnit.SECONDS);
                if (message == null) {
                    continue;
                }
                processMessage(message, reg, listener);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private StreamMQListener createConsumerFor(ListenerRegistration<?> reg, boolean retryMode) {
        boolean broadcast = reg.getConsumeMode() == ConsumeMode.BROADCASTING;
        ListenerConfig config = ListenerConfig.builder()
            .topic(reg.getTopic())
            .consumerGroup(reg.getGroup())
            .consumerName(reg.getGroup() + "-" + UUID.randomUUID().toString().substring(0, 8))
            .namespace(reg.getNamespace())
            .dlqMode(reg.isDlqMode())
            .retryMode(retryMode)
            .broadcast(broadcast)
            .targetBodyType(reg.getTargetBodyType())
            .converter(perConsumerEnabled ? perConsumerConverters.get(reg.key()) : null)
            .build();
        return consumerFactory.createListener(config);
    }

    /**
     * 处理单条消息：支持消费超时取消，以 {@code onMessage} 返回值为路由标准。
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void processMessage(Message<?> message, ListenerRegistration reg, StreamMQListener listener) {
        ConsumeContext ctx = new DefaultConsumeContextConsume(message, reg);
        ConsumerMdcTrace.inject(message, reg);
        ConsumeAction finalAction = ConsumeAction.RECONSUME_LATER;
        RetryAndDlqHandler handler = perConsumerEnabled
            ? perConsumerHandlers.get(reg.key()) : sharedRetryDlqHandler;
        long consumeTimeoutMs = reg.getConsumeTimeoutMillis();
        try {
            if (!interceptorChain.applyBefore(message)) {
                finalAction = ConsumeAction.SUCCESS;
                handler.handleAction(ConsumeAction.SUCCESS, message, reg, listener, null);
                return;
            }
            // 消费超时控制：使用 Future.get(timeout) 包裹 onMessage 调用
            if (consumeTimeoutMs > 0 && reg.getType() != ListenerType.ORDERLY) {
                processWithTimeout(message, reg, listener, ctx, handler);
                return;
            }
            try {
                if (reg.getType() == ListenerType.ORDERLY) {
                    StreamMessageOrderlyConsumer orderly = (StreamMessageOrderlyConsumer) reg.getConsumer();
                    OrderlyAction orderlyAction = shardLockManager.consumeWithShardLock(
                        message, reg, (ConsumeOrderlyContext) ctx, orderly);
                    if (orderlyAction == OrderlyAction.SUCCESS) {
                        handler.handleAction(ConsumeAction.SUCCESS, message, reg, listener, null);
                        finalAction = ConsumeAction.SUCCESS;
                    } else {
                        LOG.debug("Suspend current shard (messageId={}): message stays in PEL", message.getMessageId());
                        finalAction = ConsumeAction.RECONSUME_LATER;
                    }
                } else {
                    StreamMessageConcurrentlyConsumer consumer = (StreamMessageConcurrentlyConsumer) reg.getConsumer();
                    ConsumeAction action = consumer.onMessage(message, ctx);
                    if (action == null) {
                        action = ConsumeAction.RECONSUME_LATER;
                    }
                    handler.handleAction(action, message, reg, listener, null);
                    finalAction = action;
                }
            } catch (Exception ex) {
                LOG.warn("Listener onMessage threw exception (topic={}, group={}, messageId={}): {}",
                    reg.getTopic(), reg.getGroup(), message.getMessageId(), ex.getMessage(), ex);
                interceptorChain.notifyException(message, ex, InvokeTiming.EXECUTING);
                finalAction = ConsumeAction.RECONSUME_LATER;
                handler.handleAction(ConsumeAction.RECONSUME_LATER, message, reg, listener, ex);
            }
        } finally {
            interceptorChain.applyAfter(message, finalAction);
            ConsumerMdcTrace.clear();
        }
    }

    /**
     * 使用 Future.get(timeout) 包裹 onMessage 调用，超时后取消并进入重试。
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void processWithTimeout(Message<?> message, ListenerRegistration reg, StreamMQListener listener,
                                     ConsumeContext ctx, RetryAndDlqHandler handler) {
        Future<ConsumeAction> future = consumeExecutor.submit(() -> {
            StreamMessageConcurrentlyConsumer consumer = (StreamMessageConcurrentlyConsumer) reg.getConsumer();
            return consumer.onMessage(message, ctx);
        });
        try {
            ConsumeAction action = future.get(reg.getConsumeTimeoutMillis(), TimeUnit.MILLISECONDS);
            if (action == null) {
                action = ConsumeAction.RECONSUME_LATER;
            }
            handler.handleAction(action, message, reg, listener, null);
        } catch (TimeoutException e) {
            future.cancel(true);
            LOG.warn("Consume timeout ({}ms) for message, cancelling and retrying: topic={}, group={}, messageId={}",
                reg.getConsumeTimeoutMillis(), reg.getTopic(), reg.getGroup(), message.getMessageId());
            handler.handleAction(ConsumeAction.RECONSUME_LATER, message, reg, listener, e);
        } catch (ExecutionException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            handler.handleAction(ConsumeAction.RECONSUME_LATER, message, reg, listener, e);
        }
    }

    private void checkBeforeStart() {
        ContainerState current = state.get();
        if (current != ContainerState.INIT) {
            throw new IllegalStateException("Cannot register listener after container started: " + current);
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
