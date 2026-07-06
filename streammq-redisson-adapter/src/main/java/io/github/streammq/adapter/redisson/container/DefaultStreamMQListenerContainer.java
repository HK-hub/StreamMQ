package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.enums.AcknowledgeMode;
import io.github.streammq.core.enums.ConsumeAction;
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
import io.github.streammq.core.policy.OrderlyShardLockManager;
import io.github.streammq.core.policy.RetryAndDlqHandler;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.util.BodyTypeResolver;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link StreamMQListenerContainer} 默认实现，编排 Listener 的生命周期与消费循环。
 *
 * <p>核心职责（单一职责：消费循环编排）：
 * <ul>
 *   <li>注册并发 / 顺序 / DLQ Listener（统一通过 {@link StreamMQConsumer} 注解驱动）</li>
 *   <li>管理容器生命周期（start / stop / pause / resume）</li>
 *   <li>为每个 Listener 启动虚拟线程消费循环</li>
 * </ul>
 *
 * <p>手动 ACK 模式由 {@link AcknowledgeMode#MANUAL} 配置驱动，
 * 不再单独设立 Listener 类型，仍走 {@link ListenerType#AUTO_ACK} 或 {@link ListenerType#ORDERLY} 分支。
 *
 * <p>以下职责已委托给独立的策略类（组合模式）：
 * <ul>
 *   <li>{@link ConsumerInterceptorChain} - 拦截器链管理与执行</li>
 *   <li>{@link RetryAndDlqHandler} - ACK / 重试 / DLQ 路由</li>
 *   <li>{@link OrderlyShardLockManager} - 顺序消费分片锁管理</li>
 *   <li>{@link ConsumerMdcTrace} - MDC 结构化日志上下文</li>
 *   <li>{@link DefaultConsumeContextConsume} / {@link DefaultAcknowledgment} - 消费上下文与 ACK 实现</li>
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
    private final MessageConverter messageConverter;
    private final RetryPolicy retryPolicy;
    private final String defaultNamespace;

    /** Listener 注册表 */
    private final ConcurrentMap<String, ListenerRegistration<?>> registrations = new ConcurrentHashMap<>();
    /** 消费线程池（虚拟线程） */
    private final ExecutorService consumeExecutor = Executors.newVirtualThreadPerTaskExecutor();
    /** 容器状态 */
    private final AtomicReference<ContainerState> state = new AtomicReference<>(ContainerState.INIT);
    /** 消费任务 Future 表 */
    private final ConcurrentMap<String, Future<?>> consumeFutures = new ConcurrentHashMap<>();
    /** 暂停标志 */
    private volatile boolean paused = false;

    /** 策略类：拦截器链 */
    private final ConsumerInterceptorChain interceptorChain;
    /** 策略类：ACK/重试/DLQ 路由 */
    private final RetryAndDlqHandler retryDlqHandler;
    /** 策略类：顺序消费分片锁 */
    private final OrderlyShardLockManager shardLockManager;

    /**
     * 构造容器（向后兼容：内部创建默认策略实现）。
     *
     * @param redisson Redisson 客户端
     * @param consumerFactory 消费者工厂
     * @param messageConverter 消息转换器
     * @param retryPolicy 重试策略
     * @param defaultNamespace 默认命名空间（可为空字符串）
     */
    public DefaultStreamMQListenerContainer(RedissonClient redisson,
                                            StreamMQListenerFactory consumerFactory,
                                            MessageConverter messageConverter,
                                            RetryPolicy retryPolicy,
                                            String defaultNamespace) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.consumerFactory = Objects.requireNonNull(consumerFactory, "consumerFactory");
        this.messageConverter = Objects.requireNonNull(messageConverter, "messageConverter");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
        this.defaultNamespace = defaultNamespace == null ? "" : defaultNamespace;
        // 拦截器链创建一次，由容器字段与 RetryAndDlqHandler 共享同一实例
        DefaultConsumerInterceptorChain chain = new DefaultConsumerInterceptorChain();
        this.interceptorChain = chain;
        this.shardLockManager = new RedissonOrderlyShardLockManager(redisson);
        this.retryDlqHandler = new DefaultRetryAndDlqHandler(redisson, messageConverter, retryPolicy, chain);
    }

    /**
     * 构造容器并注入自定义策略实现（依赖接口而非实现）。
     *
     * <p>允许调用方提供自定义的拦截器链、ACK/重试/DLQ 路由处理器、顺序消费分片锁管理器，
     * 三个策略均面向 core 模块接口编程，便于扩展与替换。
     *
     * @param redisson Redisson 客户端
     * @param consumerFactory 消费者工厂
     * @param messageConverter 消息转换器
     * @param retryPolicy 重试策略
     * @param defaultNamespace 默认命名空间（可为空字符串）
     * @param interceptorChain 消费者拦截器链
     * @param retryDlqHandler ACK/重试/DLQ 路由处理器
     * @param shardLockManager 顺序消费分片锁管理器
     */
    public DefaultStreamMQListenerContainer(RedissonClient redisson,
                                            StreamMQListenerFactory consumerFactory,
                                            MessageConverter messageConverter,
                                            RetryPolicy retryPolicy,
                                            String defaultNamespace,
                                            ConsumerInterceptorChain interceptorChain,
                                            RetryAndDlqHandler retryDlqHandler,
                                            OrderlyShardLockManager shardLockManager) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.consumerFactory = Objects.requireNonNull(consumerFactory, "consumerFactory");
        this.messageConverter = Objects.requireNonNull(messageConverter, "messageConverter");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
        this.defaultNamespace = defaultNamespace == null ? "" : defaultNamespace;
        this.interceptorChain = Objects.requireNonNull(interceptorChain, "interceptorChain");
        this.retryDlqHandler = Objects.requireNonNull(retryDlqHandler, "retryDlqHandler");
        this.shardLockManager = Objects.requireNonNull(shardLockManager, "shardLockManager");
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
        boolean dlqMode = annotation.dlqConsumerGroup() != null && !annotation.dlqConsumerGroup().isEmpty();
        String effectiveGroup = annotation.consumerGroup();
        String dlqOriginalGroup = null;
        if (dlqMode) {
            // DLQ 模式：consumerGroup 作为原始组名，dlqConsumerGroup 作为实际消费组名
            dlqOriginalGroup = annotation.dlqOriginalGroup();
            if (dlqOriginalGroup == null || dlqOriginalGroup.isEmpty()) {
                dlqOriginalGroup = annotation.consumerGroup();
            }
            effectiveGroup = annotation.dlqConsumerGroup();
        }
        ListenerRegistration<T> reg = ListenerRegistration.<T>builder()
            .type(ListenerType.AUTO_ACK)
            .consumer(consumer)
            .topic(annotation.topic())
            .group(effectiveGroup)
            .consumeMode(annotation.consumeMode())
            .ackMode(annotation.acknowledgeMode())
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
            .dlqOriginalGroup(dlqOriginalGroup)
            .targetBodyType(bodyType)
            .namespace(annotation.namespace())
            .build();
        reg.resolveNamespace(defaultNamespace);
        registrations.put(reg.key(), reg);
        if (dlqMode) {
            LOG.info("Registered StreamMQ DLQ Consumer: topic={}, originalGroup={}, dlqConsumerGroup={}, ackMode={}, bodyType={}",
                annotation.topic(), dlqOriginalGroup, effectiveGroup, annotation.acknowledgeMode(), bodyType);
        } else {
            LOG.info("Registered StreamMQ Consumer: topic={}, group={}, ackMode={}, bodyType={}",
                annotation.topic(), effectiveGroup, annotation.acknowledgeMode(), bodyType);
        }
    }

    @Override
    public <T> void registerOrderlyConsumer(StreamMessageOrderlyConsumer<T> consumer, StreamMQConsumer annotation) {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(annotation, "annotation");
        checkBeforeStart();
        int shardCount = annotation.shardCount();
        RLock[] shardLocks = shardLockManager.createShardLocks(defaultNamespace, annotation.topic(),
            annotation.consumerGroup(), annotation.namespace(), shardCount);
        Class<?> bodyType = BodyTypeResolver.resolve(consumer);
        ListenerRegistration<T> reg = ListenerRegistration.<T>builder()
            .type(ListenerType.ORDERLY)
            .consumer(consumer)
            .topic(annotation.topic())
            .group(annotation.consumerGroup())
            .consumeMode(annotation.consumeMode())
            .ackMode(annotation.acknowledgeMode())
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
            .dlqOriginalGroup(null)
            .targetBodyType(bodyType)
            .namespace(annotation.namespace())
            .build();
        reg.resolveNamespace(defaultNamespace);
        registrations.put(reg.key(), reg);
        LOG.info("Registered StreamMQ Orderly Consumer: topic={}, group={}, shardCount={}, ackMode={}, bodyType={}",
            annotation.topic(), annotation.consumerGroup(), annotation.shardCount(),
            annotation.acknowledgeMode(), bodyType);
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
            Future<?> future = consumeExecutor.submit(() -> consumeLoop(reg));
            consumeFutures.put(reg.key(), future);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void consumeLoop(ListenerRegistration reg) {
        StreamMQListener listener;
        try {
            listener = createConsumerFor(reg);
        } catch (RuntimeException ex) {
            LOG.error("Failed to create consumer for listener (topic={}, group={}): {}, listener will not consume",
                reg.getTopic(), reg.getGroup(), ex.getMessage(), ex);
            return;
        }
        LOG.info("Consume loop started: topic={}, group={}, listener={}",
            reg.getTopic(), reg.getGroup(), reg.getConsumer().getClass().getSimpleName());
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
                        handleMessage(message, reg, listener);
                    }
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
            LOG.info("Consume loop exited: topic={}, group={}", reg.getTopic(), reg.getGroup());
        }
    }

    private StreamMQListener createConsumerFor(ListenerRegistration<?> reg) {
        ListenerConfig config = ListenerConfig.builder()
            .topic(reg.getTopic())
            .consumerGroup(reg.getGroup())
            .consumerName(reg.getGroup() + "-" + UUID.randomUUID().toString().substring(0, 8))
            .namespace(reg.getNamespace())
            .dlqMode(reg.isDlqMode())
            .dlqOriginalGroup(reg.getDlqOriginalGroup())
            .targetBodyType(reg.getTargetBodyType())
            .build();
        return consumerFactory.createListener(config);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void handleMessage(Message<?> message, ListenerRegistration reg, StreamMQListener listener) {
        DefaultConsumeContextConsume ctx = new DefaultConsumeContextConsume(message, reg, listener);
        ConsumerMdcTrace.inject(message, reg);
        ConsumeAction finalAction = ConsumeAction.RECONSUME_LATER;
        try {
            if (!interceptorChain.applyBefore(message)) {
                finalAction = ConsumeAction.SUCCESS;
                retryDlqHandler.handleAction(ConsumeAction.SUCCESS, message, reg, listener);
                return;
            }
            try {
                if (reg.getType() == ListenerType.ORDERLY) {
                    StreamMessageOrderlyConsumer orderly = (StreamMessageOrderlyConsumer) reg.getConsumer();
                    OrderlyAction orderlyAction = shardLockManager.consumeWithShardLock(message, reg, ctx, orderly);
                    if (reg.getAckMode() == AcknowledgeMode.MANUAL) {
                        // MANUAL 模式：忽略 onMessage 返回值，通过 context.acknowledge() 控制
                        if (ctx.isAcked()) {
                            retryDlqHandler.handleAction(ConsumeAction.SUCCESS, message, reg, listener);
                            finalAction = ConsumeAction.SUCCESS;
                        } else {
                            LOG.debug("Orderly MANUAL consumer exited without acknowledge, message stays in PEL: messageId={}",
                                message.getMessageId());
                            finalAction = ConsumeAction.RECONSUME_LATER;
                        }
                    } else {
                        // AUTO 模式：根据 OrderlyAction 决定
                        if (orderlyAction == OrderlyAction.SUCCESS) {
                            retryDlqHandler.handleAction(ConsumeAction.SUCCESS, message, reg, listener);
                            finalAction = ConsumeAction.SUCCESS;
                        } else {
                            // SUSPEND_CURRENT_QUEUE_A_MOMENT：不 ACK，消息留在 PEL 等待 XAUTOCLAIM
                            LOG.debug("Suspend current shard (messageId={}): message stays in PEL",
                                message.getMessageId());
                            finalAction = ConsumeAction.RECONSUME_LATER;
                        }
                    }
                } else {
                    // AUTO_ACK（并发消费）
                    StreamMessageConcurrentlyConsumer consumer = (StreamMessageConcurrentlyConsumer) reg.getConsumer();
                    ConsumeAction action = consumer.onMessage(message, ctx);
                    if (reg.getAckMode() == AcknowledgeMode.MANUAL) {
                        // MANUAL 模式：忽略 onMessage 返回值，通过 context.acknowledge() 控制
                        if (ctx.isAcked()) {
                            retryDlqHandler.handleAction(ConsumeAction.SUCCESS, message, reg, listener);
                            finalAction = ConsumeAction.SUCCESS;
                        } else {
                            LOG.debug("Concurrent MANUAL consumer exited without acknowledge, message will retry: messageId={}",
                                message.getMessageId());
                            retryDlqHandler.handleAction(ConsumeAction.RECONSUME_LATER, message, reg, listener);
                            finalAction = ConsumeAction.RECONSUME_LATER;
                        }
                    } else {
                        // AUTO 模式：根据 ConsumeAction 决定
                        retryDlqHandler.handleAction(action, message, reg, listener);
                        finalAction = action;
                    }
                }
            } catch (Exception ex) {
                LOG.warn("Listener onMessage threw exception (topic={}, group={}, messageId={}): {}",
                    reg.getTopic(), reg.getGroup(), message.getMessageId(), ex.getMessage(), ex);
                interceptorChain.notifyException(message, ex, InvokeTiming.EXECUTING);
                finalAction = ConsumeAction.RECONSUME_LATER;
                retryDlqHandler.handleAction(finalAction, message, reg, listener);
            }
        } finally {
            interceptorChain.applyAfter(message, finalAction);
            ConsumerMdcTrace.clear();
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
