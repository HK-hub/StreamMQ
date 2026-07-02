package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.core.StreamMqConstants;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.annotation.StreamMQDlqConsumer;
import io.github.streammq.core.annotation.StreamMQOrderlyConsumer;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.consumer.StreamMessageManualAckConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.enums.Action;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.exception.StreamMqBrokerException;
import io.github.streammq.core.listener.ListenerConfig;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.spi.ConsumerInterceptor;
import io.github.streammq.core.spi.MessageConverter;
import io.github.streammq.core.spi.RetryPolicy;
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
 *   <li>注册并发 / 手动 ACK / 顺序 / DLQ Listener</li>
 *   <li>管理容器生命周期（start / stop / pause / resume）</li>
 *   <li>为每个 Listener 启动虚拟线程消费循环</li>
 * </ul>
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
 * @author StreamMq Contributors
 * @since 0.1.0
 */
public class DefaultStreamMQListenerContainer implements StreamMQListenerContainer {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamMQListenerContainer.class);

    /** 单次 pull 批量大小 */
    private static final int DEFAULT_BATCH_SIZE = StreamMqConstants.DEFAULT_CONSUME_BATCH_SIZE;
    /** pullBlock 超时（秒），控制消费循环响应停止信号的延迟 */
    private static final Duration PULL_BLOCK_TIMEOUT = Duration.ofSeconds(1);
    /** 暂停状态下消费循环的休眠间隔（毫秒） */
    private static final long PAUSED_SLEEP_MILLIS = StreamMqConstants.DEFAULT_PAUSED_SLEEP_MS;
    /** Broker 异常后消费循环的退避休眠间隔（毫秒） */
    private static final long BROKER_ERROR_BACKOFF_MILLIS = StreamMqConstants.DEFAULT_BROKER_ERROR_BACKOFF_MS;
    /** 关闭消费线程池时的等待超时（秒） */
    private static final long AWAIT_TERMINATION_SECONDS = StreamMqConstants.DEFAULT_AWAIT_TERMINATION_SECONDS;

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
    private final ConsumerInterceptorChain interceptorChain = new ConsumerInterceptorChain();
    /** 策略类：ACK/重试/DLQ 路由 */
    private final RetryAndDlqHandler retryDlqHandler;
    /** 策略类：顺序消费分片锁 */
    private final OrderlyShardLockManager shardLockManager;

    /**
     * 构造容器。
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
        this.shardLockManager = new OrderlyShardLockManager(redisson);
        this.retryDlqHandler = new RetryAndDlqHandler(redisson, messageConverter, retryPolicy, interceptorChain);
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
        ListenerRegistration<T> reg = ListenerRegistration.<T>builder()
            .type(ListenerType.AUTO_ACK)
            .consumer(consumer)
            .topic(annotation.topic())
            .group(annotation.consumerGroup())
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
            .dlqMode(false)
            .dlqOriginalGroup(null)
            .targetBodyType(bodyType)
            .namespace(annotation.namespace())
            .build();
        reg.resolveNamespace(defaultNamespace);
        registrations.put(reg.key(), reg);
        LOG.info("Registered StreamMqConsumer: topic={}, group={}, ackMode={}, bodyType={}",
            annotation.topic(), annotation.consumerGroup(), annotation.acknowledgeMode(), bodyType);
    }

    @Override
    public <T> void registerAckConsumer(StreamMessageManualAckConsumer<T> consumer, StreamMQConsumer annotation) {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(annotation, "annotation");
        checkBeforeStart();
        Class<?> bodyType = BodyTypeResolver.resolve(consumer);
        ListenerRegistration<T> reg = ListenerRegistration.<T>builder()
            .type(ListenerType.MANUAL_ACK)
            .consumer(consumer)
            .topic(annotation.topic())
            .group(annotation.consumerGroup())
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
            .dlqMode(false)
            .dlqOriginalGroup(null)
            .targetBodyType(bodyType)
            .namespace(annotation.namespace())
            .build();
        reg.resolveNamespace(defaultNamespace);
        registrations.put(reg.key(), reg);
        LOG.info("Registered StreamMqAckConsumer: topic={}, group={}, ackMode={}, bodyType={}",
            annotation.topic(), annotation.consumerGroup(), annotation.acknowledgeMode(), bodyType);
    }

    @Override
    public <T> void registerOrderlyConsumer(StreamMessageOrderlyConsumer<T> consumer, StreamMQOrderlyConsumer annotation) {
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
        LOG.info("Registered StreamMqOrderlyConsumer: topic={}, group={}, shardCount={}, bodyType={}",
            annotation.topic(), annotation.consumerGroup(), annotation.shardCount(), bodyType);
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
     * 注册 DLQ 消费者。
     *
     * <p>DLQ 消费者从死信队列 Stream 消费消息，使用独立的消费者组名，消费失败直接 ACK 丢弃。
     *
     * @param consumer DLQ 消息处理器
     * @param annotation @StreamMqDlqConsumer 注解实例
     * @param <T> body 类型
     */
    public <T> void registerDlqConsumer(StreamMessageConcurrentlyConsumer<T> consumer, StreamMQDlqConsumer annotation) {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(annotation, "annotation");
        checkBeforeStart();
        String originalTopic = annotation.topic();
        String originalGroup = annotation.consumerGroup();
        String dlqConsumerGroup = annotation.dlqConsumerGroup() == null || annotation.dlqConsumerGroup().isEmpty()
            ? "dlq-consumer-" + originalGroup
            : annotation.dlqConsumerGroup();
        Class<?> bodyType = BodyTypeResolver.resolve(consumer);
        ListenerRegistration<T> reg = ListenerRegistration.<T>builder()
            .type(ListenerType.AUTO_ACK)
            .consumer(consumer)
            .topic(originalTopic)
            .group(dlqConsumerGroup)
            .consumeMode(ConsumeMode.CLUSTERING)
            .ackMode(annotation.acknowledgeMode())
            .maxReconsumeTimes(annotation.maxReconsumeTimes())
            .shardCount(0)
            .consumeTimeoutMillis(annotation.consumeTimeout())
            .shardLocks(null)
            .pullBatchSize(annotation.pullBatchSize())
            .pullBlockTimeoutMillis(annotation.consumeTimeout())
            .pullIntervalMillis(0L)
            .selectorExpression("*")
            .serializer(annotation.serializer())
            .retryPolicy(RetryPolicy.class)
            .messageConverter(annotation.messageConverter())
            .rebalanceStrategy(io.github.streammq.core.spi.RebalanceStrategy.class)
            .suspendCurrentQueueTimeMillis(StreamMqConstants.DEFAULT_SUSPEND_CURRENT_QUEUE_TIME_MS)
            .streamMaxLen(0)
            .enableMsgTrace(false)
            .dlqMode(true)
            .dlqOriginalGroup(originalGroup)
            .targetBodyType(bodyType)
            .namespace(annotation.namespace())
            .build();
        reg.resolveNamespace(defaultNamespace);
        registrations.put(reg.key(), reg);
        LOG.info("Registered StreamMqDlqConsumer: topic={}, originalGroup={}, dlqConsumerGroup={}, bodyType={}",
            originalTopic, originalGroup, dlqConsumerGroup, bodyType);
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
                } catch (StreamMqBrokerException ex) {
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
        Action finalAction = Action.RECONSUME_LATER;
        try {
            if (!interceptorChain.applyBefore(message)) {
                finalAction = Action.SUCCESS;
                retryDlqHandler.handleAction(Action.SUCCESS, message, reg, listener);
                return;
            }
            try {
                if (reg.getType() == ListenerType.ORDERLY) {
                    StreamMessageOrderlyConsumer orderly = (StreamMessageOrderlyConsumer) reg.getConsumer();
                    finalAction = shardLockManager.consumeWithShardLock(message, reg, ctx, orderly);
                    retryDlqHandler.handleAction(finalAction, message, reg, listener);
                } else if (reg.getType() == ListenerType.MANUAL_ACK) {
                    StreamMessageManualAckConsumer ackListener = (StreamMessageManualAckConsumer) reg.getConsumer();
                    ackListener.onMessage(message, ctx);
                    if (!ctx.isAcked()) {
                        LOG.debug("AckListener exited without acknowledge, message stays in PEL: messageId={}",
                            message.getMessageId());
                    }
                    finalAction = ctx.isAcked() ? Action.SUCCESS : Action.RECONSUME_LATER;
                } else {
                    StreamMessageConcurrentlyConsumer consumer = (StreamMessageConcurrentlyConsumer) reg.getConsumer();
                    finalAction = consumer.onMessage(message, ctx);
                    retryDlqHandler.handleAction(finalAction, message, reg, listener);
                }
            } catch (Exception ex) {
                LOG.warn("Listener onMessage threw exception (topic={}, group={}, messageId={}): {}",
                    reg.getTopic(), reg.getGroup(), message.getMessageId(), ex.getMessage(), ex);
                interceptorChain.notifyException(message, ex, InvokeTiming.EXECUTING);
                finalAction = Action.RECONSUME_LATER;
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
