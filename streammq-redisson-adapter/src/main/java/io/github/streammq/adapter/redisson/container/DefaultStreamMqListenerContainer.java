package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.adapter.redisson.support.MdcKeys;
import io.github.streammq.adapter.redisson.support.StreamMqKeys;
import io.github.streammq.core.StreamMqConstants;
import io.github.streammq.core.annotation.StreamMqListener;
import io.github.streammq.core.annotation.StreamMqOrderlyListener;
import io.github.streammq.core.consumer.StreamMqConsumer;
import io.github.streammq.core.consumer.StreamMqConsumerFactory;
import io.github.streammq.core.consumer.StreamMqListenerContainer;
import io.github.streammq.core.enums.Action;
import io.github.streammq.core.enums.AcknowledgeMode;
import io.github.streammq.core.exception.StreamMqBrokerException;
import io.github.streammq.core.listener.Acknowledgment;
import io.github.streammq.core.listener.OrderlyContext;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.spi.ConsumerInterceptor;
import io.github.streammq.core.spi.MessageConverter;
import io.github.streammq.core.spi.RetryPolicy;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamAddArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link StreamMqListenerContainer} 默认实现，管理所有 Listener 的生命周期与消费循环。
 *
 * <p>核心职责：
 * <ul>
 *   <li>注册并发 Listener（{@link io.github.streammq.core.listener.StreamMqListener}）</li>
 *   <li>注册手动 ACK Listener（{@link io.github.streammq.core.listener.StreamMqAckListener}）</li>
 *   <li>注册顺序 Listener（{@link io.github.streammq.core.listener.StreamMqOrderlyListener}）</li>
 *   <li>为每个 Listener 创建 Consumer 并在虚拟线程上启动消费循环</li>
 *   <li>按 {@link Action} 处理 ACK / RECONSUME_LATER（写入 retry ZSet + payload Hash + ACK 原消息）</li>
 *   <li>支持 pause / resume（不释放资源，仅暂停消费循环）</li>
 * </ul>
 *
 * <p>线程安全：注册方法与生命周期方法均线程安全；消费循环在独立线程执行。
 *
 * <p>注：本实现不依赖 Spring SmartLifecycle，由 spring-boot-starter 模块包装适配。
 * 重试队列（retry ZSet）与死信队列（DLQ）的完整调度由 {@link RetryScheduler} 实现，
 * 容器仅负责将 RECONSUME_LATER 的消息写入 retry ZSet 后立即 ACK 原消息（从 PEL 移除），
 * 避免 PEL 无限增长。后续重试次数决策与 DLQ 路由由 {@link RetryScheduler} 完成。
 *
 * <p>使用方式：starter 在创建 {@link RetryScheduler} 后，调用
 * {@link #registerRetryTargets(RetryScheduler)} 注册所有 (topic, group, maxReconsumeTimes) 目标，
 * 然后调用 {@link RetryScheduler#start()} 启动调度。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultStreamMqListenerContainer implements StreamMqListenerContainer {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamMqListenerContainer.class);

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
    /** DLQ Stream Entry 字段：进入 DLQ 的原因 */
    private static final String FIELD_DLQ_REASON = "dlqReason";
    /** DLQ Stream Entry 字段：原始消息 ID */
    private static final String FIELD_ORIGINAL_MESSAGE_ID = "originalMessageId";
    /** 默认消费者实例名后缀 */
    private static final String DEFAULT_CONSUMER_NAME_SUFFIX = "-consumer";

    private final RedissonClient redisson;
    private final StreamMqConsumerFactory consumerFactory;
    private final MessageConverter messageConverter;
    private final RetryPolicy retryPolicy;
    private final String defaultNamespace;
    private final ConcurrentMap<String, ListenerRegistration<?>> registrations = new ConcurrentHashMap<>();
    private final ExecutorService consumeExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicReference<ContainerState> state = new AtomicReference<>(ContainerState.INIT);
    private final ConcurrentMap<String, Future<?>> consumeFutures = new ConcurrentHashMap<>();
    private volatile boolean paused = false;
    /** 全局消费者拦截器列表（按 order() 升序维护，线程安全） */
    private final List<ConsumerInterceptor> consumerInterceptors = new CopyOnWriteArrayList<>();

    /**
     * 构造容器。
     *
     * @param redisson Redisson 客户端（用于写入 retry ZSet / payload Hash）
     * @param consumerFactory 消费者工厂
     * @param messageConverter 消息转换器（用于将失败 Message 转回 Stream Entry 字段写入 payload Hash）
     * @param retryPolicy 重试策略（计算下一次重试延迟）
     * @param defaultNamespace 默认命名空间（可为空字符串）
     */
    public DefaultStreamMqListenerContainer(RedissonClient redisson,
                                            StreamMqConsumerFactory consumerFactory,
                                            MessageConverter messageConverter,
                                            RetryPolicy retryPolicy,
                                            String defaultNamespace) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.consumerFactory = Objects.requireNonNull(consumerFactory, "consumerFactory");
        this.messageConverter = Objects.requireNonNull(messageConverter, "messageConverter");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
        this.defaultNamespace = defaultNamespace == null ? "" : defaultNamespace;
    }

    // ===================== 消费者拦截器 =====================

    /**
     * 添加单个消费者拦截器（按 {@link ConsumerInterceptor#order()} 升序插入）。
     *
     * @param interceptor 拦截器实例
     */
    public void addConsumerInterceptor(ConsumerInterceptor interceptor) {
        Objects.requireNonNull(interceptor, "interceptor");
        int insertIndex = 0;
        for (ConsumerInterceptor existing : consumerInterceptors) {
            if (existing.order() <= interceptor.order()) {
                insertIndex++;
            } else {
                break;
            }
        }
        consumerInterceptors.add(insertIndex, interceptor);
    }

    /**
     * 批量添加消费者拦截器。
     *
     * @param interceptors 拦截器集合
     */
    public void addConsumerInterceptors(Collection<ConsumerInterceptor> interceptors) {
        if (interceptors != null) {
            for (ConsumerInterceptor interceptor : interceptors) {
                addConsumerInterceptor(interceptor);
            }
        }
    }

    // ===================== 注册方法 =====================

    @Override
    public <T> void registerListener(io.github.streammq.core.listener.StreamMqListener<T> listener,
                                     StreamMqListener annotation) {
        Objects.requireNonNull(listener, "listener");
        Objects.requireNonNull(annotation, "annotation");
        checkBeforeStart();
        ListenerRegistration<T> reg = new ListenerRegistration<>(
            ListenerType.AUTO_ACK, listener, annotation.topic(), annotation.consumerGroup(),
            annotation.consumeMode(), annotation.acknowledgeMode(),
            annotation.maxReconsumeTimes(), annotation.namespace(),
            0, annotation.consumeTimeout(), null);
        reg.resolveNamespace(defaultNamespace);
        registrations.put(reg.key(), reg);
        LOG.info("Registered StreamMqListener: topic={}, group={}, ackMode={}",
            annotation.topic(), annotation.consumerGroup(), annotation.acknowledgeMode());
    }

    @Override
    public <T> void registerAckListener(io.github.streammq.core.listener.StreamMqAckListener<T> listener,
                                         StreamMqListener annotation) {
        Objects.requireNonNull(listener, "listener");
        Objects.requireNonNull(annotation, "annotation");
        checkBeforeStart();
        ListenerRegistration<T> reg = new ListenerRegistration<>(
            ListenerType.MANUAL_ACK, listener, annotation.topic(), annotation.consumerGroup(),
            annotation.consumeMode(), annotation.acknowledgeMode(),
            annotation.maxReconsumeTimes(), annotation.namespace(),
            0, annotation.consumeTimeout(), null);
        reg.resolveNamespace(defaultNamespace);
        registrations.put(reg.key(), reg);
        LOG.info("Registered StreamMqAckListener: topic={}, group={}, ackMode={}",
            annotation.topic(), annotation.consumerGroup(), annotation.acknowledgeMode());
    }

    @Override
    public <T> void registerOrderlyListener(io.github.streammq.core.listener.StreamMqOrderlyListener<T> listener,
                                            StreamMqOrderlyListener annotation) {
        Objects.requireNonNull(listener, "listener");
        Objects.requireNonNull(annotation, "annotation");
        checkBeforeStart();
        int shardCount = annotation.shardCount();
        long consumeTimeoutMillis = annotation.consumeTimeout();
        // 为每个 shard 创建分布式锁，保证同一 shardingKey 的消息顺序消费
        RLock[] shardLocks = createShardLocks(defaultNamespace, annotation.topic(),
            annotation.consumerGroup(), annotation.namespace(), shardCount);
        ListenerRegistration<T> reg = new ListenerRegistration<>(
            ListenerType.ORDERLY, listener, annotation.topic(), annotation.consumerGroup(),
            annotation.consumeMode(), annotation.acknowledgeMode(),
            annotation.maxReconsumeTimes(), annotation.namespace(),
            shardCount, consumeTimeoutMillis, shardLocks);
        reg.resolveNamespace(defaultNamespace);
        registrations.put(reg.key(), reg);
        LOG.info("Registered StreamMqOrderlyListener: topic={}, group={}, shardCount={}",
            annotation.topic(), annotation.consumerGroup(), annotation.shardCount());
    }

    @Override
    public Collection<ListenerMetadata> getListeners() {
        List<ListenerMetadata> list = new ArrayList<>(registrations.size());
        for (ListenerRegistration<?> reg : registrations.values()) {
            list.add(new ListenerMetadata(reg.topic, reg.group, reg.listener.getClass(), Object.class));
        }
        return Collections.unmodifiableList(list);
    }

    /**
     * 将所有已注册 Listener 的 (topic, group, maxReconsumeTimes) 注册到 {@link RetryScheduler}。
     *
     * <p>应在 {@link #start()} 之前、{@link RetryScheduler#start()} 之前调用。
     *
     * @param scheduler 重试调度器
     */
    public void registerRetryTargets(RetryScheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler");
        for (ListenerRegistration<?> reg : registrations.values()) {
            scheduler.registerRetryTarget(reg.topic, reg.group, reg.maxReconsumeTimes);
        }
        LOG.info("Registered {} retry targets to RetryScheduler", registrations.size());
    }

    // ===================== 生命周期方法 =====================

    @Override
    public void start() {
        if (!state.compareAndSet(ContainerState.INIT, ContainerState.STARTING)) {
            throw new IllegalStateException("Container already started or in invalid state: " + state.get());
        }
        LOG.info("Starting ListenerContainer with {} registration(s)", registrations.size());
        // 先设置 RUNNING 状态，再启动消费循环，避免虚拟线程在 state 仍为 STARTING 时
        // 检查 while(state==RUNNING) 为 false 导致消费循环立即退出（竞态条件）
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
        // 取消消费任务
        for (Future<?> future : consumeFutures.values()) {
            future.cancel(true);
        }
        consumeFutures.clear();
        // 关闭消费者
        consumerFactory.close();
        // 关闭消费线程池
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

    // ===================== 内部方法 =====================

    private void doStartListeners() {
        for (ListenerRegistration<?> reg : registrations.values()) {
            Future<?> future = consumeExecutor.submit(() -> consumeLoop(reg));
            consumeFutures.put(reg.key(), future);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void consumeLoop(ListenerRegistration reg) {
        StreamMqConsumer consumer;
        try {
            consumer = createConsumerFor(reg);
        } catch (RuntimeException ex) {
            LOG.error("Failed to create consumer for listener (topic={}, group={}): {}, listener will not consume",
                reg.topic, reg.group, ex.getMessage(), ex);
            return;
        }
        LOG.info("Consume loop started: topic={}, group={}, listener={}",
            reg.topic, reg.group, reg.listener.getClass().getSimpleName());
        try {
            while (state.get() == ContainerState.RUNNING) {
                if (paused) {
                    sleepQuietly(PAUSED_SLEEP_MILLIS);
                    continue;
                }
                try {
                    List<Message<?>> messages = consumer.pullBlock(DEFAULT_BATCH_SIZE, PULL_BLOCK_TIMEOUT);
                    if (messages == null || messages.isEmpty()) {
                        continue;
                    }
                    for (Message<?> message : messages) {
                        if (state.get() != ContainerState.RUNNING) {
                            break;
                        }
                        handleMessage(message, reg, consumer);
                    }
                } catch (StreamMqBrokerException ex) {
                    LOG.warn("Broker error in consume loop (topic={}, group={}): {}",
                        reg.topic, reg.group, ex.getMessage());
                    sleepQuietly(BROKER_ERROR_BACKOFF_MILLIS);
                } catch (RuntimeException ex) {
                    LOG.warn("Unexpected error in consume loop (topic={}, group={}): {}",
                        reg.topic, reg.group, ex.getMessage(), ex);
                    sleepQuietly(BROKER_ERROR_BACKOFF_MILLIS);
                }
            }
        } finally {
            LOG.info("Consume loop exited: topic={}, group={}", reg.topic, reg.group);
        }
    }

    private StreamMqConsumer createConsumerFor(ListenerRegistration<?> reg) {
        Map<String, Object> props = new HashMap<>(4);
        props.put("topic", reg.topic);
        props.put("consumer-group", reg.group);
        props.put("namespace", reg.namespace);
        return consumerFactory.createConsumer(props);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void handleMessage(Message<?> message, ListenerRegistration reg, StreamMqConsumer consumer) {
        DefaultConsumerContext ctx = new DefaultConsumerContext(message, reg, consumer);
        // 注入 MDC 结构化日志上下文
        injectConsumerMdc(message, reg);
        // 跟踪最终消费动作，用于 afterConsume 拦截器
        Action finalAction = Action.RECONSUME_LATER;
        try {
            // 执行 beforeConsume 拦截器链
            if (!applyConsumerInterceptorsBefore(message)) {
                // 拦截器中止消费，直接 ACK 跳过该消息
                finalAction = Action.SUCCESS;
                handleAction(Action.SUCCESS, message, reg, consumer);
                return;
            }
            try {
                if (reg.type == ListenerType.ORDERLY) {
                    io.github.streammq.core.listener.StreamMqOrderlyListener orderly =
                        (io.github.streammq.core.listener.StreamMqOrderlyListener) reg.listener;
                    finalAction = consumeOrderlyWithShardLock(message, reg, ctx, orderly);
                    handleAction(finalAction, message, reg, consumer);
                } else if (reg.type == ListenerType.MANUAL_ACK) {
                    io.github.streammq.core.listener.StreamMqAckListener ackListener =
                        (io.github.streammq.core.listener.StreamMqAckListener) reg.listener;
                    ackListener.onMessage(message, ctx);
                    // 手动 ACK 模式：由 listener 通过 ctx.acknowledge() 控制
                    // 若退出时未 ack，记录日志（消息留在 PEL 中，由后续 XAUTOCLAIM 补偿）
                    if (!ctx.isAcked()) {
                        LOG.debug("AckListener exited without acknowledge, message stays in PEL: messageId={}",
                            message.getMessageId());
                    }
                    finalAction = ctx.isAcked() ? Action.SUCCESS : Action.RECONSUME_LATER;
                } else {
                    io.github.streammq.core.listener.StreamMqListener listener =
                        (io.github.streammq.core.listener.StreamMqListener) reg.listener;
                    finalAction = listener.onMessage(message, ctx);
                    handleAction(finalAction, message, reg, consumer);
                }
            } catch (Exception ex) {
                LOG.warn("Listener onMessage threw exception (topic={}, group={}, messageId={}): {}",
                    reg.topic, reg.group, message.getMessageId(), ex.getMessage(), ex);
                finalAction = Action.RECONSUME_LATER;
                handleAction(finalAction, message, reg, consumer);
            }
        } finally {
            // 执行 afterConsume 拦截器链
            applyConsumerInterceptorsAfter(message, finalAction);
            // 清理 MDC 结构化日志上下文
            clearConsumerMdc();
        }
    }

    /**
     * 顺序消费：根据 shardingKey 获取 shard 级 RLock，加锁后执行消费。
     *
     * <p>同一 shardingKey 的消息始终路由到同一 shard，由同一把 RLock 串行化，
     * 保证顺序语义。不同 shard 之间可并行，提升吞吐。
     *
     * @param message 待消费消息
     * @param reg Listener 注册信息
     * @param ctx 消费上下文
     * @param orderly 顺序消费 Listener
     * @return 消费动作
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private Action consumeOrderlyWithShardLock(Message<?> message, ListenerRegistration reg,
            DefaultConsumerContext ctx, io.github.streammq.core.listener.StreamMqOrderlyListener orderly)
            throws Exception {
        // 无分片锁时直接消费
        if (reg.shardLocks == null || reg.shardCount <= 0) {
            return orderly.onMessage(message, (OrderlyContext) ctx);
        }
        // 根据 shardingKey 计算 shard 索引（null 视为空串，路由到 shard 0）
        String shardingKey = message.getShardingKey();
        if (shardingKey == null) {
            shardingKey = "";
        }
        int shardIndex = Math.abs(shardingKey.hashCode()) % reg.shardCount;
        RLock lock = reg.shardLocks[shardIndex];
        try {
            lock.lock(reg.consumeTimeoutMillis, TimeUnit.MILLISECONDS);
            return orderly.onMessage(message, (OrderlyContext) ctx);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * 为顺序消费 Listener 创建 shard 级分布式锁数组。
     *
     * @param defaultNs 默认命名空间
     * @param topic 主题
     * @param group 消费组
     * @param ns 注解指定的命名空间（可为空）
     * @param shardCount 分片数
     * @return RLock 数组，shardCount <= 0 时返回 null
     */
    private RLock[] createShardLocks(String defaultNs, String topic, String group, String ns, int shardCount) {
        if (shardCount <= 0) {
            return null;
        }
        String namespace = (ns == null || ns.isEmpty()) ? defaultNs : ns;
        RLock[] locks = new RLock[shardCount];
        for (int i = 0; i < shardCount; i++) {
            String lockKey = StreamMqKeys.shardLock(namespace, topic, group, i);
            locks[i] = redisson.getLock(lockKey);
        }
        return locks;
    }

    /**
     * 注入消费侧 MDC 结构化日志上下文。
     *
     * @param message 待消费消息
     * @param reg Listener 注册信息
     */
    private void injectConsumerMdc(Message<?> message, ListenerRegistration<?> reg) {
        MDC.put(MdcKeys.TOPIC, reg.topic);
        MDC.put(MdcKeys.CONSUMER_GROUP, reg.group);
        if (message.getMessageId() != null) {
            MDC.put(MdcKeys.MSG_ID, String.valueOf(message.getMessageId()));
        }
        if (message.getShardingKey() != null) {
            MDC.put(MdcKeys.SHARDING_KEY, message.getShardingKey());
        }
        MDC.put(MdcKeys.RECONSUME_TIMES, String.valueOf(message.getReconsumeTimes()));
    }

    /**
     * 清理消费侧 MDC 结构化日志上下文。
     */
    private void clearConsumerMdc() {
        MDC.remove(MdcKeys.TOPIC);
        MDC.remove(MdcKeys.CONSUMER_GROUP);
        MDC.remove(MdcKeys.MSG_ID);
        MDC.remove(MdcKeys.SHARDING_KEY);
        MDC.remove(MdcKeys.RECONSUME_TIMES);
    }

    /**
     * 执行 beforeConsume 拦截器链（按 order() 升序）。
     *
     * <p>拦截器异常不中断主流程，仅 LOG.warn 后继续。
     *
     * @param message 待消费消息
     * @return true 全部通过，false 任一拦截器拒绝
     */
    private boolean applyConsumerInterceptorsBefore(Message<?> message) {
        for (ConsumerInterceptor interceptor : consumerInterceptors) {
            try {
                if (!interceptor.beforeConsume(message)) {
                    LOG.debug("ConsumerInterceptor {} aborted consume: topic={}",
                        interceptor.name(), message.getTopic());
                    return false;
                }
            } catch (RuntimeException ex) {
                LOG.warn("ConsumerInterceptor {} beforeConsume threw exception: {}",
                    interceptor.name(), ex.getMessage(), ex);
            }
        }
        return true;
    }

    /**
     * 执行 afterConsume 拦截器链（按 order() 升序）。
     *
     * <p>拦截器异常不中断主流程，仅 LOG.warn 后继续。
     *
     * @param message 已消费消息
     * @param action 消费动作
     */
    private void applyConsumerInterceptorsAfter(Message<?> message, Action action) {
        for (ConsumerInterceptor interceptor : consumerInterceptors) {
            try {
                interceptor.afterConsume(message, action);
            } catch (RuntimeException ex) {
                LOG.warn("ConsumerInterceptor {} afterConsume threw exception: {}",
                    interceptor.name(), ex.getMessage(), ex);
            }
        }
    }

    private void handleAction(Action action, Message<?> message, ListenerRegistration<?> reg, StreamMqConsumer consumer) {
        MessageId messageId = message.getMessageId();
        if (messageId == null) {
            LOG.warn("Message has no messageId, cannot ack/retry: topic={}, group={}", reg.topic, reg.group);
            return;
        }
        switch (action) {
            case SUCCESS, COMMIT -> {
                try {
                    consumer.ack(messageId);
                } catch (RuntimeException ex) {
                    LOG.warn("ACK failed (messageId={}): {}", messageId, ex.getMessage(), ex);
                }
            }
            case RECONSUME_LATER, ROLLBACK -> handleReconsumeLater(message, reg, consumer, messageId);
            case SUSPEND_CURRENT_QUEUE_A_MOMENT -> {
                // 顺序消费暂停：简化为不 ACK，消息留在 PEL 中等待下次 XAUTOCLAIM
                LOG.debug("Suspend current shard (messageId={}): message stays in PEL", messageId);
            }
            default -> {
                LOG.warn("Unknown action {} for messageId={}", action, messageId);
            }
        }
    }

    /**
     * 处理 RECONSUME_LATER：将消息写入 retry ZSet + payload Hash，并 ACK 原消息（从 PEL 移除）。
     *
     * <p>流程：
     * <ol>
     *   <li>将 {@link Message} 转换回 Stream Entry 字段（{@code Map&lt;String, String&gt;}）</li>
     *   <li>计算当前重试次数（{@code message.getReconsumeTimes()}）</li>
     *   <li>调用 {@link RetryPolicy#nextRetryDelay(int, Message)} 计算下一次重试延迟</li>
     *   <li>将 Stream Entry 字段 + {@code retryCount} + {@code targetTopic} 元数据
     *       写入 payload Hash（{@code streammq:{ns}:retry:payload:{msgId}}）</li>
     *   <li>将 {@code msgId} 写入 retry ZSet（{@code streammq:{ns}:retry:{topic}:{group}}，
     *       score = now + delay）</li>
     *   <li>ACK 原消息（从 PEL 移除，避免 PEL 无限增长）</li>
     * </ol>
     *
     * <p>后续由 {@link RetryScheduler} 扫描 retry ZSet，将到期消息转投到目标 Stream 或 DLQ Stream。
     */
    private void handleReconsumeLater(Message<?> message, ListenerRegistration<?> reg,
                                       StreamMqConsumer consumer, MessageId messageId) {
        try {
            // 1. 转换为 Stream Entry 字段
            Map<String, String> fields = messageConverter.toStreamFields(message);

            // 2. 当前重试次数
            int retryCount = message.getReconsumeTimes();

            // 3. 计算下一次重试延迟
            Duration delay = retryPolicy.nextRetryDelay(retryCount, message);
            if (delay == null) {
                // 不再重试，路由到 DLQ 而非直接丢弃
                LOG.warn("RetryPolicy returned null delay, routing to DLQ " +
                        "(topic={}, group={}, messageId={}, retryCount={})",
                    reg.topic, reg.group, messageId, retryCount);
                // DLQ 路由成功才 ACK；失败则保留 PEL 等待下次 XAUTOCLAIM 重新投递
                if (routeToDlq(message, reg, messageId, RetryScheduler.DLQ_REASON_MAX_RETRY)) {
                    consumer.ack(messageId);
                } else {
                    LOG.error("DLQ routing failed, message kept in PEL for re-delivery " +
                        "(topic={}, group={}, messageId={})", reg.topic, reg.group, messageId);
                }
                return;
            }
            long nextRetryAt = System.currentTimeMillis() + delay.toMillis();

            // 4. 写入 payload Hash（Stream Entry 字段 + 调度元数据）
            String msgIdStr = messageId.getStreamEntryId();
            String payloadKey = StreamMqKeys.delayPayloadHash(reg.namespace, msgIdStr);
            Map<String, String> payload = new HashMap<>(fields.size() + 2);
            payload.putAll(fields);
            payload.put(RetryScheduler.FIELD_RETRY_COUNT, Integer.toString(retryCount));
            payload.put(RetryScheduler.FIELD_TARGET_TOPIC, reg.topic);
            RMap<String, String> payloadMap = redisson.getMap(payloadKey);
            payloadMap.putAll(payload);

            // 5. 写入 retry ZSet
            String retryKey = StreamMqKeys.retryZSet(reg.namespace, reg.topic, reg.group);
            RScoredSortedSet<String> zset = redisson.getScoredSortedSet(retryKey);
            zset.add(nextRetryAt, msgIdStr);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Message scheduled for retry: topic={}, group={}, messageId={}, " +
                        "retryCount={}, delayMs={}, nextRetryAt={}",
                    reg.topic, reg.group, messageId, retryCount, delay.toMillis(), nextRetryAt);
            }

            // 6. ACK 原消息（从 PEL 移除）
            consumer.ack(messageId);
        } catch (RuntimeException ex) {
            LOG.error("Failed to schedule retry for message (topic={}, group={}, messageId={}): {}",
                reg.topic, reg.group, messageId, ex.getMessage(), ex);
            // 失败时不 ACK，消息留在 PEL 中等待下次 XAUTOCLAIM 重新投递
        }
    }

    /**
     * 将消息路由到 DLQ Stream。
     *
     * @param message 原始消息
     * @param reg Listener 注册信息
     * @param messageId 消息 ID
     * @param reason 进入 DLQ 的原因
     * @return true 表示 DLQ 写入成功；false 表示失败，调用方不应 ACK
     */
    private boolean routeToDlq(Message<?> message, ListenerRegistration<?> reg,
                             MessageId messageId, String reason) {
        try {
            Map<String, String> fields = messageConverter.toStreamFields(message);
            fields.put(FIELD_DLQ_REASON, reason);
            fields.put(FIELD_ORIGINAL_MESSAGE_ID, messageId.getStreamEntryId());
            String dlqKey = StreamMqKeys.dlqStream(reg.namespace, reg.topic, reg.group);
            RStream<String, String> dlqStream = redisson.getStream(dlqKey);
            dlqStream.add(StreamAddArgs.entries(fields));
            LOG.info("Message routed to DLQ: topic={}, group={}, messageId={}, reason={}",
                reg.topic, reg.group, messageId, reason);
            return true;
        } catch (RuntimeException ex) {
            LOG.error("Failed to route message to DLQ (topic={}, group={}, messageId={}): {}",
                reg.topic, reg.group, messageId, ex.getMessage(), ex);
            return false;
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

    // ===================== 内部类 =====================

    /** Listener 类型 */
    private enum ListenerType {
        AUTO_ACK, MANUAL_ACK, ORDERLY
    }

    /** 容器生命周期状态 */
    public enum ContainerState {
        INIT, STARTING, RUNNING, STOPPING, STOPPED
    }

    /** Listener 注册信息 */
    private static final class ListenerRegistration<T> {
        final ListenerType type;
        final Object listener;
        final String topic;
        final String group;
        final io.github.streammq.core.enums.ConsumeMode consumeMode;
        final AcknowledgeMode ackMode;
        final int maxReconsumeTimes;
        /** 顺序消费分片数（仅 ORDERLY 类型有效，其他类型为 0） */
        final int shardCount;
        /** 单条消息消费超时（毫秒），用于 shard 锁租约时间 */
        final long consumeTimeoutMillis;
        /** 顺序消费 shard 级分布式锁数组（仅 ORDERLY 类型非 null） */
        final RLock[] shardLocks;
        String namespace;

        ListenerRegistration(ListenerType type, Object listener, String topic, String group,
                             io.github.streammq.core.enums.ConsumeMode consumeMode,
                             AcknowledgeMode ackMode, int maxReconsumeTimes, String namespace,
                             int shardCount, long consumeTimeoutMillis, RLock[] shardLocks) {
            this.type = type;
            this.listener = listener;
            this.topic = topic;
            this.group = group;
            this.consumeMode = consumeMode;
            this.ackMode = ackMode;
            this.maxReconsumeTimes = maxReconsumeTimes;
            this.namespace = namespace;
            this.shardCount = shardCount;
            this.consumeTimeoutMillis = consumeTimeoutMillis;
            this.shardLocks = shardLocks;
        }

        void resolveNamespace(String defaultNs) {
            if (namespace == null || namespace.isEmpty()) {
                namespace = defaultNs;
            }
        }

        String key() {
            return topic + ":" + group;
        }
    }

    /**
     * 默认 ConsumerContext 实现，同时实现 OrderlyContext 以支持顺序消费。
     */
    private static final class DefaultConsumerContext implements OrderlyContext {
        private final Message<?> message;
        private final ListenerRegistration<?> reg;
        private final StreamMqConsumer consumer;
        private volatile boolean acked = false;

        DefaultConsumerContext(Message<?> message, ListenerRegistration<?> reg, StreamMqConsumer consumer) {
            this.message = message;
            this.reg = reg;
            this.consumer = consumer;
        }

        boolean isAcked() {
            return acked;
        }

        @Override
        public String topic() {
            return message.getTopic();
        }

        @Override
        public String consumerGroup() {
            return reg.group;
        }

        @Override
        public String consumerName() {
            return reg.group + DEFAULT_CONSUMER_NAME_SUFFIX;
        }

        @Override
        public int reconsumeTimes() {
            return message.getReconsumeTimes();
        }

        @Override
        public long bornTimestamp() {
            return message.getBornTimestamp();
        }

        @Override
        public String bornHost() {
            return message.getBornHost();
        }

        @Override
        public Map<String, String> messageTrack() {
            return message.getProperties();
        }

        @Override
        public String ext(String key) {
            return message.getProperties().get(key);
        }

        @Override
        public AcknowledgeMode ackMode() {
            return reg.ackMode;
        }

        @Override
        public Acknowledgment acknowledge() {
            return new DefaultAcknowledgment(message, consumer, this);
        }

        @Override
        public void suspend(Duration duration) {
            LOG.debug("Suspend requested (duration={}ms, messageId={})", duration.toMillis(), message.getMessageId());
        }

        @Override
        public String shardingKey() {
            return message.getShardingKey();
        }

        @Override
        public int shardId() {
            return 0;
        }

        @Override
        public MessageId queueOffset() {
            return message.getMessageId();
        }

        @Override
        public long backlog() {
            return 0;
        }
    }

    /** 默认 Acknowledgment 实现 */
    private static final class DefaultAcknowledgment implements Acknowledgment {
        private final Message<?> message;
        private final StreamMqConsumer consumer;
        private final DefaultConsumerContext context;

        DefaultAcknowledgment(Message<?> message, StreamMqConsumer consumer, DefaultConsumerContext context) {
            this.message = message;
            this.consumer = consumer;
            this.context = context;
        }

        @Override
        public void acknowledge() {
            MessageId messageId = message.getMessageId();
            if (messageId != null) {
                consumer.ack(messageId);
                context.acked = true;
            }
        }

        @Override
        public void nack() {
            // 简化：不 ACK，消息留在 PEL 中等待 XAUTOCLAIM 补偿
            LOG.debug("nack: message stays in PEL for re-delivery (messageId={})", message.getMessageId());
        }

        @Override
        public void defer(Duration delay) {
            // 简化：不 ACK，由 p6 的 RetryScheduler 按 delay 调度重投
            LOG.debug("defer({}ms): message stays in PEL (messageId={})", delay.toMillis(), message.getMessageId());
        }
    }
}
