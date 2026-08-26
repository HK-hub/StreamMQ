/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.dlq.LogAndDropDlqFailureStrategy;
import io.github.streammq.adapter.redisson.filter.DefaultConsumerFilterChain;
import io.github.streammq.adapter.redisson.filter.SimpleSqlSelectorFilter;
import io.github.streammq.adapter.redisson.filter.SimpleTagSelectorFilter;
import io.github.streammq.adapter.redisson.handler.DefaultRetryAndDlqHandler;
import io.github.streammq.adapter.redisson.interceptor.DefaultConsumerInterceptorChain;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory;
import io.github.streammq.adapter.redisson.lock.RedissonOrderlyShardLockManager;
import io.github.streammq.adapter.redisson.manager.RedissonConsumerGroupManager;
import io.github.streammq.adapter.redisson.scheduler.PelClaimScheduler;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.annotation.StreamMQDlqConsumer;
import io.github.streammq.core.consumer.*;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.exception.StreamMQBrokerException;
import io.github.streammq.core.filter.ConsumerFilter;
import io.github.streammq.core.filter.ConsumerFilterChain;
import io.github.streammq.core.filter.ConsumerFilterResolver;
import io.github.streammq.core.filter.ExpressionSelectorFilter;
import io.github.streammq.core.interceptor.ConsumerInterceptor;
import io.github.streammq.core.interceptor.ConsumerInterceptorChain;
import io.github.streammq.core.listener.*;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.metrics.StreamMQMetrics;
import io.github.streammq.core.policy.*;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.util.BodyTypeResolver;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.SpiResolver;
import io.github.streammq.core.util.StringUtils;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link StreamMQListenerContainer} 默认实现，编排 Listener 的生命周期与消费循环。
 *
 * <p>容器保留的职责（编排层）：
 *
 * <ul>
 *   <li>注册并发 / 顺序 / DLQ Listener（统一通过 {@link StreamMQConsumer} 注解驱动）
 *   <li>管理容器生命周期（start / stop / pause / resume）
 *   <li>为每个 Listener 启动虚拟线程读循环（含并发循环数与背压队列编排）
 * </ul>
 *
 * <p>以下职责已委托给独立的协作类（组合模式，红队审查 F-02-12 God class 拆分）：
 *
 * <ul>
 *   <li>{@link RegistrationStore} - 注册表与 per-consumer 策略缓存（状态载体）
 *   <li>{@link MessageProcessor} - 单条消息消费管线（过滤器/拦截器检查、三类消费分发、超时控制、指标）
 *   <li>{@link ConsumerInterceptorChain} / {@link RetryAndDlqHandler} / {@link
 *       OrderlyShardLockManager} / {@link ConsumerMdcTrace} - 拦截器链、ACK/重试/DLQ 路由、 分片锁、MDC 日志上下文
 *   <li>{@link io.github.streammq.core.listener.ListenerConfig#from} - 底层监听器工厂 SPI 的派生视图（唯一注册模型见
 *       {@link io.github.streammq.core.listener.ListenerRegistration}）
 * </ul>
 *
 * <p>消费结果统一由 {@code onMessage} 返回值（{@link ConsumeAction}）表达。
 *
 * <p>线程安全：注册方法与生命周期方法均线程安全；消费循环在独立虚拟线程执行。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultStreamMQListenerContainer implements StreamMQListenerContainer {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultStreamMQListenerContainer.class);

    /** 单次 pull 批量大小 */
    private static final int DEFAULT_BATCH_SIZE = StreamMQConstants.DEFAULT_CONSUME_BATCH_SIZE;

    /** 暂停状态下消费循环的休眠间隔（毫秒） */
    private static final long PAUSED_SLEEP_MILLIS = StreamMQConstants.DEFAULT_PAUSED_SLEEP_MS;

    /** Broker 异常后消费循环的退避休眠间隔（毫秒） */
    private static final long BROKER_ERROR_BACKOFF_MILLIS =
            StreamMQConstants.DEFAULT_BROKER_ERROR_BACKOFF_MS;

    /** 关闭消费线程池时的等待超时（秒） */
    private static final long AWAIT_TERMINATION_SECONDS =
            StreamMQConstants.DEFAULT_AWAIT_TERMINATION_SECONDS;

    /** 消费超时取消后，等待业务线程真正终止的默认宽限期（毫秒），用于缩小与重试副本的重叠窗口 */
    private static final long DEFAULT_TIMEOUT_CANCEL_GRACE_MILLIS =
            StreamMQConstants.DEFAULT_TIMEOUT_CANCEL_GRACE_MS;

    /** 默认心跳上报间隔（毫秒） */
    private static final long DEFAULT_HEARTBEAT_INTERVAL_MS =
            StreamMQConstants.DEFAULT_HEARTBEAT_INTERVAL_MS;

    /** 默认消费者实例超时时间（毫秒） */
    private static final long DEFAULT_INSTANCE_TIMEOUT_MS =
            StreamMQConstants.DEFAULT_INSTANCE_TIMEOUT_MS;

    /** 消费 future 注册 key 的重试后缀 */
    private static final String RETRY_FUTURE_SUFFIX = ":retry";

    /** 并发消费循环的 Future 登记后缀前缀（完整 key = 基础 key + 本后缀 + 循环序号） */
    private static final String CONCURRENCY_FUTURE_SUFFIX = ":cc-";

    /** 背压处理线程的 Future 登记后缀 */
    private static final String INFLIGHT_PROCESSOR_SUFFIX = ":inflight-processor";

    /** 注册键前缀/分隔符（与 DefaultListenerRegistration.key() 保持一致） */
    private static final String DLQ_KEY_PREFIX = "dlq:";

    private static final String REG_KEY_SEPARATOR = ":";

    /** 虚拟处理线程名前缀 */
    private static final String THREAD_PROCESS_PREFIX = StreamMQConstants.THREAD_PROCESS_PREFIX;

    private final RedissonClient redisson;
    private final StreamMQListenerFactory consumerFactory;

    /** 全局消息转换器（per-consumer 未指定时的回退） */
    private final MessageConverter messageConverter;

    /** 全局重试策略（per-consumer 未指定时的回退） */
    private final RetryPolicy retryPolicy;

    /** 全局死信消费失败策略（per-consumer 未指定时的回退） */
    private final DlqFailureStrategy globalDlqFailureStrategy;

    private final String defaultNamespace;

    /** 全局消费者过滤器链 */
    private final ConsumerFilterChain consumerFilterChain = new DefaultConsumerFilterChain();

    /** 注册存储：注册表 + per-consumer 策略缓存（拆分出的状态载体） */
    private final RegistrationStore store = new RegistrationStore();

    /** 消费线程池（虚拟线程）；stop 后不可复用，restart 时在 {@link #start()} 中重建 */
    private volatile ExecutorService consumeExecutor = Executors.newVirtualThreadPerTaskExecutor();

    /** 内部队列（背压控制：拉取与处理解耦） */
    /** 背压队列容量（0=禁用背压，逐条同步处理）。启动后可通过 setter 调整（仅影响新注册的消费者）。 */
    private volatile int inflightCapacity;

    /** 容器状态 */
    private final AtomicReference<ContainerState> state =
            new AtomicReference<>(ContainerState.INIT);

    /** 消费任务 Future 表 */
    private final ConcurrentMap<String, Future<?>> consumeFutures = new ConcurrentHashMap<>();

    /** 暂停标志 */
    private volatile boolean paused = false;

    /**
     * 本容器实例的唯一标识（随机生成，容器级唯一）：广播模式消费者组名使用它区分不同容器。
     *
     * <p>注意必须是<b>容器级</b>而非进程级：同一 JVM 内可能运行多个容器实例（测试、多租户宿主），
     * 共享标识会导致它们的广播组名碰撞、消息只投递给其中一个。跨重启不保证相同——重启后产生 新组、旧组由广播组回收任务在心跳超时后清理（见
     * RedissonStreamListener#sweepStaleBroadcastGroups）。
     */
    private final String instanceToken =
            Long.toHexString(java.util.UUID.randomUUID().getMostSignificantBits() & 0xffffffffL);

    /** 消费超时取消后的宽限期（毫秒），可通过 {@link #setTimeoutCancelGraceMillis(long)} 覆盖 */
    private volatile long timeoutCancelGraceMillis = DEFAULT_TIMEOUT_CANCEL_GRACE_MILLIS;

    /** 心跳上报间隔（毫秒），可通过 {@link #setHeartbeatIntervalMs(long)} 覆盖 */
    private volatile long heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MS;

    /** 消费者实例超时时间（毫秒），可通过 {@link #setInstanceTimeoutMs(long)} 覆盖 */
    private volatile long instanceTimeoutMs = DEFAULT_INSTANCE_TIMEOUT_MS;

    /** 消费者过滤器解析器（用于从容器中获取 per-consumer 过滤器实例） */
    /** per-consumer 过滤器解析器（出厂默认反射实例化，Spring 环境可替换为容器解析） */
    private volatile ConsumerFilterResolver filterResolver =
            new io.github.streammq.adapter.redisson.filter.ReflectiveConsumerFilterResolver();

    /** 指标收集器（可选注入，用于记录消费指标，null 时为 no-op） */
    private volatile StreamMQMetrics metrics;

    /** 策略类：拦截器链 */
    private final ConsumerInterceptorChain interceptorChain;

    /**
     * 设置消费者过滤器解析器，用于从容器中获取 per-consumer 过滤器实例。
     *
     * @param filterResolver 过滤器解析器
     */
    public void setFilterResolver(ConsumerFilterResolver filterResolver) {
        this.filterResolver = filterResolver;
    }

    /**
     * 设置指标收集器，用于记录消费指标。
     *
     * @param metrics 指标收集器，可为 null（禁用指标）
     */
    public void setMetrics(StreamMQMetrics metrics) {
        this.metrics = metrics;
        messageProcessor.setMetrics(metrics);
    }

    /**
     * 设置 ACK/重试/DLQ 处理器的指标收集器。
     *
     * <p>将指标收集器传播到共享 {@link DefaultRetryAndDlqHandler} 以及所有已创建的 per-consumer 处理器；后续通过注解新注册的
     * per-consumer 处理器也会在 {@link #resolvePerConsumerSpi} 中自动注入当前指标收集器。
     *
     * @param metrics 指标收集器，可为 null
     */
    public void setHandlerMetrics(StreamMQMetrics metrics) {
        this.metrics = metrics;
        if (Objects.isNull(metrics)) {
            return;
        }
        if (sharedRetryDlqHandler instanceof DefaultRetryAndDlqHandler drh) {
            drh.setMetrics(metrics);
        }
        for (RetryAndDlqHandler handler : store.handlers()) {
            if (handler instanceof DefaultRetryAndDlqHandler drh) {
                drh.setMetrics(metrics);
            }
        }
    }

    /** 策略类：ACK/重试/DLQ 路由（per-consumer 关闭时的共享实例） */
    private final RetryAndDlqHandler sharedRetryDlqHandler;

    /** 策略类：顺序消费分片锁 */
    private final OrderlyShardLockManager shardLockManager;

    /** 策略类：单条消息消费管线（过滤器/拦截器检查、三类消费分发、超时控制、指标） */
    private final MessageProcessor messageProcessor;

    /** 是否启用 per-consumer 策略实例化（高级构造器注入自定义 handler 时关闭） */
    private final boolean perConsumerEnabled;

    /** 全局 DLQ 失败策略 */
    private final DlqFailureStrategy dlqFailureStrategy;

    /** 全局 DLQ 配置 */
    private final DlqConfig dlqConfig;

    /** 顺序消费 PEL 认领调度器（可选，注入后容器启动时注册目标） */
    private volatile PelClaimScheduler pelClaimScheduler;

    /** 消费者全局默认：单次拉取批量（注解 {@code pullBatchSize} 未显式指定时生效） */
    private volatile int defaultPullBatchSize = StreamMQConstants.DEFAULT_CONSUME_BATCH_SIZE;

    /** 消费者全局默认：拉取阻塞超时（毫秒），与注解 {@code consumeTimeout} 解耦 */
    private volatile long defaultPullBlockTimeoutMillis =
            StreamMQConstants.DEFAULT_PULL_BLOCK_TIMEOUT_MS;

    /** 消费者全局默认：拉取间隔（毫秒） */
    private volatile long defaultPullIntervalMillis = StreamMQConstants.DEFAULT_PULL_INTERVAL_MS;

    /** 单次拉取批量上界（对应 {@code streammq.consumer.max-batch-size-limit}） */
    private volatile int maxBatchSizeLimit = StreamMQConstants.MAX_BATCH_SIZE_LIMIT;

    /** 一致性哈希重平衡策略虚拟节点数（对应 {@code streammq.rebalance.virtual-nodes}） */
    private volatile int defaultVirtualNodes = StreamMQConstants.DEFAULT_VIRTUAL_NODES;

    /**
     * 注入顺序消费 PEL 认领调度器。容器启动时会将所有 ORDERLY 消费目标注册到调度器。
     *
     * @param scheduler PEL 认领调度器
     */
    public void setPelClaimScheduler(PelClaimScheduler scheduler) {
        this.pelClaimScheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    /**
     * 设置消费者全局默认单次拉取批量。
     *
     * @param batchSize 批量大小，必须 &gt; 0
     */
    public void setDefaultPullBatchSize(int batchSize) {
        if (batchSize > 0) {
            this.defaultPullBatchSize = batchSize;
        }
    }

    /**
     * 设置消费者全局默认拉取阻塞超时（毫秒）。
     *
     * @param millis 毫秒数，必须 &gt; 0
     */
    public void setDefaultPullBlockTimeoutMillis(long millis) {
        if (millis > 0) {
            this.defaultPullBlockTimeoutMillis = millis;
        }
    }

    /**
     * 设置消费者全局默认拉取间隔（毫秒）。
     *
     * @param millis 毫秒数，必须 &gt;= 0
     */
    public void setDefaultPullIntervalMillis(long millis) {
        if (millis >= 0) {
            this.defaultPullIntervalMillis = millis;
        }
    }

    /**
     * 设置单次拉取批量上界。
     *
     * @param limit 上界，必须 &gt; 0
     */
    public void setMaxBatchSizeLimit(int limit) {
        if (limit > 0) {
            this.maxBatchSizeLimit = limit;
        }
    }

    /**
     * 设置一致性哈希重平衡策略虚拟节点数。
     *
     * @param virtualNodes 虚拟节点数，必须 &gt; 0
     */
    public void setDefaultVirtualNodes(int virtualNodes) {
        if (virtualNodes > 0) {
            this.defaultVirtualNodes = virtualNodes;
        }
    }

    /**
     * 设置消费超时取消后的宽限期（毫秒）。
     *
     * @param millis 宽限期，必须 &gt; 0
     */
    public void setTimeoutCancelGraceMillis(long millis) {
        if (millis > 0) {
            this.timeoutCancelGraceMillis = millis;
        }
    }

    /**
     * 设置消费者组心跳上报间隔（毫秒）。
     *
     * @param millis 心跳间隔，必须 &gt; 0
     */
    public void setHeartbeatIntervalMs(long millis) {
        if (millis > 0) {
            this.heartbeatIntervalMs = millis;
        }
    }

    /**
     * 设置消费者实例超时时间（毫秒）。
     *
     * @param millis 实例超时，必须 &gt; 0
     */
    public void setInstanceTimeoutMs(long millis) {
        if (millis > 0) {
            this.instanceTimeoutMs = millis;
        }
    }

    /** 解析生效的拉取批量：注解显式指定时优先，否则使用全局默认，并限制在上界内。 */
    private int resolvePullBatchSize(int annotationValue) {
        int effective =
                annotationValue != StreamMQConstants.DEFAULT_CONSUME_BATCH_SIZE
                        ? annotationValue
                        : defaultPullBatchSize;
        return Math.max(1, Math.min(effective, maxBatchSizeLimit));
    }

    /** 解析生效的拉取间隔：注解显式指定时优先，否则使用全局默认。 */
    private long resolvePullInterval(long annotationValue) {
        return annotationValue != 0 ? annotationValue : defaultPullIntervalMillis;
    }

    /** 构造容器（向后兼容：内部创建默认策略实现，per-consumer 启用）。 */
    public DefaultStreamMQListenerContainer(
            RedissonClient redisson,
            StreamMQListenerFactory consumerFactory,
            MessageConverter messageConverter,
            RetryPolicy retryPolicy,
            String defaultNamespace) {
        this(
                redisson,
                consumerFactory,
                messageConverter,
                retryPolicy,
                new LogAndDropDlqFailureStrategy(),
                DlqConfig.builder().build(),
                defaultNamespace);
    }

    /** 构造容器并注入全局死信消费失败策略（per-consumer 启用）。 */
    public DefaultStreamMQListenerContainer(
            RedissonClient redisson,
            StreamMQListenerFactory consumerFactory,
            MessageConverter messageConverter,
            RetryPolicy retryPolicy,
            DlqFailureStrategy dlqFailureStrategy,
            String defaultNamespace) {
        this(
                redisson,
                consumerFactory,
                messageConverter,
                retryPolicy,
                dlqFailureStrategy,
                DlqConfig.builder().build(),
                defaultNamespace);
    }

    /** 构造容器并注入全局 DLQ 策略与配置（per-consumer 启用）。 */
    public DefaultStreamMQListenerContainer(
            RedissonClient redisson,
            StreamMQListenerFactory consumerFactory,
            MessageConverter messageConverter,
            RetryPolicy retryPolicy,
            DlqFailureStrategy dlqFailureStrategy,
            DlqConfig dlqConfig,
            String defaultNamespace) {
        this(
                redisson,
                consumerFactory,
                messageConverter,
                retryPolicy,
                dlqFailureStrategy,
                dlqConfig,
                defaultNamespace,
                StreamMQConstants.DEFAULT_INFLIGHT_CAPACITY);
    }

    /** 全参构造（含背压队列容量）。 */
    public DefaultStreamMQListenerContainer(
            RedissonClient redisson,
            StreamMQListenerFactory consumerFactory,
            MessageConverter messageConverter,
            RetryPolicy retryPolicy,
            DlqFailureStrategy dlqFailureStrategy,
            DlqConfig dlqConfig,
            String defaultNamespace,
            int inflightCapacity) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.consumerFactory = Objects.requireNonNull(consumerFactory, "consumerFactory");
        this.messageConverter = Objects.requireNonNull(messageConverter, "messageConverter");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
        this.globalDlqFailureStrategy =
                Objects.requireNonNull(dlqFailureStrategy, "dlqFailureStrategy");
        this.dlqFailureStrategy = Objects.requireNonNull(dlqFailureStrategy, "dlqFailureStrategy");
        this.dlqConfig = Objects.requireNonNull(dlqConfig, "dlqConfig");
        this.defaultNamespace = Objects.isNull(defaultNamespace) ? "" : defaultNamespace;
        this.inflightCapacity = Math.max(0, inflightCapacity);
        DefaultConsumerInterceptorChain chain = new DefaultConsumerInterceptorChain();
        this.interceptorChain = chain;
        this.shardLockManager = new RedissonOrderlyShardLockManager(redisson);
        this.sharedRetryDlqHandler =
                new DefaultRetryAndDlqHandler(
                        redisson,
                        messageConverter,
                        retryPolicy,
                        chain,
                        dlqFailureStrategy,
                        dlqConfig);
        this.perConsumerEnabled = true;
        this.messageProcessor =
                new MessageProcessor(
                        chain,
                        this.shardLockManager,
                        store,
                        this.sharedRetryDlqHandler,
                        true,
                        () -> consumeExecutor);
    }

    /**
     * 构造容器并注入自定义策略实现（依赖接口而非实现，per-consumer 关闭，使用传入的共享 handler）。
     *
     * <p>适用于需要完全自定义 ACK/重试/DLQ 路由的高级场景。此时 per-consumer 注解策略实例化关闭， 所有消费者共用传入的 {@code
     * retryDlqHandler}。
     *
     * @param redisson Redisson 客户端
     * @param consumerFactory 消费者工厂
     * @param messageConverter 全局消息转换器
     * @param retryPolicy 全局重试策略（回退）
     * @param dlqFailureStrategy 全局 DLQ 失败策略（回退）
     * @param dlqConfig 全局 DLQ 配置（回退）
     * @param defaultNamespace 默认命名空间
     * @param interceptorChain 消费者拦截器链
     * @param retryDlqHandler 共享 ACK/重试/DLQ 路由处理器
     * @param shardLockManager 顺序消费分片锁管理器
     */
    public DefaultStreamMQListenerContainer(
            RedissonClient redisson,
            StreamMQListenerFactory consumerFactory,
            MessageConverter messageConverter,
            RetryPolicy retryPolicy,
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
        this.globalDlqFailureStrategy =
                Objects.requireNonNull(dlqFailureStrategy, "dlqFailureStrategy");
        this.dlqFailureStrategy = Objects.requireNonNull(dlqFailureStrategy, "dlqFailureStrategy");
        this.dlqConfig = Objects.requireNonNull(dlqConfig, "dlqConfig");
        this.defaultNamespace = Objects.isNull(defaultNamespace) ? "" : defaultNamespace;
        this.interceptorChain = Objects.requireNonNull(interceptorChain, "interceptorChain");
        this.sharedRetryDlqHandler = Objects.requireNonNull(retryDlqHandler, "retryDlqHandler");
        this.shardLockManager = Objects.requireNonNull(shardLockManager, "shardLockManager");
        this.perConsumerEnabled = false;
        this.inflightCapacity = StreamMQConstants.DEFAULT_INFLIGHT_CAPACITY;
        this.messageProcessor =
                new MessageProcessor(
                        interceptorChain,
                        this.shardLockManager,
                        store,
                        this.sharedRetryDlqHandler,
                        false,
                        () -> consumeExecutor);
    }

    /**
     * 设置背压队列容量（{@code >0} 启用：拉取与处理解耦，队列满时拉取阻塞；{@code 0} 禁用）。
     *
     * <p>默认 {@link StreamMQConstants#DEFAULT_INFLIGHT_CAPACITY}（禁用）。可在容器启动前或
     * 运行期调整；运行期调整仅影响之后注册的消费者。
     */
    public void setInflightCapacity(int capacity) {
        this.inflightCapacity = Math.max(0, capacity);
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

    // ===================== 消息过滤器 =====================

    /**
     * 添加单个消费者过滤器（全局维度，消费前过滤）。
     *
     * @param filter 过滤器实例
     */
    public void addConsumerFilter(ConsumerFilter filter) {
        consumerFilterChain.addFilter(filter);
        rebuildConsumerFilterCache();
    }

    /**
     * 批量添加消费者过滤器（全局维度）。
     *
     * @param filters 过滤器集合
     */
    public void addConsumerFilters(Collection<ConsumerFilter> filters) {
        consumerFilterChain.addFilters(filters);
        rebuildConsumerFilterCache();
    }

    /** 重建所有已注册 listener 的消费者过滤器缓存。 当全局过滤器变更时调用，确保缓存的过滤器链包含最新的全局过滤器。 */
    private void rebuildConsumerFilterCache() {
        for (ListenerRegistration<?> reg : store.registrations()) {
            List<ConsumerFilter> filters = buildConsumerFilters(reg);
            store.putFilters(reg.key(), filters);
        }
        LOG.debug("Rebuilt consumer filter cache for {} registrations", store.registrationCount());
    } // ===================== 注册方法 =====================

    @Override
    public <T> void registerConsumer(
            StreamMessageConcurrentlyConsumer<T> consumer, StreamMQConsumer annotation) {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(annotation, "annotation");
        checkBeforeStart();
        StringUtils.requireValidTopic(annotation.topic());
        StringUtils.requireValidGroup(annotation.consumerGroup());
        StringUtils.requireValidNamespace(annotation.namespace());
        Class<?> bodyType = BodyTypeResolver.resolve(consumer);
        String effectiveGroup = annotation.consumerGroup();
        boolean isDlqMode = annotation.dlqMode();
        ListenerRegistration<T> reg =
                ListenerRegistration.<T>builder()
                        .type(ListenerType.AUTO_ACK)
                        .consumer(consumer)
                        .topic(annotation.topic())
                        .group(effectiveGroup)
                        .consumeMode(annotation.consumeMode())
                        .maxReconsumeTimes(annotation.maxReconsumeTimes())
                        .shardCount(0)
                        .consumeTimeoutMillis(annotation.consumeTimeout())
                        .shardLocks(null)
                        .pullBatchSize(resolvePullBatchSize(annotation.pullBatchSize()))
                        .pullBlockTimeoutMillis(defaultPullBlockTimeoutMillis)
                        .pullIntervalMillis(resolvePullInterval(annotation.pullInterval()))
                        .selectorExpression(annotation.selectorExpression())
                        .serializer(annotation.serializer())
                        .retryPolicy(annotation.retryPolicy())
                        .messageConverter(annotation.messageConverter())
                        .rebalanceStrategy(annotation.rebalanceStrategy())
                        .suspendCurrentQueueTimeMillis(annotation.suspendCurrentQueueTimeMillis())
                        .streamMaxLen(annotation.streamMaxLen())
                        .enableMsgTrace(annotation.enableMsgTrace())
                        .dlqMode(isDlqMode)
                        .targetBodyType(bodyType)
                        .dlqFailureStrategy(DlqFailureStrategy.class)
                        .consumerFilter(annotation.consumerFilter())
                        .selectorType(annotation.selectorType())
                        .namespace(annotation.namespace())
                        .consumerName(annotation.consumerGroup() + "-" + instanceToken)
                        .consumeThreadMin(annotation.consumeThreadMin())
                        .consumeThreadMax(annotation.consumeThreadMax())
                        .build();
        reg.resolveNamespace(defaultNamespace);
        resolvePerConsumerSpi(reg);
        store.putRegistration(reg);
        wireRegistrationIfRunning(reg);
        LOG.info(
                "Registered StreamMQ Consumer: topic={}, group={}, dlqMode={}, bodyType={}",
                annotation.topic(),
                effectiveGroup,
                isDlqMode,
                bodyType);
    }

    @Override
    public <T> void registerOrderlyConsumer(
            StreamMessageOrderlyConsumer<T> consumer, StreamMQConsumer annotation) {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(annotation, "annotation");
        checkBeforeStart();
        StringUtils.requireValidTopic(annotation.topic());
        StringUtils.requireValidGroup(annotation.consumerGroup());
        StringUtils.requireValidNamespace(annotation.namespace());
        int shardCount = annotation.shardCount();
        Lock[] shardLockArray =
                shardLockManager.createShardLocks(
                        defaultNamespace,
                        annotation.topic(),
                        annotation.consumerGroup(),
                        annotation.namespace(),
                        shardCount);
        List<Lock> shardLocks =
                Objects.nonNull(shardLockArray) ? Arrays.asList(shardLockArray) : null;
        Class<?> bodyType = BodyTypeResolver.resolve(consumer);
        ListenerRegistration<T> reg =
                ListenerRegistration.<T>builder()
                        .type(ListenerType.ORDERLY)
                        .consumer(consumer)
                        .topic(annotation.topic())
                        .group(annotation.consumerGroup())
                        .consumeMode(annotation.consumeMode())
                        .maxReconsumeTimes(annotation.maxReconsumeTimes())
                        .shardCount(shardCount)
                        .consumeTimeoutMillis(annotation.consumeTimeout())
                        .shardLocks(shardLocks)
                        .pullBatchSize(resolvePullBatchSize(annotation.pullBatchSize()))
                        .pullBlockTimeoutMillis(defaultPullBlockTimeoutMillis)
                        .pullIntervalMillis(resolvePullInterval(annotation.pullInterval()))
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
                        .dlqFailureStrategy(DlqFailureStrategy.class)
                        .consumerFilter(annotation.consumerFilter())
                        .selectorType(annotation.selectorType())
                        .namespace(annotation.namespace())
                        .consumerName(annotation.consumerGroup() + "-" + instanceToken)
                        .build();
        reg.resolveNamespace(defaultNamespace);
        resolvePerConsumerSpi(reg);
        store.putRegistration(reg);
        wireRegistrationIfRunning(reg);
        LOG.info(
                "Registered StreamMQ Orderly Consumer: topic={}, group={}, shardCount={},"
                        + " bodyType={}",
                annotation.topic(),
                annotation.consumerGroup(),
                annotation.shardCount(),
                bodyType);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> void registerDlqConsumer(
            DlqMessageConsumer<T> consumer, StreamMQDlqConsumer annotation) {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(annotation, "annotation");
        checkBeforeStart();
        StringUtils.requireValidGroup(annotation.consumerGroup());
        StringUtils.requireValidNamespace(annotation.namespace());
        String effectiveGroup = annotation.consumerGroup();
        Class<?> bodyType = BodyTypeResolver.resolve(consumer);
        ListenerRegistration<T> reg =
                ListenerRegistration.<T>builder()
                        .type(ListenerType.AUTO_ACK)
                        .consumer(consumer)
                        .topic(effectiveGroup)
                        .group(effectiveGroup)
                        .consumeMode(ConsumeMode.CLUSTERING)
                        .maxReconsumeTimes(0)
                        .shardCount(0)
                        .consumeTimeoutMillis(StreamMQConstants.DEFAULT_CONSUME_TIMEOUT_MS)
                        .shardLocks(null)
                        .pullBatchSize(
                                resolvePullBatchSize(StreamMQConstants.DEFAULT_CONSUME_BATCH_SIZE))
                        .pullBlockTimeoutMillis(defaultPullBlockTimeoutMillis)
                        .pullIntervalMillis(0L)
                        .selectorExpression(StreamMQConstants.SELECTOR_WILDCARD)
                        .serializer(MessageSerializer.class)
                        .retryPolicy(RetryPolicy.class)
                        .messageConverter(MessageConverter.class)
                        .rebalanceStrategy(RebalanceStrategy.class)
                        .suspendCurrentQueueTimeMillis(
                                StreamMQConstants.DEFAULT_SUSPEND_CURRENT_QUEUE_TIME_MS)
                        .streamMaxLen(StreamMQConstants.DEFAULT_STREAM_MAX_LEN)
                        .enableMsgTrace(false)
                        .dlqMode(true)
                        .targetBodyType(bodyType)
                        .dlqFailureStrategy(annotation.failureStrategy())
                        .consumerFilter(new Class[0])
                        .selectorType(SelectorType.TAG)
                        .namespace(annotation.namespace())
                        .consumerName(effectiveGroup + "-" + instanceToken)
                        .build();
        reg.resolveNamespace(defaultNamespace);
        resolvePerConsumerSpi(reg);
        store.putRegistration(reg);
        wireRegistrationIfRunning(reg);
        LOG.info(
                "Registered StreamMQ DLQ Consumer: group={}, bodyType={}",
                effectiveGroup,
                bodyType);
    }

    /**
     * 按注解 per-consumer 实例化 {@link RetryPolicy} / {@link DlqFailureStrategy} / {@link
     * MessageConverter} / {@link MessageSerializer}，并创建 per-consumer {@link
     * DefaultRetryAndDlqHandler}，缓存到 {@link #perConsumerHandlers}，转换器实例直接回填注册模型。
     *
     * <p>注解以 SPI 接口本身（如 {@code RetryPolicy.class}）作为"使用全局"的 marker； marker
     * 时回退到容器全局实例，否则以无参构造器实例化自定义实现。
     *
     * @param reg Listener 注册信息
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void resolvePerConsumerSpi(ListenerRegistration<?> reg) {
        if (!perConsumerEnabled) {
            return;
        }
        // 1. per-consumer 消息转换器（含 per-consumer 序列化器）——直接回填到注册模型
        MessageConverter converter = resolveConverter(reg);
        reg.setConverterInstance(converter);

        // 2. per-consumer 重试策略
        RetryPolicy policy =
                SpiResolver.resolveOrInstantiate(
                        reg.getRetryPolicy(), RetryPolicy.class, this.retryPolicy);

        // 3. per-consumer 死信失败策略
        DlqFailureStrategy dlqStrategy =
                SpiResolver.resolveOrInstantiate(
                        reg.getDlqFailureStrategy(),
                        DlqFailureStrategy.class,
                        this.globalDlqFailureStrategy);

        // 4. per-consumer 路由处理器
        RetryAndDlqHandler handler =
                new DefaultRetryAndDlqHandler(
                        redisson, converter, policy, interceptorChain, dlqStrategy, this.dlqConfig);
        if (Objects.nonNull(this.metrics) && handler instanceof DefaultRetryAndDlqHandler drh) {
            drh.setMetrics(this.metrics);
        }
        store.putHandler(reg.key(), handler);

        // 5. per-consumer 重平衡策略（实例化校验，运行期 Rebalance 模块启用后使用）
        Class<? extends RebalanceStrategy> rebalanceClass = reg.getRebalanceStrategy();
        if (Objects.nonNull(rebalanceClass) && rebalanceClass != RebalanceStrategy.class) {
            try {
                SpiResolver.resolveOrInstantiate(
                        (Class) rebalanceClass, RebalanceStrategy.class, null);
            } catch (RuntimeException ex) {
                LOG.warn(
                        "Failed to pre-instantiate rebalanceStrategy for {} ({}): {}",
                        reg.key(),
                        rebalanceClass.getName(),
                        ex.getMessage());
            }
        }

        // 6. per-consumer 过滤器链（预构建并缓存，避免每次消息处理时重复创建）
        List<ConsumerFilter> filters = buildConsumerFilters(reg);
        store.putFilters(reg.key(), filters);

        LOG.debug(
                "Resolved per-consumer SPI: key={}, retryPolicy={}, converter={},"
                        + " dlqFailureStrategy={}, filters={}",
                reg.key(),
                policy.name(),
                converter.getClass().getSimpleName(),
                dlqStrategy.name(),
                filters.stream().map(ConsumerFilter::name).toList());
    }

    /** 解析 per-consumer 重平衡策略实例。 */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private RebalanceStrategy resolveRebalanceStrategy(ListenerRegistration<?> reg) {
        Class<? extends RebalanceStrategy> rebalanceClass = reg.getRebalanceStrategy();
        if (Objects.isNull(rebalanceClass) || rebalanceClass == RebalanceStrategy.class) {
            return new io.github.streammq.adapter.redisson.rebalance.AverageRebalanceStrategy();
        }
        // 一致性哈希策略支持通过 streammq.rebalance.virtual-nodes 配置虚拟节点数
        if (rebalanceClass
                == io.github.streammq.adapter.redisson.rebalance.ConsistentHashRebalanceStrategy
                        .class) {
            return new io.github.streammq.adapter.redisson.rebalance
                    .ConsistentHashRebalanceStrategy(defaultVirtualNodes);
        }
        try {
            return SpiResolver.resolveOrInstantiate(
                    (Class) rebalanceClass, RebalanceStrategy.class, null);
        } catch (RuntimeException ex) {
            LOG.warn(
                    "Failed to instantiate rebalanceStrategy for {}, using default: {}",
                    reg.key(),
                    ex.getMessage());
            return new io.github.streammq.adapter.redisson.rebalance.AverageRebalanceStrategy();
        }
    }

    /**
     * 解析 per-consumer 消息转换器：
     *
     * <ul>
     *   <li>注解指定自定义 {@code messageConverter} 类 → 无参实例化
     *   <li>注解指定自定义 {@code serializer} 类 → 实例化后包装为 {@link DefaultMessageConverter}
     *   <li>均为 marker → 回退全局转换器
     * </ul>
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private MessageConverter resolveConverter(ListenerRegistration<?> reg) {
        Class<? extends MessageConverter> converterClass = reg.getMessageConverter();
        Class<? extends MessageSerializer> serializerClass = reg.getSerializer();
        if (Objects.nonNull(converterClass) && converterClass != MessageConverter.class) {
            return SpiResolver.resolveOrInstantiate(
                    (Class) converterClass, MessageConverter.class, this.messageConverter);
        }
        if (Objects.nonNull(serializerClass) && serializerClass != MessageSerializer.class) {
            try {
                MessageSerializer<?> serializer =
                        serializerClass.getDeclaredConstructor().newInstance();
                return new DefaultMessageConverter(serializer);
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException(
                        "Failed to instantiate serializer "
                                + serializerClass.getName()
                                + " (requires public no-arg constructor)",
                        e);
            }
        }
        return this.messageConverter;
    }

    @Override
    public Collection<ConsumerMetadata> getConsumers() {
        List<ConsumerMetadata> list = new ArrayList<>(store.registrationCount());
        for (ListenerRegistration<?> reg : store.registrations()) {
            list.add(
                    new ConsumerMetadata(
                            reg.getTopic(),
                            reg.getGroup(),
                            reg.getConsumer().getClass(),
                            Objects.nonNull(reg.getTargetBodyType())
                                    ? reg.getTargetBodyType()
                                    : Object.class));
        }
        return Collections.unmodifiableList(list);
    }

    /**
     * 手动触发指定消费者组的分片重平衡（仅对已注册的 ORDERLY 消费者生效）。
     *
     * <p>计算 {@link RebalanceStrategy} 分配并写入 assignment Hash + 广播 REBALANCE 通知， 供可观测与管理端点使用；
     * 顺序消费的实际并发控制由分片分布式锁保证。
     *
     * @param group 消费者组名
     * @return true 表示已执行重平衡；false 表示未找到对应 ORDERLY 消费者或无可执行
     */
    public boolean rebalanceGroup(String group) {
        for (ListenerRegistration<?> reg : store.snapshotRegistrations()) {
            if (reg.getType() == ListenerType.ORDERLY && reg.getGroup().equals(group)) {
                ConsumerGroupManager cgm = store.groupManager(reg.key());
                if (cgm != null && reg.getShardCount() > 0) {
                    cgm.rebalance(reg.getShardCount());
                    LOG.info(
                            "Rebalance triggered for orderly group={}, shardCount={}",
                            group,
                            reg.getShardCount());
                    return true;
                }
            }
        }
        LOG.warn("Rebalance requested for unknown/non-orderly group: {}", group);
        return false;
    }

    /**
     * 将所有已注册 Listener 的 (topic, group, maxReconsumeTimes) 注册到 {@link RetryScheduler}。
     *
     * <p>DLQ 模式 Listener 额外以 {@code (group, group)} 注册扫描目标——其重试调度条目 （{@code scheduleDlqRetry}）统一写入
     * {@code retryZSet(ns, group, group)}，否则 DLQ 重试 永远不会被转移（历史缺陷：条目落在无任何扫描目标覆盖的 ZSet 上）。
     *
     * @param scheduler 重试调度器
     */
    public void registerRetryTargets(RetryScheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler");
        int count = 0;
        for (ListenerRegistration<?> reg : store.registrations()) {
            if (!reg.isDlqMode()) {
                scheduler.registerRetryTarget(
                        reg.getTopic(), reg.getGroup(), reg.getMaxReconsumeTimes());
                count++;
            } else {
                scheduler.registerRetryTarget(reg.getGroup(), reg.getGroup(), 0);
            }
        }
        LOG.info(
                "Registered {} retry targets to RetryScheduler ({} listeners total)",
                count,
                store.registrationCount());
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
        int orderlyCount = 0;
        int concurrentCount = 0;
        for (ListenerRegistration<?> reg : store.registrations()) {
            if (reg.isDlqMode()) {
                continue;
            }
            if (reg.getType() == ListenerType.ORDERLY) {
                scheduler.registerTarget(
                        reg.getTopic(),
                        reg.getGroup(),
                        reg.getMaxReconsumeTimes(),
                        true,
                        reg.getShardCount());
                orderlyCount++;
            } else if (reg.getType() == ListenerType.AUTO_ACK
                    && reg.getConsumeMode()
                            != io.github.streammq.core.enums.ConsumeMode.BROADCASTING) {
                // 并发消费组同样纳入 PEL 认领：覆盖实例崩溃后由其它实例接管遗留消息的场景
                // （本实例自身的 PEL 由启动排空 drainOwnPending 处理）。广播模式每实例独立组，不适用。
                scheduler.registerTarget(
                        reg.getTopic(), reg.getGroup(), reg.getMaxReconsumeTimes());
                concurrentCount++;
            }
        }
        LOG.info(
                "Registered {} PelClaim targets ({} orderly, {} concurrent)",
                orderlyCount + concurrentCount,
                orderlyCount,
                concurrentCount);
    }

    // ===================== 生命周期方法 =====================

    @Override
    public void start() {
        if (state.get() == ContainerState.STOPPED
                && !state.compareAndSet(ContainerState.STOPPED, ContainerState.INIT)) {
            throw new IllegalStateException(
                    "Container restart raced with another lifecycle change: " + state.get());
        }
        if (!state.compareAndSet(ContainerState.INIT, ContainerState.STARTING)) {
            throw new IllegalStateException(
                    "Container already started or in invalid state: " + state.get());
        }
        ensureRuntimeAlive();
        LOG.info("Starting ListenerContainer with {} registration(s)", store.registrationCount());
        // start 语义为全新启动：复位 pause 状态，避免"pause 后 stop 再 start"
        // 重启进静默暂停、消费循环空转的诡异现象
        paused = false;
        state.set(ContainerState.RUNNING);
        // 注册所有消费者组的 ConsumerGroupManager
        for (ListenerRegistration<?> reg : store.registrations()) {
            if (!reg.isDlqMode()) {
                createAndRegisterGroupManager(reg);
            }
        }
        doStartListeners();
        if (pelClaimScheduler != null) {
            registerPelClaimTargets(pelClaimScheduler);
        }
        LOG.info("ListenerContainer started, state=RUNNING");
    }

    /** 确保 executor 与 listener 工厂可用：容器 stop 后二者均不可复用，restart 前重建/重开。 */
    private synchronized void ensureRuntimeAlive() {
        if (consumeExecutor.isShutdown()) {
            consumeExecutor = Executors.newVirtualThreadPerTaskExecutor();
            LOG.info("Recreated consume executor for container restart");
        }
        if (consumerFactory instanceof RedissonStreamListenerFactory redissonFactory) {
            redissonFactory.reopen();
        }
    }

    /** 为单个注册创建并登记 ConsumerGroupManager（start 与动态注册共用）。 */
    private void createAndRegisterGroupManager(ListenerRegistration<?> reg) {
        String instanceId = reg.getGroup() + "-" + UUID.randomUUID().toString().substring(0, 8);
        ConsumerGroupManager cgm =
                new RedissonConsumerGroupManager(
                        redisson,
                        reg.getNamespace(),
                        reg.getGroup(),
                        instanceId,
                        resolveRebalanceStrategy(reg),
                        heartbeatIntervalMs,
                        instanceTimeoutMs);
        cgm.register();
        cgm.cleanupStaleGroups();
        store.putGroupManager(reg.key(), cgm);
    }

    @Override
    public void stop() {
        // CAS 守卫：并发/重复 stop 只允许一个线程进入停机流程
        ContainerState current = state.get();
        if (current == ContainerState.STOPPED || current == ContainerState.INIT) {
            return;
        }
        if (!state.compareAndSet(current, ContainerState.STOPPING)) {
            LOG.debug("Stop skipped, another lifecycle change won the race: {}", state.get());
            return;
        }
        LOG.info("Stopping ListenerContainer...");
        // 先取消消费循环，再注销组管理器：若顺序颠倒，实例已从组内除名却仍在拉取消息，
        // 此窗口内的 rebalance 会把分片分配给其他实例，造成短暂的双重消费。
        for (Future<?> future : consumeFutures.values()) {
            future.cancel(true);
        }
        consumeFutures.clear();
        // 注销所有消费者组管理器
        store.clearGroupManagers();
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
        paused = false;
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
        for (ListenerRegistration<?> reg : store.registrations()) {
            submitConsumeLoops(reg);
        }
    }

    /**
     * 计算注册的并发消费循环数。
     *
     * <p>仅并发（CONCURRENT）集群消费生效，取 {@code consumeThreadMin}（夹取到 {@code [1, consumeThreadMax]}）；顺序 /
     * DLQ / 广播固定为 1。 此前两个注解属性在适配层从未被读取， Spring Cloud Stream 的 {@code concurrency} 经 binder
     * 映射后被静默忽略。
     *
     * @param reg 注册信息
     * @return 消费循环数（&gt;= 1）
     */
    private int effectiveConcurrency(ListenerRegistration<?> reg) {
        if (reg.getType() != ListenerType.AUTO_ACK
                || reg.isDlqMode()
                || reg.getConsumeMode() == ConsumeMode.BROADCASTING
                || reg.getType() == ListenerType.ORDERLY) {
            return 1;
        }
        int max = Math.max(1, reg.getConsumeThreadMax());
        return Math.max(1, Math.min(max, reg.getConsumeThreadMin()));
    }

    /** 为单个注册提交消费任务（并发非 DLQ 双 listener：original + retry；顺序/DLQ 单 listener）。 */
    private void submitConsumeLoops(ListenerRegistration<?> reg) {
        // 幂等守卫：动态注册（wireRegistrationIfRunning）与 start() 遍历可能并发触发同一 reg，
        // 双重提交会导致旧 Future 被覆盖、cancel 漏掉一条循环。
        if (isConsumeLoopActive(reg.key())
                || isConsumeLoopActive(reg.key() + RETRY_FUTURE_SUFFIX)) {
            LOG.debug(
                    "Consume loop already active, skip duplicate submission: topic={}, group={}",
                    reg.getTopic(),
                    reg.getGroup());
            return;
        }
        // 并发非 DLQ 消费者：双 listener（original + retry），对齐 RocketMQ 订阅 original + %RETRY%；
        // consumeThreadMin>1 时为每个 listener 提交多条读循环（共享同一 consumer name，
        // XREADGROUP 原子分配保证各循环拿到不相交的新消息），index 0 负责启动 PEL 排空。
        if (reg.getType() == ListenerType.AUTO_ACK && !reg.isDlqMode()) {
            int concurrency = effectiveConcurrency(reg);
            Future<?> origFuture = consumeExecutor.submit(() -> consumeLoop(reg, false, true));
            if (Objects.nonNull(consumeFutures.putIfAbsent(reg.key(), origFuture))) {
                origFuture.cancel(true);
                return;
            }
            for (int i = 1; i < concurrency; i++) {
                final int idx = i;
                Future<?> f = consumeExecutor.submit(() -> consumeLoop(reg, false, false));
                String futureKey = reg.key() + CONCURRENCY_FUTURE_SUFFIX + idx;
                if (Objects.nonNull(consumeFutures.putIfAbsent(futureKey, f))) {
                    f.cancel(true);
                }
            }
            Future<?> retryFuture = consumeExecutor.submit(() -> consumeLoop(reg, true, true));
            if (Objects.nonNull(
                    consumeFutures.putIfAbsent(reg.key() + RETRY_FUTURE_SUFFIX, retryFuture))) {
                retryFuture.cancel(true);
            } else {
                for (int i = 1; i < concurrency; i++) {
                    final int idx = i;
                    Future<?> f = consumeExecutor.submit(() -> consumeLoop(reg, true, false));
                    String futureKey =
                            reg.key() + RETRY_FUTURE_SUFFIX + CONCURRENCY_FUTURE_SUFFIX + idx;
                    if (Objects.nonNull(consumeFutures.putIfAbsent(futureKey, f))) {
                        f.cancel(true);
                    }
                }
            }
        } else {
            // 顺序消费 / DLQ 消费者：单 listener
            Future<?> future = consumeExecutor.submit(() -> consumeLoop(reg, false, true));
            if (Objects.nonNull(consumeFutures.putIfAbsent(reg.key(), future))) {
                future.cancel(true);
            }
        }
    }

    /** 判断指定 key 的消费循环是否存在且尚未结束。 */
    private boolean isConsumeLoopActive(String futureKey) {
        Future<?> f = consumeFutures.get(futureKey);
        return Objects.nonNull(f) && !f.isDone();
    }

    /** 启动前排空本消费者 PEL 的遗留消息：循环拉取直至清空或容器停止，逐条走正常处理路径 （重试/DLQ 策略照常生效）。 */
    private void drainOwnPending(ListenerRegistration<?> reg, StreamMQListener listener) {
        int drained = 0;
        while (state.get() == ContainerState.RUNNING && !paused) {
            List<Message<?>> pending = listener.drainPendingOnce(reg.getPullBatchSize());
            if (pending.isEmpty()) {
                if (drained > 0) {
                    LOG.info(
                            "PEL drain complete: topic={}, group={}, recovered={}",
                            reg.getTopic(),
                            reg.getGroup(),
                            drained);
                }
                return;
            }
            for (Message<?> message : pending) {
                if (state.get() != ContainerState.RUNNING) {
                    return;
                }
                messageProcessor.processMessage(message, reg, listener);
                drained++;
            }
        }
    }

    @Override
    public void unregister(String topic, String consumerGroup) {
        StringUtils.requireValidTopic(topic);
        StringUtils.requireValidGroup(consumerGroup);
        boolean running = state.get() == ContainerState.RUNNING;
        boolean removed = false;
        for (String suffix : new String[] {"", DLQ_KEY_PREFIX}) {
            String key = suffix + topic + REG_KEY_SEPARATOR + consumerGroup;
            ListenerRegistration<?> reg = store.removeRegistration(key);
            if (Objects.isNull(reg)) {
                continue;
            }
            removed = true;
            cancelRegistrationFutures(key);
            store.removeFilters(key);
            store.removeAndUnregisterGroupManager(key);
            LOG.info(
                    "Unregistered StreamMQ listener: topic={}, group={}, wasRunning={}",
                    topic,
                    consumerGroup,
                    running);
        }
        if (!removed) {
            LOG.info(
                    "Unregister ignored, no registration found: topic={}, group={}",
                    topic,
                    consumerGroup);
        }
    }

    /** 取消单个注册的全部消费任务（含 retry 任务、并发循环与背压处理线程），等待中的拉取会因中断退出。 */
    private void cancelRegistrationFutures(String key) {
        java.util.List<Future<?>> cancelled = new java.util.ArrayList<>();
        for (java.util.Iterator<java.util.Map.Entry<String, Future<?>>> it =
                        consumeFutures.entrySet().iterator();
                it.hasNext(); ) {
            var entry = it.next();
            String k = entry.getKey();
            boolean belongs =
                    k.equals(key)
                            || k.startsWith(key + CONCURRENCY_FUTURE_SUFFIX)
                            || k.equals(key + RETRY_FUTURE_SUFFIX)
                            || k.startsWith(key + RETRY_FUTURE_SUFFIX + CONCURRENCY_FUTURE_SUFFIX)
                            || k.equals(key + INFLIGHT_PROCESSOR_SUFFIX);
            if (belongs) {
                cancelled.add(entry.getValue());
                it.remove();
            }
        }
        for (Future<?> f : cancelled) {
            f.cancel(true);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void consumeLoop(ListenerRegistration reg, boolean retryMode, boolean primaryLoop) {
        StreamMQListener listener;
        try {
            listener = createConsumerFor(reg, retryMode);
        } catch (RuntimeException ex) {
            LOG.error(
                    "Failed to create consumer for listener (topic={}, group={}, retryMode={}): {},"
                            + " listener will not consume",
                    reg.getTopic(),
                    reg.getGroup(),
                    retryMode,
                    ex.getMessage(),
                    ex);
            return;
        }
        LOG.info(
                "Consume loop started: topic={}, group={}, retryMode={}, concurrencySlot={},"
                        + " listener={}",
                reg.getTopic(),
                reg.getGroup(),
                retryMode,
                primaryLoop ? 0 : "aux",
                reg.getConsumer().getClass().getSimpleName());

        // 背压队列（inflightCapacity > 0 时启用）
        final BlockingQueue<Message<?>> inflightQueue;
        if (inflightCapacity > 0) {
            inflightQueue = new LinkedBlockingQueue<>(inflightCapacity);
            // 处理线程提交到 consumeExecutor 并登记 Future：否则 unregister/stop 时
            // 无法取消该线程，会残留持有 listener 引用的孤儿虚拟线程
            Future<?> processorFuture =
                    consumeExecutor.submit(
                            () -> processFromInflightQueue(reg, listener, inflightQueue));
            consumeFutures.put(reg.key() + INFLIGHT_PROCESSOR_SUFFIX, processorFuture);
        } else {
            inflightQueue = null;
        }

        try {
            // 启动排空：恢复上次实例崩溃/停止时遗留在本消费者 PEL 中的消息（at-least-once 补齐）。
            // 仅并发消费启用——顺序消费由 PelClaimScheduler 按 shard 语义处理，直接排空会破坏顺序。
            // 多并发循环共享同一 consumer name：仅 primary（index 0）排空，避免并发重复处理同一 PEL entry。
            if (primaryLoop && reg.getType() == ListenerType.AUTO_ACK && !reg.isDlqMode()) {
                drainOwnPending(reg, listener);
            }
            while (state.get() == ContainerState.RUNNING) {
                if (paused) {
                    sleepQuietly(PAUSED_SLEEP_MILLIS);
                    continue;
                }
                try {
                    List<Message<?>> messages =
                            listener.pullBlock(
                                    reg.getPullBatchSize(),
                                    Duration.ofMillis(reg.getPullBlockTimeoutMillis()));
                    if (CollectionUtils.isEmpty(messages)) {
                        if (reg.getPullIntervalMillis() > 0) {
                            sleepQuietly(reg.getPullIntervalMillis());
                        }
                        continue;
                    }
                    for (Message<?> message : messages) {
                        if (state.get() != ContainerState.RUNNING) {
                            break;
                        }
                        if (Objects.nonNull(inflightQueue)) {
                            // 背压：队列满时阻塞等待
                            inflightQueue.put(message);
                        } else {
                            messageProcessor.processMessage(message, reg, listener);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (StreamMQBrokerException ex) {
                    LOG.warn(
                            "Broker error in consume loop (topic={}, group={}): {}",
                            reg.getTopic(),
                            reg.getGroup(),
                            ex.getMessage());
                    sleepQuietly(BROKER_ERROR_BACKOFF_MILLIS);
                } catch (RuntimeException ex) {
                    LOG.warn(
                            "Unexpected error in consume loop (topic={}, group={}): {}",
                            reg.getTopic(),
                            reg.getGroup(),
                            ex.getMessage(),
                            ex);
                    sleepQuietly(BROKER_ERROR_BACKOFF_MILLIS);
                }
            }
        } finally {
            LOG.info(
                    "Consume loop exited: topic={}, group={}, retryMode={}",
                    reg.getTopic(),
                    reg.getGroup(),
                    retryMode);
        }
    }

    /** 从背压队列中取出消息并处理（独立虚拟线程）。 */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void processFromInflightQueue(
            ListenerRegistration reg,
            StreamMQListener listener,
            BlockingQueue<Message<?>> inflightQueue) {
        try {
            while (state.get() == ContainerState.RUNNING) {
                Message<?> message = inflightQueue.poll(1, TimeUnit.SECONDS);
                if (Objects.isNull(message)) {
                    continue;
                }
                messageProcessor.processMessage(message, reg, listener);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private StreamMQListener createConsumerFor(ListenerRegistration<?> reg, boolean retryMode) {
        // 0.1.0 起注册模型是唯一持有者：声明式配置由 ListenerConfig.from(reg) 单点派生，
        // 不再在容器内重复罗列字段（消除双模型漂移）
        return consumerFactory.createListener(ListenerConfig.from(reg, retryMode));
    }

    private void checkBeforeStart() {
        ContainerState current = state.get();
        if (current == ContainerState.INIT) {
            return;
        }
        // 运行中动态注册：立即接线单个监听器（见 wireRegistrationIfRunning）
        if (current == ContainerState.RUNNING) {
            return;
        }
        // 完整 stop 后重新开放注册：回到 INIT，等待下一次 start()
        if (current == ContainerState.STOPPED
                && state.compareAndSet(ContainerState.STOPPED, ContainerState.INIT)) {
            return;
        }
        throw new IllegalStateException(
                "Cannot register listener in container state "
                        + current
                        + " (rebinding in"
                        + " progress or container starting)");
    }

    /** 容器已运行时，为新注册项立即创建组管理器并提交消费任务（动态绑定场景，如 Spring Cloud Stream binder 的 rebind）。 */
    private void wireRegistrationIfRunning(ListenerRegistration<?> reg) {
        if (state.get() != ContainerState.RUNNING) {
            return;
        }
        if (!reg.isDlqMode() && Objects.isNull(store.groupManager(reg.key()))) {
            createAndRegisterGroupManager(reg);
        }
        submitConsumeLoops(reg);
        LOG.info(
                "Dynamically wired registration while container running: topic={}, group={}",
                reg.getTopic(),
                reg.getGroup());
    }

    /**
     * 构建 per-consumer 过滤器链（在注册时调用一次，缓存结果）。
     *
     * <p>执行顺序：
     *
     * <ol>
     *   <li>selectorExpression（SimpleTagSelectorFilter / SimpleSqlSelectorFilter，order = -1）- 最先执行
     *   <li>全局过滤器（顺序由 order 决定）
     *   <li>per-consumer 过滤器（顺序由 order 决定，从 Spring 容器获取）
     * </ol>
     *
     * @param reg Listener 注册信息
     * @return 预构建的过滤器列表
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<ConsumerFilter> buildConsumerFilters(ListenerRegistration<?> reg) {
        List<ConsumerFilter> allFilters = new ArrayList<>();

        String selectorExpression = reg.getSelectorExpression();
        if (StringUtils.isNotEmpty(selectorExpression)
                && !StreamMQConstants.SELECTOR_WILDCARD.equals(selectorExpression)) {
            SelectorType selectorType = reg.getSelectorType();
            ExpressionSelectorFilter selectorFilter =
                    switch (selectorType) {
                        case TAG -> new SimpleTagSelectorFilter(selectorExpression);
                        case SQL92 -> new SimpleSqlSelectorFilter(selectorExpression);
                    };
            allFilters.add(selectorFilter);
            LOG.debug(
                    "Built selector filter for {}: type={}, expression={}",
                    reg.key(),
                    selectorType,
                    selectorExpression);
        }

        allFilters.addAll(consumerFilterChain.getFilters());

        Class<? extends ConsumerFilter>[] perConsumerFilterClasses =
                (Class<? extends ConsumerFilter>[]) reg.getConsumerFilter();
        if (Objects.nonNull(perConsumerFilterClasses) && perConsumerFilterClasses.length > 0) {
            for (Class<? extends ConsumerFilter> filterClass : perConsumerFilterClasses) {
                ConsumerFilter filter = resolveConsumerFilter(filterClass);
                if (Objects.nonNull(filter)) {
                    allFilters.add(filter);
                    LOG.debug("Added per-consumer filter for {}: {}", reg.key(), filter.name());
                }
            }
        }

        allFilters.sort(Comparator.comparingInt(ConsumerFilter::order));

        return Collections.unmodifiableList(allFilters);
    }

    /**
     * 解析 per-consumer 过滤器：优先通过 filterResolver 获取，回退到反射实例化。
     *
     * @param filterClass 过滤器类
     * @return 过滤器实例，可为 null
     */
    private ConsumerFilter resolveConsumerFilter(Class<? extends ConsumerFilter> filterClass) {
        if (Objects.isNull(filterClass) || filterClass == ConsumerFilter.class) {
            return null;
        }

        ConsumerFilterResolver resolver = this.filterResolver;
        if (Objects.nonNull(resolver)) {
            ConsumerFilter filter = resolver.resolve(filterClass);
            if (Objects.nonNull(filter)) {
                return filter;
            }
            LOG.debug(
                    "Filter {} not resolved by filterResolver, trying reflection",
                    filterClass.getName());
        }

        try {
            return filterClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            LOG.warn(
                    "Failed to instantiate per-consumer filter {}: {}",
                    filterClass.getName(),
                    e.getMessage());
            return null;
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
