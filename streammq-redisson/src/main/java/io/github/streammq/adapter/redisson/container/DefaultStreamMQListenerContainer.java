/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.dlq.LogAndDropDlqFailureStrategy;
import io.github.streammq.adapter.redisson.filter.DefaultConsumerFilterChain;
import io.github.streammq.adapter.redisson.handler.DefaultRetryAndDlqHandler;
import io.github.streammq.adapter.redisson.interceptor.DefaultConsumerInterceptorChain;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory;
import io.github.streammq.adapter.redisson.lock.RedissonOrderlyShardLockManager;
import io.github.streammq.adapter.redisson.metrics.RuntimeStatsRegistry;
import io.github.streammq.adapter.redisson.scheduler.PelClaimScheduler;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.annotation.StreamMQDlqConsumer;
import io.github.streammq.core.consumer.*;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.ConsumeFromWhere;
import io.github.streammq.core.filter.ConsumerFilter;
import io.github.streammq.core.filter.ConsumerFilterChain;
import io.github.streammq.core.filter.ConsumerFilterResolver;
import io.github.streammq.core.interceptor.ConsumerInterceptor;
import io.github.streammq.core.interceptor.ConsumerInterceptorChain;
import io.github.streammq.core.listener.*;
import io.github.streammq.core.metrics.StreamMQMetrics;
import io.github.streammq.core.policy.*;
import io.github.streammq.core.util.StringUtils;
import java.util.*;
import java.util.concurrent.*;
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

    /** 注册存储（接口注入，默认实现见构造器；start 前可通过 setter 覆盖） */
    private final RegistrationStore store = new DefaultRegistrationStore();

    /** 消费线程池：默认统一使用虚拟线程池；Spring 环境由自动装配注入用户自定义实现。 所有权规则：容器内部创建的默认池在 stop 时关闭；外部注入的池由提供方管理生命周期。 */
    private ExecutorService consumeExecutor = Executors.newVirtualThreadPerTaskExecutor();

    /** 是否拥有消费线程池所有权（决定 stop 是否关闭） */
    private boolean ownsExecutor = true;

    /**
     * 注入自定义执行器（仅容器 INIT 状态允许）。
     *
     * <p><b>所有权转移：</b>构造器字段初始化时创建的默认内部执行器会在此被关闭——否则每注入一次 就泄漏一个执行器（Spring 环境下每个容器实例都会走这条路径）。语义与
     * {@code DefaultStreamMessageTemplate#setAsyncSendExecutor} 保持一致：谁创建谁关闭。
     */
    public void setConsumeExecutor(ExecutorService executor) {
        assertInitState("consumeExecutor");
        Objects.requireNonNull(executor, "executor");
        ExecutorService previous = this.consumeExecutor;
        boolean previousOwned = this.ownsExecutor;
        this.consumeExecutor = executor;
        this.ownsExecutor = false;
        // 必须先把新执行器同步给所有"构造时捕获了执行器引用"的协作类，再关闭旧执行器。
        // 顺序颠倒会让 messageProcessor 继续指向已关闭的执行器，消费回调抛
        // RejectedExecutionException——表现为"消费者静默不消费"，极难定位。
        messageProcessor.setExecutor(executor);
        if (previousOwned && previous != executor) {
            previous.shutdown();
        }
    }

    /**
     * 注入自定义实例标识（仅容器 INIT 状态允许）。
     *
     * <p>广播消费模式下，instanceToken 用于构造消费者组名。持久化标识保证容器重启后组名不变， 避免每次重启产生新组导致 Redis PEL 内存泄漏。
     *
     * @param instanceToken 实例标识（非空时会覆盖自动推导值）
     */
    public void setInstanceToken(String instanceToken) {
        assertInitState("instanceToken");
        this.instanceToken = resolveInstanceToken(instanceToken);
    }

    /**
     * 返回当前实例标识（广播模式下用于构造消费者组名）。
     *
     * @return 实例标识
     */
    public String getInstanceToken() {
        return instanceToken;
    }

    /**
     * 解析实例标识：按优先级自动推导持久化标识。
     *
     * <p>优先级：显式值 &gt; 系统属性 {@code streammq.instance.id} &gt; 环境变量 {@code STREAMMQ_INSTANCE_ID} &gt;
     * 本地主机名（追加进程内容器序号，保证容器级唯一）&gt; UUID 回退。
     *
     * @param configured 显式配置值，可为 null/空
     * @return 非空实例标识
     */
    public static String resolveInstanceToken(String configured) {
        if (io.github.streammq.core.util.StringUtils.isNotEmpty(configured)) {
            return configured.trim();
        }
        // 1. 系统属性
        String fromSys =
                System.getProperty(
                        io.github.streammq.core.StreamMQConstants.INSTANCE_ID_SYSTEM_PROPERTY);
        if (io.github.streammq.core.util.StringUtils.isNotEmpty(fromSys)) {
            return fromSys.trim();
        }
        // 2. 环境变量
        String fromEnv =
                System.getenv(io.github.streammq.core.StreamMQConstants.INSTANCE_ID_ENV_VARIABLE);
        if (io.github.streammq.core.util.StringUtils.isNotEmpty(fromEnv)) {
            return fromEnv.trim();
        }
        // 3. 本地主机名 + 进程内容器序号（容器级隔离）
        try {
            String hostName = java.net.InetAddress.getLocalHost().getHostName();
            if (io.github.streammq.core.util.StringUtils.isNotEmpty(hostName)) {
                // 主机名是进程级值：同一 JVM 内的多个容器实例（测试、多租户宿主）会解析到
                // 同一主机名，若不加区分，它们的广播消费者组名将完全相同（组名 =
                // group:consumerName），广播语义退化为集群——消息只投递给组内一个消费者。
                // 因此在主机名后追加进程内容器序号：首个容器保持纯主机名（单容器生产常态，
                // 重启后组名不变、PEL 可恢复），同 JVM 内后续容器依次 -2、-3……保证容器级唯一。
                int seq = CONTAINER_SEQUENCE.incrementAndGet();
                return seq == 1 ? hostName : hostName + "-" + seq;
            }
        } catch (Exception ignored) {
            // 网络不可用时回退到 UUID
        }
        // 4. UUID 回退（每次 JVM 随机，天然容器级唯一，无碰撞风险）
        return java.util.UUID.randomUUID().toString();
    }

    /**
     * 进程内容器实例计数器：为主机名分支提供容器级唯一序号。
     *
     * <p>见 {@link #resolveInstanceToken(String)}——主机名是进程级值，同 JVM 多容器直接复用会令 广播组名碰撞（违反 {@link
     * #instanceToken} 的容器级唯一约束）。
     */
    private static final java.util.concurrent.atomic.AtomicInteger CONTAINER_SEQUENCE =
            new java.util.concurrent.atomic.AtomicInteger();

    /**
     * 消费循环启动失败登记表：loopKey → 失败原因。
     *
     * <p>消费循环若在创建监听器阶段失败（Redis 认证失败、消费者组非法、配置错误等），此前只会打一条 ERROR 日志后静默退出：消费者在管理端点仍可见、健康检查仍为
     * UP，运维只能靠"消息没人消费"反推。 登记后该失败会体现在 {@link #isConsumeLoopsHealthy()} 与管理端点中。
     *
     * <p>{@code start()} 会清空登记表（全新启动），{@code stop()} 同样清空——停止后的历史失败不应 影响下一次启动的健康判定。
     */
    private final java.util.Map<String, String> consumeLoopFailures =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * 返回当前尚未恢复的消费循环启动失败（loopKey → 失败原因），空 map 表示全部正常。
     *
     * @return 不可修改的失败快照
     */
    public java.util.Map<String, String> getConsumeLoopFailures() {
        return java.util.Map.copyOf(consumeLoopFailures);
    }

    /** 是否存在消费循环启动/运行期失败（纳入健康检查，避免"静默不消费"）。 */
    public boolean isConsumeLoopsHealthy() {
        return consumeLoopFailures.isEmpty();
    }

    /** 清除指定循环键的健康失败条目（运行期故障恢复时由消费循环回调）。 */
    private void clearConsumeLoopFailure(String loopKey) {
        consumeLoopFailures.remove(loopKey);
    }

    private void assertInitState(String what) {
        if (lifecycle.current() != ContainerState.INIT) {
            throw new IllegalStateException(
                    "Cannot customize "
                            + what
                            + " after container left INIT (current="
                            + lifecycle.current()
                            + ")");
        }
    }

    /** 生命周期状态机（State：集中迁移表） */
    private final ContainerStateMachine lifecycle = new DefaultContainerStateMachine();

    /** 运行期暂停标志（独立于生命周期状态） */
    private volatile boolean paused = false;

    /** 消费循环监督者（Command 登记表：幂等提交/并发度/按注册取消） */
    private final ConsumeLoopSupervisor loopSupervisor =
            new DefaultConsumeLoopSupervisor(this::launchLoop);

    /**
     * 本容器实例的唯一标识：广播模式消费者组名使用它区分不同容器。
     *
     * <p>默认按以下优先级自动推导持久化标识（避免每次重启产生新组导致 PEL 泄漏）：
     *
     * <ol>
     *   <li>显式配置的 {@code instanceToken}
     *   <li>系统属性 {@code streammq.instance.id}
     *   <li>环境变量 {@code STREAMMQ_INSTANCE_ID}
     *   <li>本地主机名
     *   <li>UUID 回退
     * </ol>
     *
     * <p>注意必须是<b>容器级</b>而非进程级：同一 JVM 内可能运行多个容器实例（测试、多租户宿主）， 共享标识会导致它们的广播组名碰撞、消息只投递给其中一个。
     */
    private volatile String instanceToken = resolveInstanceToken(null);

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

    /** 过滤器 / 拦截器协调器（从 DefaultStreamMQListenerContainer 拆分出来，专门负责 filter chain 的注册与缓存重建）。 */
    private volatile ListenerContainerFilterCoordinator filterCoordinator;

    /** 元数据门面（从 DefaultStreamMQListenerContainer 拆分出来，专门负责消费者元数据查询与 scheduler target 绑定）。 */
    private volatile ListenerContainerMetadata metadataCoordinator;

    /** 指标收集器（可选注入，用于记录消费指标，null 时为 no-op） */
    private volatile StreamMQMetrics metrics;

    /**
     * 进程内运行时统计登记表（发布前修复 P1-3）。
     *
     * <p>为 {@code GET /actuator/streammq/stats/{group}/{topic}} 提供真实数据源——此前该端点读取的 Redis key
     * 无任何写入方，永远返回空 map。本登记表与 {@link StreamMQMetrics} 正交： metrics 面向 Micrometer（需要 Actuator +
     * MeterRegistry），登记表始终可用。
     */
    private final RuntimeStatsRegistry runtimeStats = new RuntimeStatsRegistry();

    /**
     * 返回进程内运行时统计登记表（供管理端点读取真实统计）。
     *
     * @return 统计登记表，永不为 null
     */
    public RuntimeStatsRegistry runtimeStats() {
        return runtimeStats;
    }

    /** 策略类：拦截器链 */
    private final ConsumerInterceptorChain interceptorChain;

    /**
     * 设置消费者过滤器解析器，用于从容器中获取 per-consumer 过滤器实例。
     *
     * @param filterResolver 过滤器解析器
     */
    public void setFilterResolver(ConsumerFilterResolver filterResolver) {
        this.filterResolver = filterResolver;
        if (filterCoordinator != null) {
            filterCoordinator.setFilterResolver(filterResolver);
        }
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

    /**
     * 把进程内运行时统计登记表传播到共享 handler 与所有已创建的 per-consumer handler。
     *
     * <p>必须在容器构造后立即调用（自动装配阶段），确保重试/死信计数能被 {@code /actuator/streammq/stats} 读取到（发布前修复 P1-3）。
     */
    public void propagateRuntimeStats() {
        if (sharedRetryDlqHandler instanceof DefaultRetryAndDlqHandler drh) {
            drh.setRuntimeStats(runtimeStats);
        }
        for (RetryAndDlqHandler handler : store.handlers()) {
            if (handler instanceof DefaultRetryAndDlqHandler drh) {
                drh.setRuntimeStats(runtimeStats);
            }
        }
    }

    /** 策略类：ACK/重试/DLQ 路由（per-consumer 关闭时的共享实例） */
    private final RetryAndDlqHandler sharedRetryDlqHandler;

    /** 策略类：顺序消费分片锁 */
    private final OrderlyShardLockManager shardLockManager;

    /** 单条消息消费管线（接口注入，懒构建） */
    private final MessageProcessor messageProcessor;

    /** 是否启用 per-consumer 策略实例化（高级构造器注入自定义 handler 时关闭） */
    private final boolean perConsumerEnabled;

    /** 全局 DLQ 失败策略 */
    private final DlqFailureStrategy dlqFailureStrategy;

    /** 全局 DLQ 配置 */
    private final DlqConfig dlqConfig;

    /** 顺序消费 PEL 认领调度器（可选，注入后容器启动时注册目标） */
    private volatile PelClaimScheduler pelClaimScheduler;

    /** 拉取运行参数（Parameter Object） */
    private final DefaultConsumerTuning tuning = new DefaultConsumerTuning();

    /**
     * 全局新消费者组起始消费位点（来自 {@code streammq.consumer.consume-from-where}）， 默认 {@link
     * ConsumeFromWhere#DEFAULT}。消费者注解未显式声明时回落到本值。
     */
    private volatile ConsumeFromWhere defaultConsumeFromWhere = ConsumeFromWhere.DEFAULT;

    /** per-consumer SPI 解析器（接口注入，懒构建） */
    private PerConsumerSpiResolver spiResolver;

    /** 监听器注册服务（接口注入，懒构建） */
    private ListenerRegistrar registrar;

    /** 组管理器工厂（接口注入，懒构建） */
    private ConsumerGroupManagerFactory groupManagerFactory;

    /** 调度器目标绑定器（接口注入，懒构建） */
    private SchedulerTargetBinder schedulerBinder;

    /** 一致性哈希重平衡策略虚拟节点数（透传给 SPI 解析器） */
    private volatile int defaultVirtualNodes = StreamMQConstants.DEFAULT_VIRTUAL_NODES;

    /** 全局默认 RebalanceStrategy（来自 streammq.rebalance.strategy，可为 null） */
    private volatile Class<? extends RebalanceStrategy> defaultRebalanceStrategy;

    /**
     * 设置全局默认 RebalanceStrategy（仅 INIT 状态允许）。
     *
     * <p>per-consumer 注解未显式指定 rebalanceStrategy 时回退到该值； 传 null 表示回退到 {@code
     * AverageRebalanceStrategy}。
     */
    public void setDefaultRebalanceStrategy(Class<? extends RebalanceStrategy> strategy) {
        assertInitState("defaultRebalanceStrategy");
        this.defaultRebalanceStrategy = strategy;
    }

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
        tuning.setDefaultPullBatchSize(batchSize);
    }

    /**
     * 设置消费者全局默认拉取阻塞超时（毫秒）。
     *
     * @param millis 毫秒数，必须 &gt; 0
     */
    public void setDefaultPullBlockTimeoutMillis(long millis) {
        tuning.setDefaultPullBlockTimeoutMillis(millis);
    }

    /**
     * 设置消费者全局默认拉取间隔（毫秒）。
     *
     * @param millis 毫秒数，必须 &gt;= 0
     */
    public void setDefaultPullIntervalMillis(long millis) {
        tuning.setDefaultPullIntervalMillis(millis);
    }

    /**
     * 设置单次拉取批量上界。
     *
     * @param limit 上界，必须 &gt; 0
     */
    public void setMaxBatchSizeLimit(int limit) {
        tuning.setMaxBatchSizeLimit(limit);
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

    /** 注入暂停休眠间隔（毫秒，{@code streammq.consumer.paused-sleep-millis}）。 */
    public void setPausedSleepMillis(long millis) {
        tuning.setPausedSleepMillis(millis);
    }

    /** 注入 Broker 异常退避间隔（毫秒，{@code streammq.consumer.broker-error-backoff-millis}）。 */
    public void setBrokerErrorBackoffMillis(long millis) {
        tuning.setBrokerErrorBackoffMillis(millis);
    }

    /**
     * 注入全局顺序消费超时（毫秒），{@code streammq.consumer.orderly-consume-timeout}。
     *
     * <p>仅作为消费者注解未显式声明 {@code orderlyConsumeTimeout} 时的回落值：注解值 {@code > 0} 时始终优先。 默认 0
     * 表示不启用——顺序消费超时后走串行重试、耗尽即进 DLQ，默认开启会把慢消息系统性误杀。
     *
     * @param millis 超时毫秒数，{@code 0} 表示不启用
     */
    public void setDefaultOrderlyConsumeTimeoutMillis(long millis) {
        tuning.setDefaultOrderlyConsumeTimeoutMillis(millis);
    }

    /** 注入全局并发消费超时（毫秒，{@code streammq.consumer.consume-timeout-millis}）。 */
    public void setDefaultConsumeTimeoutMillis(long millis) {
        tuning.setDefaultConsumeTimeoutMillis(millis);
    }

    /** 注入全局最大重试次数（{@code streammq.retry.max-reconsume-times}）。 */
    public void setDefaultMaxReconsumeTimes(int times) {
        tuning.setDefaultMaxReconsumeTimes(times);
    }

    /**
     * 注入全局新消费者组起始消费位点（{@code streammq.consumer.consume-from-where}）。
     *
     * <p>消费者注解未显式声明 {@code consumeFromWhere}（即取枚举默认值 {@link ConsumeFromWhere#DEFAULT}）时回落到本值。
     * 本值本身默认 {@link ConsumeFromWhere#DEFAULT}（= {@code CONSUME_FROM_LAST}），与配置默认值单一来源一致。
     */
    public void setDefaultConsumeFromWhere(ConsumeFromWhere where) {
        this.defaultConsumeFromWhere = Objects.isNull(where) ? ConsumeFromWhere.DEFAULT : where;
    }

    public ConsumeFromWhere getDefaultConsumeFromWhere() {
        return defaultConsumeFromWhere;
    }

    /**
     * 设置消费超时取消后的宽限期（毫秒）。
     *
     * @param millis 宽限期，必须 &gt; 0
     */
    public void setTimeoutCancelGraceMillis(long millis) {
        if (millis > 0) {
            this.timeoutCancelGraceMillis = millis;
            messageProcessor.setTimeoutCancelGraceMillis(millis);
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

    /**
     * 构造容器（向后兼容：内部创建默认策略实现，per-consumer 启用）。
     *
     * @deprecated 兼容重载，默认值分散在多个重载间，易产生语义漂移。请使用全参构造 {@link
     *     #DefaultStreamMQListenerContainer(RedissonClient, StreamMQListenerFactory,
     *     MessageConverter, RetryPolicy, DlqFailureStrategy, DlqConfig, String, int)}（DLQ
     *     失败策略与配置按需显式传入）， 计划在 0.2.0 移除。
     */
    @Deprecated(since = "0.1.1")
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

    /**
     * 构造容器并注入全局死信消费失败策略（per-consumer 启用）。
     *
     * @deprecated 兼容重载，请使用全参构造 {@link #DefaultStreamMQListenerContainer(RedissonClient,
     *     StreamMQListenerFactory, MessageConverter, RetryPolicy, DlqFailureStrategy, DlqConfig,
     *     String, int)}，计划在 0.2.0 移除。
     */
    @Deprecated(since = "0.1.1")
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

    /**
     * 构造容器并注入全局 DLQ 策略与配置（per-consumer 启用）。
     *
     * @deprecated 兼容重载，背压容量默认值由本重载硬编码、与全参构造的显式传参并存，改默认值易漏改。 请使用全参构造 {@link
     *     #DefaultStreamMQListenerContainer(RedissonClient, StreamMQListenerFactory,
     *     MessageConverter, RetryPolicy, DlqFailureStrategy, DlqConfig, String, int)}，计划在 0.2.0 移除。
     */
    @Deprecated(since = "0.1.1", forRemoval = true)
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
        tuning.setInflightCapacity(inflightCapacity);
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
                new DefaultMessageProcessor(
                        chain,
                        this.shardLockManager,
                        store,
                        this.sharedRetryDlqHandler,
                        true,
                        consumeExecutor);
        this.messageProcessor.setRuntimeStats(runtimeStats);
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
        this.messageProcessor =
                new DefaultMessageProcessor(
                        interceptorChain,
                        this.shardLockManager,
                        store,
                        this.sharedRetryDlqHandler,
                        false,
                        consumeExecutor);
        this.messageProcessor.setRuntimeStats(runtimeStats);
    }

    /**
     * 设置背压队列容量（{@code >0} 启用：拉取与处理解耦，队列满时拉取阻塞；{@code 0} 禁用）。
     *
     * <p>默认 {@link StreamMQConstants#DEFAULT_INFLIGHT_CAPACITY}（禁用）。可在容器启动前或
     * 运行期调整；运行期调整仅影响之后注册的消费者。
     */
    public void setInflightCapacity(int capacity) {
        tuning.setInflightCapacity(capacity);
    }

    // ===================== 消费者拦截器 =====================

    /**
     * 添加单个消费者拦截器。
     *
     * @param interceptor 拦截器实例
     */
    public void addConsumerInterceptor(ConsumerInterceptor interceptor) {
        ensureFilterCoordinator().addInterceptor(interceptor);
    }

    /**
     * 批量添加消费者拦截器。
     *
     * @param interceptors 拦截器集合
     */
    public void addConsumerInterceptors(Collection<ConsumerInterceptor> interceptors) {
        ensureFilterCoordinator().addInterceptors(interceptors);
    }

    // ===================== 消息过滤器 =====================

    /**
     * 添加单个消费者过滤器（全局维度，消费前过滤）。
     *
     * @param filter 过滤器实例
     */
    public void addConsumerFilter(ConsumerFilter filter) {
        ensureFilterCoordinator().addFilter(filter, store, spiResolver());
    }

    /**
     * 批量添加消费者过滤器（全局维度）。
     *
     * @param filters 过滤器集合
     */
    public void addConsumerFilters(Collection<ConsumerFilter> filters) {
        ensureFilterCoordinator().addFilters(filters, store, spiResolver());
    }

    /**
     * 重建 per-consumer 过滤器缓存（已注册消费者的过滤器变更后调用）。
     *
     * <p>公开方法，便于高级用户在直接修改过滤器后手动触发缓存重建。
     */
    public void rebuildConsumerFilterCache() {
        ensureFilterCoordinator().rebuildFilters(store, spiResolver());
    }

    @Override
    public <T> void registerConsumer(
            StreamMessageConcurrentlyConsumer<T> consumer, StreamMQConsumer annotation) {
        registrar().registerConcurrent(consumer, annotation);
    }

    @Override
    public <T> void registerOrderlyConsumer(
            StreamMessageOrderlyConsumer<T> consumer, StreamMQConsumer annotation) {
        registrar().registerOrderly(consumer, annotation);
    }

    @Override
    public <T> void registerDlqConsumer(
            DlqMessageConsumer<T> consumer, StreamMQDlqConsumer annotation) {
        registrar().registerDlq(consumer, annotation);
    }

    private void resolvePerConsumerSpi(ListenerRegistration<?> reg) {
        spiResolver().resolveInto(reg, store);
    }

    @Override
    public Collection<ConsumerMetadata> getConsumers() {
        return ensureMetadata().getConsumers();
    }

    public boolean rebalanceGroup(String group) {
        return ensureMetadata().rebalanceGroup(group);
    }

    public void registerRetryTargets(RetryScheduler scheduler) {
        ensureMetadata().registerRetryTargets(scheduler);
    }

    public void registerPelClaimTargets(PelClaimScheduler scheduler) {
        ensureMetadata().registerPelClaimTargets(scheduler);
    }

    // ===================== 拆分协调器懒加载 =====================

    private ListenerContainerFilterCoordinator ensureFilterCoordinator() {
        ListenerContainerFilterCoordinator c = filterCoordinator;
        if (c == null) {
            synchronized (this) {
                c = filterCoordinator;
                if (c == null) {
                    c =
                            new ListenerContainerFilterCoordinator(
                                    consumerFilterChain, interceptorChain);
                    filterCoordinator = c;
                }
            }
        }
        return c;
    }

    private ListenerContainerMetadata ensureMetadata() {
        ListenerContainerMetadata m = metadataCoordinator;
        if (m == null) {
            synchronized (this) {
                m = metadataCoordinator;
                if (m == null) {
                    m = new ListenerContainerMetadata(store, schedulerBinder());
                    metadataCoordinator = m;
                }
            }
        }
        return m;
    }

    // ===================== 生命周期方法 =====================
    @Override
    public void start() {
        lifecycle.beginStart();
        ensureRuntimeAlive();
        LOG.info("Starting ListenerContainer with {} registration(s)", store.registrationCount());
        // start 语义为全新启动：复位 pause，避免"pause 后 stop 再 start"重启进静默暂停
        paused = false;
        // 清空上一轮遗留的消费循环启动失败：全新启动后失败会重新登记
        consumeLoopFailures.clear();
        if (!lifecycle.markRunning()) {
            // 竞态守卫：启动期间发生并发 stop（状态已离开 STARTING）时必须中止——
            // 继续登记组管理器/提交读循环会复活已停止的容器并泄漏 Redis 侧注册数据
            LOG.warn(
                    "Container start aborted: lifecycle changed to {} during startup (stop won"
                            + " the race)",
                    lifecycle.current());
            return;
        }
        for (ListenerRegistration<?> reg : store.registrations()) {
            if (!reg.isDlqMode()) {
                store.putGroupManager(reg.key(), groupManagerFactory().createAndRegister(reg));
            }
        }
        doStartListeners();
        if (pelClaimScheduler != null) {
            schedulerBinder().bindPelClaimTargets(pelClaimScheduler);
        }
        LOG.info("ListenerContainer started, state=RUNNING");
    }

    /** 确保执行器与监听器工厂可用。执行器为外部注入时不做任何处理（生命周期归提供方）； 内部默认池在 stop 时关闭、restart 由本方法重建。 */
    private synchronized void ensureRuntimeAlive() {
        if (consumeExecutor.isShutdown()) {
            if (!ownsExecutor) {
                throw new IllegalStateException(
                        "Injected consumeExecutor is shutdown; provide a live executor or call"
                                + " setConsumeExecutor again before restart");
            }
            consumeExecutor = Executors.newVirtualThreadPerTaskExecutor();
            // 必须把新执行器同步给所有"构造时捕获了执行器引用"的协作类（与 setConsumeExecutor 一致）：
            // 否则 restart 后 messageProcessor 仍持有已 shutdown 的旧执行器，消费回调抛
            // RejectedExecutionException——表现为"消费者静默不消费"，极难定位。
            messageProcessor.setExecutor(consumeExecutor);
            LOG.info("Recreated internal consume executor for container restart");
        }
        if (consumerFactory instanceof RedissonStreamListenerFactory redissonFactory) {
            redissonFactory.reopen();
        }
    }

    @Override
    public void stop() {
        if (!lifecycle.tryBeginStop()) {
            LOG.debug("Stop skipped, container already stopped or another stop won the race");
            return;
        }
        LOG.info("Stopping ListenerContainer...");
        // 先取消消费循环，再注销组管理器：避免除名后仍在拉取导致 rebalance 短暂双重消费
        loopSupervisor.cancelAll();
        store.clearGroupManagers();
        consumerFactory.close();
        if (ownsExecutor) {
            consumeExecutor.shutdown();
        }
        try {
            if (!consumeExecutor.awaitTermination(AWAIT_TERMINATION_SECONDS, TimeUnit.SECONDS)) {
                consumeExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            consumeExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        paused = false;
        // 停止后清空：历史失败不应影响下一次 start 的健康判定
        consumeLoopFailures.clear();
        lifecycle.finishStop();
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
        return lifecycle.isRunning();
    }

    public ContainerState getState() {
        return lifecycle.current();
    }

    // ===================== 内部编排方法 =====================
    private void doStartListeners() {
        for (ListenerRegistration<?> reg : store.registrations()) {
            loopSupervisor.submitLoops(reg);
        }
    }

    @Override
    public void unregister(String topic, String consumerGroup) {
        StringUtils.requireValidTopic(topic);
        StringUtils.requireValidGroup(consumerGroup);
        boolean running = lifecycle.isRunning();
        boolean removed = false;
        for (String suffix : new String[] {"", DLQ_KEY_PREFIX}) {
            String key = suffix + topic + REG_KEY_SEPARATOR + consumerGroup;
            ListenerRegistration<?> reg = store.removeRegistration(key);
            if (Objects.isNull(reg)) {
                continue;
            }
            removed = true;
            loopSupervisor.cancelForRegistration(key);
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

    private StreamMQListener createConsumerFor(ListenerRegistration<?> reg, boolean retryMode) {
        // 注册模型是唯一持有者：声明式配置由 ListenerConfig.from 单点派生
        return consumerFactory.createListener(ListenerConfig.from(reg, retryMode));
    }

    /** 循环命令工厂：装配 LoopContext 并提交到消费线程池。 */
    private Future<?> launchLoop(
            ListenerRegistration<?> reg, boolean retryMode, boolean primaryLoop, int loopIndex) {
        ConsumeLoopTask.LoopContext ctx =
                new ConsumeLoopTask.LoopContext(
                        reg,
                        retryMode,
                        primaryLoop,
                        loopIndex,
                        messageProcessor,
                        loopSupervisor,
                        consumeExecutor,
                        lifecycle::isRunning,
                        () -> paused,
                        tuning::inflightCapacity,
                        this::createConsumerFor,
                        this::reportConsumeLoopFailure,
                        this::clearConsumeLoopFailure);
        return consumeExecutor.submit(
                new ConsumeLoopTask(
                        ctx, tuning.getPausedSleepMillis(), tuning.getBrokerErrorBackoffMillis()));
    }

    /**
     * 记录消费循环启动失败，供健康检查与管理端点暴露。
     *
     * <p>失败原因取 {@code rootCause} 的一行摘要：完整堆栈已由 {@code ConsumeLoopTask} 以 ERROR 输出， 这里只保留可放进
     * JSON/健康详情的简短描述，避免把多行堆栈塞进 Actuator 响应。
     */
    private void reportConsumeLoopFailure(String loopKey, Throwable cause) {
        Throwable root = cause;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String reason = root.getClass().getSimpleName();
        if (root.getMessage() != null && !root.getMessage().isBlank()) {
            reason = reason + ": " + root.getMessage();
        }
        consumeLoopFailures.put(loopKey, reason);
        LOG.error(
                "Consume loop failed to start and will not consume: loopKey={}, reason={}."
                        + " The container health indicator will report DOWN for this condition.",
                loopKey,
                reason);
    }

    private void checkBeforeStart() {
        lifecycle.assertRegistrable();
    }

    private void wireRegistrationIfRunning(ListenerRegistration<?> reg) {
        if (!lifecycle.isRunning()) {
            return;
        }
        if (!reg.isDlqMode() && Objects.isNull(store.groupManager(reg.key()))) {
            store.putGroupManager(reg.key(), groupManagerFactory().createAndRegister(reg));
        }
        loopSupervisor.submitLoops(reg);
        LOG.info(
                "Dynamically wired registration while container running: topic={}, group={}",
                reg.getTopic(),
                reg.getGroup());
    }

    // ===================== 协作类懒构建与覆盖点（仅 INIT 状态可覆盖） =====================

    /**
     * 懒构建协作类统一采用 double-checked locking（与 {@link #ensureFilterCoordinator} / {@link
     * #ensureMetadata} 保持一致）：并发注册消费者 / 并发 start 时保证单例，避免创建出多个不等价实例（此前 4 个懒加载点无同步， 多线程同时注册可能创建多个
     * {@link DefaultListenerRegistrar} / 组管理器工厂）。
     */
    private PerConsumerSpiResolver spiResolver() {
        PerConsumerSpiResolver current = spiResolver;
        if (current == null) {
            synchronized (this) {
                current = spiResolver;
                if (current == null) {
                    current =
                            new DefaultPerConsumerSpiResolver(
                                    redisson,
                                    messageConverter,
                                    retryPolicy,
                                    globalDlqFailureStrategy,
                                    dlqConfig,
                                    interceptorChain,
                                    consumerFilterChain,
                                    () -> filterResolver,
                                    () -> defaultVirtualNodes,
                                    () -> metrics,
                                    defaultRebalanceStrategy,
                                    perConsumerEnabled);
                    spiResolver = current;
                }
            }
        }
        return current;
    }

    private ConsumerGroupManagerFactory groupManagerFactory() {
        ConsumerGroupManagerFactory current = groupManagerFactory;
        if (current == null) {
            synchronized (this) {
                current = groupManagerFactory;
                if (current == null) {
                    current =
                            new DefaultConsumerGroupManagerFactory(
                                    redisson,
                                    spiResolver(),
                                    () -> heartbeatIntervalMs,
                                    () -> instanceTimeoutMs);
                    groupManagerFactory = current;
                }
            }
        }
        return current;
    }

    private ListenerRegistrar registrar() {
        ListenerRegistrar current = registrar;
        if (current == null) {
            synchronized (this) {
                current = registrar;
                if (current == null) {
                    current =
                            new DefaultListenerRegistrar(
                                    lifecycle,
                                    store,
                                    spiResolver(),
                                    tuning,
                                    defaultNamespace,
                                    instanceToken,
                                    defaultConsumeFromWhere,
                                    (defaultNs, topic, group, ns, shardCount) -> {
                                        Lock[] array =
                                                shardLockManager.createShardLocks(
                                                        defaultNs, topic, group, ns, shardCount);
                                        return Objects.nonNull(array) ? Arrays.asList(array) : null;
                                    },
                                    this::wireRegistrationIfRunning);
                    registrar = current;
                }
            }
        }
        return current;
    }

    private SchedulerTargetBinder schedulerBinder() {
        SchedulerTargetBinder current = schedulerBinder;
        if (current == null) {
            synchronized (this) {
                current = schedulerBinder;
                if (current == null) {
                    current = new DefaultSchedulerTargetBinder(store);
                    schedulerBinder = current;
                }
            }
        }
        return current;
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
