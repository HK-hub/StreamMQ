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
import io.github.streammq.adapter.redisson.support.BroadcastGroupNaming;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.annotation.StreamMQDlqConsumer;
import io.github.streammq.core.broadcast.BroadcastInstanceIdResolver;
import io.github.streammq.core.consumer.*;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.ConsumeFromWhere;
import io.github.streammq.core.enums.ConsumeMode;
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
 *   <li>实现 core 契约 {@link InFlightAware}（K2）：以真实在途消息计数支撑优雅关闭的收敛判据
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
public class DefaultStreamMQListenerContainer implements StreamMQListenerContainer, InFlightAware {

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

    /**
     * 广播消费实例身份解析器（可选注入）。
     *
     * <p>非 null 时，每个广播模式注册都会通过它申请<b>跨重启稳定</b>的持久化实例身份（配置 → 本地持久文件 → Redis 注册中心回收 → Redis 注册中心分配 →
     * 随机降级）。这是广播消费"重启不丢 PEL、不重放历史"的根本保障。
     *
     * <p>为 null 时退化为 0.1.1 行为（主机名 + 进程内序号），重启会产生新的广播消费者组。
     */
    private volatile BroadcastInstanceIdResolver broadcastInstanceResolver;

    /** 显式配置的广播实例身份（可为 null，表示交由解析器自行读取系统属性/环境变量）。 */
    private volatile java.util.function.Supplier<String> configuredBroadcastInstanceId;

    /** 全局重试策略（per-consumer 未指定时的回退） */
    private final RetryPolicy retryPolicy;

    /** 全局死信消费失败策略（per-consumer 未指定时的回退） */
    private final DlqFailureStrategy globalDlqFailureStrategy;

    private final String defaultNamespace;

    /** 全局消费者过滤器链 */
    private final ConsumerFilterChain consumerFilterChain = new DefaultConsumerFilterChain();

    /** 注册存储（接口注入，默认实现见构造器；start 前可通过 setter 覆盖） */
    private final RegistrationStore store = new DefaultRegistrationStore();

    /**
     * 消费线程池：默认统一使用虚拟线程池；Spring 环境由自动装配注入用户自定义实现。 所有权规则：容器内部创建的默认池在 stop 时关闭；外部注入的池由提供方管理生命周期。
     *
     * <p><b>volatile 是必需的：</b>本字段会在 {@code setConsumeExecutor} / {@code ensureRuntimeAlive}（持有
     * {@code synchronized(this)}）中被替换，而在<b>不持锁</b>的 {@code stop()} / {@code launchLoop()} 中被读取。 非
     * volatile 时，另一个线程（如调用 stop 的 Spring 生命周期线程）可能读到过期的执行器引用， 导致"关闭了旧池却仍向旧池提交任务"或反之。
     */
    private volatile ExecutorService consumeExecutor = Executors.newVirtualThreadPerTaskExecutor();

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
     * 注入广播消费实例身份解析器（仅 INIT 状态允许）。
     *
     * <p>注入后，广播模式注册的消费者组名将使用<b>跨重启稳定</b>的持久化身份， 重启后复用同一 Redis 消费者组：PEL 不丢、位点连续、不重放历史。 未注入时退化为旧的"主机名
     * + 进程内序号"，重启会产生新组。
     *
     * @param resolver 解析器，null 表示禁用持久化广播身份
     * @param configuredId 显式配置的实例身份提供源，可为 null
     */
    public void setBroadcastInstanceResolver(
            BroadcastInstanceIdResolver resolver,
            java.util.function.Supplier<String> configuredId) {
        assertInitState("broadcastInstanceResolver");
        this.broadcastInstanceResolver = resolver;
        this.configuredBroadcastInstanceId = configuredId;
        // 把注册中心同步给监听器工厂：广播监听器需在每次心跳时续租其身份槽位，
        // 否则运行中的实例也会落进"可回收"窗口，被同主机上的另一个同 group 广播消费者抢走。
        if (consumerFactory instanceof RedissonStreamListenerFactory redissonFactory) {
            redissonFactory.setBroadcastInstanceRegistry(
                    Objects.isNull(resolver) ? null : resolver.registry());
        }
    }

    /**
     * 返回当前广播消费实例身份解析器（可能为 null）。
     *
     * @return 解析器
     */
    public BroadcastInstanceIdResolver getBroadcastInstanceResolver() {
        return broadcastInstanceResolver;
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

    /** 运行期暂停标志（独立于生命周期状态）——容器级：暂停全部注册 */
    private volatile boolean paused = false;

    /**
     * 运行期"按注册维度"暂停的消费者组集合（R1-6 ②）。
     *
     * <p>此前 {@code paused} 是容器级布尔：管理端点对某个 group 暂停会停掉整容器的全部注册。现按 group 维度 记录暂停标志，每个读循环在每轮迭代读取
     * {@code containerPaused || groupPaused(reg.group)}；容器级 {@link #pause()} 仍可全停。
     */
    private final java.util.Set<String> pausedGroups =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** 消费循环监督者（Command 登记表：幂等提交/并发度/按注册取消/重注册替换） */
    private final DefaultConsumeLoopSupervisor loopSupervisor =
            new DefaultConsumeLoopSupervisor(this::launchLoop);

    /**
     * 已接线的注册键集合（R1-7）：用于在"同一 (topic, group) 运行期重复注册"时改走 {@link
     * DefaultConsumeLoopSupervisor#replaceLoops}，以新注册替换旧循环（此前静默不生效）。
     */
    private final java.util.Set<String> wiredRegistrations =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

    /**
     * 顺序消费延迟重投队列（R1-1）：分片锁竞争 / ORDERLY defer / DLQ 转投失败三条路径的本地重投， 由各注册的 primary 读循环驱动；容器停止时清空（剩余消息由
     * PEL 认领兜底）。
     */
    private final OrderlyDeferredRetryQueue orderlyDeferredRetryQueue =
            new OrderlyDeferredRetryQueue();

    /** 每个注册键最近一次启动循环时采用的背压容量快照（R1-6 ③：回显 inflightCapacity 是否已生效）。 */
    private final java.util.Map<String, Integer> appliedInflightCapacity =
            new java.util.concurrent.ConcurrentHashMap<>();

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

    /**
     * 单条消息消费管线。
     *
     * <p>声明为实现类（而非 {@link MessageProcessor} 接口）：容器需要调用接口之外的容器内部扩展点 （延迟重投队列注入 / DLQ
     * 转投重试，R1-1）。接口文件不在本轮修改范围内，故不提升到接口。
     */
    private final DefaultMessageProcessor messageProcessor;

    /** 是否启用 per-consumer 策略实例化（高级构造器注入自定义 handler 时关闭） */
    private final boolean perConsumerEnabled;

    /** 全局 DLQ 失败策略 */
    private final DlqFailureStrategy dlqFailureStrategy;

    /** 全局 DLQ 配置 */
    private final DlqConfig dlqConfig;

    /** 顺序消费 PEL 认领调度器（可选，注入后容器启动时注册目标） */
    private volatile PelClaimScheduler pelClaimScheduler;

    /**
     * 重试调度器（可选，经 {@link #registerRetryTargets(RetryScheduler)} 注入后保留引用）。
     *
     * <p>保留引用的唯一目的：运行期动态注册的消费者必须能<b>单独</b>补绑其重试目标——否则该消费者 失败消息写进重试 ZSet 后无人扫描，仅在进程重启时才被补绑（期间
     * payload 过期即静默丢失）。
     */
    private volatile RetryScheduler retryScheduler;

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
     * Spring 应用上下文（可选注入，Object 类型以保持 redisson 模块零 Spring 编译依赖）。
     *
     * <p>启动器（spring-boot-starter）在装配时通过 {@link #setApplicationContext(Object)} 注入； 用于 per-consumer
     * SPI 的「容器优先 → 反射兜底」解析（P1-4 修复：避免 Spring Bean 被静默忽略）。
     */
    private volatile Object applicationContext;

    /**
     * 注入 Spring 应用上下文（仅 INIT 状态允许；非 Spring 环境保持 null 即可）。
     *
     * @param applicationContext Spring ApplicationContext 实例（反射调用其 getBeanNamesForType / getBean
     *     方法）
     */
    public void setApplicationContext(Object applicationContext) {
        assertInitState("applicationContext");
        this.applicationContext = applicationContext;
    }

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

    /**
     * 注入暂停休眠间隔（毫秒，{@code streammq.consumer.paused-sleep-millis}）。
     *
     * <p><b>R1-6 ①：</b>消费循环每轮读取该值（supplier），运行期修改对<b>已运行</b>的循环即时生效。
     */
    public void setPausedSleepMillis(long millis) {
        tuning.setPausedSleepMillis(millis);
    }

    /**
     * 注入 Broker 异常退避间隔（毫秒，{@code streammq.consumer.broker-error-backoff-millis}）。
     *
     * <p><b>R1-6 ①：</b>消费循环每轮读取该值（supplier），运行期修改对<b>已运行</b>的循环即时生效。
     */
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
     * 构造容器（便捷重载：内部创建默认 DLQ 失败策略与配置，背压容量取 {@link StreamMQConstants#DEFAULT_INFLIGHT_CAPACITY}）。
     *
     * <p>所有默认值在此显式声明并统一委派给全参构造，避免多重载间默认语义漂移。 需要自定义 DLQ 策略/配置或背压容量时请使用 {@link
     * #DefaultStreamMQListenerContainer(RedissonClient, StreamMQListenerFactory, MessageConverter,
     * RetryPolicy, DlqFailureStrategy, DlqConfig, String, int)}。
     */
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
                defaultNamespace,
                StreamMQConstants.DEFAULT_INFLIGHT_CAPACITY);
    }

    /**
     * 构造容器并注入 DLQ 失败策略与配置（便捷重载，背压容量取 {@link StreamMQConstants#DEFAULT_INFLIGHT_CAPACITY}）。
     *
     * <p>默认值统一委派给全参构造，避免多重载间默认语义漂移。
     */
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
        // R1-1：延迟重投队列必须与读循环共享同一实例（三条登记路径 → primary 循环排空）
        this.messageProcessor.setOrderlyDeferredRetryQueue(orderlyDeferredRetryQueue);
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
        // R1-1：延迟重投队列必须与读循环共享同一实例（三条登记路径 → primary 循环排空）
        this.messageProcessor.setOrderlyDeferredRetryQueue(orderlyDeferredRetryQueue);
        this.messageProcessor.setRuntimeStats(runtimeStats);
    }

    /**
     * 设置背压队列容量（{@code >0} 启用：拉取与处理解耦，队列满时拉取阻塞；{@code 0} 禁用）。
     *
     * <p>默认 {@link StreamMQConstants#DEFAULT_INFLIGHT_CAPACITY}（禁用）。
     *
     * <p><b>生效时机（R1-6 ③）：</b>队列容量是消费循环启动时的构造参数，<b>运行期不可热改</b>—— 本 setter 只对之后启动的循环生效。管理端点/调用方用
     * {@link #isInflightCapacityApplied(String, String)} 或 {@link
     * #isInflightCapacityAppliedForGroup(String)} 判断当前值是否已被运行中的循环采用， 不得直接宣称"已生效"。
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
        // 保留引用供运行期动态注册补绑（见 retryScheduler 字段说明），再执行启动期批量绑定。
        this.retryScheduler = Objects.requireNonNull(scheduler, "scheduler");
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
        try {
            ensureRuntimeAlive();
            // R6-CLUSTER：Cluster 部署下多 key 原子脚本（重试/DLQ、PEL 认领、事务、广播注册表）会被
            // 服务端以 CROSSSLOT 拒绝。启动即提示一次，避免用户只能从运行期裸异常反推拓扑问题。
            io.github.streammq.adapter.redisson.support.RedisClusterCompatibility.warnIfCluster(
                    redisson, "consumer container");
            LOG.info(
                    "Starting ListenerContainer with {} registration(s)",
                    store.registrationCount());
            // start 语义为全新启动：复位 pause，避免"pause 后 stop 再 start"重启进静默暂停
            paused = false;
            // 清空上一轮遗留的消费循环启动失败：全新启动后失败会重新登记
            consumeLoopFailures.clear();
            if (!lifecycle.markRunning()) {
                // 竞态守卫：启动期间发生并发 stop（状态已离开 STARTING）时必须中止——
                // 继续登记组管理器/提交读循环会复活已停止的容器并泄漏 Redis 侧注册数据
                LOG.warn(
                        "Container start aborted: lifecycle changed to {} during startup (stop"
                                + " won the race)",
                        lifecycle.current());
                return;
            }
            for (ListenerRegistration<?> reg : store.registrations()) {
                // DLQ 注册同样要建组管理器（B-10）：PelClaimScheduler 的 DLQ 目标按
                // consumerGroupInstances(ns, group) 判断 pending 属主是否存活，而该实例心跳行只能
                // 由组管理器写入。此前整体跳过 DLQ 注册 → 独立部署的 DLQ 消费者心跳缺行 →
                // isOwnerConsumerAlive 恒 false → 活跃慢 DLQ 消费者的 pending 被尾部复制重投。
                // DLQ 消费者名（{group}-{instanceToken}）与实例行 instanceId 同源，登记后判活可精确命中。
                store.putGroupManager(reg.key(), groupManagerFactory().createAndRegister(reg));
                if (!lifecycle.isRunning()) {
                    // 竞态守卫（与动态注册路径 wireRegistrationIfRunning 对齐）：
                    // createAndRegister 内部写 instances Hash + 订阅 RTopic + 启动心跳，
                    // 期间并发 stop 已清理完毕；此刻若已非 RUNNING，必须撤销刚登记的组管理器，
                    // 否则心跳线程会永久续写实例行（幽灵成员），且后续 stop 因状态非 RUNNING 直接返回，
                    // 再也无人回收。
                    LOG.warn(
                            "Container start aborted after group-manager registration:"
                                    + " lifecycle changed to {} (stop won the race);"
                                    + " unregistering the just-created manager",
                            lifecycle.current());
                    store.clearGroupManagers();
                    return;
                }
            }
            doStartListeners();
            if (pelClaimScheduler != null) {
                schedulerBinder().bindPelClaimTargets(pelClaimScheduler);
            }
            LOG.info("ListenerContainer started, state=RUNNING");
        } catch (RuntimeException ex) {
            rollbackStartOnFailure(ex);
            throw ex;
        }
    }

    /**
     * 启动中途失败时的状态回滚。
     *
     * <p>必须把 STARTING 回滚为 STOPPED：否则容器卡死在 STARTING —— 既无法重新 {@code start()}（要求 INIT），也无法再 {@code
     * setConsumeExecutor}（要求 INIT），文档给出的补救路径反而不可达。若失败已发生在 RUNNING 之后，则执行一次 stop 释放部分注册状态。
     */
    private void rollbackStartOnFailure(RuntimeException cause) {
        if (lifecycle.current() == ContainerState.STARTING && lifecycle.tryBeginStop()) {
            lifecycle.finishStop();
            LOG.warn(
                    "Container start failed during STARTING; lifecycle rolled back to STOPPED",
                    cause);
        } else if (lifecycle.isRunning()) {
            LOG.warn(
                    "Container start failed after RUNNING; stopping to release partial state",
                    cause);
            try {
                stop();
            } catch (RuntimeException stopEx) {
                LOG.error("Cleanup stop after failed start also failed", stopEx);
            }
        }
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
        // R1-1：延迟重投队列不持久化，停止时清空（未 ACK 消息留在 PEL 等认领兜底）
        orderlyDeferredRetryQueue.clearAll();
        wiredRegistrations.clear();
        appliedInflightCapacity.clear();
        pausedGroups.clear();
        for (ListenerRegistration<?> reg : store.registrations()) {
            releaseBroadcastInstance(reg);
        }
        store.clearGroupManagers();
        // C-02 修复：先排空在途消费线程（等消费循环真正退出/消息处理完成），再关闭 listener。
        // 旧顺序（先 close listener 再 await）的缺陷：停机窗口内 in-flight 消息调用 ack() 必抛
        // IllegalStateException（listener 已 closed）——SUCCESS 消息滞留 PEL，重启后重复消费。
        if (ownsExecutor) {
            consumeExecutor.shutdown();
            try {
                if (!consumeExecutor.awaitTermination(
                        AWAIT_TERMINATION_SECONDS, TimeUnit.SECONDS)) {
                    consumeExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                consumeExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            consumerFactory.close();
        } else {
            // 执行器为外部注入（共享）时绝不可关闭：其生命周期归提供方（如 Spring 的
            // streammqExecutor），误关会中断事件总线/异步发送/事务回查，并拖慢停机。
            // 共享池场景无法 await 终止；消费循环检测到 isRunning=false 后自然退出，
            // close 与在途 ack 的竞争窗口极短（at-least-once 下最坏产生一次重复消费，不丢消息）。
            LOG.debug("Injected (shared) consumeExecutor retained on stop; not shutting down");
            consumerFactory.close();
        }
        paused = false;
        // 停止后清空：历史失败不应影响下一次 start 的健康判定
        consumeLoopFailures.clear();
        lifecycle.finishStop();
        LOG.info("ListenerContainer stopped, state=STOPPED");
    }

    /** 容器级暂停：暂停本容器<b>全部</b>注册的消费循环（每轮迭代读取，立即对已运行循环生效）。 */
    @Override
    public void pause() {
        paused = true;
        LOG.info("ListenerContainer paused");
    }

    /** 容器级恢复：恢复全部注册（不影响 {@link #pauseGroup(String)} 单独暂停的组）。 */
    @Override
    public void resume() {
        paused = false;
        LOG.info("ListenerContainer resumed");
    }

    /**
     * 按消费者组暂停（R1-6 ②）：只暂停该 group 的注册，其它消费者组不受影响。
     *
     * <p>供管理端点使用（管理端点按 group 维度下发配置）。每个读循环在每轮迭代读取该标志， 对已运行的循环即时生效；恢复用 {@link #resumeGroup(String)}。
     *
     * @param consumerGroup 消费者组名
     */
    public void pauseGroup(String consumerGroup) {
        if (Objects.nonNull(consumerGroup) && !consumerGroup.isBlank()) {
            pausedGroups.add(consumerGroup);
            LOG.info("ListenerContainer paused for group={}", consumerGroup);
        }
    }

    /**
     * 按消费者组恢复（R1-6 ②）。
     *
     * @param consumerGroup 消费者组名
     */
    public void resumeGroup(String consumerGroup) {
        if (Objects.nonNull(consumerGroup) && pausedGroups.remove(consumerGroup)) {
            LOG.info("ListenerContainer resumed for group={}", consumerGroup);
        }
    }

    /** 指定消费者组是否处于暂停状态（R1-6 ②；含容器级暂停——容器级暂停时所有组都视为暂停）。 */
    public boolean isGroupPaused(String consumerGroup) {
        return paused || (Objects.nonNull(consumerGroup) && pausedGroups.contains(consumerGroup));
    }

    /** 容器级暂停标志（不含按组暂停）。 */
    public boolean isPaused() {
        return paused;
    }

    /**
     * 读循环的暂停判定（R1-6 ②）：容器级暂停 或 该注册所属 group 被单独暂停。
     *
     * <p>由 {@code launchLoop} 注入 LoopContext，读循环每轮迭代调用——因此运行期 {@link #pauseGroup(String)}
     * 对已运行循环立即生效，且只影响目标 group 的注册。
     *
     * @param reg 注册信息
     * @return 该注册当前是否应暂停
     */
    java.util.function.BooleanSupplier pausedSupplierFor(ListenerRegistration<?> reg) {
        return () -> paused || pausedGroups.contains(reg.getGroup());
    }

    @Override
    public boolean isRunning() {
        return lifecycle.isRunning();
    }

    public ContainerState getState() {
        return lifecycle.current();
    }

    // ===================== 在途消息计数（K2：优雅关闭的收敛判据） =====================

    /**
     * {@inheritDoc}
     *
     * <p><b>口径：</b>当前正在执行 handler（消息已进入消费管线、尚未完成 ACK/NACK 路由）的条数，由 {@link
     * DefaultMessageProcessor#processMessage} 入口自增、finally 自减；不包含排队待处理、重试 ZSet 中、 PEL
     * 中滞留的消息。读值随处理进度实时变化，停止后自然归零（{@code processMessage} 的 finally 保证无泄漏）。
     *
     * <p>kubernetes 优雅关闭处理器（{@code GracefulShutdownHandler}）在 pause 后按有界轮询读取本值：归零即提前结束 grace
     * 等待。已知近似：消费超时取消后业务线程超出宽限期仍未终止时，计数会先归零（消息已按 RECONSUME_LATER 路由）。
     */
    @Override
    public int getInFlightCount() {
        return messageProcessor.inFlightCount();
    }

    // ===================== 内部编排方法 =====================
    private void doStartListeners() {
        for (ListenerRegistration<?> reg : store.registrations()) {
            loopSupervisor.submitLoops(reg);
            // R1-7：登记"已接线"，运行期重注册才能识别为替换而非新增
            wiredRegistrations.add(reg.key());
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
            // R1-7：解除"已接线"标记；R1-1：清空该注册的延迟重投条目（未 ACK 消息留在 PEL 等认领）
            wiredRegistrations.remove(key);
            appliedInflightCapacity.remove(key);
            orderlyDeferredRetryQueue.clear(reg);
            // 解除调度目标：否则调度器会一直扫描已注销的 (topic, group)，开销随注册变更单调增长
            schedulerBinder().unbindTargets(retryScheduler, pelClaimScheduler, reg);
            store.removeFilters(key);
            store.removeHandler(key);
            store.removeAndUnregisterGroupManager(key);
            releaseBroadcastInstance(reg, topic);
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

    /**
     * 整机停机释放：刷新实例身份槽位心跳并保留其全部主题，使同主机实例在宽限期内重启能回收同一身份、复用 PEL。 消费者组的真正销毁由 sweep 在超过宽限期后执行（{@code
     * release} 语义为"释放 ≠ 销毁"）。
     */
    private void releaseBroadcastInstance(ListenerRegistration<?> reg) {
        if (broadcastInstanceResolver == null || reg.getConsumeMode() != ConsumeMode.BROADCASTING) {
            return;
        }
        String instanceId =
                BroadcastGroupNaming.instanceIdFromConsumerName(
                        reg.getGroup(), reg.getConsumerName());
        if (instanceId == null) {
            return;
        }
        try {
            broadcastInstanceResolver.release(reg.getNamespace(), reg.getGroup(), instanceId);
        } catch (RuntimeException ignore) {
            // 优雅释放失败不影响停机：心跳过期后由清扫任务兜底回收
        }
    }

    /**
     * 注销单个主题时的按 topic 维度释放：仅从槽位主题集合移除该主题。若移除后槽位仍持有其它主题则保留（保 PEL），
     * 若集合清空则删除槽位，使被注销主题的消费者组可由清扫任务回收，而不会误伤其它仍在消费的同组主题。
     */
    private void releaseBroadcastInstance(ListenerRegistration<?> reg, String topic) {
        if (broadcastInstanceResolver == null
                || reg.getConsumeMode() != ConsumeMode.BROADCASTING
                || topic == null
                || topic.isBlank()) {
            return;
        }
        String instanceId =
                BroadcastGroupNaming.instanceIdFromConsumerName(
                        reg.getGroup(), reg.getConsumerName());
        if (instanceId == null) {
            return;
        }
        try {
            broadcastInstanceResolver.release(
                    reg.getNamespace(), reg.getGroup(), instanceId, List.of(topic));
        } catch (RuntimeException ignore) {
            // 优雅释放失败不影响注销：心跳过期后由清扫任务兜底回收
        }
    }

    private StreamMQListener createConsumerFor(ListenerRegistration<?> reg, boolean retryMode) {
        // 注册模型是唯一持有者：声明式配置由 ListenerConfig.from 单点派生
        return consumerFactory.createListener(ListenerConfig.from(reg, retryMode));
    }

    /** 循环命令工厂：装配 LoopContext 并提交到消费线程池。 */
    private Future<?> launchLoop(
            ListenerRegistration<?> reg, boolean retryMode, boolean primaryLoop, int loopIndex) {
        // R1-6 ③：记录本循环启动时采用的背压容量快照，供 isInflightCapacityApplied 如实回显
        appliedInflightCapacity.put(reg.key(), tuning.inflightCapacity());
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
                        // R1-6 ②：容器级暂停 或 该 group 被单独暂停
                        pausedSupplierFor(reg),
                        tuning::inflightCapacity,
                        this::createConsumerFor,
                        this::reportConsumeLoopFailure,
                        this::clearConsumeLoopFailure,
                        // R1-6 ①：每轮读取，运行期 setter 对已运行循环即时生效
                        tuning::getPausedSleepMillis,
                        tuning::getBrokerErrorBackoffMillis,
                        // R1-5：暂停期心跳按容器心跳间隔节流
                        () -> heartbeatIntervalMs,
                        // R1-1：延迟重投队列（仅 primary 循环排空）
                        orderlyDeferredRetryQueue,
                        this::dispatchDeferredRetry);
        return consumeExecutor.submit(new ConsumeLoopTask(ctx));
    }

    /**
     * R1-1：延迟重投执行器——重投走与正常消息相同的消费管线。
     *
     * <p>{@code DLQ_ROUTE} 条目只重试 DLQ 转投（重试预算已耗尽，不重新执行业务 handler）；其余条目 走 {@code
     * processMessage}（自然再次经过分片锁：锁空闲即成功并由既有 ACK 路径 ACK，仍繁忙则再次登记退避）。
     */
    private void dispatchDeferredRetry(
            OrderlyDeferredRetryQueue.Entry entry,
            ListenerRegistration<?> reg,
            StreamMQListener listener) {
        if (entry.kind() == OrderlyDeferredRetryQueue.Kind.DLQ_ROUTE) {
            if (!messageProcessor.retryDeferredDlqRoute(entry.message(), reg, listener)) {
                // 仍失败：重新登记（退避），消息始终留在 PEL
                orderlyDeferredRetryQueue.deferDlqRouteFailure(reg, entry.message());
            }
            return;
        }
        messageProcessor.processMessage(entry.message(), reg, listener);
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
        // R1-7：同一 (topic, group) 运行期重复注册 = 替换而非新增。旧循环继续消费、新 consumer
        // 永不生效（旧实现 submitLoops 幂等守卫静默返回）——已接线过的注册键必须走 replaceLoops。
        boolean reRegistered = !wiredRegistrations.add(reg.key());
        // DLQ 注册与业务注册一致地建组管理器：DLQ 消费者同样需要实例心跳行，
        // 否则 PelClaim 的 DLQ 目标判活恒为 false（活跃慢 DLQ 消费者被复制重投，见 B-10）。
        if (Objects.isNull(store.groupManager(reg.key()))) {
            ConsumerGroupManager manager = groupManagerFactory().createAndRegister(reg);
            store.putGroupManager(reg.key(), manager);
            // 竞态防护（C-01）：若 put 期间容器已进入停止流程（stop 的 clearGroupManagers 已执行完），
            // 该 manager 会被漏掉，其心跳持续上报形成僵尸组（daemon 线程虽不致 JVM 挂死，但会污染注册表）。
            // 复查状态并立即回滚注销——与 stop() 的 clearGroupManagers 二者必有一个先看到对方。
            if (!lifecycle.isRunning()) {
                store.removeAndUnregisterGroupManager(reg.key());
                wiredRegistrations.remove(reg.key());
                LOG.warn(
                        "Container stopped during dynamic registration; orphan group manager"
                                + " rolled back: topic={}, group={}",
                        reg.getTopic(),
                        reg.getGroup());
                return;
            }
        }
        if (reRegistered) {
            // 先取消旧循环与 inflight 泵，再以新注册提交新循环（R1-7）
            loopSupervisor.replaceLoops(reg);
            orderlyDeferredRetryQueue.clear(reg);
        } else {
            loopSupervisor.submitLoops(reg);
        }
        // 调度目标必须同步补绑：动态注册的消费者若缺重试目标，其失败消息写入重试 ZSet 后
        // 永远无人扫描（payload 7 天后过期 → 隔离/丢失）；缺 PEL 认领目标则崩溃遗留的 pending
        // 无人恢复。两条都属于"能消费但部分消息静默不重投"，极难排查。
        schedulerBinder().bindTargets(retryScheduler, pelClaimScheduler, reg);
        LOG.info(
                "Dynamically {} registration while container running: topic={}, group={}",
                reRegistered ? "replaced" : "wired",
                reg.getTopic(),
                reg.getGroup());
    }

    // ===================== 运行期配置生效性回显（R1-6 ③） =====================

    /**
     * 判断当前配置的背压队列容量是否已被该注册的运行中循环采用（R1-6 ③，供管理端点如实回显）。
     *
     * <p>背压队列容量是消费循环启动时构造的（{@code MessageSink.forCapacity}），<b>无法热改</b>： 运行期 {@link
     * #setInflightCapacity(int)} 只对该注册之后启动的循环生效。本方法语义：
     *
     * <ul>
     *   <li>{@code true} = 该注册最近一次启动的循环读取到的容量 == 当前配置值（或该注册尚未启动过循环， 不存在"运行中的旧值"）
     *   <li>{@code false} = 循环启动后配置又被修改，需要重启消费循环（换新值）才能生效
     * </ul>
     *
     * @param topic 主题
     * @param consumerGroup 消费者组
     * @return 运行中的循环是否已采用当前值
     */
    public boolean isInflightCapacityApplied(String topic, String consumerGroup) {
        Integer applied = appliedInflightCapacity.get(topic + REG_KEY_SEPARATOR + consumerGroup);
        return Objects.isNull(applied) || applied == tuning.inflightCapacity();
    }

    /**
     * group 维度版本：该 group 下<b>全部</b>注册的运行中循环是否都已采用当前背压容量（R1-6 ③）。
     *
     * <p>管理端点按 group 下发配置，用本方法决定 {@code inflightCapacity} 是回显"已生效"还是 "下次循环启动时生效"。
     *
     * @param consumerGroup 消费者组
     * @return true = 该组全部注册均满足 {@link #isInflightCapacityApplied(String, String)}
     */
    public boolean isInflightCapacityAppliedForGroup(String consumerGroup) {
        for (ListenerRegistration<?> reg : store.registrations()) {
            if (Objects.equals(reg.getGroup(), consumerGroup)
                    && !isInflightCapacityApplied(reg.getTopic(), consumerGroup)) {
                return false;
            }
        }
        return true;
    }

    /** 测试钩子：登记某注册的循环启动容量快照（仅同包测试使用；运行期由 {@code launchLoop} 记录）。 */
    void recordAppliedInflightCapacityForTest(String topic, String consumerGroup, int capacity) {
        appliedInflightCapacity.put(topic + REG_KEY_SEPARATOR + consumerGroup, capacity);
    }

    /**
     * 设置顺序消费分片锁的有限租约（毫秒，R1-9；{@code 0} = 默认看门狗续期 + 严格有序）。
     *
     * <p>由 starter 侧属性 {@code streammq.consumer.orderly-shard-lock-lease-millis} 注入。{@code > 0} 时
     * 分片锁使用有限租约且不续期：持有者进程卡死（handler 不响应中断）超时后其它实例可接管， 语义降级为"至多一次重叠执行、可能乱序"（详见 {@code
     * RedissonOrderlyShardLockManager} javadoc）。
     *
     * @param millis 租约毫秒数，{@code >= 0}；{@code > 0} 建议不小于 5000
     */
    public void setOrderlyShardLockLeaseMillis(long millis) {
        tuning.setOrderlyShardLockLeaseMillis(millis);
        if (shardLockManager
                instanceof
                io.github.streammq.adapter.redisson.lock.RedissonOrderlyShardLockManager
                                redissonLockManager) {
            redissonLockManager.setLeaseMillis(tuning.getOrderlyShardLockLeaseMillis());
        }
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
                                    applicationContext,
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
                                    this::wireRegistrationIfRunning,
                                    broadcastInstanceResolver,
                                    configuredBroadcastInstanceId);
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
