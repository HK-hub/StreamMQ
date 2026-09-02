/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.exception.StreamMQBrokerException;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.ListenerType;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.util.CollectionUtils;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 单个读循环任务（God class 拆分第二轮）。
 *
 * <p><b>设计模式：Template Method。</b>{@link #run()} 固化读循环骨架：
 *
 * <pre>
 * 创建监听器 → [钩子] 装配派发策略 → [钩子] PEL 启动排空 → 主循环{ 暂停让位 | 拉取 | 派发 } → 退出日志
 * </pre>
 *
 * 变化点全部以参数/钩子注入：{@code retryMode}（拉取 retry Stream）、{@code primaryLoop} （是否负责 PEL 排空与 inflight
 * 泵登记）、{@link MessageSink}（同步直发 vs 有界队列解耦）。 错误处理策略固化在骨架中：中断退出、Broker 异常退避、意外异常退避。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
final class ConsumeLoopTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumeLoopTask.class);

    /**
     * 运行期连续可恢复失败的告警阈值。
     *
     * <p>连续失败达到该值后，循环通过 {@code LoopFailureReporter} 上报健康信号（HealthIndicator DOWN）；
     * 任一成功拉取即复位。阈值避免把瞬时抖动（单次网络闪断）误判为持续故障，也不让真实故障 长期对健康检查失明。
     */
    static final int RUNTIME_FAILURE_REPORT_THRESHOLD = 10;

    /** 暂停状态下消费循环的休眠间隔（毫秒）——由容器从 {@code streammq.consumer.paused-sleep-millis} 注入 */
    private final long pausedSleepMillis;

    /** Broker 异常后消费循环的退避休眠间隔（毫秒）——由容器从 {@code streammq.consumer.broker-error-backoff-millis} 注入 */
    private final long brokerErrorBackoffMillis;

    /** 循环依赖集合（由容器装配，字段收拢避免长参数列表——Parameter Object）。 */
    record LoopContext(
            ListenerRegistration<?> reg,
            boolean retryMode,
            boolean primaryLoop,
            int loopIndex,
            MessageProcessor processor,
            ConsumeLoopSupervisor supervisor,
            java.util.concurrent.ExecutorService executor,
            java.util.function.BooleanSupplier running,
            java.util.function.BooleanSupplier paused,
            java.util.function.IntSupplier inflightCapacity,
            ListenerFactory listenerFactory,
            LoopFailureReporter failureReporter,
            LoopFailureCleaner failureCleaner) {

        /** 监听器创建函数式抽象（容器侧委托 {@code ListenerConfig.from(reg, retryMode)}）。 */
        interface ListenerFactory {
            StreamMQListener create(ListenerRegistration<?> reg, boolean retryMode);
        }

        /**
         * 循环失败上报通道（loopKey, 失败原因）。
         *
         * <p>必须存在：此前监听器创建失败只会打一条 ERROR 日志然后静默退出循环——消费者在 {@code /actuator/streammq/groups}
         * 里仍然可见、健康检查仍然 UP，运维只能靠"消息没人消费" 这一现象反推。上报后容器可将其纳入健康状态与管理端点。
         *
         * <p><b>覆盖范围：</b>启动期失败（监听器创建）与运行期持续失败（连续 {@link
         * ConsumeLoopTask#RUNTIME_FAILURE_REPORT_THRESHOLD} 次可恢复异常）都通过本通道上报，避免"运行期故障对健康检查失明"。
         */
        @FunctionalInterface
        interface LoopFailureReporter {
            void report(String loopKey, Throwable cause);
        }

        /**
         * 运行期故障恢复清除通道（loopKey）。
         *
         * <p>失败上报后，消费循环一旦成功拉取一批消息即调用 {@link #clear} 复位健康条目——"持续失败 → DOWN、 恢复 → UP"闭环。无恢复语义的实现（默认
         * no-op）不破坏健康判定，仅影响自动恢复精度。
         */
        @FunctionalInterface
        interface LoopFailureCleaner {
            void clear(String loopKey);
        }
    }

    private final LoopContext ctx;
    private StreamMQListener listener;

    /** 泵登记键（循环唯一）：启动/运行期失败上报与恢复清除用它定位对应健康条目 */
    private String pumpKey;

    /** 连续可恢复失败计数：达到 {@link #RUNTIME_FAILURE_REPORT_THRESHOLD} 后上报健康信号，成功拉取后复位 */
    private int consecutiveFailures = 0;

    ConsumeLoopTask(LoopContext ctx, long pausedSleepMillis, long brokerErrorBackoffMillis) {
        this.ctx = Objects.requireNonNull(ctx, "ctx");
        this.pausedSleepMillis = pausedSleepMillis;
        this.brokerErrorBackoffMillis = brokerErrorBackoffMillis;
    }

    // ===================== Template Method =====================

    @Override
    public void run() {
        // 泵登记键按循环唯一：同一注册的多个并发循环（consumeThreadMin>1）各自持有独立泵，
        // 取消时可全部命中（此前固定键互相覆盖导致泵泄漏）
        String pumpKey =
                ctx.reg().key()
                        + (ctx.retryMode() ? DefaultConsumeLoopSupervisor.RETRY_FUTURE_SUFFIX : "")
                        + "#"
                        + ctx.loopIndex();
        this.pumpKey = pumpKey;
        if (!createListener(pumpKey)) {
            return;
        }
        LOG.info(
                "Consume loop started: topic={}, group={}, retryMode={}, concurrencySlot={},"
                        + " listener={}",
                ctx.reg().getTopic(),
                ctx.reg().getGroup(),
                ctx.retryMode(),
                ctx.primaryLoop() ? 0 : "aux",
                ctx.reg().getConsumer().getClass().getSimpleName());

        // 暂停期间保活钩子：广播监听器暂停时不读流、心跳停止，超过 BROADCAST_GROUP_STALE_TTL_MS
        // 会被僵尸组回收任务销毁，resume 后全量重放历史。暂停每个休眠周期触发一次广播组心跳。
        Runnable heartbeatHook = buildPauseHeartbeatHook();
        // P1-6 修复：以下整段（含 MessageSink 构造）必须处于异常兜底之内。
        // MessageSink.forCapacity 在背压启用时会 executor.submit 泵线程，若执行器已关闭则抛
        // RejectedExecutionException；此前该调用在 try 之外，异常从 run() 逃逸进一个无人检查的
        // Future —— 消费循环直接消失，无日志、无健康条目，消费者在管理端点仍显示为正常。
        MessageSink sink;
        try {
            sink =
                    MessageSink.forCapacity(
                            ctx.inflightCapacity().getAsInt(),
                            ctx.reg(),
                            listener,
                            ctx.processor(),
                            ctx.supervisor(),
                            ctx.executor(),
                            ctx.running(),
                            pumpKey);
        } catch (RuntimeException ex) {
            LOG.error(
                    "Failed to initialise consume sink (topic={}, group={}), loop will not"
                            + " consume: {}",
                    ctx.reg().getTopic(),
                    ctx.reg().getGroup(),
                    ex.toString(),
                    ex);
            reportRuntimeFailure(ex);
            return;
        }
        try {
            hookDrainOwnPending(sink);
            while (ctx.running().getAsBoolean()) {
                if (ctx.paused().getAsBoolean()) {
                    heartbeatHook.run();
                    ContainerSupport.sleepQuietly(pausedSleepMillis);
                    continue;
                }
                if (!pullAndDispatch(sink)) {
                    break;
                }
            }
        } catch (Throwable t) {
            // P1-6 兜底：主循环内绝大多数可恢复异常已由 pullAndDispatch 处理，此处拦截
            // 逃逸的 Error 与未被分类的故障。必须上报——否则消费线程消失后
            // isConsumeLoopsHealthy() 仍返回 true，运维只能从"没人消费"反推。
            LOG.error(
                    "Consume loop terminated by unrecoverable error (topic={}, group={},"
                            + " retryMode={}): {}",
                    ctx.reg().getTopic(),
                    ctx.reg().getGroup(),
                    ctx.retryMode(),
                    t.toString(),
                    t);
            reportRuntimeFailure(t);
        } finally {
            LOG.info(
                    "Consume loop exited: topic={}, group={}, retryMode={}",
                    ctx.reg().getTopic(),
                    ctx.reg().getGroup(),
                    ctx.retryMode());
        }
    }

    /**
     * 构建暂停期心跳钩子：广播监听器刷新注册表心跳（非广播为 no-op—— 心跳方法内部按 broadcast 标志自过滤）。
     *
     * <p>异常已在心跳实现内静默（debug 日志），此处不再包裹。
     */
    private Runnable buildPauseHeartbeatHook() {
        if (listener
                instanceof
                io.github.streammq.adapter.redisson.listener.RedissonStreamListener
                                redissonListener) {
            return redissonListener::heartbeatBroadcastRegistry;
        }
        return () -> {};
    }

    /**
     * 钩子：primary 且并发集群消费时排空本消费者 PEL 遗留消息（at-least-once 补齐）。
     *
     * <p>背压启用时排空条目经由 inflight sink 派发（与主循环一致的解耦路径）； 背压禁用（sink 为同步直发）时保持原内联同步处理。
     *
     * <p><b>并发度门控：</b>并发消费（{@code consumeThreadMin>1}）时，所有循环共享同一消费者名， 本循环启动排空会持续读取其它并发循环刚
     * XREADGROUP 读入、尚未 ACK 的在途消息（XREADGROUP id=0 按消费者名读取整段 PEL），导致同一消息被两条循环各处理一次——重复投递。因此并发度 &gt; 1
     * 时跳过 启动排空：遗留未 ACK 消息由 PelClaimScheduler 按 group 级空闲阈值（默认 60s）认领重投， at-least-once 语义不变；并发度 =
     * 1（单循环独占该消费者 PEL）时保留快速恢复路径。
     */
    private void hookDrainOwnPending(MessageSink sink) {
        if (!ctx.primaryLoop()
                || ctx.reg().getType() != ListenerType.AUTO_ACK
                || ctx.reg().isDlqMode()
                || DefaultConsumeLoopSupervisor.effectiveConcurrency(ctx.reg()) > 1) {
            return;
        }
        boolean viaSink = ctx.inflightCapacity().getAsInt() > 0;
        int drained = 0;
        while (ctx.running().getAsBoolean() && !ctx.paused().getAsBoolean()) {
            List<Message<?>> pending = listener.drainPendingOnce(ctx.reg().getPullBatchSize());
            if (pending.isEmpty()) {
                if (drained > 0) {
                    LOG.info(
                            "PEL drain complete: topic={}, group={}, recovered={}",
                            ctx.reg().getTopic(),
                            ctx.reg().getGroup(),
                            drained);
                }
                return;
            }
            for (Message<?> message : pending) {
                if (!ctx.running().getAsBoolean()) {
                    return;
                }
                if (viaSink) {
                    try {
                        sink.dispatch(message);
                    } catch (InterruptedException ie) {
                        // 停机中断：恢复中断位并退出排空（剩余条目仍在 PEL，可再次恢复）
                        Thread.currentThread().interrupt();
                        return;
                    }
                } else {
                    ctx.processor().processMessage(message, ctx.reg(), listener);
                }
                drained++;
            }
        }
    }

    /** 拉取一批并逐条派发；返回 false 表示应退出主循环（中断/容器停止/执行器失效）。 */
    private boolean pullAndDispatch(MessageSink sink) {
        try {
            List<Message<?>> messages =
                    listener.pullBlock(
                            ctx.reg().getPullBatchSize(),
                            Duration.ofMillis(ctx.reg().getPullBlockTimeoutMillis()));
            // 成功拉取 = 运行期故障已恢复：复位连续失败计数并清除健康条目，形成"持续失败 → DOWN、恢复 → UP"闭环
            resetRuntimeFailure();
            if (CollectionUtils.isEmpty(messages)) {
                long interval = ctx.reg().getPullIntervalMillis();
                if (interval > 0) {
                    ContainerSupport.sleepQuietly(interval);
                }
                return true;
            }
            for (Message<?> message : messages) {
                if (!ctx.running().getAsBoolean()) {
                    return false;
                }
                sink.dispatch(message);
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (RejectedExecutionException ex) {
            // 执行器已关闭：重试无意义（容器停止中或执行器引用失效），上报并退出——继续循环只会无限退避刷 WARN
            LOG.error(
                    "Consume executor rejected task (topic={}, group={}), exiting consume loop: {}",
                    ctx.reg().getTopic(),
                    ctx.reg().getGroup(),
                    ex.getMessage());
            reportRuntimeFailure(ex);
            return false;
        } catch (StreamMQBrokerException ex) {
            LOG.warn(
                    "Broker error in consume loop (topic={}, group={}): {}",
                    ctx.reg().getTopic(),
                    ctx.reg().getGroup(),
                    ex.getMessage());
            return handleRecoverableError(ex);
        } catch (RuntimeException ex) {
            LOG.warn(
                    "Unexpected error in consume loop (topic={}, group={}): {}",
                    ctx.reg().getTopic(),
                    ctx.reg().getGroup(),
                    ex.getMessage(),
                    ex);
            return handleRecoverableError(ex);
        }
    }

    /**
     * 处理一次可恢复失败：计数并退避；连续失败达到阈值时上报健康信号。
     *
     * <p>不立即上报的原因：瞬时抖动（单次网络闪断、Broker 短暂不可用）是自愈的，过早把健康检查打成 DOWN
     * 会让运维对"短暂红点"脱敏；但长期持续失败必须暴露——这是"静默不消费"的最后一层防线。
     */
    private boolean handleRecoverableError(Exception ex) {
        consecutiveFailures++;
        if (consecutiveFailures == RUNTIME_FAILURE_REPORT_THRESHOLD) {
            LOG.error(
                    "Consume loop hit {} consecutive recoverable failures, reporting to health"
                            + " indicator (topic={}, group={}): {}",
                    RUNTIME_FAILURE_REPORT_THRESHOLD,
                    ctx.reg().getTopic(),
                    ctx.reg().getGroup(),
                    ex.getMessage());
            reportRuntimeFailure(ex);
        }
        ContainerSupport.sleepQuietly(brokerErrorBackoffMillis);
        return true;
    }

    /** 复位连续失败计数并清除健康条目（成功拉取时调用）。 */
    private void resetRuntimeFailure() {
        if (consecutiveFailures > 0) {
            consecutiveFailures = 0;
            LoopContext.LoopFailureCleaner cleaner = ctx.failureCleaner();
            if (cleaner != null) {
                try {
                    cleaner.clear(pumpKey);
                } catch (RuntimeException clearEx) {
                    LOG.warn(
                            "Failed to clear consume loop runtime failure: {}", clearEx.toString());
                }
            }
        }
    }

    private void reportRuntimeFailure(Throwable cause) {
        LoopContext.LoopFailureReporter reporter = ctx.failureReporter();
        if (reporter != null) {
            try {
                reporter.report(pumpKey, cause);
            } catch (RuntimeException reportEx) {
                LOG.warn("Failed to report consume loop runtime failure: {}", reportEx.toString());
            }
        }
    }

    private boolean createListener(String loopKey) {
        try {
            listener = ctx.listenerFactory().create(ctx.reg(), ctx.retryMode());
            return true;
        } catch (RuntimeException ex) {
            LOG.error(
                    "Failed to create consumer for listener (topic={}, group={}, retryMode={}): {},"
                            + " listener will not consume. Check Redis connectivity/credentials and"
                            + " the consumer group name; the failure is also reported to the health"
                            + " indicator and /actuator/streammq/groups",
                    ctx.reg().getTopic(),
                    ctx.reg().getGroup(),
                    ctx.retryMode(),
                    ex.getMessage(),
                    ex);
            // 上报给容器：静默失败是最难排查的一类故障，必须让它进入健康状态与可观测端点。
            ConsumeLoopTask.LoopContext.LoopFailureReporter reporter = ctx.failureReporter();
            if (reporter != null) {
                try {
                    reporter.report(loopKey, ex);
                } catch (RuntimeException reportEx) {
                    LOG.warn("Failed to report consume loop startup failure: {}", ex.toString());
                }
            }
            return false;
        }
    }
}
