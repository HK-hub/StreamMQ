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
            ListenerFactory listenerFactory) {

        /** 监听器创建函数式抽象（容器侧委托 {@code ListenerConfig.from(reg, retryMode)}）。 */
        interface ListenerFactory {
            StreamMQListener create(ListenerRegistration<?> reg, boolean retryMode);
        }
    }

    private final LoopContext ctx;
    private StreamMQListener listener;

    ConsumeLoopTask(LoopContext ctx, long pausedSleepMillis, long brokerErrorBackoffMillis) {
        this.ctx = Objects.requireNonNull(ctx, "ctx");
        this.pausedSleepMillis = pausedSleepMillis;
        this.brokerErrorBackoffMillis = brokerErrorBackoffMillis;
    }

    // ===================== Template Method =====================

    @Override
    public void run() {
        if (!createListener()) {
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

        // 泵登记键按循环唯一：同一注册的多个并发循环（consumeThreadMin>1）各自持有独立泵，
        // 取消时可全部命中（此前固定键互相覆盖导致泵泄漏）
        String pumpKey =
                ctx.reg().key()
                        + (ctx.retryMode() ? DefaultConsumeLoopSupervisor.RETRY_FUTURE_SUFFIX : "")
                        + "#"
                        + ctx.loopIndex();
        MessageSink sink =
                MessageSink.forCapacity(
                        ctx.inflightCapacity().getAsInt(),
                        ctx.reg(),
                        listener,
                        ctx.processor(),
                        ctx.supervisor(),
                        ctx.executor(),
                        ctx.running(),
                        pumpKey);
        // 暂停期间保活钩子：广播监听器暂停时不读流、心跳停止，超过 BROADCAST_GROUP_STALE_TTL_MS
        // 会被僵尸组回收任务销毁，resume 后全量重放历史。暂停每个休眠周期触发一次广播组心跳。
        Runnable heartbeatHook = buildPauseHeartbeatHook();
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
     */
    private void hookDrainOwnPending(MessageSink sink) {
        if (!ctx.primaryLoop()
                || ctx.reg().getType() != ListenerType.AUTO_ACK
                || ctx.reg().isDlqMode()) {
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

    /** 拉取一批并逐条派发；返回 false 表示应退出主循环（中断/容器停止）。 */
    private boolean pullAndDispatch(MessageSink sink) {
        try {
            List<Message<?>> messages =
                    listener.pullBlock(
                            ctx.reg().getPullBatchSize(),
                            Duration.ofMillis(ctx.reg().getPullBlockTimeoutMillis()));
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
        } catch (StreamMQBrokerException ex) {
            LOG.warn(
                    "Broker error in consume loop (topic={}, group={}): {}",
                    ctx.reg().getTopic(),
                    ctx.reg().getGroup(),
                    ex.getMessage());
            ContainerSupport.sleepQuietly(brokerErrorBackoffMillis);
            return true;
        } catch (RuntimeException ex) {
            LOG.warn(
                    "Unexpected error in consume loop (topic={}, group={}): {}",
                    ctx.reg().getTopic(),
                    ctx.reg().getGroup(),
                    ex.getMessage(),
                    ex);
            ContainerSupport.sleepQuietly(brokerErrorBackoffMillis);
            return true;
        }
    }

    private boolean createListener() {
        try {
            listener = ctx.listenerFactory().create(ctx.reg(), ctx.retryMode());
            return true;
        } catch (RuntimeException ex) {
            LOG.error(
                    "Failed to create consumer for listener (topic={}, group={}, retryMode={}): {},"
                            + " listener will not consume",
                    ctx.reg().getTopic(),
                    ctx.reg().getGroup(),
                    ctx.retryMode(),
                    ex.getMessage(),
                    ex);
            return false;
        }
    }
}
