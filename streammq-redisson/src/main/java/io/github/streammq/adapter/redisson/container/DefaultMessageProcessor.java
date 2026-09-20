/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.metrics.RuntimeStatsRegistry;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.ConsumeOrderlyContext;
import io.github.streammq.core.consumer.DlqMessageConsumer;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.exception.OrderlyShardBusyException;
import io.github.streammq.core.interceptor.ConsumerInterceptorChain;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.ListenerType;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.metrics.StreamMQMetrics;
import io.github.streammq.core.policy.OrderlyShardLockManager;
import io.github.streammq.core.policy.RetryAndDlqHandler;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * 单条消息的消费管线（God class 拆分，红队审查 F-02-12）。
 *
 * <p>从 {@code DefaultStreamMQListenerContainer} 迁出的按消息处理职责：
 *
 * <ul>
 *   <li>过滤器/拦截器前置检查（{@link #acceptMessage} + {@code applyBefore}）
 *   <li>三类消费者的分发：DLQ / 顺序（分片锁内重试）/ 并发
 *   <li>消费超时取消与宽限期等待（{@link #processWithTimeout}）
 *   <li>拦截器 after 钩子与消费指标（超时路径只记录一次）
 *   <li>异常统一转 RECONSUME_LATER 路由
 * </ul>
 *
 * <p>容器保留生命周期、注册管理与读循环编排；本类仅持有分片锁竞争日志的限频时间戳（{@link AtomicLong}）， 其余无状态，可并发调用。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultMessageProcessor implements MessageProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(MessageProcessor.class);

    /** 消费超时取消后，等待业务线程真正终止的默认宽限期（毫秒） */
    static final long DEFAULT_TIMEOUT_CANCEL_GRACE_MILLIS =
            io.github.streammq.core.StreamMQConstants.DEFAULT_TIMEOUT_CANCEL_GRACE_MS;

    /** 分片锁竞争 WARN 的限频间隔（毫秒）：高竞争期间（多实例 rebalance、慢 handler 持锁） 每容器每 N 秒最多一条，避免日志风暴。 */
    static final long SHARD_BUSY_WARN_INTERVAL_MILLIS = 10_000L;

    private final ConsumerInterceptorChain interceptorChain;
    private final OrderlyShardLockManager shardLockManager;
    private final RegistrationStore store;
    private final RetryAndDlqHandler sharedRetryDlqHandler;
    private final boolean perConsumerEnabled;

    /**
     * 执行业务消费回调的执行器。
     *
     * <p>非 final：容器可在 INIT 阶段通过 {@link #setExecutor} 替换（与容器自身的 {@code setConsumeExecutor} 联动）。若此处保持
     * final 而容器换了执行器，消费回调会持续抛 {@code RejectedExecutionException}。
     */
    private volatile ExecutorService executor;

    /** 指标收集器（可选注入，null 时为 no-op） */
    private volatile StreamMQMetrics metrics;

    /**
     * 进程内运行时统计登记表（可选注入，null 时不上报）。
     *
     * <p>发布前修复 P1-3：为 {@code /actuator/streammq/stats} 提供真实的消费计数来源。
     */
    private volatile RuntimeStatsRegistry runtimeStats;

    /** 消费超时取消后的宽限期（毫秒） */
    private volatile long timeoutCancelGraceMillis = DEFAULT_TIMEOUT_CANCEL_GRACE_MILLIS;

    /**
     * 顺序消费延迟重投队列（红队审查 R1-1，容器装配时注入；未注入时退化为"消息留在 PEL"）。
     *
     * <p>分片锁竞争、ORDERLY 的 {@code defer(delay)}、DLQ 转投失败三条路径都会向它登记；由注册的 primary 读循环按节拍重投（{@code
     * ConsumeLoopTask}），避免"属主存活期间消息永远不被重投"的静默黑洞。
     */
    private volatile OrderlyDeferredRetryQueue deferredRetryQueue;

    /**
     * 上一次分片锁竞争 WARN 的时间戳（毫秒）。
     *
     * <p>仅用于 {@link #SHARD_BUSY_WARN_INTERVAL_MILLIS} 限频：竞争是正常调度状态， 但连续竞争（如 rebalance 窗口）可能每秒数十次，逐条
     * WARN 会淹没日志。
     */
    private final AtomicLong lastShardBusyWarnMillis = new AtomicLong(0L);

    /**
     * 在途消息计数（K2）：{@link #processMessage} 入口自增、finally 自减。
     *
     * <p><b>口径：</b>统计已进入消费管线、尚未完成 ACK/NACK 路由的消息条数（正常路径上等价于「正在执行 handler 的消息 条数」+ 过滤/拦截器拒绝后仍在做 ACK
     * 路由的瞬时条数）；不包含排队待处理、重试 ZSet 中、PEL 中滞留的消息。该口径与 {@link
     * io.github.streammq.core.listener.InFlightAware#getInFlightCount()} 一致，由容器实现转发给优雅关闭流程。
     *
     * <p><b>唯一收口：</b>所有会执行 handler 的路径（内联 sink / inflight 泵 / PEL 排空 / 延迟重投 / 超时包装）都经由 {@link
     * #processMessage}；{@code retryDeferredDlqRoute} 只重试 DLQ 转投、不执行 handler，故不计入。
     *
     * <p><b>并发安全：</b>{@link #processMessage} 可被多条虚拟线程并发调用（背压泵 + 多读循环）， {@link AtomicInteger}
     * 的无锁自增/自减相对一次 Redis ACK 的开销可忽略。
     *
     * <p><b>已知近似：</b>消费超时取消后若业务线程在宽限期内仍未终止，计数会随 {@link #processMessage} 返回而先归零 （此时消息已按
     * RECONSUME_LATER 路由，at-least-once 由重试 / PEL 认领兜底）。
     */
    private final AtomicInteger inFlightMessages = new AtomicInteger();

    public DefaultMessageProcessor(
            ConsumerInterceptorChain interceptorChain,
            OrderlyShardLockManager shardLockManager,
            RegistrationStore store,
            RetryAndDlqHandler sharedRetryDlqHandler,
            boolean perConsumerEnabled,
            ExecutorService executor) {
        this.interceptorChain = Objects.requireNonNull(interceptorChain, "interceptorChain");
        this.shardLockManager = Objects.requireNonNull(shardLockManager, "shardLockManager");
        this.store = Objects.requireNonNull(store, "store");
        this.sharedRetryDlqHandler =
                Objects.requireNonNull(sharedRetryDlqHandler, "sharedRetryDlqHandler");
        this.perConsumerEnabled = perConsumerEnabled;
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    public void setExecutor(ExecutorService executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    /**
     * 注入顺序消费延迟重投队列（容器装配；R1-1）。
     *
     * <p>未注入（如独立单测直接构造）时，三条登记路径退化为"消息留在 PEL 等认领兜底"的历史行为， 不阻断主流程。
     *
     * @param queue 延迟重投队列
     */
    void setOrderlyDeferredRetryQueue(OrderlyDeferredRetryQueue queue) {
        this.deferredRetryQueue = queue;
    }

    @Override
    public void setMetrics(StreamMQMetrics metrics) {
        this.metrics = metrics;
    }

    /** {@inheritDoc} */
    @Override
    public void setRuntimeStats(RuntimeStatsRegistry runtimeStats) {
        this.runtimeStats = runtimeStats;
    }

    StreamMQMetrics metrics() {
        return metrics;
    }

    @Override
    public void setTimeoutCancelGraceMillis(long millis) {
        if (millis > 0) {
            this.timeoutCancelGraceMillis = millis;
        }
    }

    // ===================== 主入口 =====================

    /**
     * 处理单条消息：支持消费超时取消，以 {@code onMessage} 返回值为路由标准。
     *
     * <p><b>在途计数（K2）：</b>入口自增、finally 自减，是容器 {@code InFlightAware} 判据的唯一计数点（口径与唯一收口见 {@link
     * #inFlightMessages}）。
     *
     * <p><b>Throwable 兜底（发布前修复 P1-6/P1-8）：</b>{@link #doProcessMessage} 内部已捕获业务 {@code Exception}
     * 并路由重试/DLQ；此处再拦截逃逸的 {@code Error}（OOM / StackOverflowError）与 handler 二次故障（如 Redis
     * 彻底不可用导致路由本身再抛）。捕获后统一按 {@code RECONSUME_LATER} 路由——消息要么进入重试 ZSet、要么留在 PEL，绝不因为一次异常
     * 就从内存队列消失且无人认领（旧行为：背压泵吞掉异常，消息静默停滞至 PEL 认领超时）。
     */
    @Override
    public void processMessage(
            Message<?> message, ListenerRegistration<?> reg, StreamMQListener listener) {
        inFlightMessages.incrementAndGet();
        try {
            doProcessMessage(message, reg, listener);
        } catch (Throwable t) {
            handleFailure(message, reg, listener, t);
        } finally {
            inFlightMessages.decrementAndGet();
        }
    }

    /**
     * 返回当前在途（处理中、尚未完成 ACK/NACK 路由）消息条数。
     *
     * <p>供 {@code DefaultStreamMQListenerContainer} 实现 {@link
     * io.github.streammq.core.listener.InFlightAware#getInFlightCount()}；口径与近似说明见 {@link
     * #inFlightMessages}。
     *
     * @return 在途消息数，不小于 0
     */
    int inFlightCount() {
        return inFlightMessages.get();
    }

    /**
     * 处理失败后的兜底路由：按 {@code RECONSUME_LATER} 交给 {@link RetryAndDlqHandler}。
     *
     * <p>路由本身再次失败时只记录 ERROR——此时消息仍留在 PEL 中，由 {@code PelClaimScheduler} 在空闲阈值后重投（at-least-once
     * 的最后一道防线）。
     */
    @Override
    public void handleFailure(
            Message<?> message,
            ListenerRegistration<?> reg,
            StreamMQListener listener,
            Throwable cause) {
        LOG.error(
                "Unrecoverable consume failure, routing to retry/DLQ (topic={}, group={},"
                        + " messageId={}): {}",
                reg.getTopic(),
                reg.getGroup(),
                message.getMessageId(),
                cause.toString(),
                cause);
        try {
            resolveHandler(reg)
                    .handleAction(ConsumeAction.RECONSUME_LATER, message, reg, listener, cause);
        } catch (RuntimeException routeEx) {
            LOG.error(
                    "Failure routing also failed, message stays in PEL for PelClaimScheduler"
                            + " redelivery (topic={}, group={}, messageId={}): {}",
                    reg.getTopic(),
                    reg.getGroup(),
                    message.getMessageId(),
                    routeEx.toString());
        } finally {
            ConsumerMdcTrace.clear();
        }
    }

    /** 单条消息消费管线的真实实现（由 {@link #processMessage} 包裹 Throwable 兜底）。 */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void doProcessMessage(
            Message<?> message, ListenerRegistration<?> reg, StreamMQListener listener) {
        ConsumeContext ctx = new DefaultConsumeContextConsume(message, reg, ctxConsumerName(reg));
        ConsumerMdcTrace.inject(message, reg);
        ConsumeAction finalAction = ConsumeAction.RECONSUME_LATER;
        RetryAndDlqHandler handler = resolveHandler(reg);
        long consumeTimeoutMs = reg.getConsumeTimeoutMillis();
        long consumeStart = System.nanoTime();
        boolean recordedByTimeout = false;
        LOG.debug(
                "processMessage: topic={}, group={}, dlqMode={}, type={}, messageId={}",
                reg.getTopic(),
                reg.getGroup(),
                reg.isDlqMode(),
                reg.getType(),
                message.getMessageId());
        try {
            // 消费者过滤器检查（全局 + per-consumer + selectorExpression）
            try {
                if (!acceptMessage(message, reg)) {
                    LOG.debug(
                            "Message filtered: topic={}, tag={}, group={}",
                            message.getTopic(),
                            message.getTag(),
                            reg.getGroup());
                    handler.handleAction(ConsumeAction.SUCCESS, message, reg, listener, null);
                    finalAction = ConsumeAction.SUCCESS;
                    return;
                }
            } catch (Exception filterEx) {
                // 过滤器求值失败 ≠ 不匹配：与消费者抛异常同路径处理（scheduleRetry /
                // handleReconsumeLater，超限转 DLQ），绝不静默 ACK 丢消息
                LOG.warn(
                        "Consumer filter evaluation failed, routing to retry/DLQ"
                                + " (topic={}, group={}, messageId={}): {}",
                        reg.getTopic(),
                        reg.getGroup(),
                        message.getMessageId(),
                        filterEx.getMessage(),
                        filterEx);
                interceptorChain.notifyException(message, filterEx, InvokeTiming.EXECUTING, ctx);
                finalAction = ConsumeAction.RECONSUME_LATER;
                handler.handleAction(
                        ConsumeAction.RECONSUME_LATER, message, reg, listener, filterEx);
                return;
            }

            if (!interceptorChain.applyBefore(message, ctx)) {
                LOG.debug(
                        "Message rejected by interceptor: topic={}, group={}",
                        message.getTopic(),
                        reg.getGroup());
                handler.handleAction(ConsumeAction.SUCCESS, message, reg, listener, null);
                finalAction = ConsumeAction.SUCCESS;
                return;
            }
            // 消费超时控制：使用 Future.get(timeout) 包裹 onMessage 调用
            if (consumeTimeoutMs > 0 && reg.getType() != ListenerType.ORDERLY && !reg.isDlqMode()) {
                processWithTimeout(message, reg, listener, ctx, handler, consumeStart);
                recordedByTimeout = true;
                return;
            }
            try {
                if (reg.isDlqMode()) {
                    ConsumeAction dlqAction = processDlqMessage(message, reg, ctx);
                    LOG.debug(
                            "DLQ onMessage returned: topic={}, group={}, messageId={}, action={}",
                            reg.getTopic(),
                            reg.getGroup(),
                            message.getMessageId(),
                            dlqAction);
                    handler.handleAction(dlqAction, message, reg, listener, null);
                    finalAction = dlqAction;
                } else if (reg.getType() == ListenerType.ORDERLY) {
                    StreamMessageOrderlyConsumer orderly =
                            (StreamMessageOrderlyConsumer) reg.getConsumer();
                    ConsumeAction orderlyAction =
                            consumeOrderlyWithRetry(
                                    message,
                                    reg,
                                    (ConsumeOrderlyContext) ctx,
                                    orderly,
                                    listener,
                                    handler);
                    finalAction = orderlyAction;
                } else {
                    StreamMessageConcurrentlyConsumer consumer =
                            (StreamMessageConcurrentlyConsumer) reg.getConsumer();
                    LOG.debug(
                            "Calling onMessage: topic={}, group={}, messageId={}, consumerClass={}",
                            reg.getTopic(),
                            reg.getGroup(),
                            message.getMessageId(),
                            consumer.getClass().getSimpleName());
                    ConsumeAction action = consumer.onMessage(message, ctx);
                    LOG.debug(
                            "onMessage returned: topic={}, group={}, messageId={}, action={}",
                            reg.getTopic(),
                            reg.getGroup(),
                            message.getMessageId(),
                            action);
                    if (Objects.isNull(action)) {
                        action = ConsumeAction.RECONSUME_LATER;
                    }
                    handler.handleAction(action, message, reg, listener, null);
                    finalAction = action;
                }
            } catch (Exception ex) {
                LOG.warn(
                        "Listener onMessage threw exception (topic={}, group={}, messageId={}): {}",
                        reg.getTopic(),
                        reg.getGroup(),
                        message.getMessageId(),
                        ex.getMessage(),
                        ex);
                interceptorChain.notifyException(message, ex, InvokeTiming.EXECUTING, ctx);
                if (reg.getType() == ListenerType.ORDERLY) {
                    // ORDERLY 没有 retry 消费者循环（DefaultConsumeLoopSupervisor:166-169），
                    // RECONSUME_LATER 进 retry Stream 永久无人消费 → 直接路由到 DLQ
                    LOG.warn(
                            "Orderly consumer has no retry loop, routing exception to DLQ:"
                                    + " topic={}, group={}, messageId={}",
                            reg.getTopic(),
                            reg.getGroup(),
                            message.getMessageId());
                    if (handler.routeToDlq(
                            message,
                            reg,
                            message.getMessageId(),
                            RetryScheduler.DLQ_REASON_MAX_RETRY)) {
                        listener.ack(message.getMessageId());
                    } else {
                        // R1-1 ②：转投失败此前无分支（消息静默留在 PEL，属主存活期间永不重投）。
                        // 现登记本地延迟重试——只重试 DLQ 转投，不重新执行 handler。
                        deferDlqRouteFailure(message, reg);
                    }
                    finalAction = ConsumeAction.SUCCESS;
                } else {
                    finalAction = ConsumeAction.RECONSUME_LATER;
                    handler.handleAction(ConsumeAction.RECONSUME_LATER, message, reg, listener, ex);
                }
            }
        } finally {
            // 超时路径的 applyAfter/指标已由 processWithTimeout 内部负责（携带真实 action），
            // 此处再执行会导致拦截器 after 钩子被调用两次
            if (!recordedByTimeout) {
                interceptorChain.applyAfter(message, finalAction, ctx);
            }
            ConsumerMdcTrace.clear();
            if (!recordedByTimeout) {
                recordConsumeMetrics(reg, consumeStart, finalAction.isSuccess());
            }
        }
    }

    private String ctxConsumerName(ListenerRegistration<?> reg) {
        String name = reg.getConsumerName();
        return Objects.nonNull(name) ? name : reg.getGroup() + "-" + reg.key();
    }

    private RetryAndDlqHandler resolveHandler(ListenerRegistration<?> reg) {
        if (perConsumerEnabled) {
            RetryAndDlqHandler handler = store.handler(reg.key());
            return Objects.nonNull(handler)
                    ? handler
                    : sharedRetryDlqHandler; // per-consumer 解析失败时的兜底
        }
        return sharedRetryDlqHandler;
    }

    // ===================== 超时控制 =====================

    /**
     * 使用 Future.get(timeout) 包裹 onMessage 调用，超时后取消并进入重试。
     *
     * <p><b>投递语义（at-least-once）：</b>超时取消只中断业务线程（{@code cancel(true)}）；若 handler 不响应 中断（阻塞式 DB/IO
     * 调用通常如此），原始任务仍在后台运行，而消息已在宽限期（{@code timeoutCancelGraceMillis}） 后按 {@code RECONSUME_LATER}
     * 重投——因此<b>原消费与重试副本可能并发执行，业务层必须实现幂等</b>。 超时重投计入 {@code maxReconsumeTimes}（默认 16 次），耗尽后进入 DLQ。
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void processWithTimeout(
            Message<?> message,
            ListenerRegistration reg,
            StreamMQListener listener,
            ConsumeContext ctx,
            RetryAndDlqHandler handler,
            long consumeStart) {
        AtomicReference<Thread> taskThread = new AtomicReference<>();
        // R1-10：MDC 只注入在读循环线程，业务回调实际运行在执行器线程——不传递上下文会丢失
        // topic/groupId/messageId 结构化日志字段。提交前快照，任务内恢复，任务结束清理。
        Map<String, String> mdcSnapshot = MDC.getCopyOfContextMap();
        Future<ConsumeAction> future =
                executor.submit(
                        () -> {
                            taskThread.set(Thread.currentThread());
                            if (Objects.nonNull(mdcSnapshot)) {
                                MDC.setContextMap(mdcSnapshot);
                            }
                            try {
                                if (reg.isDlqMode()) {
                                    return processDlqMessage(message, reg, ctx);
                                }
                                StreamMessageConcurrentlyConsumer consumer =
                                        (StreamMessageConcurrentlyConsumer) reg.getConsumer();
                                ConsumeAction action = consumer.onMessage(message, ctx);
                                return action;
                            } finally {
                                MDC.clear();
                            }
                        });
        ConsumeAction action = ConsumeAction.RECONSUME_LATER;
        try {
            action = future.get(reg.getConsumeTimeoutMillis(), TimeUnit.MILLISECONDS);
            if (Objects.isNull(action)) {
                action = ConsumeAction.RECONSUME_LATER;
            }
            handler.handleAction(action, message, reg, listener, null);
        } catch (TimeoutException e) {
            future.cancel(true);
            // 等待业务线程真正终止（上限为宽限期）：缩小「原消费与重试副本并发执行」的窗口。
            Thread t = taskThread.get();
            if (t != null && t != Thread.currentThread()) {
                try {
                    t.join(timeoutCancelGraceMillis);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            LOG.warn(
                    "Consume timeout ({}ms) for message, cancelling and retrying: topic={},"
                            + " group={}, messageId={}",
                    reg.getConsumeTimeoutMillis(),
                    reg.getTopic(),
                    reg.getGroup(),
                    message.getMessageId());
            handler.handleAction(ConsumeAction.RECONSUME_LATER, message, reg, listener, e);
        } catch (ExecutionException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOG.warn(
                    "processWithTimeout exception: topic={}, group={}, error={}",
                    reg.getTopic(),
                    reg.getGroup(),
                    e.getMessage());
            handler.handleAction(ConsumeAction.RECONSUME_LATER, message, reg, listener, e);
        } finally {
            interceptorChain.applyAfter(message, action, ctx);
            recordConsumeMetrics(reg, consumeStart, action.isSuccess());
        }
    }

    // ===================== DLQ 分发 =====================

    /**
     * 处理 DLQ 消息，支持 {@link DlqMessageConsumer} 与 {@link StreamMessageConcurrentlyConsumer} 两种消费者类型。
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private ConsumeAction processDlqMessage(
            Message<?> message, ListenerRegistration reg, ConsumeContext ctx) throws Exception {
        Object consumer = reg.getConsumer();
        if (consumer instanceof DlqMessageConsumer dlqConsumer) {
            dlqConsumer.onDlqMessage(message, ctx);
            return ConsumeAction.SUCCESS;
        } else if (consumer instanceof StreamMessageConcurrentlyConsumer concurrentConsumer) {
            ConsumeAction action = concurrentConsumer.onMessage(message, ctx);
            return Objects.isNull(action) ? ConsumeAction.RECONSUME_LATER : action;
        } else {
            LOG.warn(
                    "Unknown DLQ consumer type: {}, defaulting to SUCCESS",
                    consumer.getClass().getSimpleName());
            return ConsumeAction.SUCCESS;
        }
    }

    // ===================== 顺序消费 =====================

    /**
     * 顺序消费：失败时在当前线程内重试（最多 maxReconsumeTimes 次），每次失败后挂起
     * suspendCurrentQueueTimeMillis，保证同一分片不越过失败消息（严格有序）；耗尽后进 DLQ。
     *
     * <p>当 {@link ListenerRegistration#getOrderlyConsumeTimeoutMillis()} 大于 0 时，每次尝试由 {@link
     * #attemptOrderlyConsume} 以消费超时保护：卡死 handler 不再永久阻塞消费循环。
     *
     * <p><b>竞争 ≠ 业务失败（红队审查 R4-B02 / R3-25）：</b>{@link OrderlyShardBusyException}
     * 表示"分片锁被其它实例持有，本条消息未被 handler 处理"。该信号在 {@code attempt++} 之前单独捕获， 直接返回 {@code
     * RECONSUME_LATER}——不计数、不挂起等待、不写 retry ZSet、不进 DLQ、不 ACK， 消息继续留在 PEL 并登记到延迟重投队列（R1-1）。否则多实例
     * rebalance 窗口内的锁竞争会白白耗尽 maxReconsumeTimes 预算，把从未被处理过的消息送进 DLQ。
     *
     * <p><b>DEFER 语义（红队审查 R1-2）：</b>消费端返回 {@link ConsumeAction#defer(Duration)} 时，按声明的
     * 延迟登记延迟重投并结束本轮——<b>不消耗重试预算、不进 DLQ、不原地 sleep 重试</b>（此前 DEFER 被当作失败， 原地重试后在预算耗尽时进 DLQ）。
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private ConsumeAction consumeOrderlyWithRetry(
            Message<?> message,
            ListenerRegistration reg,
            ConsumeOrderlyContext ctx,
            StreamMessageOrderlyConsumer orderly,
            StreamMQListener listener,
            RetryAndDlqHandler handler)
            throws Exception {
        int maxRetries = Math.max(0, reg.getMaxReconsumeTimes());
        long suspendMillis = Math.max(0, reg.getSuspendCurrentQueueTimeMillis());
        long orderlyTimeout = Math.max(0, reg.getOrderlyConsumeTimeoutMillis());
        ConsumeAction action;
        try {
            action = attemptOrderlyConsume(message, reg, ctx, orderly, orderlyTimeout);
        } catch (OrderlyShardBusyException busy) {
            return deferShardBusy(message, reg, busy);
        }
        int attempt = 0;
        while (true) {
            if (action.isSuccess()) {
                handler.handleAction(ConsumeAction.SUCCESS, message, reg, listener, null);
                return ConsumeAction.SUCCESS;
            }
            if (action.isDefer()) {
                return deferByOrderlyAction(message, reg, action);
            }
            if (attempt >= maxRetries) {
                break;
            }
            attempt++;
            LOG.debug(
                    "Orderly consume failed (attempt {}/{}), suspending shard for {}ms: topic={},"
                            + " group={}, messageId={}",
                    attempt,
                    maxRetries,
                    suspendMillis,
                    reg.getTopic(),
                    reg.getGroup(),
                    message.getMessageId());
            ContainerSupport.sleepQuietly(suspendMillis);
            try {
                action = attemptOrderlyConsume(message, reg, ctx, orderly, orderlyTimeout);
            } catch (OrderlyShardBusyException busy) {
                return deferShardBusy(message, reg, busy);
            }
        }
        LOG.warn(
                "Orderly consume exhausted retries (max={}), routing to DLQ: topic={}, group={},"
                        + " messageId={}",
                maxRetries,
                reg.getTopic(),
                reg.getGroup(),
                message.getMessageId());
        if (handler.routeToDlq(
                message, reg, message.getMessageId(), RetryScheduler.DLQ_REASON_MAX_RETRY)) {
            listener.ack(message.getMessageId());
        } else {
            // R1-1 ②：DLQ 转投失败不得只留在 PEL 等认领（属主存活期间认领会跳过）——
            // 登记本地延迟重试（仅重试 DLQ 转投，不重新执行业务 handler）。
            deferDlqRouteFailure(message, reg);
        }
        return ConsumeAction.RECONSUME_LATER;
    }

    /**
     * R1-2：ORDERLY 消费端返回 {@code defer(delay)} 时按声明延迟登记延迟重投。
     *
     * <p>不消耗 {@code maxReconsumeTimes} 预算、不写 retry ZSet、不进 DLQ；本轮结束（消息留在 PEL， 到期后由注册的 primary
     * 读循环重投走正常消费管线，成功即 ACK）。
     */
    private ConsumeAction deferByOrderlyAction(
            Message<?> message, ListenerRegistration<?> reg, ConsumeAction action) {
        Duration delay = action.deferDelay();
        long delayMillis = Objects.nonNull(delay) ? Math.max(1L, delay.toMillis()) : 1L;
        OrderlyDeferredRetryQueue queue = deferredRetryQueue;
        if (Objects.nonNull(queue)) {
            boolean registered = queue.deferByAction(reg, message, delayMillis);
            LOG.debug(
                    "Orderly deferred by consumer action: delay={}ms, registered={}, topic={},"
                            + " group={}, messageId={}",
                    delayMillis,
                    registered,
                    reg.getTopic(),
                    reg.getGroup(),
                    message.getMessageId());
        } else {
            LOG.warn(
                    "Orderly defer action cannot be scheduled locally (no deferred retry queue),"
                            + " message stays in PEL for the claim backstop: topic={}, group={},"
                            + " messageId={}, delay={}ms",
                    reg.getTopic(),
                    reg.getGroup(),
                    message.getMessageId(),
                    delayMillis);
        }
        return ConsumeAction.RECONSUME_LATER;
    }

    /**
     * R1-1 ②：DLQ 转投失败后的本地延迟重试登记（只重试转投，不重新执行 handler）。
     *
     * <p>队列不可用/容量溢出时保持历史行为（消息留在 PEL），但必须打 ERROR——绝不静默。
     */
    private void deferDlqRouteFailure(Message<?> message, ListenerRegistration<?> reg) {
        OrderlyDeferredRetryQueue queue = deferredRetryQueue;
        boolean registered = Objects.nonNull(queue) && queue.deferDlqRouteFailure(reg, message);
        if (registered) {
            LOG.error(
                    "DLQ routing failed, message kept in PEL and scheduled for local DLQ retry:"
                            + " topic={}, group={}, messageId={}",
                    reg.getTopic(),
                    reg.getGroup(),
                    message.getMessageId());
        } else {
            LOG.error(
                    "DLQ routing failed, message kept in PEL (no local DLQ retry scheduled):"
                            + " topic={}, group={}, messageId={}",
                    reg.getTopic(),
                    reg.getGroup(),
                    message.getMessageId());
        }
    }

    /**
     * 分片锁竞争（{@link OrderlyShardBusyException}）退避路径：竞争不是业务失败。
     *
     * <p>本条消息<b>未被 handler 处理</b>，因此：
     *
     * <ul>
     *   <li>不消耗 {@code maxReconsumeTimes} 预算（不计 attempt、不挂起等待）
     *   <li>不写 retry ZSet、不进 DLQ、不 ACK——消息保持 pending 留在 PEL 中， 同时登记到延迟重投队列（R1-1），由注册的 primary
     *       读循环按退避重投； 进程死亡后仍由 {@code PelClaimScheduler} 认领兜底（at-least-once）
     *   <li>返回 {@code RECONSUME_LATER} 仅作为当前投递轮次的结束信号（顺序消费分支不再路由该动作）
     * </ul>
     *
     * <p><b>R1-1 根因修复：</b>此前只依赖 PEL 认领兜底，而认领对心跳新鲜的属主实例直接跳过—— 属主存活期间消息永远不会被重投（静默黑洞）。现由队列提供进程内本地重投，
     * 队列不可用/容量溢出时退化为"留在 PEL"并打限频 ERROR（绝不静默）。
     *
     * <p>日志按 {@link #SHARD_BUSY_WARN_INTERVAL_MILLIS} 限频（每容器每 10s 最多一条）。 竞争计数复用现有消费失败指标（本方法返回后
     * {@code recordConsumeMetrics} 记一次 failure）， 不新增公开 API。
     *
     * @param message 未被处理的消息
     * @param reg Listener 注册信息
     * @param busy 锁管理器抛出的竞争异常
     * @return 恒为 {@link ConsumeAction#RECONSUME_LATER}
     */
    private ConsumeAction deferShardBusy(
            Message<?> message, ListenerRegistration<?> reg, OrderlyShardBusyException busy) {
        OrderlyDeferredRetryQueue queue = deferredRetryQueue;
        boolean registered = Objects.nonNull(queue) && queue.deferShardBusy(reg, message);
        long now = System.currentTimeMillis();
        long last = lastShardBusyWarnMillis.get();
        if (now - last >= SHARD_BUSY_WARN_INTERVAL_MILLIS
                && lastShardBusyWarnMillis.compareAndSet(last, now)) {
            LOG.warn(
                    "Orderly shard contended, message deferred WITHOUT consuming retry budget"
                            + " (stays in PEL, local redelivery registered={}): topic={},"
                            + " group={}, messageId={}, cause={}",
                    registered,
                    reg.getTopic(),
                    reg.getGroup(),
                    message.getMessageId(),
                    busy.getMessage());
        }
        return ConsumeAction.RECONSUME_LATER;
    }

    /**
     * 延迟重投执行：只重试 DLQ 转投（R1-1 ② 的 {@code DLQ_ROUTE} 条目）。
     *
     * <p>重试预算已在首次耗尽时消费完毕，此处<b>不得</b>重新执行业务 handler；转投成功即 ACK 走既有 ACK 路径，失败则由队列按退避再次登记（消息始终留在 PEL）。
     *
     * @return true 表示转投成功并已 ACK（本轮结束）；false 表示仍失败（交由队列重新登记）
     */
    boolean retryDeferredDlqRoute(
            Message<?> message, ListenerRegistration<?> reg, StreamMQListener listener) {
        RetryAndDlqHandler handler = resolveHandler(reg);
        if (handler.routeToDlq(
                message, reg, message.getMessageId(), RetryScheduler.DLQ_REASON_MAX_RETRY)) {
            listener.ack(message.getMessageId());
            return true;
        }
        return false;
    }

    /**
     * 单次顺序消费尝试。
     *
     * <p>{@code timeoutMillis > 0} 时以 Future 包裹「获取分片锁 + onMessage + 释放分片锁」：
     *
     * <ul>
     *   <li>超时后取消任务并等待宽限期，返回 {@code RECONSUME_LATER}——消费循环不再被卡死 handler 阻塞
     *   <li>若 handler 不响应中断，分片锁会由任务自身的 finally 在 handler 返回时释放；该窗口内同分片的 其它投递会被 tryLock
     *       拒绝并重试，不会破坏严格有序
     *   <li>超时路径语义与 {@link #processWithTimeout} 一致：业务层必须保证幂等
     * </ul>
     *
     * <p><b>分片锁竞争信号透传：</b>超时包装下，任务线程内的 {@link OrderlyShardBusyException} 会被 记入 {@code busyRef}
     * 并在判定为非业务失败后抛出——不能在任务内吞成 {@code RECONSUME_LATER}， 否则竞争会被误计为一次业务尝试。
     */
    private ConsumeAction attemptOrderlyConsume(
            Message<?> message,
            ListenerRegistration reg,
            ConsumeOrderlyContext ctx,
            StreamMessageOrderlyConsumer orderly,
            long timeoutMillis)
            throws Exception {
        if (timeoutMillis <= 0) {
            return shardLockManager.consumeWithShardLock(message, reg, ctx, orderly);
        }
        AtomicReference<Thread> taskThread = new AtomicReference<>();
        AtomicReference<OrderlyShardBusyException> busyRef = new AtomicReference<>();
        // R1-10：ORDERLY 超时包装路径同样要把读循环线程的 MDC 上下文带进业务线程
        Map<String, String> mdcSnapshot = MDC.getCopyOfContextMap();
        Future<ConsumeAction> future =
                executor.submit(
                        () -> {
                            taskThread.set(Thread.currentThread());
                            if (Objects.nonNull(mdcSnapshot)) {
                                MDC.setContextMap(mdcSnapshot);
                            }
                            try {
                                ConsumeAction a =
                                        shardLockManager.consumeWithShardLock(
                                                message, reg, ctx, orderly);
                                return Objects.isNull(a) ? ConsumeAction.RECONSUME_LATER : a;
                            } catch (OrderlyShardBusyException busy) {
                                // 竞争信号：本条消息未被处理，独立于业务失败向上抛出
                                busyRef.set(busy);
                                return ConsumeAction.RECONSUME_LATER;
                            } catch (Exception ex) {
                                // 异常按一次失败处理（与同步路径的 catch 语义一致），保证 Future 正常返回
                                return ConsumeAction.RECONSUME_LATER;
                            } finally {
                                MDC.clear();
                            }
                        });
        try {
            ConsumeAction action = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
            OrderlyShardBusyException busy = busyRef.get();
            if (Objects.nonNull(busy)) {
                throw busy;
            }
            return Objects.isNull(action) ? ConsumeAction.RECONSUME_LATER : action;
        } catch (TimeoutException e) {
            future.cancel(true);
            // 等待业务线程真正终止（上限为宽限期）：缩小「原消费与重试副本并发执行」的窗口
            Thread t = taskThread.get();
            if (t != null && t != Thread.currentThread()) {
                try {
                    t.join(timeoutCancelGraceMillis);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            // 超时与竞争同刻发生的竞态：若任务已判定为竞争，按竞争处理（不消耗预算），不按超时记账
            OrderlyShardBusyException busy = busyRef.get();
            if (Objects.nonNull(busy)) {
                throw busy;
            }
            LOG.warn(
                    "Orderly consume timeout ({}ms), cancelling and retrying: topic={}, group={},"
                            + " messageId={}",
                    timeoutMillis,
                    reg.getTopic(),
                    reg.getGroup(),
                    message.getMessageId());
            return ConsumeAction.RECONSUME_LATER;
        } catch (ExecutionException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOG.warn(
                    "Orderly consume attempt failed: topic={}, group={}, error={}",
                    reg.getTopic(),
                    reg.getGroup(),
                    e.getMessage());
            return ConsumeAction.RECONSUME_LATER;
        }
    }

    // ===================== 过滤与指标 =====================

    /** 判断消息是否应该被消费：使用预缓存的过滤器链（全局链由构造注入，per-consumer 列表来自存储）。 */
    private boolean acceptMessage(Message<?> message, ListenerRegistration<?> reg) {
        List<io.github.streammq.core.filter.ConsumerFilter> filters = store.filters(reg.key());
        if (Objects.nonNull(filters)) {
            for (io.github.streammq.core.filter.ConsumerFilter filter : filters) {
                if (!filter.accept(message)) {
                    LOG.debug(
                            "Message rejected by filter: {} (topic={}, tag={})",
                            filter.name(),
                            message.getTopic(),
                            message.getTag());
                    return false;
                }
            }
        }
        return true;
    }

    private void recordConsumeMetrics(
            ListenerRegistration<?> reg, long startNanos, boolean success) {
        long elapsedNanos = System.nanoTime() - startNanos;
        if (Objects.nonNull(metrics)) {
            try {
                metrics.recordConsume(
                        reg.getTopic(), reg.getGroup(), success, Duration.ofNanos(elapsedNanos));
            } catch (Exception ignored) {
                LOG.debug("Metrics collection failed", ignored);
            }
        }
        // P1-3：进程内统计始终上报（不依赖 Actuator/Micrometer 是否在 classpath）
        RuntimeStatsRegistry stats = runtimeStats;
        if (Objects.nonNull(stats)) {
            stats.recordConsume(reg.getGroup(), reg.getTopic(), success, elapsedNanos);
        }
    }
}
