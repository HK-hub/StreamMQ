/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.ConsumeOrderlyContext;
import io.github.streammq.core.consumer.DlqMessageConsumer;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.InvokeTiming;
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
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * <p>容器保留生命周期、注册管理与读循环编排；本类无状态可并发调用。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultMessageProcessor implements MessageProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(MessageProcessor.class);

    /** 消费超时取消后，等待业务线程真正终止的默认宽限期（毫秒） */
    static final long DEFAULT_TIMEOUT_CANCEL_GRACE_MILLIS =
            io.github.streammq.core.StreamMQConstants.DEFAULT_TIMEOUT_CANCEL_GRACE_MS;

    private final ConsumerInterceptorChain interceptorChain;
    private final OrderlyShardLockManager shardLockManager;
    private final RegistrationStore store;
    private final RetryAndDlqHandler sharedRetryDlqHandler;
    private final boolean perConsumerEnabled;
    private final ExecutorService executor;

    /** 指标收集器（可选注入，null 时为 no-op） */
    private volatile StreamMQMetrics metrics;

    /** 消费超时取消后的宽限期（毫秒） */
    private volatile long timeoutCancelGraceMillis = DEFAULT_TIMEOUT_CANCEL_GRACE_MILLIS;

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
    public void setMetrics(StreamMQMetrics metrics) {
        this.metrics = metrics;
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

    /** 处理单条消息：支持消费超时取消，以 {@code onMessage} 返回值为路由标准。 */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void processMessage(
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
                finalAction = ConsumeAction.RECONSUME_LATER;
                handler.handleAction(ConsumeAction.RECONSUME_LATER, message, reg, listener, ex);
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

    /** 使用 Future.get(timeout) 包裹 onMessage 调用，超时后取消并进入重试。 */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void processWithTimeout(
            Message<?> message,
            ListenerRegistration reg,
            StreamMQListener listener,
            ConsumeContext ctx,
            RetryAndDlqHandler handler,
            long consumeStart) {
        AtomicReference<Thread> taskThread = new AtomicReference<>();
        Future<ConsumeAction> future =
                executor.submit(
                        () -> {
                            taskThread.set(Thread.currentThread());
                            if (reg.isDlqMode()) {
                                return processDlqMessage(message, reg, ctx);
                            }
                            StreamMessageConcurrentlyConsumer consumer =
                                    (StreamMessageConcurrentlyConsumer) reg.getConsumer();
                            ConsumeAction action = consumer.onMessage(message, ctx);
                            return action;
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
        ConsumeAction action = shardLockManager.consumeWithShardLock(message, reg, ctx, orderly);
        int attempt = 0;
        while (!action.isSuccess() && attempt < maxRetries) {
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
            action = shardLockManager.consumeWithShardLock(message, reg, ctx, orderly);
        }
        if (action.isSuccess()) {
            handler.handleAction(ConsumeAction.SUCCESS, message, reg, listener, null);
            return ConsumeAction.SUCCESS;
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
            LOG.error(
                    "DLQ routing failed, message kept in PEL (topic={}, group={}, messageId={})",
                    reg.getTopic(),
                    reg.getGroup(),
                    message.getMessageId());
        }
        return ConsumeAction.RECONSUME_LATER;
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
        if (Objects.nonNull(metrics)) {
            try {
                metrics.recordConsume(
                        reg.getTopic(),
                        reg.getGroup(),
                        success,
                        Duration.ofNanos(System.nanoTime() - startNanos));
            } catch (Exception ignored) {
                LOG.debug("Metrics collection failed", ignored);
            }
        }
    }
}
