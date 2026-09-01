/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.template;

import io.github.streammq.adapter.redisson.filter.DefaultProducerFilterChain;
import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.enums.LocalTransactionState;
import io.github.streammq.core.event.StreamMQEventBus;
import io.github.streammq.core.exception.StreamMQException;
import io.github.streammq.core.exception.TransactionException;
import io.github.streammq.core.filter.ProducerFilter;
import io.github.streammq.core.filter.ProducerFilterChain;
import io.github.streammq.core.interceptor.ProducerInterceptor;
import io.github.streammq.core.message.*;
import io.github.streammq.core.metrics.StreamMQMetrics;
import io.github.streammq.core.producer.ProducerConfig;
import io.github.streammq.core.producer.StreamMessageProducer;
import io.github.streammq.core.producer.StreamMessageProducerFactory;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.core.transaction.TransactionCallback;
import io.github.streammq.core.transaction.TransactionContext;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Setter;
import org.redisson.api.StreamMessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link StreamMessageTemplate} 的默认实现。
 *
 * <p>组合 {@link StreamMessageProducerFactory} 与拦截器链，对上层提供业务友好的发送 API。
 *
 * <p>核心职责：
 *
 * <ul>
 *   <li>调度 {@link ProducerInterceptor} beforeSend / afterSend 链
 *   <li>选择合适的 Producer（按 group）委派实际发送
 *   <li>事务消息编排：半消息发送 + 本地事务执行 + 状态更新（基础实现，回查调度在 p6）
 *   <li>批量发送：转发到 Producer 的 syncSendBatch
 * </ul>
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型，可在多线程间共享单例。
 *
 * <p><b>泛型设计</b>：泛型参数 {@code <T>} 声明在方法级别，单例 Template 可发送任意 body 类型的消息。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultStreamMessageTemplate implements StreamMessageTemplate, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamMessageTemplate.class);

    private final StreamMessageProducer producer;
    private final StreamMessageProducerFactory producerFactory;
    private final String defaultGroup;
    private final MessageConverter messageConverter;
    private final ProducerInterceptorChain interceptorChain;
    private final ProducerFilterChain producerFilterChain = new DefaultProducerFilterChain();
    private final ProducerConfig defaultConfig;
    private final String transactionGroup;

    /**
     * 事务扫描器（可选注入，用于半消息 + 回查的完整事务流程）。
     *
     * <p>为 null 时回退到简化实现（直接发送 + 本地事务，无回查保护）。
     */
    @Setter private volatile TransactionScanner transactionScanner;

    /** 指标收集器（可选注入，用于记录发送指标，null 时为 no-op）。 */
    @Setter private volatile StreamMQMetrics metrics;

    /** 事件总线（可选注入，用于异步发布消息发送事件，解耦 Tracing/Metrics）。 */
    @Setter private volatile StreamMQEventBus eventBus;

    /**
     * 异步发送专用执行器：默认统一虚拟线程池（不占用 ForkJoinPool.commonPool）。 Spring 环境由自动装配注入统一管理的实现（{@link
     * #setAsyncSendExecutor}）。
     */
    private volatile java.util.concurrent.ExecutorService asyncSendExecutor =
            java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();

    /** Whether the template owns and therefore must close the current executor. */
    private volatile boolean ownsAsyncSendExecutor = true;

    private volatile boolean closed;

    /**
     * 注入异步发送执行器（须在首次发送前调用；容器不关闭外部池，生命周期归提供方）。
     *
     * @param executor 异步执行器
     */
    public synchronized void setAsyncSendExecutor(java.util.concurrent.ExecutorService executor) {
        Objects.requireNonNull(executor, "executor");
        if (closed) {
            throw new IllegalStateException("StreamMessageTemplate is already closed");
        }
        java.util.concurrent.ExecutorService previous = this.asyncSendExecutor;
        boolean previousOwned = this.ownsAsyncSendExecutor;
        this.asyncSendExecutor = executor;
        this.ownsAsyncSendExecutor = false;
        if (previousOwned && previous != executor) {
            previous.shutdown();
        }
    }

    /**
     * Releases resources owned by this template. An executor injected through {@link
     * #setAsyncSendExecutor(java.util.concurrent.ExecutorService)} is intentionally left running
     * because its lifecycle belongs to the caller.
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (ownsAsyncSendExecutor) {
            asyncSendExecutor.shutdown();
        }
    }

    /**
     * 构造 Template（直接注入 Producer）。
     *
     * <p>推荐在 Spring 环境中使用此构造函数：Producer 作为 Bean 由容器管理生命周期，
     * Template 直接复用，避免 Factory 中间层的歧义与性能开销。
     *
     * @param producer 生产者实例（线程安全，可复用）
     * @param defaultGroup 默认生产组名
     * @param messageConverter 消息转换器
     */
    public DefaultStreamMessageTemplate(
            StreamMessageProducer producer,
            String defaultGroup,
            MessageConverter messageConverter) {
        this(
                producer,
                defaultGroup,
                messageConverter,
                ProducerConfig.builder().group(defaultGroup).build(),
                null);
    }

    /**
     * 全参构造（直接注入 Producer）。
     *
     * @param producer 生产者实例（线程安全，可复用）
     * @param defaultGroup 默认生产组名
     * @param messageConverter 消息转换器
     * @param defaultConfig 默认生产者配置
     * @param transactionGroup 事务组名（用于事务消息），可为 null
     */
    public DefaultStreamMessageTemplate(
            StreamMessageProducer producer,
            String defaultGroup,
            MessageConverter messageConverter,
            ProducerConfig defaultConfig,
            String transactionGroup) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.producerFactory = null;
        this.defaultGroup = Objects.requireNonNull(defaultGroup, "defaultGroup");
        this.messageConverter = Objects.requireNonNull(messageConverter, "messageConverter");
        this.interceptorChain = new ProducerInterceptorChain(this.defaultGroup);
        this.defaultConfig = Objects.requireNonNull(defaultConfig, "defaultConfig");
        this.transactionGroup = transactionGroup;
    }

    /**
     * 构造 Template（通过 Factory 创建 Producer）。
     *
     * <p>保留此构造函数供非 Spring / 需要动态创建 Producer 的场景使用。Factory 内部有缓存，
     * 同配置多次调用返回同一实例，但设计上仍建议在构造期解析一次并持有。
     *
     * @param producerFactory 生产者工厂
     * @param defaultGroup 默认生产组名
     * @param messageConverter 消息转换器
     * @param defaultConfig 默认生产者配置（用于创建 Producer）
     * @param transactionGroup 事务组名（用于事务消息），可为 null
     */
    public DefaultStreamMessageTemplate(
            StreamMessageProducerFactory producerFactory,
            String defaultGroup,
            MessageConverter messageConverter,
            ProducerConfig defaultConfig,
            String transactionGroup) {
        Objects.requireNonNull(producerFactory, "producerFactory");
        this.producer = producerFactory.createProducer(defaultConfig);
        this.producerFactory = producerFactory;
        this.defaultGroup = Objects.requireNonNull(defaultGroup, "defaultGroup");
        this.messageConverter = Objects.requireNonNull(messageConverter, "messageConverter");
        this.interceptorChain = new ProducerInterceptorChain(this.defaultGroup);
        this.defaultConfig = Objects.requireNonNull(defaultConfig, "defaultConfig");
        this.transactionGroup = transactionGroup;
    }

    // ===================== 发送 API（0.1.0 收敛为 SendOptions 规范形） =====================

    @Override
    public <T> SendResult syncSend(Message<T> message, SendOptions options) {
        Objects.requireNonNull(message, "message");
        // 当调用方未传 SendOptions（使用默认）或未显式覆盖 retryTimes 时，
        // 优先采用 ProducerConfig.retryTimes（即 streammq.producer.retry-times 配置）。
        // 显式 options 才覆盖 ProducerConfig。
        // 夹取到 [0, MAX_SYNC_RETRY_TIMES] 防止误配 Integer.MAX_VALUE 导致无限重试
        int rawRetryTimes =
                Objects.nonNull(options)
                        ? options.effectiveRetryTimes()
                        : defaultConfig.getRetryTimes();
        int retryTimes =
                Math.max(
                        0,
                        Math.min(
                                rawRetryTimes,
                                io.github.streammq.core.StreamMQConstants.MAX_SYNC_RETRY_TIMES));
        long timeoutMillis =
                Objects.nonNull(options)
                        ? options.effectiveTimeoutMillis()
                        : defaultConfig.getSendMessageTimeout();
        return doSyncSend(message, timeoutMillis, retryTimes);
    }

    /**
     * 同步发送核心实现（含拦截器链、过滤器、重试与指标）。
     *
     * <p><b>重试安全规则：</b>仅对"确定未送达"的异常重试（序列化/客户端校验等发送前失败）。 {@link
     * io.github.streammq.core.exception.ProducerTimeoutException} / StreamMQBrokerException 意味着
     * XADD 可能已落库，重试会产生重复消息——直接抛出， 由业务侧按 at-least-once 语义处理。
     */
    private <T> SendResult doSyncSend(Message<T> message, long timeoutMillis, int retryTimes) {
        if (timeoutMillis <= 0) {
            timeoutMillis = DEFAULT_SEND_TIMEOUT_MILLIS;
        }
        if (retryTimes < 0) {
            retryTimes = 0;
        }

        // 注入 MDC 结构化日志上下文
        interceptorChain.injectMdc(message);
        try {
            // 1. 拦截器 before（串联派生消息；中止时不回调 afterSend）
            String abortTopic = message.getTopic();
            String abortTag = message.getTag();
            long abortBornTs = message.getBornTimestamp();
            Message<T> intercepted = interceptorChain.beforeSend(message);
            if (Objects.isNull(intercepted)) {
                return new SendResult(
                        MessageId.sentinel(),
                        abortTopic,
                        abortTag,
                        SendStatus.SEND_FAILED,
                        abortBornTs,
                        null,
                        "Aborted by interceptor");
            }
            message = intercepted;

            // 2. 生产者过滤器检查
            if (!producerFilterChain.accept(message)) {
                // 被过滤器拒绝
                SendResult filtered =
                        new SendResult(
                                MessageId.sentinel(),
                                message.getTopic(),
                                message.getTag(),
                                SendStatus.SEND_FAILED,
                                message.getBornTimestamp(),
                                null,
                                "Filtered by producer filter");
                interceptorChain.afterSend(message, filtered);
                return filtered;
            }

            // 3. 委派 Producer 发送（含重试）
            // 重试安全规则：仅对"确定未送达"的异常重试（序列化/客户端校验等发送前失败）。
            // ProducerTimeoutException / StreamMQBrokerException 意味着 XADD 可能已落库，
            // 重试会产生重复消息——直接抛出，由业务侧按 at-least-once 语义处理。
            StreamMessageProducer producer = resolveProducer(message.getTopic());
            long sendStart = System.nanoTime();
            StreamMQException lastError = null;
            for (int attempt = 0; attempt <= retryTimes; attempt++) {
                try {
                    SendResult result = producer.syncSend(message, timeoutMillis);
                    interceptorChain.afterSend(message, result);
                    recordSendMetrics(message.getTopic(), result.isSuccess(), sendStart);
                    return result;
                } catch (StreamMQException ex) {
                    lastError = ex;
                    if (!RetrySafetyPolicy.isSafeToRetry(ex)) {
                        interceptorChain.notifyException(message, ex, InvokeTiming.EXECUTING);
                        recordSendMetrics(message.getTopic(), false, sendStart);
                        throw ex;
                    }
                    interceptorChain.notifyException(message, ex, InvokeTiming.EXECUTING);
                    LOG.warn(
                            "syncSend attempt {}/{} failed for topic {}: {}",
                            attempt + 1,
                            retryTimes + 1,
                            message.getTopic(),
                            ex.getMessage(),
                            ex);
                } catch (RuntimeException ex) {
                    interceptorChain.notifyException(message, ex, InvokeTiming.EXECUTING);
                    recordSendMetrics(message.getTopic(), false, sendStart);
                    throw ex;
                }
            }
            interceptorChain.afterSend(
                    message, RetrySafetyPolicy.buildFailedResult(message, lastError));
            recordSendMetrics(message.getTopic(), false, sendStart);
            throw Objects.nonNull(lastError)
                    ? lastError
                    : new StreamMQException(
                            "syncSend failed for unknown reason: " + message.getTopic());
        } finally {
            // 清理 MDC 结构化日志上下文
            interceptorChain.clearMdc();
        }
    }

    @Override
    public <T> CompletableFuture<SendResult> asyncSend(Message<T> message, SendOptions options) {
        if (closed) {
            CompletableFuture<SendResult> failed = new CompletableFuture<>();
            failed.completeExceptionally(
                    new IllegalStateException("StreamMessageTemplate is closed"));
            return failed;
        }
        Objects.requireNonNull(message, "message");
        // 捕获调用线程的 MDC 快照，并在虚拟线程中恢复（虚拟线程不会继承 InheritableThreadLocal）。
        // 这是修复 README "MDC.put('traceId', 't-001'); template.asyncSend(message)" 失效的关键。
        java.util.Map<String, String> mdcSnapshot = org.slf4j.MDC.getCopyOfContextMap();
        try {
            return CompletableFuture.supplyAsync(
                    () -> {
                        java.util.Map<String, String> previous =
                                org.slf4j.MDC.getCopyOfContextMap();
                        if (mdcSnapshot != null) {
                            org.slf4j.MDC.setContextMap(mdcSnapshot);
                        } else {
                            org.slf4j.MDC.clear();
                        }
                        try {
                            return syncSend(message, options);
                        } finally {
                            if (previous != null) {
                                org.slf4j.MDC.setContextMap(previous);
                            } else {
                                org.slf4j.MDC.clear();
                            }
                        }
                    },
                    asyncSendExecutor);
        } catch (java.util.concurrent.RejectedExecutionException ex) {
            CompletableFuture<SendResult> failed = new CompletableFuture<>();
            failed.completeExceptionally(
                    new IllegalStateException("StreamMessageTemplate executor is unavailable", ex));
            return failed;
        }
    }

    @Override
    public <T> void sendOneway(Message<T> message) {
        Objects.requireNonNull(message, "message");
        // 注入 MDC 结构化日志上下文
        interceptorChain.injectMdc(message);
        try {
            Message<T> interceptedOneway = interceptorChain.beforeSend(message);
            if (Objects.isNull(interceptedOneway)) {
                return;
            }
            message = interceptedOneway;
            StreamMessageProducer producer = resolveProducer(message.getTopic());
            try {
                producer.sendOneway(message);
            } catch (RuntimeException ex) {
                interceptorChain.notifyException(message, ex, InvokeTiming.EXECUTING);
                throw ex;
            }
        } finally {
            // 清理 MDC 结构化日志上下文
            interceptorChain.clearMdc();
        }
    }

    @Override
    public <T> List<SendResult> syncSendBatch(BatchMessage<T> batch, SendOptions options) {
        Objects.requireNonNull(batch, "batch");
        if (batch.isEmpty()) {
            throw new IllegalArgumentException("batch is empty");
        }
        SendOptions effective = Objects.nonNull(options) ? options : SendOptions.defaults();
        long timeoutMillis = effective.effectiveTimeoutMillis();
        int retryTimes =
                Math.max(
                        0,
                        Math.min(
                                effective.effectiveRetryTimes(),
                                io.github.streammq.core.StreamMQConstants.MAX_SYNC_RETRY_TIMES));

        List<Message<T>> interceptedMessages = new ArrayList<>(batch.getMessages().size());
        for (Message<T> message : batch.getMessages()) {
            Message<T> intercepted = interceptorChain.beforeSend(message);
            if (Objects.isNull(intercepted)) {
                throw new StreamMQException(
                        "Batch send aborted by interceptor for topic: " + message.getTopic());
            }
            interceptedMessages.add(intercepted);
        }

        StreamMessageProducer producer = resolveProducer(batch.getTopic());
        StreamMQException lastError = null;
        for (int attempt = 0; attempt <= retryTimes; attempt++) {
            try {
                List<SendResult> results =
                        producer.syncSendBatch(interceptedMessages, timeoutMillis);
                // 区分：单条失败 vs 整批失败
                // 1) 若 results 是 partial（个别 SEND_FAILED），直接透传（每条结果独立标识）
                // 2) 若整批抛异常，进入重试路径
                int successCount = 0;
                List<SendResult> finalResults = new ArrayList<>(results.size());
                for (int i = 0; i < results.size(); i++) {
                    SendResult result = results.get(i);
                    interceptorChain.afterSend(interceptedMessages.get(i), result);
                    finalResults.add(result);
                    if (result != null && result.isSuccess()) {
                        successCount++;
                    }
                }
                if (successCount > 0 || results.size() == 0) {
                    // 至少一条成功，或空 batch——直接返回（per-message 结果由调用方处理）
                    LOG.debug(
                            "Batch send completed: topic={}, total={}, success={}",
                            batch.getTopic(),
                            results.size(),
                            successCount);
                    return finalResults;
                }
                // 全部失败但无异常（极少见）——记 lastError 并按重试路径处理
                lastError =
                        new StreamMQException(
                                "Batch send: all "
                                        + results.size()
                                        + " messages failed without exception");
                for (Message<T> msg : interceptedMessages) {
                    interceptorChain.notifyException(msg, lastError, InvokeTiming.EXECUTING);
                }
                LOG.warn(
                        "syncSendBatch attempt {}/{}: all messages failed for topic {}",
                        attempt + 1,
                        retryTimes + 1,
                        batch.getTopic());
            } catch (StreamMQException ex) {
                lastError = ex;
                for (Message<T> msg : interceptedMessages) {
                    interceptorChain.notifyException(msg, ex, InvokeTiming.EXECUTING);
                }
                LOG.warn(
                        "syncSendBatch attempt {}/{} failed for topic {}: {}",
                        attempt + 1,
                        retryTimes + 1,
                        batch.getTopic(),
                        ex.getMessage(),
                        ex);
            }
        }
        // 重试耗尽：所有消息都标记为失败并发出失败结果
        List<SendResult> failureResults = new ArrayList<>(interceptedMessages.size());
        for (Message<T> msg : interceptedMessages) {
            SendResult failed = RetrySafetyPolicy.buildFailedResult(msg, lastError);
            interceptorChain.afterSend(msg, failed);
            failureResults.add(failed);
        }
        throw Objects.nonNull(lastError)
                ? lastError
                : new StreamMQException(
                        "syncSendBatch failed for unknown reason: " + batch.getTopic());
    }

    @Override
    public <T> SendResult executeInTransaction(
            Message<T> message, TransactionCallback<T> callback) {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(callback, "callback");
        if (Objects.isNull(transactionGroup)) {
            throw new TransactionException("Transaction group not configured", null, null);
        }

        String transactionId = UUID.randomUUID().toString();
        message = message.withTransactionId(transactionId);

        // 完整半消息 + 回查语义必须由 TransactionScanner 提供，缺失时快速失败（fail fast）。
        //
        // 这里刻意**不**降级为「同步本地事务 + 即时发送/回滚」的简化路径：简化路径在 JVM 崩溃时
        // 会留下永久悬挂的半消息，且没有任何回查机制能补偿——对事务消息而言，静默降级为
        // 低一致性语义比直接报错更危险。历史上该降级路径曾以 executeInTransactionInline 存在，
        // 但因从未被调用（死代码）且与公开文档矛盾，已于 0.1.0 发布前移除。
        TransactionScanner scanner = this.transactionScanner;
        if (Objects.isNull(scanner)) {
            throw new TransactionException(
                    "TransactionScanner is required for executeInTransaction; register the"
                        + " TransactionScanner bean (Spring Boot auto-configuration registers it"
                        + " when streammq.transaction.enabled=true) so half messages can be"
                        + " committed, rolled back or checked back",
                    transactionId,
                    transactionGroup);
        }
        return executeInTransactionWithScanner(message, callback, transactionId, scanner);
    }

    /**
     * 使用 TransactionScanner 的完整半消息事务流程。
     *
     * <p>流程：
     *
     * <ol>
     *   <li>将消息字段写入 half Stream（半消息），对消费者不可见
     *   <li>执行本地事务回调
     *   <li>根据本地事务结果：COMMIT → 转投到业务 Stream；ROLLBACK → 删除半消息； UNKNOWN → 保留半消息，等待 TransactionScanner
     *       回查
     * </ol>
     *
     * @param message 消息
     * @param callback 本地事务回调
     * @param transactionId 事务 ID
     * @param scanner 事务扫描器
     * @param <T> 消息体类型
     * @return 发送结果
     */
    private <T> SendResult executeInTransactionWithScanner(
            Message<T> message,
            TransactionCallback<T> callback,
            String transactionId,
            TransactionScanner scanner) {
        // 1. 将消息转换为 Stream fields 并注册半消息
        Map<String, String> fields = messageConverter.toStreamFields(message);
        StreamMessageId halfId;
        try {
            halfId =
                    scanner.registerHalfMessage(
                            transactionId, transactionGroup, message.getTopic(), fields);
            LOG.debug(
                    "Half message registered: txId={}, txGroup={}, halfId={}",
                    transactionId,
                    transactionGroup,
                    halfId);
        } catch (RuntimeException ex) {
            throw new TransactionException(
                    "Half message register failed: " + ex.getMessage(),
                    transactionId,
                    transactionGroup,
                    ex);
        }

        // 2. 构造半消息发送结果（真实半消息 Entry ID）
        MessageId msgId = MessageId.fromStreamMessageId(halfId);
        SendResult halfResult =
                new SendResult(
                        msgId, message.getTopic(), message.getTag(), message.getBornTimestamp());

        // 3. 执行本地事务
        TransactionContext ctx =
                new TransactionContext(
                        transactionId,
                        transactionGroup,
                        defaultGroup,
                        System.currentTimeMillis(),
                        new ConcurrentHashMap<>());
        LocalTransactionState state;
        try {
            state = callback.execute(message, ctx);
        } catch (Exception ex) {
            LOG.warn(
                    "Local transaction failed, rolling back: txId={}: {}",
                    transactionId,
                    ex.getMessage(),
                    ex);
            try {
                scanner.markRollback(transactionId, transactionGroup);
            } catch (RuntimeException rollbackEx) {
                LOG.error(
                        "Failed to rollback transaction after local tx failure: txId={}: {}",
                        transactionId,
                        rollbackEx.getMessage(),
                        rollbackEx);
            }
            throw new TransactionException(
                    "Local transaction execute failed", transactionId, transactionGroup, ex);
        }

        // 4. 根据状态决定 commit / rollback / unknown
        // null 视为 UNKNOWN：回调返回 null 与"状态未知"是同一语义，都交给回查兜底。
        if (state == null) {
            state = LocalTransactionState.UNKNOWN;
        }
        switch (state) {
            case COMMIT_MESSAGE:
                try {
                    scanner.markCommit(transactionId, transactionGroup);
                    LOG.info(
                            "Transaction committed: txId={}, txGroup={}, targetTopic={}",
                            transactionId,
                            transactionGroup,
                            message.getTopic());
                } catch (RuntimeException ex) {
                    LOG.error(
                            "Failed to commit transaction: txId={}: {}",
                            transactionId,
                            ex.getMessage(),
                            ex);
                    throw new TransactionException(
                            "Transaction commit failed: " + ex.getMessage(),
                            transactionId,
                            transactionGroup,
                            ex);
                }
                return halfResult;
            case ROLLBACK_MESSAGE:
                String rollbackFailure = null;
                try {
                    scanner.markRollback(transactionId, transactionGroup);
                    LOG.info(
                            "Transaction rolled back: txId={}, txGroup={}",
                            transactionId,
                            transactionGroup);
                } catch (RuntimeException ex) {
                    rollbackFailure = ex.getMessage();
                    LOG.error(
                            "Failed to rollback transaction, half message remains and"
                                    + " TransactionScanner will force-rollback after bounded"
                                    + " rechecks: txId={}, txGroup={}: {}",
                            transactionId,
                            transactionGroup,
                            ex.getMessage(),
                            ex);
                }
                return new SendResult(
                        msgId,
                        message.getTopic(),
                        message.getTag(),
                        SendStatus.SEND_FAILED,
                        message.getBornTimestamp(),
                        null,
                        Objects.nonNull(rollbackFailure)
                                ? "Rollback failed (scanner will reconcile): " + rollbackFailure
                                : "Transaction rolled back");
            case LocalTransactionState.UNKNOWN:
                // 保留半消息，等待 TransactionScanner 周期回查
                LOG.info(
                        "Transaction state UNKNOWN, waiting for check-back: txId={}, txGroup={}",
                        transactionId,
                        transactionGroup);
                return halfResult;
            default:
                try {
                    scanner.markRollback(transactionId, transactionGroup);
                } catch (RuntimeException rollbackEx) {
                    LOG.warn("Failed to rollback on unknown state: txId={}", transactionId);
                }
                throw new TransactionException(
                        "Unknown transaction state: " + state, transactionId, transactionGroup);
        }
    }

    @Override
    public MessageConverter getMessageConverter() {
        return messageConverter;
    }

    @Override
    public List<ProducerInterceptor> getProducerInterceptors() {
        return interceptorChain.snapshot();
    }

    @Override
    public void setProducerInterceptors(List<ProducerInterceptor> interceptors) {
        interceptorChain.setAll(interceptors);
    }

    @Override
    public void addProducerInterceptor(ProducerInterceptor interceptor) {
        interceptorChain.add(interceptor);
    }

    // ===================== 内部方法 =====================

    /**
     * 记录发送指标（null 安全，指标异常不影响业务主流程）。
     *
     * @param topic 消息主题
     * @param success 是否发送成功
     * @param startNanos 发送起始时间（{@link System#nanoTime()}）
     */
    private void recordSendMetrics(String topic, boolean success, long startNanos) {
        if (Objects.nonNull(metrics)) {
            try {
                metrics.recordSend(
                        topic, success, Duration.ofNanos(System.nanoTime() - startNanos));
            } catch (Exception ignored) {
                // 指标收集失败不得影响业务主流程
                LOG.debug("Metrics collection failed", ignored);
            }
        }
    }

    /**
     * 解析消息对应的 Producer。当前实现：所有消息使用同一个已缓存的 Producer 实例。
     *
     * @param topic 当前主题（用于未来扩展路由）
     * @return Producer 实例
     */
    private StreamMessageProducer resolveProducer(String topic) {
        return producer;
    }

    @Override
    public void addProducerFilter(ProducerFilter filter) {
        producerFilterChain.addFilter(filter);
    }

    // StreamMessageService 桥接已统一在 DefaultStreamMessageService 中实现，
    // 通过 @Autowired StreamMessageService 注入业务门面；本类保留 StreamMessageTemplate 完整 API。

    private static SendOptions metadataToOptions(MessageMetadataBuilder metadata) {
        if (Objects.isNull(metadata)) {
            return SendOptions.defaults();
        }
        long timeout = metadata.getTimeoutMillis();
        int retries = metadata.getRetryTimes();
        if (timeout <= 0 && retries < 0) {
            return SendOptions.defaults();
        }
        return SendOptions.of(timeout > 0 ? timeout : -1, retries >= 0 ? retries : -1);
    }
}
