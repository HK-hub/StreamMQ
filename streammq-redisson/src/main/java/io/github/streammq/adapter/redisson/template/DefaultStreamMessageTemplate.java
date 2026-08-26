/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.template;

import io.github.streammq.adapter.redisson.filter.DefaultProducerFilterChain;
import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.adapter.redisson.support.MdcKeys;
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
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Setter;
import org.redisson.api.StreamMessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

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
public class DefaultStreamMessageTemplate implements StreamMessageTemplate {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamMessageTemplate.class);

    private final StreamMessageProducerFactory producerFactory;
    private final String defaultGroup;
    private final MessageConverter messageConverter;
    private final List<ProducerInterceptor> interceptors = new CopyOnWriteArrayList<>();
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

    /**
     * 注入异步发送执行器（须在首次发送前调用；容器不关闭外部池，生命周期归提供方）。
     *
     * @param executor 异步执行器
     */
    public void setAsyncSendExecutor(java.util.concurrent.ExecutorService executor) {
        this.asyncSendExecutor = java.util.Objects.requireNonNull(executor, "executor");
    }

    /**
     * 构造 Template。
     *
     * @param producerFactory 生产者工厂
     * @param defaultGroup 默认生产组名
     * @param messageConverter 消息转换器
     */
    public DefaultStreamMessageTemplate(
            StreamMessageProducerFactory producerFactory,
            String defaultGroup,
            MessageConverter messageConverter) {
        this(
                producerFactory,
                defaultGroup,
                messageConverter,
                ProducerConfig.builder().group(defaultGroup).build(),
                null);
    }

    /**
     * 全参构造。
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
        this.producerFactory = Objects.requireNonNull(producerFactory, "producerFactory");
        this.defaultGroup = Objects.requireNonNull(defaultGroup, "defaultGroup");
        this.messageConverter = Objects.requireNonNull(messageConverter, "messageConverter");
        this.defaultConfig = Objects.requireNonNull(defaultConfig, "defaultConfig");
        this.transactionGroup = transactionGroup;
    }

    // ===================== 发送 API（0.1.0 收敛为 SendOptions 规范形） =====================

    @Override
    public <T> SendResult syncSend(Message<T> message, SendOptions options) {
        Objects.requireNonNull(message, "message");
        SendOptions effective = Objects.nonNull(options) ? options : SendOptions.defaults();
        return doSyncSend(
                message, effective.effectiveTimeoutMillis(), effective.effectiveRetryTimes());
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
        injectProducerMdc(message);
        try {
            // 1. 拦截器 before（串联派生消息；中止时不回调 afterSend）
            String abortTopic = message.getTopic();
            String abortTag = message.getTag();
            long abortBornTs = message.getBornTimestamp();
            Message<T> intercepted = applyInterceptorsBefore(message);
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
                applyInterceptorsAfter(message, filtered);
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
                    applyInterceptorsAfter(message, result);
                    recordSendMetrics(message.getTopic(), result.isSuccess(), sendStart);
                    return result;
                } catch (StreamMQException ex) {
                    lastError = ex;
                    if (!isSafeToRetry(ex)) {
                        notifyProducerException(message, ex, InvokeTiming.EXECUTING);
                        recordSendMetrics(message.getTopic(), false, sendStart);
                        throw ex;
                    }
                    notifyProducerException(message, ex, InvokeTiming.EXECUTING);
                    LOG.warn(
                            "syncSend attempt {}/{} failed for topic {}: {}",
                            attempt + 1,
                            retryTimes + 1,
                            message.getTopic(),
                            ex.getMessage(),
                            ex);
                } catch (RuntimeException ex) {
                    notifyProducerException(message, ex, InvokeTiming.EXECUTING);
                    recordSendMetrics(message.getTopic(), false, sendStart);
                    throw ex;
                }
            }
            applyInterceptorsAfter(message, buildFailedResult(message, lastError));
            recordSendMetrics(message.getTopic(), false, sendStart);
            throw Objects.nonNull(lastError)
                    ? lastError
                    : new StreamMQException(
                            "syncSend failed for unknown reason: " + message.getTopic());
        } finally {
            // 清理 MDC 结构化日志上下文
            clearProducerMdc();
        }
    }

    @Override
    public <T> CompletableFuture<SendResult> asyncSend(Message<T> message, SendOptions options) {
        Objects.requireNonNull(message, "message");
        // 在专用虚拟线程执行器中按 SendOptions 语义执行（含拦截器/重试/指标），
        // 保证调用方非阻塞；不使用 ForkJoinPool.commonPool 以免饿死其它框架组件
        return CompletableFuture.supplyAsync(() -> syncSend(message, options), asyncSendExecutor);
    }

    @Override
    public <T> void sendOneway(Message<T> message) {
        Objects.requireNonNull(message, "message");
        // 注入 MDC 结构化日志上下文
        injectProducerMdc(message);
        try {
            Message<T> interceptedOneway = applyInterceptorsBefore(message);
            if (Objects.isNull(interceptedOneway)) {
                return;
            }
            message = interceptedOneway;
            StreamMessageProducer producer = resolveProducer(message.getTopic());
            try {
                producer.sendOneway(message);
            } catch (RuntimeException ex) {
                notifyProducerException(message, ex, InvokeTiming.EXECUTING);
                throw ex;
            }
        } finally {
            // 清理 MDC 结构化日志上下文
            clearProducerMdc();
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
        int retryTimes = Math.max(0, effective.effectiveRetryTimes());

        List<Message<T>> interceptedMessages = new ArrayList<>(batch.getMessages().size());
        for (Message<T> message : batch.getMessages()) {
            Message<T> intercepted = applyInterceptorsBefore(message);
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
                List<SendResult> finalResults = new ArrayList<>(results.size());
                for (int i = 0; i < results.size(); i++) {
                    SendResult result = results.get(i);
                    applyInterceptorsAfter(interceptedMessages.get(i), result);
                    finalResults.add(result);
                }
                return finalResults;
            } catch (StreamMQException ex) {
                lastError = ex;
                for (Message<T> msg : interceptedMessages) {
                    notifyProducerException(msg, ex, InvokeTiming.EXECUTING);
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
        for (Message<T> msg : interceptedMessages) {
            applyInterceptorsAfter(msg, buildFailedResult(msg, lastError));
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

        // 完整半消息流程依赖 TransactionScanner（回查调度器）。
        TransactionScanner scanner = this.transactionScanner;
        if (Objects.isNull(scanner)) {
            // 快速失败：无扫描器时无法提供半消息/回查保证，绝不允许"先投递再回滚"的假事务——
            // 那会导致 ROLLBACK 时消费者已经收到业务消息。
            throw new TransactionException(
                    "Transactional send requires an active TransactionScanner"
                            + " (transaction scheduler disabled?). Enable the transaction scheduler"
                            + " or inject a TransactionScanner instance.",
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
     *   <li>根据本地事务结果：COMMIT → 转投到业务 Stream；ROLLBACK → 删除半消息； UNKNOW → 保留半消息，等待 TransactionScanner
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
            case UNKNOW:
                // 保留半消息，等待 TransactionScanner 周期回查
                LOG.info(
                        "Transaction state UNKNOW, waiting for check-back: txId={}, txGroup={}",
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
        return Collections.unmodifiableList(interceptors);
    }

    @Override
    public void setProducerInterceptors(List<ProducerInterceptor> interceptors) {
        this.interceptors.clear();
        if (Objects.nonNull(interceptors)) {
            List<ProducerInterceptor> sorted = new ArrayList<>(interceptors);
            sorted.sort((a, b) -> Integer.compare(a.order(), b.order()));
            this.interceptors.addAll(sorted);
        }
    }

    @Override
    public void addProducerInterceptor(ProducerInterceptor interceptor) {
        Objects.requireNonNull(interceptor, "interceptor");
        // 保持按 order 升序
        int insertIndex = 0;
        for (ProducerInterceptor existing : interceptors) {
            if (existing.order() <= interceptor.order()) {
                insertIndex++;
            } else {
                break;
            }
        }
        interceptors.add(insertIndex, interceptor);
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
     * 执行 before 拦截器链，串联各拦截器返回的派生消息。
     *
     * @param message 待发送消息
     * @param <T> body 类型
     * @return 链路末端输出的消息实例；{@code null} 表示被任一拦截器中止
     */
    @SuppressWarnings("unchecked")
    private <T> Message<T> applyInterceptorsBefore(Message<T> message) {
        Message<?> current = message;
        for (ProducerInterceptor interceptor : interceptors) {
            try {
                current = interceptor.beforeSend(current);
                if (Objects.isNull(current)) {
                    LOG.debug(
                            "Interceptor {} aborted send: topic={}",
                            interceptor.name(),
                            message.getTopic());
                    return null;
                }
            } catch (RuntimeException ex) {
                LOG.warn(
                        "Interceptor {} beforeSend threw exception: {}",
                        interceptor.name(),
                        ex.getMessage(),
                        ex);
                notifyProducerException(message, ex, InvokeTiming.BEFORE);
                return null;
            }
        }
        return (Message<T>) current;
    }

    /**
     * 执行 after 拦截器链。
     *
     * @param message 已发送消息
     * @param result 发送结果
     */
    private void applyInterceptorsAfter(Message<?> message, SendResult result) {
        for (ProducerInterceptor interceptor : interceptors) {
            try {
                interceptor.afterSend(message, result);
            } catch (RuntimeException ex) {
                LOG.warn(
                        "Interceptor {} afterSend threw exception: {}",
                        interceptor.name(),
                        ex.getMessage(),
                        ex);
                notifyProducerException(message, ex, InvokeTiming.AFTER);
            }
        }
    }

    /**
     * 通知所有生产者拦截器发生异常（按 order() 升序）。
     *
     * <p>拦截器自身的 onException 异常被忽略，不影响主流程。
     *
     * @param message 消息
     * @param ex 异常
     * @param timing 触发时机（BEFORE/EXECUTING/AFTER）
     */
    private void notifyProducerException(Message<?> message, Exception ex, InvokeTiming timing) {
        for (ProducerInterceptor interceptor : interceptors) {
            try {
                interceptor.onException(message, ex, timing);
            } catch (Exception ignored) {
                // 拦截器异常不应影响主流程
                LOG.debug("Interceptor exception", ignored);
            }
        }
    }

    /**
     * 注入发送侧 MDC 结构化日志上下文。
     *
     * @param message 待发送消息
     */
    private void injectProducerMdc(Message<?> message) {
        MDC.put(MdcKeys.TOPIC, message.getTopic());
        MDC.put(MdcKeys.PRODUCER_GROUP, defaultGroup);
        if (Objects.nonNull(message.getMessageId())) {
            MDC.put(MdcKeys.MSG_ID, String.valueOf(message.getMessageId()));
        }
        if (Objects.nonNull(message.getShardingKey())) {
            MDC.put(MdcKeys.SHARDING_KEY, message.getShardingKey());
        }
    }

    /** 清理发送侧 MDC 结构化日志上下文。 */
    private void clearProducerMdc() {
        MDC.remove(MdcKeys.TOPIC);
        MDC.remove(MdcKeys.PRODUCER_GROUP);
        MDC.remove(MdcKeys.MSG_ID);
        MDC.remove(MdcKeys.SHARDING_KEY);
    }

    /**
     * 解析消息对应的 Producer。 当前实现：所有消息使用同一个 defaultGroup Producer。 未来可扩展：按 message.properties 中的 group
     * 字段路由。
     *
     * @param topic 当前主题（用于未来扩展路由）
     * @return Producer 实例
     */
    private StreamMessageProducer resolveProducer(String topic) {
        return producerFactory.createProducer(defaultConfig);
    }

    /**
     * 构造失败结果。
     *
     * @param message 消息
     * @param error 异常
     * @return 失败 SendResult
     */
    /**
     * 判断该异常是否可以安全重试：仅限发送尚未开始（消息确定未落库）的失败。
     *
     * <p>可重试：{@link io.github.streammq.core.exception.SerializationException}（序列化失败，未触达
     * Redis）、{@link io.github.streammq.core.exception.StreamMQClientException}（客户端校验/本地错误）。
     * 不可重试：{@link ProducerTimeoutException} / {@link
     * io.github.streammq.core.exception.StreamMQBrokerException} —— XADD 可能已成功，重试将产生重复消息。
     */
    private static boolean isSafeToRetry(StreamMQException ex) {
        return ex instanceof io.github.streammq.core.exception.SerializationException
                || ex instanceof io.github.streammq.core.exception.StreamMQClientException;
    }

    private SendResult buildFailedResult(Message<?> message, StreamMQException error) {
        return new SendResult(
                new MessageId(System.currentTimeMillis() + "-0"),
                message.getTopic(),
                message.getTag(),
                SendStatus.SEND_FAILED,
                message.getBornTimestamp(),
                null,
                Objects.nonNull(error) ? error.getMessage() : "unknown error");
    }

    @Override
    public void addProducerFilter(ProducerFilter filter) {
        producerFilterChain.addFilter(filter);
    }
}
