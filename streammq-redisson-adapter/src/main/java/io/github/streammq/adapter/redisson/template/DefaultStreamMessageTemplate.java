package io.github.streammq.adapter.redisson.template;

import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.adapter.redisson.support.MdcKeys;
import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.enums.LocalTransactionState;
import io.github.streammq.core.exception.StreamMQException;
import io.github.streammq.core.exception.TransactionException;
import io.github.streammq.core.message.*;
import io.github.streammq.core.producer.ProducerConfig;
import io.github.streammq.core.producer.SendCallback;
import io.github.streammq.core.producer.StreamMessageProducer;
import io.github.streammq.core.producer.StreamMessageProducerFactory;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.interceptor.ProducerInterceptor;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.core.transaction.TransactionCallback;
import io.github.streammq.core.transaction.TransactionContext;
import org.redisson.api.StreamMessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * {@link StreamMessageTemplate} 的默认实现。
 *
 * <p>组合 {@link StreamMessageProducerFactory} 与拦截器链，对上层提供业务友好的发送 API。
 *
 * <p>核心职责：
 * <ul>
 *   <li>调度 {@link ProducerInterceptor} beforeSend / afterSend 链</li>
 *   <li>选择合适的 Producer（按 group）委派实际发送</li>
 *   <li>事务消息编排：半消息发送 + 本地事务执行 + 状态更新（基础实现，回查调度在 p6）</li>
 *   <li>批量发送：转发到 Producer 的 syncSendBatch</li>
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
    private final ProducerConfig defaultConfig;
    private final String transactionGroup;
    /** 事务扫描器（可选注入，用于半消息 + 回查的完整事务流程） */
    private volatile TransactionScanner transactionScanner;

    /**
     * 构造 Template。
     *
     * @param producerFactory 生产者工厂
     * @param defaultGroup 默认生产组名
     * @param messageConverter 消息转换器
     */
    public DefaultStreamMessageTemplate(StreamMessageProducerFactory producerFactory,
                                        String defaultGroup,
                                        MessageConverter messageConverter) {
        this(producerFactory, defaultGroup, messageConverter,
            ProducerConfig.builder().group(defaultGroup).build(), null);
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
    public DefaultStreamMessageTemplate(StreamMessageProducerFactory producerFactory,
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

    @Override
    public <T> SendResult syncSend(Message<T> message) {
        return syncSend(message, DEFAULT_SEND_TIMEOUT_MILLIS, DEFAULT_SYNC_RETRY_TIMES);
    }

    @Override
    public <T> SendResult syncSend(Message<T> message, long timeoutMillis) {
        return syncSend(message, timeoutMillis, DEFAULT_SYNC_RETRY_TIMES);
    }

    @Override
    public <T> SendResult syncSend(Message<T> message, long timeoutMillis, int retryTimes) {
        Objects.requireNonNull(message, "message");
        if (timeoutMillis <= 0) {
            timeoutMillis = DEFAULT_SEND_TIMEOUT_MILLIS;
        }
        if (retryTimes < 0) {
            retryTimes = 0;
        }

        // 注入 MDC 结构化日志上下文
        injectProducerMdc(message);
        try {
            // 1. 拦截器 before
            if (!applyInterceptorsBefore(message)) {
                // 被拦截器中止
                SendResult aborted = new SendResult(
                    new MessageId(System.currentTimeMillis() + "-0"),
                    message.getTopic(), message.getTag(), SendStatus.SEND_FAILED,
                    message.getBornTimestamp(), null, "Aborted by interceptor");
                applyInterceptorsAfter(message, aborted);
                return aborted;
            }

            // 2. 委派 Producer 发送（含重试）
            StreamMessageProducer producer = resolveProducer(message.getTopic());
            StreamMQException lastError = null;
            for (int attempt = 0; attempt <= retryTimes; attempt++) {
                try {
                    SendResult result = producer.syncSend(message, timeoutMillis);
                    applyInterceptorsAfter(message, result);
                    return result;
                } catch (StreamMQException ex) {
                    lastError = ex;
                    notifyProducerException(message, ex, InvokeTiming.EXECUTING);
                    LOG.warn("syncSend attempt {}/{} failed for topic {}: {}",
                        attempt + 1, retryTimes + 1, message.getTopic(), ex.getMessage(), ex);
                } catch (RuntimeException ex) {
                    notifyProducerException(message, ex, InvokeTiming.EXECUTING);
                    throw ex;
                }
            }
            applyInterceptorsAfter(message, buildFailedResult(message, lastError));
            throw lastError != null ? lastError
                : new StreamMQException("syncSend failed for unknown reason: " + message.getTopic());
        } finally {
            // 清理 MDC 结构化日志上下文
            clearProducerMdc();
        }
    }

    @Override
    public <T> CompletableFuture<SendResult> asyncSend(Message<T> message) {
        Objects.requireNonNull(message, "message");
        // 注入 MDC 结构化日志上下文
        injectProducerMdc(message);
        try {
            // 先执行 before 拦截器，被中止时返回 failedFuture
            if (!applyInterceptorsBefore(message)) {
                return CompletableFuture.failedFuture(
                    new StreamMQException("Aborted by interceptor"));
            }
            StreamMessageProducer producer = resolveProducer(message.getTopic());
            return producer.asyncSend(message).whenComplete((result, ex) -> {
                if (ex == null) {
                    applyInterceptorsAfter(message, result);
                } else {
                    Exception e = ex instanceof Exception ? (Exception) ex
                        : new StreamMQException("async send failed", ex);
                    notifyProducerException(message, e, InvokeTiming.EXECUTING);
                }
            });
        } finally {
            // 清理 MDC 结构化日志上下文
            clearProducerMdc();
        }
    }

    @Override
    public <T> void asyncSend(Message<T> message, SendCallback callback) {
        asyncSend(message, callback, DEFAULT_SEND_TIMEOUT_MILLIS);
    }

    @Override
    public <T> void asyncSend(Message<T> message, SendCallback callback, long timeoutMillis) {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(callback, "callback");
        // 注入 MDC 结构化日志上下文
        injectProducerMdc(message);
        try {
            if (!applyInterceptorsBefore(message)) {
                callback.onException(new StreamMQException("Aborted by interceptor"));
                return;
            }
            StreamMessageProducer producer = resolveProducer(message.getTopic());
            producer.asyncSend(message).whenComplete((result, ex) -> {
                if (ex == null) {
                    applyInterceptorsAfter(message, result);
                    callback.onSuccess(result);
                } else {
                    Exception e = ex instanceof Exception ? (Exception) ex
                        : new StreamMQException("async send failed", ex);
                    notifyProducerException(message, e, InvokeTiming.EXECUTING);
                    callback.onException(ex instanceof RuntimeException re ? re : new StreamMQException("async send failed", ex));
                }
            });
        } finally {
            // 清理 MDC 结构化日志上下文
            clearProducerMdc();
        }
    }

    @Override
    public <T> void sendOneway(Message<T> message) {
        Objects.requireNonNull(message, "message");
        // 注入 MDC 结构化日志上下文
        injectProducerMdc(message);
        try {
            applyInterceptorsBefore(message);
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
    public <T> List<SendResult> syncSendBatch(BatchMessage<T> batch) {
        Objects.requireNonNull(batch, "batch");
        if (batch.isEmpty()) {
            throw new IllegalArgumentException("batch is empty");
        }

        for (Message<T> message : batch.getMessages()) {
            if (!applyInterceptorsBefore(message)) {
                throw new StreamMQException(
                    "Batch send aborted by interceptor for topic: " + message.getTopic());
            }
        }

        StreamMessageProducer producer = resolveProducer(batch.getTopic());
        List<SendResult> results;
        try {
            results = producer.syncSendBatch(batch.getMessages());
        } catch (RuntimeException ex) {
            for (Message<T> msg : batch.getMessages()) {
                notifyProducerException(msg, ex, InvokeTiming.EXECUTING);
            }
            throw ex;
        }

        List<SendResult> finalResults = new ArrayList<>(results.size());
        for (int i = 0; i < results.size(); i++) {
            SendResult result = results.get(i);
            applyInterceptorsAfter(batch.getMessages().get(i), result);
            finalResults.add(result);
        }
        return finalResults;
    }

    @Override
    public <T> SendResult executeInTransaction(Message<T> message, TransactionCallback<T> callback) {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(callback, "callback");
        if (transactionGroup == null) {
            throw new TransactionException("Transaction group not configured",
                null, null);
        }

        String transactionId = UUID.randomUUID().toString();
        message.setTransactionId(transactionId);

        // 完整半消息流程：使用 TransactionScanner（如果已注入）
        // 否则回退到简化实现（直接发送 + 本地事务，无回查保护）
        TransactionScanner scanner = this.transactionScanner;
        if (scanner != null) {
            return executeInTransactionWithScanner(message, callback, transactionId, scanner);
        }

        // === 简化实现（向后兼容，TransactionScanner 未注入时使用） ===
        // 1. 先发送业务消息到目标 Stream
        SendResult sendResult = syncSend(message);
        if (!sendResult.isSuccess()) {
            throw new TransactionException("Half message send failed: " + sendResult.getErrorMessage(),
                transactionId, transactionGroup);
        }

        // 2. 执行本地事务
        TransactionContext ctx = new TransactionContext(
            transactionId, transactionGroup, defaultGroup,
            System.currentTimeMillis(), new ConcurrentHashMap<>());
        LocalTransactionState state;
        try {
            state = callback.execute(message, ctx);
        } catch (Exception ex) {
            LOG.warn("Local transaction failed, txId={}: {}", transactionId, ex.getMessage(), ex);
            throw new TransactionException("Local transaction execute failed",
                transactionId, transactionGroup, ex);
        }

        // 3. 根据状态决定结果
        switch (state) {
            case COMMIT_MESSAGE:
                LOG.debug("Transaction committed: txId={}", transactionId);
                return sendResult;
            case ROLLBACK_MESSAGE:
                LOG.warn("Transaction rolled back: txId={}, message may need manual cleanup", transactionId);
                return new SendResult(sendResult.getMessageId(),
                    sendResult.getTopic(), sendResult.getTag(),
                    SendStatus.SEND_FAILED, sendResult.getBornTimestamp(),
                    null, "Transaction rolled back");
            case UNKNOW:
                LOG.warn("Transaction state UNKNOWN: txId={}, will be checked later", transactionId);
                return sendResult;
            default:
                throw new TransactionException("Unknown transaction state: " + state,
                    transactionId, transactionGroup);
        }
    }

    /**
     * 使用 TransactionScanner 的完整半消息事务流程。
     *
     * <p>流程：
     * <ol>
     *   <li>将消息字段写入 half Stream（半消息），对消费者不可见</li>
     *   <li>执行本地事务回调</li>
     *   <li>根据本地事务结果：COMMIT → 转投到业务 Stream；ROLLBACK → 删除半消息；
     *       UNKNOW → 保留半消息，等待 TransactionScanner 回查</li>
     * </ol>
     *
     * @param message 消息
     * @param callback 本地事务回调
     * @param transactionId 事务 ID
     * @param scanner 事务扫描器
     * @param <T> 消息体类型
     * @return 发送结果
     */
    private <T> SendResult executeInTransactionWithScanner(Message<T> message, TransactionCallback<T> callback,
                                                            String transactionId, TransactionScanner scanner) {
        // 1. 将消息转换为 Stream fields 并注册半消息
        Map<String, String> fields = messageConverter.toStreamFields(message);
        StreamMessageId halfId;
        try {
            halfId = scanner.registerHalfMessage(transactionId, transactionGroup,
                message.getTopic(), fields);
            LOG.debug("Half message registered: txId={}, txGroup={}, halfId={}",
                transactionId, transactionGroup, halfId);
        } catch (RuntimeException ex) {
            throw new TransactionException("Half message register failed: " + ex.getMessage(),
                transactionId, transactionGroup, ex);
        }

        // 2. 构造半消息发送结果
        MessageId msgId = new MessageId(halfId.toString());
        message.setMessageId(msgId);
        SendResult halfResult = new SendResult(msgId, message.getTopic(), message.getTag(),
            message.getBornTimestamp());

        // 3. 执行本地事务
        TransactionContext ctx = new TransactionContext(
            transactionId, transactionGroup, defaultGroup,
            System.currentTimeMillis(), new ConcurrentHashMap<>());
        LocalTransactionState state;
        try {
            state = callback.execute(message, ctx);
        } catch (Exception ex) {
            LOG.warn("Local transaction failed, rolling back: txId={}: {}", transactionId, ex.getMessage(), ex);
            try {
                scanner.markRollback(transactionId, transactionGroup);
            } catch (RuntimeException rollbackEx) {
                LOG.error("Failed to rollback transaction after local tx failure: txId={}: {}",
                    transactionId, rollbackEx.getMessage(), rollbackEx);
            }
            throw new TransactionException("Local transaction execute failed",
                transactionId, transactionGroup, ex);
        }

        // 4. 根据状态决定 commit / rollback / unknown
        switch (state) {
            case COMMIT_MESSAGE:
                try {
                    scanner.markCommit(transactionId, transactionGroup);
                    LOG.info("Transaction committed: txId={}, txGroup={}, targetTopic={}",
                        transactionId, transactionGroup, message.getTopic());
                } catch (RuntimeException ex) {
                    LOG.error("Failed to commit transaction: txId={}: {}", transactionId, ex.getMessage(), ex);
                    throw new TransactionException("Transaction commit failed: " + ex.getMessage(),
                        transactionId, transactionGroup, ex);
                }
                return halfResult;
            case ROLLBACK_MESSAGE:
                try {
                    scanner.markRollback(transactionId, transactionGroup);
                    LOG.info("Transaction rolled back: txId={}, txGroup={}", transactionId, transactionGroup);
                } catch (RuntimeException ex) {
                    LOG.warn("Failed to rollback transaction: txId={}: {}", transactionId, ex.getMessage(), ex);
                }
                return new SendResult(msgId, message.getTopic(), message.getTag(),
                    SendStatus.SEND_FAILED, message.getBornTimestamp(),
                    null, "Transaction rolled back");
            case UNKNOW:
                // 保留半消息，等待 TransactionScanner 周期回查
                LOG.info("Transaction state UNKNOW, waiting for check-back: txId={}, txGroup={}",
                    transactionId, transactionGroup);
                return halfResult;
            default:
                try {
                    scanner.markRollback(transactionId, transactionGroup);
                } catch (RuntimeException rollbackEx) {
                    LOG.warn("Failed to rollback on unknown state: txId={}", transactionId);
                }
                throw new TransactionException("Unknown transaction state: " + state,
                    transactionId, transactionGroup);
        }
    }

    /**
     * 设置事务扫描器（可选注入，启用完整的半消息 + 回查事务流程）。
     *
     * @param transactionScanner 事务扫描器，可为 null（回退到简化实现）
     */
    public void setTransactionScanner(TransactionScanner transactionScanner) {
        this.transactionScanner = transactionScanner;
    }

    @Override
    public MessageConverter getMessageConverter() {
        return messageConverter;
    }

    @Override
    public void setMessageConverter(MessageConverter converter) {
        throw new UnsupportedOperationException(
            "DefaultStreamMessageTemplate does not support changing converter after construction");
    }

    @Override
    public List<ProducerInterceptor> getProducerInterceptors() {
        return Collections.unmodifiableList(interceptors);
    }

    @Override
    public void setProducerInterceptors(List<ProducerInterceptor> interceptors) {
        this.interceptors.clear();
        if (interceptors != null) {
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
     * 执行 before 拦截器链。
     *
     * @param message 待发送消息
     * @return true 全部通过，false 任一拦截器拒绝
     */
    private boolean applyInterceptorsBefore(Message<?> message) {
        for (ProducerInterceptor interceptor : interceptors) {
            try {
                if (!interceptor.beforeSend(message)) {
                    LOG.debug("Interceptor {} aborted send: topic={}",
                        interceptor.name(), message.getTopic());
                    return false;
                }
            } catch (RuntimeException ex) {
                LOG.warn("Interceptor {} beforeSend threw exception: {}",
                    interceptor.name(), ex.getMessage(), ex);
                notifyProducerException(message, ex, InvokeTiming.BEFORE);
                return false;
            }
        }
        return true;
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
                LOG.warn("Interceptor {} afterSend threw exception: {}",
                    interceptor.name(), ex.getMessage(), ex);
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
        if (message.getMessageId() != null) {
            MDC.put(MdcKeys.MSG_ID, String.valueOf(message.getMessageId()));
        }
        if (message.getShardingKey() != null) {
            MDC.put(MdcKeys.SHARDING_KEY, message.getShardingKey());
        }
    }

    /**
     * 清理发送侧 MDC 结构化日志上下文。
     */
    private void clearProducerMdc() {
        MDC.remove(MdcKeys.TOPIC);
        MDC.remove(MdcKeys.PRODUCER_GROUP);
        MDC.remove(MdcKeys.MSG_ID);
        MDC.remove(MdcKeys.SHARDING_KEY);
    }

    /**
     * 解析消息对应的 Producer。
     * 当前实现：所有消息使用同一个 defaultGroup Producer。
     * 未来可扩展：按 message.properties 中的 group 字段路由。
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
    private SendResult buildFailedResult(Message<?> message, StreamMQException error) {
        return new SendResult(
            new MessageId(System.currentTimeMillis() + "-0"),
            message.getTopic(), message.getTag(),
            SendStatus.SEND_FAILED,
            message.getBornTimestamp(),
            null,
            error != null ? error.getMessage() : "unknown error");
    }
}
