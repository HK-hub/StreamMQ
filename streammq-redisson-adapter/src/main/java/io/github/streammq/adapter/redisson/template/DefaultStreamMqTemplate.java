package io.github.streammq.adapter.redisson.template;

import io.github.streammq.core.exception.StreamMqException;
import io.github.streammq.core.exception.TransactionException;
import io.github.streammq.core.message.BatchMessage;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.message.SendStatus;
import io.github.streammq.core.producer.SendCallback;
import io.github.streammq.core.producer.StreamMqProducer;
import io.github.streammq.core.producer.StreamMqProducerFactory;
import io.github.streammq.core.spi.MessageConverter;
import io.github.streammq.core.spi.ProducerInterceptor;
import io.github.streammq.core.template.StreamMqTemplate;
import io.github.streammq.core.transaction.TransactionCallback;
import io.github.streammq.core.transaction.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * {@link StreamMqTemplate} 的默认实现。
 *
 * <p>组合 {@link StreamMqProducerFactory} 与拦截器链，对上层提供业务友好的发送 API。
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
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultStreamMqTemplate<T> extends StreamMqTemplate<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamMqTemplate.class);

    private final StreamMqProducerFactory producerFactory;
    private final String defaultGroup;
    private final MessageConverter messageConverter;
    private final List<ProducerInterceptor> interceptors = new CopyOnWriteArrayList<>();
    private final Map<String, Object> defaultProperties;
    private final String transactionGroup;

    /**
     * 构造 Template。
     *
     * @param producerFactory 生产者工厂
     * @param defaultGroup 默认生产组名
     * @param messageConverter 消息转换器
     */
    public DefaultStreamMqTemplate(StreamMqProducerFactory producerFactory,
                                   String defaultGroup,
                                   MessageConverter messageConverter) {
        this(producerFactory, defaultGroup, messageConverter,
            Collections.emptyMap(), null);
    }

    /**
     * 全参构造。
     *
     * @param producerFactory 生产者工厂
     * @param defaultGroup 默认生产组名
     * @param messageConverter 消息转换器
     * @param defaultProperties 默认属性（用于创建 Producer）
     * @param transactionGroup 事务组名（用于事务消息），可为 null
     */
    public DefaultStreamMqTemplate(StreamMqProducerFactory producerFactory,
                                   String defaultGroup,
                                   MessageConverter messageConverter,
                                   Map<String, Object> defaultProperties,
                                   String transactionGroup) {
        this.producerFactory = Objects.requireNonNull(producerFactory, "producerFactory");
        this.defaultGroup = Objects.requireNonNull(defaultGroup, "defaultGroup");
        this.messageConverter = Objects.requireNonNull(messageConverter, "messageConverter");
        this.defaultProperties = defaultProperties == null ? Collections.emptyMap() : defaultProperties;
        this.transactionGroup = transactionGroup;
    }

    @Override
    public SendResult syncSend(Message<T> message) {
        return syncSend(message, DEFAULT_SEND_TIMEOUT_MILLIS, DEFAULT_SYNC_RETRY_TIMES);
    }

    @Override
    public SendResult syncSend(Message<T> message, long timeoutMillis) {
        return syncSend(message, timeoutMillis, DEFAULT_SYNC_RETRY_TIMES);
    }

    @Override
    public SendResult syncSend(Message<T> message, long timeoutMillis, int retryTimes) {
        Objects.requireNonNull(message, "message");
        if (timeoutMillis <= 0) {
            timeoutMillis = DEFAULT_SEND_TIMEOUT_MILLIS;
        }
        if (retryTimes < 0) {
            retryTimes = 0;
        }

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
        StreamMqProducer producer = resolveProducer(message);
        StreamMqException lastError = null;
        for (int attempt = 0; attempt <= retryTimes; attempt++) {
            try {
                SendResult result = producer.syncSend(message, timeoutMillis);
                applyInterceptorsAfter(message, result);
                return result;
            } catch (StreamMqException ex) {
                lastError = ex;
                LOG.warn("syncSend attempt {}/{} failed for topic {}: {}",
                    attempt + 1, retryTimes + 1, message.getTopic(), ex.getMessage());
            }
        }
        applyInterceptorsAfter(message, buildFailedResult(message, lastError));
        throw lastError != null ? lastError
            : new StreamMqException("syncSend failed for unknown reason: " + message.getTopic());
    }

    @Override
    public CompletableFuture<SendResult> asyncSend(Message<T> message) {
        Objects.requireNonNull(message, "message");
        StreamMqProducer producer = resolveProducer(message);
        return producer.asyncSend(message).whenComplete((result, ex) -> {
            if (ex == null) {
                applyInterceptorsBefore(message);
                applyInterceptorsAfter(message, result);
            }
        });
    }

    @Override
    public void asyncSend(Message<T> message, SendCallback callback) {
        asyncSend(message, callback, DEFAULT_SEND_TIMEOUT_MILLIS);
    }

    @Override
    public void asyncSend(Message<T> message, SendCallback callback, long timeoutMillis) {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(callback, "callback");
        if (!applyInterceptorsBefore(message)) {
            callback.onException(new StreamMqException("Aborted by interceptor"));
            return;
        }
        StreamMqProducer producer = resolveProducer(message);
        producer.asyncSend(message).whenComplete((result, ex) -> {
            if (ex == null) {
                applyInterceptorsAfter(message, result);
                callback.onSuccess(result);
            } else {
                callback.onException(ex instanceof RuntimeException re ? re : new StreamMqException("async send failed", ex));
            }
        });
    }

    @Override
    public void sendOneway(Message<T> message) {
        Objects.requireNonNull(message, "message");
        applyInterceptorsBefore(message);
        StreamMqProducer producer = resolveProducer(message);
        producer.sendOneway(message);
    }

    @Override
    public List<SendResult> syncSendBatch(BatchMessage<T> batch) {
        Objects.requireNonNull(batch, "batch");
        if (batch.isEmpty()) {
            throw new IllegalArgumentException("batch is empty");
        }

        for (Message<T> message : batch.getMessages()) {
            if (!applyInterceptorsBefore(message)) {
                throw new StreamMqException(
                    "Batch send aborted by interceptor for topic: " + message.getTopic());
            }
        }

        StreamMqProducer producer = resolveProducer(batch.getTopic());
        List<SendResult> results = producer.syncSendBatch(batch.getMessages());

        List<SendResult> finalResults = new ArrayList<>(results.size());
        for (int i = 0; i < results.size(); i++) {
            SendResult result = results.get(i);
            applyInterceptorsAfter(batch.getMessages().get(i), result);
            finalResults.add(result);
        }
        return finalResults;
    }

    @Override
    public SendResult executeInTransaction(Message<T> message, TransactionCallback<T> callback) {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(callback, "callback");
        if (transactionGroup == null) {
            throw new TransactionException("Transaction group not configured",
                null, null);
        }

        // 事务消息基础实现：先发送业务消息（视为半消息已 commit），再执行本地事务
        // 完整的半消息 + 回查调度由 TransactionScanner (p6) 实现
        // 当前简化版：发送消息 -> 执行本地事务 -> 根据 LocalTransactionState 决定 ACK 或回滚
        String transactionId = UUID.randomUUID().toString();
        message.setTransactionId(transactionId);

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
        io.github.streammq.core.enums.LocalTransactionState state;
        try {
            state = callback.execute(message, ctx);
        } catch (Exception ex) {
            // 本地事务异常：视为 ROLLBACK
            LOG.warn("Local transaction failed, txId={}: {}", transactionId, ex.getMessage());
            // 注意：此处仅记录，不删除已发送的消息（简化实现）
            // 完整实现需通过 TransactionScanner 进行回查
            throw new TransactionException("Local transaction execute failed",
                transactionId, transactionGroup, ex);
        }

        // 3. 根据状态决定结果
        switch (state) {
            case COMMIT_MESSAGE:
                // 消息已发送，无需额外操作
                LOG.debug("Transaction committed: txId={}", transactionId);
                return sendResult;
            case ROLLBACK_MESSAGE:
                // 简化实现：记录日志，需人工或后续扫描清理（完整实现由 TransactionScanner 处理）
                LOG.warn("Transaction rolled back: txId={}, message may need manual cleanup",
                    transactionId);
                return new SendResult(sendResult.getMessageId(),
                    sendResult.getTopic(), sendResult.getTag(),
                    SendStatus.SEND_FAILED, sendResult.getBornTimestamp(),
                    null, "Transaction rolled back");
            case UNKNOW:
                LOG.warn("Transaction state UNKNOWN: txId={}, will be checked later",
                    transactionId);
                return sendResult;
            default:
                throw new TransactionException("Unknown transaction state: " + state,
                    transactionId, transactionGroup);
        }
    }

    @Override
    public MessageConverter getMessageConverter() {
        return messageConverter;
    }

    @Override
    public void setMessageConverter(MessageConverter converter) {
        throw new UnsupportedOperationException(
            "DefaultStreamMqTemplate does not support changing converter after construction");
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

    /**
     * 执行 before 拦截器链。
     *
     * @param message 待发送消息
     * @return true 全部通过，false 任一拦截器拒绝
     */
    private boolean applyInterceptorsBefore(Message<T> message) {
        for (ProducerInterceptor interceptor : interceptors) {
            try {
                if (!interceptor.beforeSend(message)) {
                    LOG.debug("Interceptor {} aborted send: topic={}",
                        interceptor.name(), message.getTopic());
                    return false;
                }
            } catch (RuntimeException ex) {
                LOG.warn("Interceptor {} beforeSend threw exception: {}",
                    interceptor.name(), ex.getMessage());
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
    private void applyInterceptorsAfter(Message<T> message, SendResult result) {
        for (ProducerInterceptor interceptor : interceptors) {
            try {
                interceptor.afterSend(message, result);
            } catch (RuntimeException ex) {
                LOG.warn("Interceptor {} afterSend threw exception: {}",
                    interceptor.name(), ex.getMessage());
            }
        }
    }

    /**
     * 解析消息对应的 Producer。
     * 当前实现：所有消息使用同一个 defaultGroup Producer。
     * 未来可扩展：按 message.properties 中的 group 字段路由。
     *
     * @param topic 当前消息（用于未来扩展路由）
     * @return Producer 实例
     */
    private StreamMqProducer resolveProducer(String topic) {
        Map<String, Object> props = new java.util.HashMap<>(defaultProperties);
        props.putIfAbsent("group", defaultGroup);
        return producerFactory.createProducer(props);
    }

    /**
     * 解析消息对应的 Producer。
     *
     * @param message 消息
     * @return Producer 实例
     */
    private StreamMqProducer resolveProducer(Message<T> message) {
        return resolveProducer(message.getTopic());
    }

    /**
     * 构造失败结果。
     *
     * @param message 消息
     * @param error 异常
     * @return 失败 SendResult
     */
    private SendResult buildFailedResult(Message<T> message, StreamMqException error) {
        return new SendResult(
            new MessageId(System.currentTimeMillis() + "-0"),
            message.getTopic(), message.getTag(),
            SendStatus.SEND_FAILED,
            message.getBornTimestamp(),
            null,
            error != null ? error.getMessage() : "unknown error");
    }
}
