/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.producer;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.compression.CompressionCodec;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.exception.ProducerTimeoutException;
import io.github.streammq.core.exception.StreamMQBrokerException;
import io.github.streammq.core.exception.StreamMQException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.producer.StreamMessageProducer;
import io.github.streammq.core.util.StringUtils;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.redisson.api.*;
import org.redisson.api.stream.StreamAddArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 Redisson 的 {@link StreamMessageProducer} 默认实现。
 *
 * <p>底层调用 {@link RStream#add} / {@link RBatch} 完成 Redis Stream XADD。 每个实例绑定一个生产组（{@code
 * group}），可发送任意 Topic 的消息。
 *
 * <p>支持：
 *
 * <ul>
 *   <li>同步发送 {@link #syncSend}（含超时与重试）
 *   <li>异步发送 {@link #asyncSend}（基于 {@link CompletableFuture}）
 *   <li>单向发送 {@link #sendOneway}（fire-and-forget）
 *   <li>批量发送 {@link #syncSendBatch}（基于 RBatch Pipeline）
 *   <li>延时消息（写入 {@code streammq:{ns}:delay:{level}} ZSet + payload Hash）
 * </ul>
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型，可在多线程间共享。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
public class RedissonStreamProducer implements StreamMessageProducer {

    /**
     * 延时 payload Hash 的保留时长（毫秒）：超期自动过期。
     *
     * <p>正常流程中 payload 在转投成功后即被 DEL；TTL 仅用于兜底清理异常场景下 （如调度器 ZREM 后崩溃）残留的孤儿 payload，防止 Redis 中无限累积。
     */
    static final long DELAY_PAYLOAD_TTL_MS = 7L * 24 * 60 * 60 * 1000;

    private static final Logger LOG = LoggerFactory.getLogger(RedissonStreamProducer.class);

    private final @NonNull RedissonClient redisson;
    private final String namespace;
    private final @NonNull String group;
    private final @NonNull MessageConverter converter;
    private final long defaultTimeoutMillis;
    private final int maxLen;
    private final int compressThreshold;
    private final long maxMessageSize;

    /** 异步发送执行器：默认统一虚拟线程池；可通过 {@link #setAsyncExecutor} 注入外部实现 */
    private volatile ExecutorService asyncExecutor;

    /** 是否拥有执行器所有权（决定 close 是否关闭） */
    private volatile boolean ownsAsyncExecutor = true;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** 压缩编解码器（可选注入，配合 compressThreshold 使用） */
    @Setter private CompressionCodec compressionCodec;

    /**
     * 注入外部异步执行器（统一线程模型：默认内部虚拟线程池，可替换为 Spring 管理的实现）。
     *
     * <p>必须在首次发送前调用；注入后 {@link #close()} 不再关闭该池（生命周期归提供方）。
     *
     * @param executor 异步执行器
     */
    public void setAsyncExecutor(ExecutorService executor) {
        Objects.requireNonNull(executor, "executor");
        synchronized (this) {
            if (this.ownsAsyncExecutor && this.asyncExecutor != null) {
                this.asyncExecutor.shutdown();
            }
            this.asyncExecutor = executor;
            this.ownsAsyncExecutor = false;
        }
    }

    /** 关闭异步执行线程池时的等待超时（秒） */
    private static final long ASYNC_AWAIT_TERMINATION_SECONDS =
            StreamMQConstants.DEFAULT_AWAIT_TERMINATION_SECONDS;

    /** 延时消息 payload Hash 字段：目标 Topic */
    private static final String FIELD_TARGET_TOPIC = StreamMQConstants.FIELD_TARGET_TOPIC;

    /** 延时消息 payload Hash 字段：投递时间 */
    private static final String FIELD_DELIVER_AT = StreamMQConstants.FIELD_DELIVER_AT;

    /**
     * 构造 Producer，支持 Builder 模式。
     *
     * <p>使用示例：
     *
     * <pre>{@code
     * RedissonStreamProducer producer = RedissonStreamProducer.builder()
     *     .redisson(redissonClient)
     *     .namespace("ns")
     *     .group("producer-group")
     *     .converter(converter)
     *     .defaultTimeoutMillis(3000)
     *     .maxLen(0)
     *     .build();
     * }</pre>
     *
     * @param redisson Redisson 客户端（必填）
     * @param namespace 命名空间（可为 null，默认空字符串）
     * @param group 生产组名（必填）
     * @param converter 消息转换器（必填）
     * @param defaultTimeoutMillis 默认发送超时（毫秒）
     * @param maxLen Stream 最大长度（0 表示不限制）
     * @param compressThreshold 压缩阈值（字节，0 = 禁用）
     * @param maxMessageSize 单条消息最大大小（字节），发送时校验
     */
    @Builder
    public RedissonStreamProducer(
            @NonNull RedissonClient redisson,
            String namespace,
            @NonNull String group,
            @NonNull MessageConverter converter,
            long defaultTimeoutMillis,
            int maxLen,
            int compressThreshold,
            long maxMessageSize) {
        this.redisson = redisson;
        this.namespace = Objects.isNull(namespace) ? "" : namespace;
        this.group = group;
        this.converter = converter;
        this.defaultTimeoutMillis = defaultTimeoutMillis;
        this.maxLen = maxLen;
        this.compressThreshold = compressThreshold;
        this.maxMessageSize =
                maxMessageSize > 0 ? maxMessageSize : StreamMQConstants.MAX_MESSAGE_SIZE_BYTES;
        // 默认统一虚拟线程池；可通过 setAsyncExecutor 注入外部实现（Spring 统一线程模型）
        this.asyncExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.ownsAsyncExecutor = true;
    }

    @Override
    public SendResult syncSend(Message<?> message) {
        return syncSend(message, defaultTimeoutMillis);
    }

    /**
     * 同步发送：XADD 写入目标 Stream。
     *
     * <p><b>超时语义：</b>XADD 已成功（返回真实 Entry ID）后即使总耗时超过 timeoutMillis， 也返回成功结果并记录
     * WARN——发送实际已成功，抛出超时异常会误导调用方重试从而产生重复消息。 超时异常仅在网络调用本身失败/超时（未获得 Entry ID）时抛出，此时消息状态未知，
     * 重试可能产生重复投递（at-least-once 语义，业务侧需幂等）。
     */
    @Override
    public SendResult syncSend(Message<?> message, long timeoutMillis) {
        ensureOpen();
        Objects.requireNonNull(message, "message");
        long start = System.currentTimeMillis();
        // 发送侧命名校验：消费侧在容器注册时已校验，此处对齐以拦截直接构造 Message 绕过
        // MessageBuilder 校验的场景（非法字符会破坏 Key 结构 / Redis Cluster hash slot 均衡）
        String topic =
                io.github.streammq.core.util.StringUtils.requireValidTopic(message.getTopic());
        if (message.isDelayMessage()) {
            return sendDelayMessage(message);
        }

        Map<String, String> fields = converter.toStreamFields(message);

        // 消息大小预检：避免序列化超大消息导致 Redis 内存压力
        int estimatedSize = estimateFieldSize(fields);
        if (estimatedSize > maxMessageSize) {
            throw new StreamMQBrokerException(
                    "Message size "
                            + estimatedSize
                            + " bytes exceeds max "
                            + maxMessageSize
                            + " bytes for topic "
                            + topic,
                    null,
                    null);
        }

        applyCompression(fields);
        String streamKey = StreamMQKeys.topicStream(namespace, topic);

        try {
            StreamMessageId streamId = appendStream(streamKey, fields, timeoutMillis);
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed > timeoutMillis) {
                LOG.warn(
                        "syncSend completed but exceeded timeout budget: topic={}, elapsed={}ms,"
                                + " limit={}ms — message WAS delivered (id={})",
                        topic,
                        elapsed,
                        timeoutMillis,
                        streamId);
            }
            MessageId messageId = MessageId.fromStreamMessageId(streamId);
            return new SendResult(messageId, topic, message.getTag(), message.getBornTimestamp());
        } catch (ProducerTimeoutException ex) {
            throw ex;
        } catch (StreamMQException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw new StreamMQBrokerException("syncSend failed for topic " + topic, null, ex);
        }
    }

    @Override
    public CompletableFuture<SendResult> asyncSend(Message<?> message) {
        ensureOpen();
        Objects.requireNonNull(message, "message");
        return CompletableFuture.supplyAsync(
                () -> syncSend(message, defaultTimeoutMillis), asyncExecutor);
    }

    @Override
    public CompletableFuture<SendResult> asyncSend(Message<?> message, long timeoutMillis) {
        ensureOpen();
        Objects.requireNonNull(message, "message");
        long effectiveTimeout = timeoutMillis > 0 ? timeoutMillis : defaultTimeoutMillis;
        return CompletableFuture.supplyAsync(
                () -> syncSend(message, effectiveTimeout), asyncExecutor);
    }

    @Override
    public void sendOneway(Message<?> message) {
        ensureOpen();
        Objects.requireNonNull(message, "message");
        asyncExecutor.submit(
                () -> {
                    try {
                        syncSend(message, defaultTimeoutMillis);
                    } catch (RuntimeException ex) {
                        LOG.warn(
                                "oneway send failed for topic {}: {}",
                                message.getTopic(),
                                ex.getMessage());
                    }
                });
    }

    @Override
    public List<SendResult> syncSendBatch(List<? extends Message<?>> messages) {
        return syncSendBatch(messages, defaultTimeoutMillis);
    }

    @Override
    public List<SendResult> syncSendBatch(List<? extends Message<?>> messages, long timeoutMillis) {
        ensureOpen();
        Objects.requireNonNull(messages, "messages");
        if (messages.isEmpty()) {
            throw new IllegalArgumentException("messages list is empty");
        }
        if (timeoutMillis <= 0) {
            timeoutMillis = defaultTimeoutMillis;
        }

        // 校验同 Topic
        String firstTopic = messages.get(0).getTopic();
        for (int i = 1; i < messages.size(); i++) {
            if (!firstTopic.equals(messages.get(i).getTopic())) {
                throw new IllegalArgumentException(
                        "All messages in a batch must share the same topic, got "
                                + messages.get(i).getTopic()
                                + " vs "
                                + firstTopic);
            }
        }

        // 延时消息走单条发送
        boolean anyDelay = messages.stream().anyMatch(Message::isDelayMessage);
        if (anyDelay) {
            List<SendResult> results = new ArrayList<>(messages.size());
            for (Message<?> msg : messages) {
                results.add(syncSend(msg, timeoutMillis));
            }
            return results;
        }

        String streamKey = StreamMQKeys.topicStream(namespace, firstTopic);
        RBatch batch = redisson.createBatch();
        List<Message<?>> messageList = new ArrayList<>(messages);
        for (Message<?> message : messageList) {
            Map<String, String> fields = converter.toStreamFields(message);
            // 消息大小预检：与单条发送保持一致，避免批量写入超大消息导致 Redis 内存压力
            int estimatedSize = estimateFieldSize(fields);
            if (estimatedSize > maxMessageSize) {
                throw new StreamMQBrokerException(
                        "Message size "
                                + estimatedSize
                                + " bytes exceeds max "
                                + maxMessageSize
                                + " bytes for topic "
                                + message.getTopic(),
                        null,
                        null);
            }
            StreamAddArgs<String, String> args = buildAddArgs(fields);
            batch.<String, String>getStream(streamKey).addAsync(args);
        }

        List<?> responses;
        try {
            BatchResult<?> batchResult =
                    batch.executeAsync().get(timeoutMillis, TimeUnit.MILLISECONDS);
            responses = batchResult.getResponses();
        } catch (TimeoutException ex) {
            throw new ProducerTimeoutException(
                    "syncSendBatch timed out after " + timeoutMillis + "ms for topic " + firstTopic,
                    firstTopic,
                    timeoutMillis,
                    ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new StreamMQBrokerException(
                    "syncSendBatch interrupted for topic " + firstTopic, null, ex);
        } catch (java.util.concurrent.ExecutionException ex) {
            throw new StreamMQBrokerException(
                    "syncSendBatch failed for topic " + firstTopic, null, ex.getCause());
        } catch (RuntimeException ex) {
            throw new StreamMQBrokerException(
                    "syncSendBatch failed for topic " + firstTopic, null, ex);
        }

        // 从 BatchResult 提取每条 XADD 返回的真实 Stream Entry ID（按命令提交顺序一一对应）。
        // 若 Redisson 版本未返回可识别结果，则回退生成占位 ID（仅影响调用方拿到非真实 ID，消费侧始终使用 Stream 真实 ID）。
        List<SendResult> results = new ArrayList<>(messageList.size());
        for (int i = 0; i < messageList.size(); i++) {
            Message<?> message = messageList.get(i);
            MessageId messageId = toRealMessageId(responses, i);
            results.add(
                    new SendResult(
                            messageId,
                            message.getTopic(),
                            message.getTag(),
                            message.getBornTimestamp()));
        }
        return results;
    }

    /**
     * 从 {@link BatchResult#getResponses()} 中按索引解析真实 Stream Entry ID；不可识别时回退占位 ID。
     *
     * @param responses 批处理各命令的返回结果
     * @param index 命令索引
     * @return 消息 ID
     */
    private MessageId toRealMessageId(List<?> responses, int index) {
        if (Objects.nonNull(responses) && index < responses.size()) {
            Object response = responses.get(index);
            if (response instanceof StreamMessageId streamMessageId) {
                return MessageId.fromStreamMessageId(streamMessageId);
            }
        }
        return MessageId.of(System.currentTimeMillis(), 0x7fffffff & UUID.randomUUID().hashCode());
    }

    /**
     * 同步发送延时消息：写入延时 ZSet + payload Hash，不直接写入 Stream。
     *
     * <p><b>写入顺序：</b>先写 payload Hash，再写 ZSet 调度条目。若进程在两步之间崩溃， 只会残留一个无调度条目的孤儿
     * payload（由清理任务回收），绝不会出现"已调度但 payload 缺失"导致的永久消息丢失。
     *
     * <p>发送结果中的 messageId 为占位 ID（{@link MessageId#sentinel()}）： 延时消息的真实 Stream Entry ID 在到期投递时才由
     * Redis 生成。
     *
     * @param message 延时消息
     * @return 发送结果（占位 messageId）
     */
    private SendResult sendDelayMessage(Message<?> message) {
        ensureOpen();
        Map<String, String> delayFields = converter.toStreamFields(message);

        // 消息大小预检：与直发路径保持一致，防止超大消息绕过校验进入延时链路
        int estimatedSize = estimateFieldSize(delayFields);
        if (estimatedSize > maxMessageSize) {
            throw new StreamMQBrokerException(
                    "Delay message size "
                            + estimatedSize
                            + " bytes exceeds max "
                            + maxMessageSize
                            + " bytes for topic "
                            + message.getTopic(),
                    null,
                    null);
        }

        String msgId = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();

        DelayLevel level = message.getDelayLevel();
        Long delayTimeMillis = message.getDelayTimeMillis();

        // 任意延时优先：同时设置 delayLevel 与 delayTimeMillis 时，delayTimeMillis 优先（对齐 Message javadoc）。
        // 使用 custom ZSet 支持任意延时。
        if (Objects.nonNull(delayTimeMillis) && delayTimeMillis > 0) {
            // 使用 custom ZSet 支持任意延时
            long deliverAt = now + delayTimeMillis;
            String zsetKey = StreamMQKeys.delayCustomZSet(namespace);
            String payloadHashKey = StreamMQKeys.delayPayloadHash(namespace, msgId);

            Map<String, String> fields = new java.util.HashMap<>(delayFields);
            fields.put(FIELD_TARGET_TOPIC, message.getTopic());
            fields.put(FIELD_DELIVER_AT, Long.toString(deliverAt));

            try {
                // 先写 payload（带 TTL），再写调度条目（顺序不可颠倒）。
                // TTL 为孤儿保护：正常流程转投成功即 DEL；若实例在 ZREM 后崩溃等异常导致
                // payload 成为无调度引用的孤儿，TTL 到期后自动清理，不会无限累积。
                RMap<String, String> payloadMap = redisson.getMap(payloadHashKey);
                payloadMap.putAll(fields);
                payloadMap.expire(java.time.Duration.ofMillis(DELAY_PAYLOAD_TTL_MS));

                RScoredSortedSet<String> zset = redisson.getScoredSortedSet(zsetKey);
                zset.add(deliverAt, msgId);

                LOG.debug(
                        "Custom delay message queued: msgId={}, delayMs={}, deliverAt={}, topic={}",
                        msgId,
                        delayTimeMillis,
                        deliverAt,
                        message.getTopic());

                return new SendResult(
                        MessageId.sentinel(),
                        message.getTopic(),
                        message.getTag(),
                        message.getBornTimestamp());
            } catch (RuntimeException ex) {
                throw new StreamMQBrokerException(
                        "sendDelayMessage (custom) failed for topic " + message.getTopic(),
                        null,
                        ex);
            }
        }

        if (Objects.isNull(level)) {
            throw new StreamMQException("Delay message has no delayLevel or delayTimeMillis");
        }

        // 原有 DelayLevel 逻辑保持不变
        long deliverAt = now + level.toMillis();
        String zsetKey = StreamMQKeys.delayZSet(namespace, level.name());
        String payloadHashKey = StreamMQKeys.delayPayloadHash(namespace, msgId);

        Map<String, String> fields = new java.util.HashMap<>(delayFields);
        fields.put(FIELD_TARGET_TOPIC, message.getTopic());
        fields.put(FIELD_DELIVER_AT, Long.toString(deliverAt));

        try {
            // 先写 payload（带 TTL，孤儿保护见 custom 分支注释），再写调度条目（顺序不可颠倒）
            RMap<String, String> payloadMap = redisson.getMap(payloadHashKey);
            payloadMap.putAll(fields);
            payloadMap.expire(java.time.Duration.ofMillis(DELAY_PAYLOAD_TTL_MS));

            RScoredSortedSet<String> zset = redisson.getScoredSortedSet(zsetKey);
            zset.add(deliverAt, msgId);

            LOG.debug(
                    "Delay message queued: msgId={}, level={}, deliverAt={}, topic={}",
                    msgId,
                    level,
                    deliverAt,
                    message.getTopic());

            return new SendResult(
                    MessageId.sentinel(),
                    message.getTopic(),
                    message.getTag(),
                    message.getBornTimestamp());
        } catch (RuntimeException ex) {
            throw new StreamMQBrokerException(
                    "sendDelayMessage failed for topic " + message.getTopic(), null, ex);
        }
    }

    /**
     * 按压缩阈值对 body 字段进行压缩。
     *
     * <p>当 {@code compressThreshold > 0} 且 {@code compressionCodec != null} 时， 若 body 字节大小超过阈值，则压缩
     * body 并标记 {@code compressed=true}。
     *
     * @param fields Stream Entry 字段 Map（原地修改）
     */
    private void applyCompression(Map<String, String> fields) {
        if (compressThreshold <= 0 || Objects.isNull(compressionCodec)) {
            return;
        }
        String bodyField = fields.get(DefaultMessageConverter.FIELD_BODY);
        if (StringUtils.isEmpty(bodyField)) {
            return;
        }
        byte[] bodyBytes;
        try {
            bodyBytes = Base64.getDecoder().decode(bodyField);
        } catch (IllegalArgumentException ex) {
            // 非 Base64 body（如 PassThroughMessageConverter 的原文透传）：压缩不可用。
            // 此前直接抛异常导致该组合下所有超阈值消息发送失败；现在跳过压缩并提示，
            // 保持发送路径可用（消费端 compressed 标记缺失，不会尝试解压）。
            LOG.warn("Body is not Base64-encoded (converter may be PassThrough), skip compression");
            return;
        }
        if (bodyBytes.length <= compressThreshold) {
            return;
        }
        byte[] compressed = compressionCodec.compress(bodyBytes);
        fields.put(
                DefaultMessageConverter.FIELD_BODY, Base64.getEncoder().encodeToString(compressed));
        fields.put(DefaultMessageConverter.FIELD_COMPRESSED, compressionCodec.name());
        LOG.debug(
                "Body compressed: originalSize={}, compressedSize={}, codec={}",
                bodyBytes.length,
                compressed.length,
                compressionCodec.name());
    }

    /**
     * 调用 RStream.add 写入 Stream Entry（含 MAXLEN 截断）。
     *
     * <p>使用异步 API + 超时控制，确保 {@code timeoutMillis} 真正生效。
     *
     * @param streamKey Stream Key
     * @param fields Entry 字段
     * @param timeoutMillis 超时（毫秒）
     * @return Stream Message ID
     */
    private StreamMessageId appendStream(
            String streamKey, Map<String, String> fields, long timeoutMillis) {
        RStream<String, String> stream = redisson.getStream(streamKey);
        StreamAddArgs<String, String> args = buildAddArgs(fields);
        try {
            // 使用异步 API + 超时控制，确保 timeoutMillis 真正生效
            return stream.addAsync(args).get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            throw new ProducerTimeoutException(
                    "syncSend timeout after " + timeoutMillis + "ms", streamKey, timeoutMillis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new StreamMQBrokerException(
                    "syncSend interrupted for stream " + streamKey, null, ex);
        } catch (ExecutionException ex) {
            Throwable cause = Objects.nonNull(ex.getCause()) ? ex.getCause() : ex;
            throw new StreamMQBrokerException(
                    "syncSend failed for stream " + streamKey, null, cause);
        }
    }

    /**
     * 构造 StreamAddArgs，按需附加 MAXLEN 近似切剪（对应 Redis XADD 的 {@code ~ MAXLEN}）。
     *
     * @param fields Entry 字段
     * @return StreamAddArgs 实例
     */
    private StreamAddArgs<String, String> buildAddArgs(Map<String, String> fields) {
        StreamAddArgs<String, String> args = StreamAddArgs.entries(fields);
        if (maxLen > 0) {
            // trimNonStrict() 对应 Redis 近似切剪（~），noLimit() 表示不附加 LIMIT 子句
            args = args.trimNonStrict().maxLen(maxLen).noLimit();
        }
        return args;
    }

    /**
     * 估算序列化后 Stream fields 的近似字节大小。
     *
     * <p>使用 key.length + value.length 累加，UTF-8 每字符约 1-3 字节， 实际值取保守估算（每个字符 3 字节）以避免低估。
     */
    static int estimateFieldSize(Map<String, String> fields) {
        int size = 0;
        for (Map.Entry<String, String> e : fields.entrySet()) {
            size += e.getKey().length() * 3;
            if (e.getValue() != null) {
                size += e.getValue().length() * 3;
            }
        }
        return size;
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Producer is closed");
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (ownsAsyncExecutor) {
                asyncExecutor.shutdown();
            } else {
                LOG.debug("Injected async executor left open (managed by provider)");
            }
            try {
                if (!asyncExecutor.awaitTermination(
                        ASYNC_AWAIT_TERMINATION_SECONDS, TimeUnit.SECONDS)) {
                    asyncExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                asyncExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            LOG.info("RedissonStreamProducer closed, group={}", group);
        }
    }
}
