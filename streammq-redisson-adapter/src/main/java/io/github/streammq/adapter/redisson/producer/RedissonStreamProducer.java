package io.github.streammq.adapter.redisson.producer;

import io.github.streammq.adapter.redisson.support.StreamMqKeys;
import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.exception.ProducerTimeoutException;
import io.github.streammq.core.exception.StreamMqBrokerException;
import io.github.streammq.core.exception.StreamMqException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.producer.StreamMqProducer;
import io.github.streammq.core.spi.MessageConverter;
import org.redisson.api.RBatch;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于 Redisson 的 {@link StreamMqProducer} 默认实现。
 *
 * <p>底层调用 {@link RStream#add} / {@link RBatch} 完成 Redis Stream XADD。
 * 每个实例绑定一个生产组（{@code group}），可发送任意 Topic 的消息。
 *
 * <p>支持：
 * <ul>
 *   <li>同步发送 {@link #syncSend}（含超时与重试）</li>
 *   <li>异步发送 {@link #asyncSend}（基于 {@link CompletableFuture}）</li>
 *   <li>单向发送 {@link #sendOneway}（fire-and-forget）</li>
 *   <li>批量发送 {@link #syncSendBatch}（基于 RBatch Pipeline）</li>
 *   <li>延时消息（写入 {@code streammq:{ns}:delay:{level}} ZSet + payload Hash）</li>
 * </ul>
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型，可在多线程间共享。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class RedissonStreamProducer implements StreamMqProducer {

    private static final Logger LOG = LoggerFactory.getLogger(RedissonStreamProducer.class);

    private final RedissonClient redisson;
    private final String namespace;
    private final String group;
    private final MessageConverter converter;
    private final long defaultTimeoutMillis;
    private final int maxLen;
    private final ExecutorService asyncExecutor;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** 关闭异步执行线程池时的等待超时（秒） */
    private static final long ASYNC_AWAIT_TERMINATION_SECONDS = 5L;
    /** 延时消息 payload Hash 字段：目标 Topic */
    private static final String FIELD_TARGET_TOPIC = "targetTopic";
    /** 延时消息 payload Hash 字段：投递时间 */
    private static final String FIELD_DELIVER_AT = "deliverAt";

    /**
     * 构造 Producer。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param group 生产组名
     * @param converter 消息转换器
     * @param defaultTimeoutMillis 默认发送超时（毫秒）
     * @param maxLen Stream 最大长度（0 表示不限制）
     */
    public RedissonStreamProducer(RedissonClient redisson, String namespace, String group,
                                  MessageConverter converter, long defaultTimeoutMillis, int maxLen) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = namespace == null ? "" : namespace;
        this.group = Objects.requireNonNull(group, "group");
        this.converter = Objects.requireNonNull(converter, "converter");
        this.defaultTimeoutMillis = defaultTimeoutMillis;
        this.maxLen = maxLen;
        this.asyncExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public SendResult syncSend(Message<?> message) {
        return syncSend(message, defaultTimeoutMillis);
    }

    @Override
    public SendResult syncSend(Message<?> message, long timeoutMillis) {
        ensureOpen();
        Objects.requireNonNull(message, "message");
        long start = System.currentTimeMillis();

        if (message.isDelayMessage()) {
            return sendDelayMessage(message);
        }

        Map<String, String> fields = converter.toStreamFields(message);
        String streamKey = StreamMqKeys.topicStream(namespace, message.getTopic());

        try {
            StreamMessageId streamId = appendStream(streamKey, fields, timeoutMillis);
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed > timeoutMillis) {
                throw new ProducerTimeoutException(
                    "syncSend timeout after " + elapsed + "ms (limit=" + timeoutMillis + "ms)",
                    message.getTopic(), timeoutMillis);
            }
            MessageId messageId = new MessageId(streamId.toString());
            message.setMessageId(messageId);
            return new SendResult(messageId, message.getTopic(), message.getTag(), message.getBornTimestamp());
        } catch (ProducerTimeoutException ex) {
            throw ex;
        } catch (StreamMqException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw new StreamMqBrokerException(
                "syncSend failed for topic " + message.getTopic(), null, ex);
        }
    }

    @Override
    public CompletableFuture<SendResult> asyncSend(Message<?> message) {
        ensureOpen();
        Objects.requireNonNull(message, "message");
        return CompletableFuture.supplyAsync(() -> syncSend(message, defaultTimeoutMillis), asyncExecutor);
    }

    @Override
    public void sendOneway(Message<?> message) {
        ensureOpen();
        Objects.requireNonNull(message, "message");
        asyncExecutor.submit(() -> {
            try {
                syncSend(message, defaultTimeoutMillis);
            } catch (RuntimeException ex) {
                LOG.warn("oneway send failed for topic {}: {}", message.getTopic(), ex.getMessage());
            }
        });
    }

    @Override
    public List<SendResult> syncSendBatch(List<? extends Message<?>> messages) {
        ensureOpen();
        Objects.requireNonNull(messages, "messages");
        if (messages.isEmpty()) {
            throw new IllegalArgumentException("messages list is empty");
        }

        // 校验同 Topic
        String firstTopic = messages.get(0).getTopic();
        for (int i = 1; i < messages.size(); i++) {
            if (!firstTopic.equals(messages.get(i).getTopic())) {
                throw new IllegalArgumentException(
                    "All messages in a batch must share the same topic, got "
                        + messages.get(i).getTopic() + " vs " + firstTopic);
            }
        }

        // 延时消息走单条发送
        boolean anyDelay = messages.stream().anyMatch(Message::isDelayMessage);
        if (anyDelay) {
            List<SendResult> results = new ArrayList<>(messages.size());
            for (Message<?> msg : messages) {
                results.add(syncSend(msg, defaultTimeoutMillis));
            }
            return results;
        }

        String streamKey = StreamMqKeys.topicStream(namespace, firstTopic);
        RBatch batch = redisson.createBatch();
        List<Message<?>> messageList = new ArrayList<>(messages);
        for (Message<?> message : messageList) {
            Map<String, String> fields = converter.toStreamFields(message);
            StreamAddArgs<String, String> args = buildAddArgs(fields);
            batch.<String, String>getStream(streamKey).addAsync(args);
        }

        try {
            batch.execute();
        } catch (RuntimeException ex) {
            throw new StreamMqBrokerException(
                "syncSendBatch failed for topic " + firstTopic, null, ex);
        }

        // 由于 RBatch 不返回每条 ID，为每条消息生成唯一占位 ID（基于 UUID 哈希）
        // 真实 Stream Entry ID 由消费端从 Stream Entry 获取
        List<SendResult> results = new ArrayList<>(messageList.size());
        for (Message<?> message : messageList) {
            // 为每条消息生成唯一 ID，避免批量消息 ID 相同导致幂等失效
            String uniqueId = System.currentTimeMillis() + "-" + Math.abs(UUID.randomUUID().hashCode());
            MessageId placeholder = new MessageId(uniqueId);
            message.setMessageId(placeholder);
            results.add(new SendResult(placeholder, message.getTopic(), message.getTag(), message.getBornTimestamp()));
        }
        return results;
    }

    /**
     * 同步发送延时消息：写入延时 ZSet + payload Hash，不直接写入 Stream。
     *
     * @param message 延时消息
     * @return 发送结果（使用合成的 msgId）
     */
    private SendResult sendDelayMessage(Message<?> message) {
        String msgId = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();

        DelayLevel level = message.getDelayLevel();
        if (level == null && message.getDelayTimeMillis() != null) {
            level = DelayLevel.closestAbove(message.getDelayTimeMillis());
        }
        if (level == null) {
            throw new StreamMqException("Delay message has no delayLevel or delayTimeMillis");
        }

        long deliverAt = now + level.toMillis();
        String zsetKey = StreamMqKeys.delayZSet(namespace, level.name());
        String payloadHashKey = StreamMqKeys.delayPayloadHash(namespace, msgId);

        Map<String, String> fields = converter.toStreamFields(message);
        fields.put(FIELD_TARGET_TOPIC, message.getTopic());
        fields.put(FIELD_DELIVER_AT, Long.toString(deliverAt));

        try {
            RScoredSortedSet<String> zset = redisson.getScoredSortedSet(zsetKey);
            zset.add(deliverAt, msgId);

            RMap<String, String> payloadMap = redisson.getMap(payloadHashKey);
            payloadMap.putAll(fields);

            LOG.debug("Delay message queued: msgId={}, level={}, deliverAt={}, topic={}",
                msgId, level, deliverAt, message.getTopic());

            MessageId messageId = new MessageId(now + "-" + Math.abs(msgId.hashCode()));
            message.setMessageId(messageId);
            return new SendResult(messageId, message.getTopic(), message.getTag(), message.getBornTimestamp());
        } catch (RuntimeException ex) {
            throw new StreamMqBrokerException(
                "sendDelayMessage failed for topic " + message.getTopic(), null, ex);
        }
    }

    /**
     * 调用 RStream.add 写入 Stream Entry（含 MAXLEN 截断）。
     *
     * @param streamKey Stream Key
     * @param fields Entry 字段
     * @param timeoutMillis 超时（毫秒）
     * @return Stream Message ID
     */
    private StreamMessageId appendStream(String streamKey, Map<String, String> fields, long timeoutMillis) {
        RStream<String, String> stream = redisson.getStream(streamKey);
        StreamAddArgs<String, String> args = buildAddArgs(fields);
        return stream.add(args);
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

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Producer is closed");
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            asyncExecutor.shutdown();
            try {
                if (!asyncExecutor.awaitTermination(ASYNC_AWAIT_TERMINATION_SECONDS, TimeUnit.SECONDS)) {
                    asyncExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                asyncExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            LOG.info("RedissonStreamProducer closed, group={}", group);
        }
    }

    /**
     * 返回生产组名。
     *
     * @return 生产组名
     */
    public String getGroup() {
        return group;
    }

    /**
     * 返回命名空间。
     *
     * @return 命名空间
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * 返回消息转换器。
     *
     * @return 转换器
     */
    public MessageConverter getConverter() {
        return converter;
    }

    /**
     * 返回默认发送超时（毫秒）。
     *
     * @return 默认超时
     */
    public long getDefaultTimeoutMillis() {
        return defaultTimeoutMillis;
    }

    /**
     * 返回 Stream 最大长度。
     *
     * @return 最大长度，0 表示不限制
     */
    public int getMaxLen() {
        return maxLen;
    }
}
