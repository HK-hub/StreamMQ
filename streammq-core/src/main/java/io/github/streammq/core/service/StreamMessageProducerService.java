package io.github.streammq.core.service;

import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.message.*;
import io.github.streammq.core.producer.SendCallback;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.core.transaction.TransactionCallback;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * MQ 消息发送服务，封装 {@link StreamMessageTemplate} 提供更简洁的 API。
 *
 * <p>用户无需手动构造 {@link Message} 对象，只需传入 body 和 topic 即可发送。
 * 类似 RocketMQ 的 DefaultMQPushProducer 封装层。
 *
 * <p><b>泛型设计</b>：所有方法均使用方法级泛型 {@code <T>}，支持同一 Service 实例
 * 发送不同 body 类型的消息，无需泛型强转。
 *
 * <p>使用示例：
 * <pre>{@code
 * @Autowired
 * private StreamMessageService mqService;
 *
 * // 同步发送
 * mqService.send("orders", order);
 * mqService.send("orders", order, "tagA");
 * mqService.send("orders", order, "tagA", "order-key-123");
 * mqService.send("orders", order, "tagA", "order-key-123", "shard-1");
 * mqService.send("orders", order, 5000L); // 超时 5s
 * mqService.send("orders", order, 5000L, 3); // 超时 5s + 重试 3 次
 *
 * // 使用 MessageMetadataBuilder 封装所有附加参数
 * MessageMetadataBuilder metadata = MessageMetadataBuilder.create()
 *     .tag("tagA")
 *     .keys("order-123")
 *     .shardingKey("shard-1")
 *     .userProperty("traceId", "t-001");
 * mqService.send("orders", order, metadata);
 * mqService.send("orders", order, metadata, 5000L, 3);
 *
 * // 异步发送
 * CompletableFuture<SendResult> future = mqService.asyncSend("orders", order);
 * mqService.asyncSend("orders", order, callback);
 *
 * // 单向发送
 * mqService.sendOneway("orders", order);
 *
 * // 批量发送
 * mqService.sendBatch("orders", List.of(order1, order2, order3));
 *
 * // 延时发送
 * mqService.sendDelay("orders", order, DelayLevel.LEVEL_5);
 * mqService.sendDelay("orders", order, 30000L); // 延时 30s
 *
 * // 事务消息
 * mqService.sendTransaction("orders", order, (msg, ctx) -> {
 *     // 执行本地事务
 *     return LocalTransactionState.COMMIT_MESSAGE;
 * });
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@RequiredArgsConstructor
public class StreamMessageProducerService {

    private final StreamMessageTemplate template;

    // ===================== 同步发送（Message 对象） =====================

    /**
     * 同步发送预构造的 {@link Message}（默认超时、默认重试）。
     *
     * @param message 消息
     * @param <T> body 类型
     * @return 发送结果
     */
    public <T> SendResult send(Message<T> message) {
        return template.syncSend(message);
    }

    /**
     * 同步发送预构造的 {@link Message}（指定超时）。
     *
     * @param message 消息
     * @param timeoutMillis 超时毫秒数
     * @param <T> body 类型
     * @return 发送结果
     */
    public <T> SendResult send(Message<T> message, long timeoutMillis) {
        return template.syncSend(message, timeoutMillis);
    }

    /**
     * 同步发送预构造的 {@link Message}（指定超时与重试次数）。
     *
     * @param message 消息
     * @param timeoutMillis 超时毫秒数
     * @param retryTimes 重试次数（0 表示不重试）
     * @param <T> body 类型
     * @return 发送结果
     */
    public <T> SendResult send(Message<T> message, long timeoutMillis, int retryTimes) {
        return template.syncSend(message, timeoutMillis, retryTimes);
    }

    // ===================== 同步发送（topic + body） =====================

    /**
     * 同步发送消息。
     *
     * @param topic 主题
     * @param body 消息体
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult send(String topic, T body) {
        return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body).build());
    }

    /**
     * 同步发送消息（带 Tag）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult send(String topic, T body, String tag) {
        return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build());
    }

    /**
     * 同步发送消息（带 Tag 和 Keys）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param keys 消息 Keys
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult send(String topic, T body, String tag, String keys) {
        return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body)
            .tag(tag).keys(keys).build());
    }

    /**
     * 同步发送消息（带 Tag、Keys 和 ShardingKey）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param keys 消息 Keys
     * @param shardingKey 分片 Key
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult send(String topic, T body, String tag, String keys, String shardingKey) {
        return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body)
            .tag(tag).keys(keys).shardingKey(shardingKey).build());
    }

    // ===================== 同步发送（带超时） =====================

    /**
     * 同步发送消息（带超时）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param timeoutMillis 超时毫秒数
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult send(String topic, T body, long timeoutMillis) {
        return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body).build(), timeoutMillis);
    }

    /**
     * 同步发送消息（带 Tag 和超时）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param timeoutMillis 超时毫秒数
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult send(String topic, T body, String tag, long timeoutMillis) {
        return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build(), timeoutMillis);
    }

    /**
     * 同步发送消息（带 Tag、Keys 和超时）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param keys 消息 Keys
     * @param timeoutMillis 超时毫秒数
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult send(String topic, T body, String tag, String keys, long timeoutMillis) {
        return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body)
            .tag(tag).keys(keys).build(), timeoutMillis);
    }

    /**
     * 同步发送消息（带 Tag、Keys、ShardingKey 和超时）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param keys 消息 Keys
     * @param shardingKey 分片 Key
     * @param timeoutMillis 超时毫秒数
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult send(String topic, T body, String tag, String keys,
                                String shardingKey, long timeoutMillis) {
        return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body)
            .tag(tag).keys(keys).shardingKey(shardingKey).build(), timeoutMillis);
    }

    // ===================== 同步发送（带超时和重试） =====================

    /**
     * 同步发送消息（带超时和重试次数）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param timeoutMillis 超时毫秒数
     * @param retryTimes 重试次数（0 表示不重试）
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult send(String topic, T body, long timeoutMillis, int retryTimes) {
        return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body).build(),
            timeoutMillis, retryTimes);
    }

    /**
     * 同步发送消息（带 Tag、超时和重试次数）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param timeoutMillis 超时毫秒数
     * @param retryTimes 重试次数（0 表示不重试）
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult send(String topic, T body, String tag, long timeoutMillis, int retryTimes) {
        return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build(),
            timeoutMillis, retryTimes);
    }

    /**
     * 同步发送消息（带 Tag、Keys、超时和重试次数）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param keys 消息 Keys
     * @param timeoutMillis 超时毫秒数
     * @param retryTimes 重试次数（0 表示不重试）
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult send(String topic, T body, String tag, String keys,
                                long timeoutMillis, int retryTimes) {
        return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body)
            .tag(tag).keys(keys).build(), timeoutMillis, retryTimes);
    }

    /**
     * 同步发送消息（带 Tag、Keys、ShardingKey、超时和重试次数）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param keys 消息 Keys
     * @param shardingKey 分片 Key
     * @param timeoutMillis 超时毫秒数
     * @param retryTimes 重试次数（0 表示不重试）
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult send(String topic, T body, String tag, String keys,
                                String shardingKey, long timeoutMillis, int retryTimes) {
        return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body)
            .tag(tag).keys(keys).shardingKey(shardingKey).build(), timeoutMillis, retryTimes);
    }

    // ===================== 同步发送（MessageMetadataBuilder 模式） =====================

    /**
     * 同步发送消息（使用 {@link MessageMetadataBuilder} 封装附加参数）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param metadata 消息元数据（Tag、Keys、ShardingKey、延时、属性等）
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult send(String topic, T body, MessageMetadataBuilder metadata) {
        MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
        if (metadata != null) {
            metadata.applyTo(builder);
        }
        return template.syncSend(builder.build());
    }

    /**
     * 同步发送消息（使用 {@link MessageMetadataBuilder} + 指定超时）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param metadata 消息元数据
     * @param timeoutMillis 超时毫秒数
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult send(String topic, T body, MessageMetadataBuilder metadata, long timeoutMillis) {
        MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
        if (metadata != null) {
            metadata.applyTo(builder);
        }
        return template.syncSend(builder.build(), timeoutMillis);
    }

    /**
     * 同步发送消息（使用 {@link MessageMetadataBuilder} + 指定超时和重试次数）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param metadata 消息元数据
     * @param timeoutMillis 超时毫秒数
     * @param retryTimes 重试次数（0 表示不重试）
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult send(String topic, T body, MessageMetadataBuilder metadata,
                                long timeoutMillis, int retryTimes) {
        MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
        if (metadata != null) {
            metadata.applyTo(builder);
        }
        return template.syncSend(builder.build(), timeoutMillis, retryTimes);
    }

    // ===================== 异步发送（返回 CompletableFuture） =====================

    /**
     * 异步发送预构造的 {@link Message}。
     *
     * @param message 消息
     * @param <T> body 类型
     * @return 异步结果
     */
    public <T> CompletableFuture<SendResult> asyncSend(Message<T> message) {
        return template.asyncSend(message);
    }

    /**
     * 异步发送消息。
     *
     * @param topic 主题
     * @param body 消息体
     * @param <T> 消息体类型
     * @return 异步结果
     */
    public <T> CompletableFuture<SendResult> asyncSend(String topic, T body) {
        return template.asyncSend(MessageBuilder.<T>withTopic(topic).body(body).build());
    }

    /**
     * 异步发送消息（带 Tag）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param <T> 消息体类型
     * @return 异步结果
     */
    public <T> CompletableFuture<SendResult> asyncSend(String topic, T body, String tag) {
        return template.asyncSend(MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build());
    }

    /**
     * 异步发送消息（带 Tag 和 Keys）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param keys 消息 Keys
     * @param <T> 消息体类型
     * @return 异步结果
     */
    public <T> CompletableFuture<SendResult> asyncSend(String topic, T body, String tag, String keys) {
        return template.asyncSend(MessageBuilder.<T>withTopic(topic).body(body)
            .tag(tag).keys(keys).build());
    }

    /**
     * 异步发送消息（带 Tag、Keys 和 ShardingKey）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param keys 消息 Keys
     * @param shardingKey 分片 Key
     * @param <T> 消息体类型
     * @return 异步结果
     */
    public <T> CompletableFuture<SendResult> asyncSend(String topic, T body, String tag,
                                                        String keys, String shardingKey) {
        return template.asyncSend(MessageBuilder.<T>withTopic(topic).body(body)
            .tag(tag).keys(keys).shardingKey(shardingKey).build());
    }

    /**
     * 异步发送消息（带超时）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param timeoutMillis 超时毫秒数
     * @param <T> 消息体类型
     * @return 异步结果
     */
    public <T> CompletableFuture<SendResult> asyncSend(String topic, T body, long timeoutMillis) {
        return template.asyncSend(MessageBuilder.<T>withTopic(topic).body(body).build())
            .orTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 异步发送消息（使用 {@link MessageMetadataBuilder} 封装附加参数）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param metadata 消息元数据
     * @param <T> 消息体类型
     * @return 异步结果
     */
    public <T> CompletableFuture<SendResult> asyncSend(String topic, T body,
                                                        MessageMetadataBuilder metadata) {
        MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
        if (metadata != null) {
            metadata.applyTo(builder);
        }
        return template.asyncSend(builder.build());
    }

    /**
     * 异步发送消息（使用 {@link MessageMetadataBuilder} + 指定超时）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param metadata 消息元数据
     * @param timeoutMillis 超时毫秒数
     * @param <T> 消息体类型
     * @return 异步结果
     */
    public <T> CompletableFuture<SendResult> asyncSend(String topic, T body,
                                                        MessageMetadataBuilder metadata, long timeoutMillis) {
        MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
        if (metadata != null) {
            metadata.applyTo(builder);
        }
        return template.asyncSend(builder.build())
            .orTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    // ===================== 异步发送（回调通知） =====================

    /**
     * 异步发送消息（回调通知）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param callback 发送回调
     * @param <T> 消息体类型
     */
    public <T> void asyncSend(String topic, T body, SendCallback callback) {
        template.asyncSend(MessageBuilder.<T>withTopic(topic).body(body).build(), callback);
    }

    /**
     * 异步发送消息（带 Tag + 回调通知）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param callback 发送回调
     * @param <T> 消息体类型
     */
    public <T> void asyncSend(String topic, T body, String tag, SendCallback callback) {
        template.asyncSend(MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build(), callback);
    }

    /**
     * 异步发送消息（回调通知 + 指定超时）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param callback 发送回调
     * @param timeoutMillis 超时毫秒数
     * @param <T> 消息体类型
     */
    public <T> void asyncSend(String topic, T body, SendCallback callback, long timeoutMillis) {
        template.asyncSend(MessageBuilder.<T>withTopic(topic).body(body).build(), callback, timeoutMillis);
    }

    /**
     * 异步发送消息（使用 {@link MessageMetadataBuilder} + 回调通知）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param metadata 消息元数据
     * @param callback 发送回调
     * @param <T> 消息体类型
     */
    public <T> void asyncSend(String topic, T body, MessageMetadataBuilder metadata,
                               SendCallback callback) {
        MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
        if (metadata != null) {
            metadata.applyTo(builder);
        }
        template.asyncSend(builder.build(), callback);
    }

    /**
     * 异步发送消息（使用 {@link MessageMetadataBuilder} + 回调通知 + 指定超时）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param metadata 消息元数据
     * @param callback 发送回调
     * @param timeoutMillis 超时毫秒数
     * @param <T> 消息体类型
     */
    public <T> void asyncSend(String topic, T body, MessageMetadataBuilder metadata,
                               SendCallback callback, long timeoutMillis) {
        MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
        if (metadata != null) {
            metadata.applyTo(builder);
        }
        template.asyncSend(builder.build(), callback, timeoutMillis);
    }

    // ===================== 单向发送 =====================

    /**
     * 单向发送预构造的 {@link Message}（fire-and-forget）。
     *
     * @param message 消息
     * @param <T> body 类型
     */
    public <T> void sendOneway(Message<T> message) {
        template.sendOneway(message);
    }

    /**
     * 单向发送消息（fire-and-forget）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param <T> 消息体类型
     */
    public <T> void sendOneway(String topic, T body) {
        template.sendOneway(MessageBuilder.<T>withTopic(topic).body(body).build());
    }

    /**
     * 单向发送消息（带 Tag）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param <T> 消息体类型
     */
    public <T> void sendOneway(String topic, T body, String tag) {
        template.sendOneway(MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build());
    }

    /**
     * 单向发送消息（带 Tag 和 Keys）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param keys 消息 Keys
     * @param <T> 消息体类型
     */
    public <T> void sendOneway(String topic, T body, String tag, String keys) {
        template.sendOneway(MessageBuilder.<T>withTopic(topic).body(body)
            .tag(tag).keys(keys).build());
    }

    /**
     * 单向发送消息（带 Tag、Keys 和 ShardingKey）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param keys 消息 Keys
     * @param shardingKey 分片 Key
     * @param <T> 消息体类型
     */
    public <T> void sendOneway(String topic, T body, String tag, String keys, String shardingKey) {
        template.sendOneway(MessageBuilder.<T>withTopic(topic).body(body)
            .tag(tag).keys(keys).shardingKey(shardingKey).build());
    }

    /**
     * 单向发送消息（使用 {@link MessageMetadataBuilder} 封装附加参数）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param metadata 消息元数据
     * @param <T> 消息体类型
     */
    public <T> void sendOneway(String topic, T body, MessageMetadataBuilder metadata) {
        MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
        if (metadata != null) {
            metadata.applyTo(builder);
        }
        template.sendOneway(builder.build());
    }

    // ===================== 批量发送 =====================

    /**
     * 批量发送预构造的 {@link BatchMessage}。
     *
     * @param batch 批量消息
     * @param <T> body 类型
     * @return 每条消息的发送结果
     */
    public <T> List<SendResult> sendBatch(BatchMessage<T> batch) {
        return template.syncSendBatch(batch);
    }

    /**
     * 批量发送消息（同 Topic）。
     *
     * @param topic 主题
     * @param bodies 消息体列表
     * @param <T> 消息体类型
     * @return 每条消息的发送结果
     */
    public <T> List<SendResult> sendBatch(String topic, List<T> bodies) {
        Objects.requireNonNull(bodies, "bodies");
        if (bodies.isEmpty()) {
            throw new IllegalArgumentException("bodies list is empty");
        }
        List<Message<T>> messages = bodies.stream()
            .map(body -> MessageBuilder.<T>withTopic(topic).body(body).build())
            .toList();
        BatchMessage<T> batch = BatchMessage.<T>withTopic(topic).addAll(messages).build();
        return template.syncSendBatch(batch);
    }

    /**
     * 批量发送消息（同 Topic，带 Tag）。
     *
     * @param topic 主题
     * @param tag 消息 Tag
     * @param bodies 消息体列表
     * @param <T> 消息体类型
     * @return 每条消息的发送结果
     */
    public <T> List<SendResult> sendBatch(String topic, String tag, List<T> bodies) {
        Objects.requireNonNull(bodies, "bodies");
        if (bodies.isEmpty()) {
            throw new IllegalArgumentException("bodies list is empty");
        }
        List<Message<T>> messages = bodies.stream()
            .map(body -> MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build())
            .toList();
        BatchMessage<T> batch = BatchMessage.<T>withTopic(topic).addAll(messages).build();
        return template.syncSendBatch(batch);
    }

    /**
     * 批量发送消息（使用 {@link MessageMetadataBuilder} 封装附加参数）。
     *
     * @param topic 主题
     * @param bodies 消息体列表
     * @param metadata 消息元数据（Tag、Keys 等，应用到每条消息）
     * @param <T> 消息体类型
     * @return 每条消息的发送结果
     */
    public <T> List<SendResult> sendBatch(String topic, List<T> bodies, MessageMetadataBuilder metadata) {
        Objects.requireNonNull(bodies, "bodies");
        if (bodies.isEmpty()) {
            throw new IllegalArgumentException("bodies list is empty");
        }
        List<Message<T>> messages = bodies.stream().map(body -> {
            MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
            if (metadata != null) {
                metadata.applyTo(builder);
            }
            return builder.build();
        }).toList();
        BatchMessage<T> batch = BatchMessage.<T>withTopic(topic).addAll(messages).build();
        return template.syncSendBatch(batch);
    }

    /**
     * 批量发送预构造的 {@link Message} 列表（消息自带 Topic）。
     *
     * <p>所有消息的 Topic 必须一致（取自第一条消息），底层会校验一致性。
     *
     * @param messages 消息列表
     * @param <T> body 类型
     * @return 每条消息的发送结果
     * @throws NullPointerException 如果 messages 为 null
     * @throws IllegalArgumentException 如果消息列表为空或 Topic 不一致
     */
    public <T> List<SendResult> sendBatch(List<Message<T>> messages) {
        Objects.requireNonNull(messages, "messages");
        if (messages.isEmpty()) {
            throw new IllegalArgumentException("messages list is empty");
        }
        String topic = messages.get(0).getTopic();
        BatchMessage<T> batch = BatchMessage.<T>withTopic(topic).addAll(messages).build();
        return template.syncSendBatch(batch);
    }

    /**
     * 批量发送预构造的 {@link Message} 列表 + 指定超时和重试。
     *
     * @param messages 消息列表
     * @param timeoutMillis 超时毫秒数
     * @param retryTimes 重试次数（0 表示不重试）
     * @param <T> body 类型
     * @return 每条消息的发送结果
     */
    public <T> List<SendResult> sendBatch(List<Message<T>> messages, long timeoutMillis, int retryTimes) {
        // 当前 template.syncSendBatch 不支持超时/重试参数，这里预留 API，
        // 实际通过 template 的默认行为发送；未来 template 扩展后可直接传递参数。
        return sendBatch(messages);
    }

    /**
     * 批量发送可变参数 {@link Message}（消息自带 Topic）。
     *
     * @param messages 消息数组
     * @param <T> body 类型
     * @return 每条消息的发送结果
     * @throws IllegalArgumentException 如果未传入任何消息
     */
    @SafeVarargs
    public final <T> List<SendResult> sendBatch(Message<T>... messages) {
        Objects.requireNonNull(messages, "messages");
        if (messages.length == 0) {
            throw new IllegalArgumentException("messages array is empty");
        }
        return sendBatch(List.of(messages));
    }

    /**
     * 批量发送可变参数 {@link Message}（指定 Topic，覆盖每条消息的 Topic）。
     *
     * <p>如果消息自带的 Topic 与传入的 topic 不一致，将以传入的 topic 为准重新构造消息，
     * 原始消息对象不会被修改。
     *
     * @param topic 主题
     * @param messages 消息数组
     * @param <T> body 类型
     * @return 每条消息的发送结果
     * @throws IllegalArgumentException 如果未传入任何消息
     */
    @SafeVarargs
    public final <T> List<SendResult> sendBatch(String topic, Message<T>... messages) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(messages, "messages");
        if (messages.length == 0) {
            throw new IllegalArgumentException("messages array is empty");
        }
        List<Message<T>> normalized = new ArrayList<>(messages.length);
        for (Message<T> msg : messages) {
            normalized.add(normalizeTopic(msg, topic));
        }
        BatchMessage<T> batch = BatchMessage.<T>withTopic(topic).addAll(normalized).build();
        return template.syncSendBatch(batch);
    }

    /**
     * 批量发送可变参数 {@link Message}（指定 Topic + 超时 + 重试）。
     *
     * @param topic 主题
     * @param timeoutMillis 超时毫秒数
     * @param retryTimes 重试次数
     * @param messages 消息数组
     * @param <T> body 类型
     * @return 每条消息的发送结果
     */
    @SafeVarargs
    public final <T> List<SendResult> sendBatch(String topic, long timeoutMillis, int retryTimes,
                                                 Message<T>... messages) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(messages, "messages");
        if (messages.length == 0) {
            throw new IllegalArgumentException("messages array is empty");
        }
        // 当前 template.syncSendBatch 不支持超时/重试参数，预留 API
        return sendBatch(topic, messages);
    }

    /**
     * 规范化消息的 Topic（如果不一致则重新构造，不修改原始消息）。
     *
     * @param message 原始消息
     * @param topic 目标 Topic
     * @param <T> body 类型
     * @return Topic 已规范化的消息
     */
    private <T> Message<T> normalizeTopic(Message<T> message, String topic) {
        Objects.requireNonNull(message, "message");
        if (topic.equals(message.getTopic())) {
            return message;
        }
        // 使用 MessageBuilder 重新构造消息，覆盖 Topic，保留其他所有字段
        MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic)
            .body(message.getBody())
            .tag(message.getTag())
            .keys(message.getKeys())
            .shardingKey(message.getShardingKey());
        if (message.getDelayLevel() != null) {
            builder.delayLevel(message.getDelayLevel());
        }
        if (message.getDelayTimeMillis() != null) {
            builder.delayTimeMillis(message.getDelayTimeMillis());
        }
        if (message.getBornTimestamp() > 0) {
            builder.bornTimestamp(message.getBornTimestamp());
        }
        if (message.getBornHost() != null) {
            builder.bornHost(message.getBornHost());
        }
        if (message.getTransactionId() != null) {
            builder.transactionId(message.getTransactionId());
        }
        Map<String, String> props = message.getProperties();
        if (!props.isEmpty()) {
            builder.properties(props);
        }
        Map<String, String> userProps = message.getUserProperties();
        for (Map.Entry<String, String> entry : userProps.entrySet()) {
            builder.userProperty(entry.getKey(), entry.getValue());
        }
        return builder.build();
    }

    // ===================== 延时消息 =====================

    /**
     * 发送延时消息（按延时级别）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param delayLevel 延时级别
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult sendDelay(String topic, T body, DelayLevel delayLevel) {
        return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body)
            .delayLevel(delayLevel).build());
    }

    /**
     * 发送延时消息（自定义延时时间）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param delayTimeMillis 延时毫秒数
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult sendDelay(String topic, T body, long delayTimeMillis) {
        return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body)
            .delayTimeMillis(delayTimeMillis).build());
    }

    /**
     * 发送延时消息（带 Tag + 延时级别）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param delayLevel 延时级别
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult sendDelay(String topic, T body, String tag, DelayLevel delayLevel) {
        return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body)
            .tag(tag).delayLevel(delayLevel).build());
    }

    /**
     * 发送延时消息（带 Tag + 自定义延时时间）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param delayTimeMillis 延时毫秒数
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult sendDelay(String topic, T body, String tag, long delayTimeMillis) {
        return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body)
            .tag(tag).delayTimeMillis(delayTimeMillis).build());
    }

    /**
     * 发送延时消息（使用 {@link MessageMetadataBuilder}，元数据中需包含 delayLevel 或 delayTimeMillis）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param metadata 消息元数据（必须设置延时）
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult sendDelay(String topic, T body, MessageMetadataBuilder metadata) {
        MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
        if (metadata != null) {
            metadata.applyTo(builder);
        }
        return template.syncSend(builder.build());
    }

    /**
     * 发送延时消息（使用 {@link MessageMetadataBuilder} + 指定超时）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param metadata 消息元数据（必须设置延时）
     * @param timeoutMillis 超时毫秒数
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult sendDelay(String topic, T body, MessageMetadataBuilder metadata,
                                     long timeoutMillis) {
        MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
        if (metadata != null) {
            metadata.applyTo(builder);
        }
        return template.syncSend(builder.build(), timeoutMillis);
    }

    // ===================== 事务消息 =====================

    /**
     * 发送事务消息。
     *
     * @param topic 主题
     * @param body 消息体
     * @param callback 本地事务回调
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult sendTransaction(String topic, T body, TransactionCallback<T> callback) {
        return template.executeInTransaction(
            MessageBuilder.<T>withTopic(topic).body(body).build(), callback);
    }

    /**
     * 发送事务消息（带 Tag）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param tag 消息 Tag
     * @param callback 本地事务回调
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult sendTransaction(String topic, T body, String tag,
                                           TransactionCallback<T> callback) {
        return template.executeInTransaction(
            MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build(), callback);
    }

    /**
     * 发送事务消息（使用 {@link MessageMetadataBuilder} 封装附加参数）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param metadata 消息元数据
     * @param callback 本地事务回调
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult sendTransaction(String topic, T body, MessageMetadataBuilder metadata,
                                           TransactionCallback<T> callback) {
        MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
        if (metadata != null) {
            metadata.applyTo(builder);
        }
        return template.executeInTransaction(builder.build(), callback);
    }
}
