/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.message;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.util.StringUtils;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 消息流式构造器，对齐 RocketMQ {@code MessageBuilder} 风格。
 *
 * <p>使用示例：
 *
 * <pre>{@code
 * Message<String> msg = MessageBuilder.<String>withTopic("order-topic")
 *     .tag("created")
 *     .keys("order-123")
 *     .shardingKey("order-123")
 *     .body("{\"orderId\":123}")
 *     .withUserProperty("traceId", "t-001")
 *     .build();
 * }</pre>
 *
 * <p>命名约定：遵循 Builder 模式，实例方法无前缀（{@link #topic(String)} / {@link #tag(String)} / {@link
 * #body(Object)} 等）。 仅 {@link #withProperty(String, String)} / {@link #withUserProperty(String,
 * String)} 保留 with 前缀以区分"添加单条"和"批量设置"。 静态工厂方法使用语义化命名：{@link #withTopic(String)} / {@link
 * #withPayload(Object)} / {@link #create()} / {@link #from(Message)}。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class MessageBuilder<T> {

    private String topic;
    private String tag;
    private String keys;
    private String shardingKey;
    private final Map<String, String> properties = new LinkedHashMap<>();
    private final Map<String, String> userProperties = new LinkedHashMap<>();
    private T body;
    private DelayLevel delayLevel;
    private Long delayTimeMillis;
    private long bornTimestamp;
    private String bornHost;
    private int reconsumeTimes;
    private String transactionId;
    private MessageId messageId;

    private MessageBuilder() {}

    /**
     * 创建新的 Builder。
     *
     * @param <T> body 类型
     * @return 新的 Builder 实例
     */
    public static <T> MessageBuilder<T> create() {
        return new MessageBuilder<>();
    }

    /**
     * 创建指定 Topic 的 Builder。
     *
     * @param topic 主题
     * @param <T> body 类型
     * @return Builder 实例
     */
    public static <T> MessageBuilder<T> withTopic(String topic) {
        return MessageBuilder.<T>create().topic(topic);
    }

    /**
     * 创建指定 body 的 Builder。
     *
     * @param body 消息体
     * @param <T> body 类型
     * @return Builder 实例
     */
    public static <T> MessageBuilder<T> withPayload(T body) {
        return MessageBuilder.<T>create().body(body);
    }

    /**
     * 从已有消息复制创建 Builder，预填充所有字段。
     *
     * <p>使用示例：
     *
     * <pre>{@code
     * Message<String> copy = MessageBuilder.from(original)
     *     .topic("new-topic")
     *     .tag("new-tag")
     *     .build();
     * }</pre>
     *
     * @param message 源消息
     * @param <T> body 类型
     * @return 预填充的 Builder 实例
     */
    public static <T> MessageBuilder<T> from(Message<T> message) {
        Objects.requireNonNull(message, "message");
        MessageBuilder<T> builder =
                MessageBuilder.<T>create()
                        .topic(message.getTopic())
                        .tag(message.getTag())
                        .keys(message.getKeys())
                        .shardingKey(message.getShardingKey())
                        .body(message.getBody())
                        .bornTimestamp(message.getBornTimestamp())
                        .bornHost(message.getBornHost())
                        .reconsumeTimes(message.getReconsumeTimes())
                        .transactionId(message.getTransactionId())
                        .messageId(message.getMessageId());
        builder.properties.putAll(message.getProperties());
        builder.userProperties.putAll(message.getUserProperties());
        if (Objects.nonNull(message.getDelayLevel())) {
            builder.delayLevel(message.getDelayLevel());
        }
        if (Objects.nonNull(message.getDelayTimeMillis())) {
            builder.delayTimeMillis(message.getDelayTimeMillis());
        }
        return builder;
    }

    /**
     * 设置 Topic（必填）。
     *
     * @param topic 主题
     * @return this
     */
    public MessageBuilder<T> topic(String topic) {
        this.topic = StringUtils.requireValidTopic(topic);
        return this;
    }

    /**
     * 设置 Tag。
     *
     * @param tag 标签
     * @return this
     */
    public MessageBuilder<T> tag(String tag) {
        this.tag = Objects.isNull(tag) ? null : tag.trim();
        return this;
    }

    /**
     * 设置业务键。
     *
     * @param keys 业务键
     * @return this
     */
    public MessageBuilder<T> keys(String keys) {
        this.keys = keys;
        return this;
    }

    /**
     * 设置分片键（顺序消息使用）。
     *
     * @param shardingKey 分片键
     * @return this
     */
    public MessageBuilder<T> shardingKey(String shardingKey) {
        this.shardingKey = shardingKey;
        return this;
    }

    /**
     * 设置消息体（必填）。
     *
     * @param body 消息体
     * @return this
     */
    public MessageBuilder<T> body(T body) {
        this.body = body;
        return this;
    }

    /**
     * 添加单条系统属性（保留 with 前缀以区分"添加单条"和"批量设置"）。
     *
     * @param key 属性键
     * @param value 属性值
     * @return this
     * @throws NullPointerException 如果 key 或 value 为 null（立即失败）
     */
    public MessageBuilder<T> withProperty(String key, String value) {
        this.properties.put(
                Objects.requireNonNull(key, "property key"),
                Objects.requireNonNull(value, "property value"));
        return this;
    }

    /**
     * 批量设置系统属性（追加，不覆盖已有同名键）。
     *
     * <p><b>null 契约（0.1.2 统一，与 {@link #withProperty(String, String)} / {@link
     * MessageMetadataBuilder} 同口径）：</b>Map 中<b>任一 key/value 为 null 立即抛 {@link
     * NullPointerException}</b>（不把非法值推迟到 {@link #build()} 或属性快照时才暴露）；{@code properties} 为 {@code
     * null} 表示"不追加"（保持既有属性不变）。
     *
     * @param properties 系统属性 Map，可为 null（表示不追加）
     * @return this
     * @throws NullPointerException 如果任一 key 或 value 为 null
     */
    public MessageBuilder<T> properties(Map<String, String> properties) {
        if (Objects.nonNull(properties)) {
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                withProperty(entry.getKey(), entry.getValue());
            }
        }
        return this;
    }

    /**
     * 添加单条用户属性（保留 with 前缀以区分"添加单条"和"批量设置"）。
     *
     * @param key 属性键
     * @param value 属性值
     * @return this
     * @throws NullPointerException 如果 key 或 value 为 null（立即失败）
     */
    public MessageBuilder<T> withUserProperty(String key, String value) {
        this.userProperties.put(
                Objects.requireNonNull(key, "userProperty key"),
                Objects.requireNonNull(value, "userProperty value"));
        return this;
    }

    /**
     * 批量设置用户属性（追加，不覆盖已有同名键）。
     *
     * <p><b>null 契约（0.1.2 统一，与 {@link #withUserProperty(String, String)} / {@link
     * MessageMetadataBuilder} 同口径）：</b>Map 中<b>任一 key/value 为 null 立即抛 {@link
     * NullPointerException}</b>；{@code userProperties} 为 {@code null} 表示"不追加"（保持既有属性不变）。
     *
     * @param userProperties 用户属性 Map，可为 null（表示不追加）
     * @return this
     * @throws NullPointerException 如果任一 key 或 value 为 null
     */
    public MessageBuilder<T> userProperties(Map<String, String> userProperties) {
        if (Objects.nonNull(userProperties)) {
            for (Map.Entry<String, String> entry : userProperties.entrySet()) {
                withUserProperty(entry.getKey(), entry.getValue());
            }
        }
        return this;
    }

    /**
     * 设置延时级别（固定延时）。
     *
     * @param delayLevel 延时级别
     * @return this
     */
    public MessageBuilder<T> delayLevel(DelayLevel delayLevel) {
        this.delayLevel = delayLevel;
        return this;
    }

    /**
     * 设置任意延时时间（v1.0+）。
     *
     * <p>取值范围与 {@link Message} 构造期校验一致：{@code > 0} 且 {@code <= }{@link
     * StreamMQConstants#MAX_DELAY_TIME_MILLIS}（7 天）。
     *
     * @param delayTimeMillis 延时毫秒数，必须 &gt; 0 且不超过 7 天
     * @return this
     * @throws IllegalArgumentException 如果取值不在 {@code (0, 7 天]} 区间内
     */
    public MessageBuilder<T> delayTimeMillis(long delayTimeMillis) {
        if (delayTimeMillis <= 0) {
            throw new IllegalArgumentException(
                    "delayTimeMillis must be positive: " + delayTimeMillis);
        }
        if (delayTimeMillis > StreamMQConstants.MAX_DELAY_TIME_MILLIS) {
            throw new IllegalArgumentException(
                    "delayTimeMillis must be <= "
                            + StreamMQConstants.MAX_DELAY_TIME_MILLIS
                            + " (7 days), got: "
                            + delayTimeMillis);
        }
        this.delayTimeMillis = delayTimeMillis;
        return this;
    }

    /**
     * 设置出生时间戳（一般由框架自动填入）。
     *
     * @param bornTimestamp 出生时间戳（毫秒）
     * @return this
     */
    public MessageBuilder<T> bornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
        return this;
    }

    /**
     * 设置出生主机（一般由框架自动填入）。
     *
     * @param bornHost 出生主机
     * @return this
     */
    public MessageBuilder<T> bornHost(String bornHost) {
        this.bornHost = bornHost;
        return this;
    }

    /**
     * 设置事务 ID（仅事务消息场景）。
     *
     * @param transactionId 事务 ID
     * @return this
     */
    public MessageBuilder<T> transactionId(String transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    /**
     * 设置消息 ID（对应 Redis Stream Entry ID；一般由框架在消费/重试/派生场景填入）。
     *
     * @param messageId 消息 ID，可为 null 表示未设置
     * @return this
     */
    public MessageBuilder<T> messageId(MessageId messageId) {
        this.messageId = messageId;
        return this;
    }

    /**
     * 设置已重试消费次数。
     *
     * @param reconsumeTimes 重试次数
     * @return this
     */
    public MessageBuilder<T> reconsumeTimes(int reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
        return this;
    }

    /**
     * 构造 {@link Message} 实例。
     *
     * @return 消息对象
     * @throws NullPointerException 如果 topic 或 body 为 null
     * @throws IllegalArgumentException 如果 topic 为空字符串
     */
    public Message<T> build() {
        Objects.requireNonNull(topic, "topic");
        // 边界（与 Message 值对象刻意不同）：Message 允许 body == null（消费端可能收到无载荷消息，
        // 内置序列化器统一 serialize(null) -> null / deserialize(null|empty) -> null），
        // 但 MessageBuilder 是**发送侧**构造器，发送一条无载荷消息几乎总是调用方漏设 body 的笔误
        // （历史上正是它让"占位消息"污染了业务流），因此这里保持 fail-fast。
        // 由此推出一个重要推论：MessageBuilder.from(m) 无法往返 body 为 null 的消息——
        // 那是"把收到的无载荷消息原样再发一次"，语义上属于发送 null body，同样被拒绝。
        if (body == null) {
            throw new NullPointerException(
                    "body must not be null: MessageBuilder builds send-side messages and a null"
                        + " body (a payload-less message) cannot be sent; use a non-null body, or"
                        + " forward the original Message instead of rebuilding it");
        }
        if (topic.isEmpty()) {
            throw new IllegalArgumentException("topic must not be empty");
        }
        long ts = bornTimestamp > 0 ? bornTimestamp : System.currentTimeMillis();
        String host = Objects.nonNull(bornHost) ? bornHost : "unknown";
        Message<T> message =
                new Message<>(
                        topic,
                        tag,
                        keys,
                        shardingKey,
                        properties,
                        userProperties,
                        body,
                        delayLevel,
                        delayTimeMillis,
                        ts,
                        host,
                        transactionId,
                        reconsumeTimes);
        return Objects.nonNull(messageId) ? message.withMessageId(messageId) : message;
    }
}
