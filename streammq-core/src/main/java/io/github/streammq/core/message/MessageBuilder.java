package io.github.streammq.core.message;

import io.github.streammq.core.enums.DelayLevel;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 消息流式构造器，对齐 RocketMQ {@code MessageBuilder} 风格。
 *
 * <p>使用示例：
 * <pre>{@code
 * Message<String> msg = MessageBuilder.<String>withTopic("order-topic")
 *     .tag("created")
 *     .keys("order-123")
 *     .shardingKey("order-123")
 *     .body("{\"orderId\":123}")
 *     .userProperty("traceId", "t-001")
 *     .build();
 * }</pre>
 *
 * <p>命名约定：
 * <ul>
 *   <li>静态工厂方法以 {@code with} 前缀：{@link #withTopic(String)} / {@link #withPayload(Object)} / {@link #create()}</li>
 *   <li>实例方法无前缀：{@link #topic(String)} / {@link #tag(String)} / {@link #body(Object)} ...</li>
 * </ul>
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
    private String transactionId;

    private MessageBuilder() {
    }

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
     * 设置 Topic（必填）。
     *
     * @param topic 主题
     * @return this
     */
    public MessageBuilder<T> topic(String topic) {
        this.topic = Objects.requireNonNull(topic, "topic").trim();
        return this;
    }

    /**
     * 设置 Tag。
     *
     * @param tag 标签
     * @return this
     */
    public MessageBuilder<T> tag(String tag) {
        this.tag = tag == null ? null : tag.trim();
        return this;
    }

    /** with* 别名，委托到 {@link #tag(String)}。 */
    public MessageBuilder<T> withTag(String tag) {
        return tag(tag);
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

    /** with* 别名，委托到 {@link #keys(String)}。 */
    public MessageBuilder<T> withKeys(String keys) {
        return keys(keys);
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

    /** with* 别名，委托到 {@link #shardingKey(String)}。 */
    public MessageBuilder<T> withShardingKey(String shardingKey) {
        return shardingKey(shardingKey);
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

    /** with* 别名，委托到 {@link #body(Object)}。 */
    public MessageBuilder<T> withBody(T body) {
        return body(body);
    }

    /**
     * 添加系统属性。
     *
     * @param key 属性键
     * @param value 属性值
     * @return this
     */
    public MessageBuilder<T> property(String key, String value) {
        this.properties.put(
            Objects.requireNonNull(key, "property key"),
            Objects.requireNonNull(value, "property value"));
        return this;
    }

    /** with* 别名，委托到 {@link #property(String, String)}。 */
    public MessageBuilder<T> withProperty(String key, String value) {
        return property(key, value);
    }

    /**
     * 批量设置系统属性。
     *
     * @param properties 系统属性
     * @return this
     */
    public MessageBuilder<T> properties(Map<String, String> properties) {
        if (properties != null) {
            this.properties.putAll(properties);
        }
        return this;
    }

    /** with* 别名，委托到 {@link #properties(Map)}。 */
    public MessageBuilder<T> withProperties(Map<String, String> properties) {
        return properties(properties);
    }

    /**
     * 添加用户属性。
     *
     * @param key 属性键
     * @param value 属性值
     * @return this
     */
    public MessageBuilder<T> userProperty(String key, String value) {
        this.userProperties.put(
            Objects.requireNonNull(key, "userProperty key"),
            Objects.requireNonNull(value, "userProperty value"));
        return this;
    }

    /** with* 别名，委托到 {@link #userProperty(String, String)}。 */
    public MessageBuilder<T> withUserProperty(String key, String value) {
        return userProperty(key, value);
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

    /** with* 别名，委托到 {@link #delayLevel(DelayLevel)}。 */
    public MessageBuilder<T> withDelayLevel(DelayLevel delayLevel) {
        return delayLevel(delayLevel);
    }

    /**
     * 设置任意延时时间（v1.0+）。
     *
     * @param delayTimeMillis 延时毫秒数，必须 > 0
     * @return this
     */
    public MessageBuilder<T> delayTimeMillis(long delayTimeMillis) {
        if (delayTimeMillis <= 0) {
            throw new IllegalArgumentException("delayTimeMillis must be positive: " + delayTimeMillis);
        }
        this.delayTimeMillis = delayTimeMillis;
        return this;
    }

    /** with* 别名，委托到 {@link #delayTimeMillis(long)}。 */
    public MessageBuilder<T> withDelayTimeMillis(long delayTimeMillis) {
        return delayTimeMillis(delayTimeMillis);
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
     * 构造 {@link Message} 实例。
     *
     * @return 消息对象
     * @throws NullPointerException 如果 topic 或 body 为 null
     * @throws IllegalArgumentException 如果 topic 为空字符串
     */
    public Message<T> build() {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(body, "body");
        if (topic.isEmpty()) {
            throw new IllegalArgumentException("topic must not be empty");
        }
        long ts = bornTimestamp > 0 ? bornTimestamp : System.currentTimeMillis();
        String host = bornHost != null ? bornHost : "unknown";
        return new Message<>(topic, tag, keys, shardingKey, properties, userProperties,
            body, delayLevel, delayTimeMillis, ts, host, transactionId);
    }
}
