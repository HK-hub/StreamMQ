package io.github.streammq.core.message;

import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.service.StreamMessageService;
import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 消息元数据构造器，封装除 topic 和 body 之外的所有消息属性。
 *
 * <p>用于 {@link StreamMessageService} 的 {@code send(topic, body, MessageMetadataBuilder)} 模式，
 * 将 Tag、Keys、ShardingKey、延时、属性等参数统一封装，避免方法重载数量爆炸。
 *
 * <p>使用示例：
 * <pre>{@code
 * MessageMetadataBuilder metadata = MessageMetadataBuilder.create()
 *     .tag("created")
 *     .keys("order-123")
 *     .shardingKey("order-123")
 *     .delayLevel(DelayLevel.LEVEL_5)
 *     .withUserProperty("traceId", "t-001");
 *
 * service.send("order-topic", order, metadata);
 * }</pre>
 *
 * <p>线程安全：非线程安全，每个构造器实例仅用于单次消息构造。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class MessageMetadataBuilder {

    /**
     * @return Tag
     */
    @Getter
    private String tag;
    /**
     * @return Keys
     */
    @Getter
    private String keys;
    /**
     * @return 分片 Key
     */
    @Getter
    private String shardingKey;
    private final Map<String, String> properties = new LinkedHashMap<>();
    private final Map<String, String> userProperties = new LinkedHashMap<>();
    /**
     * @return 延时级别
     */
    @Getter
    private DelayLevel delayLevel;
    /**
     * @return 自定义延时时间（毫秒），null 表示未设置
     */
    @Getter
    private Long delayTimeMillis;
    /**
     * @return 消息生成时间戳，null 表示未设置
     */
    @Getter
    private Long bornTimestamp;
    /**
     * @return 消息来源主机
     */
    @Getter
    private String bornHost;

    private MessageMetadataBuilder() {
    }

    /**
     * 创建新的元数据构造器。
     *
     * @return 新的构造器实例
     */
    public static MessageMetadataBuilder create() {
        return new MessageMetadataBuilder();
    }

    /**
     * 设置 Tag。
     *
     * @param tag 消息 Tag
     * @return this
     */
    public MessageMetadataBuilder tag(String tag) {
        this.tag = tag;
        return this;
    }

    /**
     * 设置 Keys。
     *
     * @param keys 消息 Keys
     * @return this
     */
    public MessageMetadataBuilder keys(String keys) {
        this.keys = keys;
        return this;
    }

    /**
     * 设置分片 Key。
     *
     * @param shardingKey 分片 Key
     * @return this
     */
    public MessageMetadataBuilder shardingKey(String shardingKey) {
        this.shardingKey = shardingKey;
        return this;
    }

    /**
     * 设置延时级别。
     *
     * @param delayLevel 延时级别
     * @return this
     */
    public MessageMetadataBuilder delayLevel(DelayLevel delayLevel) {
        this.delayLevel = delayLevel;
        return this;
    }

    /**
     * 设置自定义延时时间（毫秒）。
     *
     * @param delayTimeMillis 延时毫秒数（必须 &gt; 0）
     * @return this
     */
    public MessageMetadataBuilder delayTimeMillis(long delayTimeMillis) {
        if (delayTimeMillis <= 0) {
            throw new IllegalArgumentException("delayTimeMillis must be > 0, got " + delayTimeMillis);
        }
        this.delayTimeMillis = delayTimeMillis;
        return this;
    }

    /**
     * 添加系统属性。
     *
     * @param key 属性 Key
     * @param value 属性 Value
     * @return this
     */
    public MessageMetadataBuilder property(String key, String value) {
        this.properties.put(Objects.requireNonNull(key, "key"), value);
        return this;
    }

    /**
     * 批量设置系统属性。
     *
     * @param properties 属性 Map
     * @return this
     */
    public MessageMetadataBuilder properties(Map<String, String> properties) {
        if (Objects.nonNull(properties)) {
            this.properties.putAll(properties);
        }
        return this;
    }

    /**
     * 添加用户属性。
     *
     * @param key 属性 Key
     * @param value 属性 Value
     * @return this
     */
    public MessageMetadataBuilder userProperty(String key, String value) {
        this.userProperties.put(Objects.requireNonNull(key, "key"), value);
        return this;
    }

    /**
     * 批量设置用户属性。
     *
     * @param userProperties 用户属性 Map
     * @return this
     */
    public MessageMetadataBuilder userProperties(Map<String, String> userProperties) {
        if (Objects.nonNull(userProperties)) {
            this.userProperties.putAll(userProperties);
        }
        return this;
    }

    /**
     * 设置消息生成时间戳。
     *
     * @param bornTimestamp 生成时间戳
     * @return this
     */
    public MessageMetadataBuilder bornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
        return this;
    }

    /**
     * 设置消息来源主机。
     *
     * @param bornHost 来源主机
     * @return this
     */
    public MessageMetadataBuilder bornHost(String bornHost) {
        this.bornHost = bornHost;
        return this;
    }

    // ===================== Getter =====================

    /** @return 系统属性（不可修改） */
    public Map<String, String> getProperties() {
        return Map.copyOf(properties);
    }

    /** @return 用户属性（不可修改） */
    public Map<String, String> getUserProperties() {
        return Map.copyOf(userProperties);
    }

    // ===================== 应用到 MessageBuilder =====================

    /**
     * 将所有元数据应用到 {@link MessageBuilder}。
     *
     * @param builder 消息构造器
     * @param <T> body 类型
     */
    public <T> void applyTo(MessageBuilder<T> builder) {
        Objects.requireNonNull(builder, "builder");
        if (Objects.nonNull(tag)) {
            builder.tag(tag);
        }
        if (Objects.nonNull(keys)) {
            builder.keys(keys);
        }
        if (Objects.nonNull(shardingKey)) {
            builder.shardingKey(shardingKey);
        }
        if (Objects.nonNull(delayLevel)) {
            builder.delayLevel(delayLevel);
        }
        if (Objects.nonNull(delayTimeMillis)) {
            builder.delayTimeMillis(delayTimeMillis);
        }
        if (!properties.isEmpty()) {
            builder.properties(properties);
        }
        if (!userProperties.isEmpty()) {
            for (Map.Entry<String, String> entry : userProperties.entrySet()) {
                builder.withUserProperty(entry.getKey(), entry.getValue());
            }
        }
        if (Objects.nonNull(bornTimestamp)) {
            builder.bornTimestamp(bornTimestamp);
        }
        if (Objects.nonNull(bornHost)) {
            builder.bornHost(bornHost);
        }
    }

    /**
     * 判断是否包含延时设置。
     *
     * @return true 如果设置了 delayLevel 或 delayTimeMillis
     */
    public boolean hasDelay() {
        return Objects.nonNull(delayLevel) || Objects.nonNull(delayTimeMillis);
    }
}
