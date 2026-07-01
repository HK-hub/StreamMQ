package io.github.streammq.core.message;

import io.github.streammq.core.enums.DelayLevel;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 消息载体，封装 Topic / Tag / Keys / ShardingKey / Properties / Body 等字段。
 *
 * <p>对应一条 Redis Stream Entry。元信息（tag/keys/shardingKey/properties）始终为 String，
 * 仅 {@link #body} 通过 {@code MessageSerializer} 序列化为 byte[]。
 *
 * <p>不可变性保证：构造后字段值不可变（properties 返回不可修改视图）。
 * 通过 {@link MessageBuilder} 构造。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class Message<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Topic（必填），对应一个 Redis Key（Stream） */
    private String topic;

    /** Tag（可选），同一 Topic 下的二级分类，用于消费端过滤 */
    private String tag;

    /** 业务键（可选），用于幂等/查询 */
    private String keys;

    /** 分片键（可选），用于分区顺序消息路由 */
    private String shardingKey;

    /** 系统属性（不可变），框架使用，例如 traceId */
    private Map<String, String> properties;

    /** 用户属性（不可变），用户自定义透传 */
    private Map<String, String> userProperties;

    /** 消息体（必填），由序列化器决定如何转 byte[] */
    private T body;

    /** 延时级别（可选），非空时表示延时消息 */
    private DelayLevel delayLevel;

    /** 任意延时毫秒数（可选），v1.0+ 支持，优先级高于 {@link #delayLevel} */
    private Long delayTimeMillis;

    /** 消息 ID（发送成功后由框架回填，对应 Redis Stream Entry ID） */
    private MessageId messageId;

    /** 出生时间戳（毫秒），发送端写入 */
    private long bornTimestamp;

    /** 出生主机（发送端 host:port） */
    private String bornHost;

    /** 已重试消费次数（消费端递增） */
    private int reconsumeTimes;

    /** 事务 ID（仅事务消息） */
    private String transactionId;

    /**
     * 默认构造（用于反序列化）。
     */
    public Message() {
        this.properties = new HashMap<>();
        this.userProperties = new HashMap<>();
    }

    /**
     * 通过 Builder 调用的全参构造。
     */
    Message(String topic, String tag, String keys, String shardingKey,
            Map<String, String> properties, Map<String, String> userProperties,
            T body, DelayLevel delayLevel, Long delayTimeMillis,
            long bornTimestamp, String bornHost, String transactionId) {
        this.topic = topic;
        this.tag = tag;
        this.keys = keys;
        this.shardingKey = shardingKey;
        this.properties = properties == null ? new HashMap<>() : new HashMap<>(properties);
        this.userProperties = userProperties == null ? new HashMap<>() : new HashMap<>(userProperties);
        this.body = body;
        this.delayLevel = delayLevel;
        this.delayTimeMillis = delayTimeMillis;
        this.bornTimestamp = bornTimestamp;
        this.bornHost = bornHost;
        this.transactionId = transactionId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getKeys() {
        return keys;
    }

    public void setKeys(String keys) {
        this.keys = keys;
    }

    public String getShardingKey() {
        return shardingKey;
    }

    public void setShardingKey(String shardingKey) {
        this.shardingKey = shardingKey;
    }

    /**
     * 返回系统属性（不可修改视图）。
     *
     * @return 系统属性 Map
     */
    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties == null ? new HashMap<>() : new LinkedHashMap<>(properties);
    }

    public void putProperty(String key, String value) {
        Objects.requireNonNull(key, "property key");
        Objects.requireNonNull(value, "property value");
        this.properties.put(key, value);
    }

    /**
     * 返回用户属性（不可修改视图）。
     *
     * @return 用户属性 Map
     */
    public Map<String, String> getUserProperties() {
        return Collections.unmodifiableMap(userProperties);
    }

    public void setUserProperties(Map<String, String> userProperties) {
        this.userProperties = userProperties == null ? new HashMap<>() : new LinkedHashMap<>(userProperties);
    }

    public void putUserProperty(String key, String value) {
        Objects.requireNonNull(key, "userProperty key");
        Objects.requireNonNull(value, "userProperty value");
        this.userProperties.put(key, value);
    }

    public T getBody() {
        return body;
    }

    public void setBody(T body) {
        this.body = body;
    }

    public DelayLevel getDelayLevel() {
        return delayLevel;
    }

    public void setDelayLevel(DelayLevel delayLevel) {
        this.delayLevel = delayLevel;
    }

    public Long getDelayTimeMillis() {
        return delayTimeMillis;
    }

    public void setDelayTimeMillis(Long delayTimeMillis) {
        this.delayTimeMillis = delayTimeMillis;
    }

    public MessageId getMessageId() {
        return messageId;
    }

    public void setMessageId(MessageId messageId) {
        this.messageId = messageId;
    }

    public long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public String getBornHost() {
        return bornHost;
    }

    public void setBornHost(String bornHost) {
        this.bornHost = bornHost;
    }

    public int getReconsumeTimes() {
        return reconsumeTimes;
    }

    public void setReconsumeTimes(int reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    /**
     * 是否为延时消息。
     *
     * @return true 如果 delayLevel 或 delayTimeMillis 非空
     */
    public boolean isDelayMessage() {
        return delayLevel != null || delayTimeMillis != null;
    }

    /**
     * 是否为事务消息。
     *
     * @return true 如果 transactionId 非空
     */
    public boolean isTransactionMessage() {
        return transactionId != null && !transactionId.isEmpty();
    }

    @Override
    public String toString() {
        return "Message{"
            + "topic='" + topic + '\''
            + ", tag='" + tag + '\''
            + ", keys='" + keys + '\''
            + ", shardingKey='" + shardingKey + '\''
            + ", messageId=" + messageId
            + ", bornTimestamp=" + bornTimestamp
            + ", reconsumeTimes=" + reconsumeTimes
            + ", transactionId='" + transactionId + '\''
            + ", delayLevel=" + delayLevel
            + ", delayTimeMillis=" + delayTimeMillis
            + ", body=" + (body == null ? "null" : body.getClass().getSimpleName())
            + ", properties.size=" + properties.size()
            + ", userProperties.size=" + userProperties.size()
            + '}';
    }
}
