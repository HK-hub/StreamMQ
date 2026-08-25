/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.message;

import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.util.StringUtils;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;

/**
 * 消息载体（不可变值对象），封装 Topic / Tag / Keys / ShardingKey / Properties / Body 等字段。
 *
 * <p>对应一条 Redis Stream Entry。元信息（tag/keys/shardingKey/properties）始终为 String， 仅 {@link #body} 通过
 * {@code MessageSerializer} 序列化为 byte[]。
 *
 * <p><b>构造方式：</b>业务代码统一使用 {@link MessageBuilder#build()}；框架代码可使用公开全参构造器或 {@code withXxx()}
 * 派生方法。所有字段均为 {@code final}，实例创建后不可修改，可安全地在多线程间共享。
 *
 * <p>发送成功后的消息 ID 由 {@link SendResult} 承载，框架不会回填修改传入的 Message 实例。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
public final class Message<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Topic（必填），对应一个 Redis Key（Stream） */
    private final String topic;

    /** Tag（可选），同一 Topic 下的二级分类，用于消费端过滤 */
    private final String tag;

    /** 业务键（可选），用于业务层幂等/查询，框架不使用此字段做去重 */
    private final String keys;

    /** 分片键（可选），仅顺序消费场景使用，相同 shardingKey 的消息路由到同一分片保证顺序 */
    private final String shardingKey;

    /** 系统属性（防御性拷贝），框架使用，例如 traceId。getter 返回不可修改视图。 */
    private final Map<String, String> properties;

    /** 用户属性（防御性拷贝），用户自定义透传。getter 返回不可修改视图。 */
    private final Map<String, String> userProperties;

    /** 消息体（必填），由序列化器决定如何转 byte[] */
    private final T body;

    /**
     * 延时级别（可选），18 级固定延时，非空时表示延时消息。 与 {@link #delayTimeMillis} 互斥，同时设置时 {@code delayTimeMillis} 优先。
     *
     * <p>实现机制：通过延时调度器周期扫描 ZSet，将到期消息转投到目标 Stream。 精度取决于扫描间隔（默认 1000ms）。
     */
    private final DelayLevel delayLevel;

    /** 任意延时毫秒数（可选）。优先级高于 {@link #delayLevel}，同时设置时此字段生效。 */
    private final Long delayTimeMillis;

    /**
     * 消息 ID（对应 Redis Stream Entry ID，格式 {@code {timestamp}-{sequence}}）。 发送场景下由发送结果 {@link
     * SendResult} 承载；消费/重试场景由框架从 Stream Entry 派生。
     */
    private final MessageId messageId;

    /** 出生时间戳（毫秒），发送端写入，用于消息溯源和超时判断 */
    private final long bornTimestamp;

    /** 出生主机（发送端 host:port），用于消息溯源，分布式环境下仅供参考 */
    private final String bornHost;

    /** 已重试消费次数（框架在每次重试时通过 {@link #withReconsumeTimes(int)} 派生新实例递增） */
    private final int reconsumeTimes;

    /** 事务 ID（仅事务消息） */
    private final String transactionId;

    /**
     * 公开全参构造器（框架与 Builder 使用），messageId 初始为 null。
     *
     * @param topic 主题
     * @param tag 标签
     * @param keys 业务键
     * @param shardingKey 分片键
     * @param properties 系统属性
     * @param userProperties 用户属性
     * @param body 消息体
     * @param delayLevel 延时级别
     * @param delayTimeMillis 延时毫秒数
     * @param bornTimestamp 出生时间戳
     * @param bornHost 出生主机
     * @param transactionId 事务 ID
     * @param reconsumeTimes 已重试消费次数
     */
    public Message(
            String topic,
            String tag,
            String keys,
            String shardingKey,
            Map<String, String> properties,
            Map<String, String> userProperties,
            T body,
            DelayLevel delayLevel,
            Long delayTimeMillis,
            long bornTimestamp,
            String bornHost,
            String transactionId,
            int reconsumeTimes) {
        this(
                topic,
                tag,
                keys,
                shardingKey,
                properties,
                userProperties,
                body,
                delayLevel,
                delayTimeMillis,
                null,
                bornTimestamp,
                bornHost,
                transactionId,
                reconsumeTimes);
    }

    /** 私有全参构造器（含 messageId，供 withXxx 与框架反序列化派生使用）。 */
    private Message(
            String topic,
            String tag,
            String keys,
            String shardingKey,
            Map<String, String> properties,
            Map<String, String> userProperties,
            T body,
            DelayLevel delayLevel,
            Long delayTimeMillis,
            MessageId messageId,
            long bornTimestamp,
            String bornHost,
            String transactionId,
            int reconsumeTimes) {
        Objects.requireNonNull(topic, "topic");
        if (topic.trim().isEmpty()) {
            throw new IllegalArgumentException("topic must not be empty");
        }
        this.topic = topic;
        this.tag = tag;
        this.keys = keys;
        this.shardingKey = shardingKey;
        this.properties = Objects.isNull(properties) ? new HashMap<>() : new HashMap<>(properties);
        this.userProperties =
                Objects.isNull(userProperties)
                        ? new HashMap<>()
                        : new LinkedHashMap<>(userProperties);
        this.body = body;
        this.delayLevel = delayLevel;
        this.delayTimeMillis = delayTimeMillis;
        this.messageId = messageId;
        this.bornTimestamp = bornTimestamp;
        this.bornHost = bornHost;
        this.transactionId = transactionId;
        this.reconsumeTimes = reconsumeTimes;
    }

    // ===================== properties / userProperties 访问器 =====================

    /**
     * 返回系统属性（不可修改视图）。
     *
     * @return 系统属性 Map
     */
    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    /**
     * 返回用户属性（不可修改视图）。
     *
     * @return 用户属性 Map
     */
    public Map<String, String> getUserProperties() {
        return Collections.unmodifiableMap(userProperties);
    }

    // ===================== 业务方法 =====================

    /**
     * 是否为延时消息。
     *
     * @return true 如果 delayLevel 或 delayTimeMillis 非空
     */
    public boolean isDelayMessage() {
        return Objects.nonNull(delayLevel) || Objects.nonNull(delayTimeMillis);
    }

    /**
     * 是否为事务消息。
     *
     * @return true 如果 transactionId 非空
     */
    public boolean isTransactionMessage() {
        return StringUtils.isNotEmpty(transactionId);
    }

    // ===================== 不可变操作（withXxx / addXxx 方法） =====================

    /**
     * 返回带有指定 topic 的新 Message 实例。
     *
     * @param topic 新的 topic
     * @return 新的 Message 实例
     */
    public Message<T> withTopic(String topic) {
        Objects.requireNonNull(topic, "topic");
        if (topic.trim().isEmpty()) {
            throw new IllegalArgumentException("topic must not be empty");
        }
        return new Message<>(
                topic,
                tag,
                keys,
                shardingKey,
                properties,
                userProperties,
                body,
                delayLevel,
                delayTimeMillis,
                messageId,
                bornTimestamp,
                bornHost,
                transactionId,
                reconsumeTimes);
    }

    /**
     * 返回带有指定 tag 的新 Message 实例。
     *
     * @param tag 新的 tag
     * @return 新的 Message 实例
     */
    public Message<T> withTag(String tag) {
        return derive(tag, keys, shardingKey, properties, userProperties, body);
    }

    /**
     * 返回带有指定 keys 的新 Message 实例。
     *
     * @param keys 新的 keys
     * @return 新的 Message 实例
     */
    public Message<T> withKeys(String keys) {
        return new Message<>(
                topic,
                tag,
                keys,
                shardingKey,
                properties,
                userProperties,
                body,
                delayLevel,
                delayTimeMillis,
                messageId,
                bornTimestamp,
                bornHost,
                transactionId,
                reconsumeTimes);
    }

    /**
     * 返回带有指定 shardingKey 的新 Message 实例。
     *
     * @param shardingKey 新的 shardingKey
     * @return 新的 Message 实例
     */
    public Message<T> withShardingKey(String shardingKey) {
        return new Message<>(
                topic,
                tag,
                keys,
                shardingKey,
                properties,
                userProperties,
                body,
                delayLevel,
                delayTimeMillis,
                messageId,
                bornTimestamp,
                bornHost,
                transactionId,
                reconsumeTimes);
    }

    /**
     * 返回带有指定 body 的新 Message 实例。
     *
     * @param body 新的 body
     * @return 新的 Message 实例
     */
    public Message<T> withBody(T body) {
        return derive(tag, keys, shardingKey, properties, userProperties, body);
    }

    /**
     * 返回带有指定延时级别的新 Message 实例。
     *
     * @param delayLevel 新的延时级别
     * @return 新的 Message 实例
     */
    public Message<T> withDelayLevel(DelayLevel delayLevel) {
        return new Message<>(
                topic,
                tag,
                keys,
                shardingKey,
                properties,
                userProperties,
                body,
                delayLevel,
                delayTimeMillis,
                messageId,
                bornTimestamp,
                bornHost,
                transactionId,
                reconsumeTimes);
    }

    /**
     * 返回带有指定延时毫秒数的新 Message 实例。
     *
     * @param delayTimeMillis 新的延时毫秒数
     * @return 新的 Message 实例
     */
    public Message<T> withDelayTimeMillis(Long delayTimeMillis) {
        return new Message<>(
                topic,
                tag,
                keys,
                shardingKey,
                properties,
                userProperties,
                body,
                delayLevel,
                delayTimeMillis,
                messageId,
                bornTimestamp,
                bornHost,
                transactionId,
                reconsumeTimes);
    }

    /**
     * 返回带有指定 MessageId 的新 Message 实例。
     *
     * @param messageId 新的 MessageId
     * @return 新的 Message 实例
     */
    public Message<T> withMessageId(MessageId messageId) {
        return new Message<>(
                topic,
                tag,
                keys,
                shardingKey,
                properties,
                userProperties,
                body,
                delayLevel,
                delayTimeMillis,
                messageId,
                bornTimestamp,
                bornHost,
                transactionId,
                reconsumeTimes);
    }

    /**
     * 返回带有指定出生时间戳的新 Message 实例。
     *
     * @param bornTimestamp 新的出生时间戳
     * @return 新的 Message 实例
     */
    public Message<T> withBornTimestamp(long bornTimestamp) {
        return new Message<>(
                topic,
                tag,
                keys,
                shardingKey,
                properties,
                userProperties,
                body,
                delayLevel,
                delayTimeMillis,
                messageId,
                bornTimestamp,
                bornHost,
                transactionId,
                reconsumeTimes);
    }

    /**
     * 返回带有指定出生主机的新 Message 实例。
     *
     * @param bornHost 新的出生主机
     * @return 新的 Message 实例
     */
    public Message<T> withBornHost(String bornHost) {
        return new Message<>(
                topic,
                tag,
                keys,
                shardingKey,
                properties,
                userProperties,
                body,
                delayLevel,
                delayTimeMillis,
                messageId,
                bornTimestamp,
                bornHost,
                transactionId,
                reconsumeTimes);
    }

    /**
     * 返回带有指定重试次数的新 Message 实例。
     *
     * @param reconsumeTimes 新的重试次数
     * @return 新的 Message 实例
     */
    public Message<T> withReconsumeTimes(int reconsumeTimes) {
        return new Message<>(
                topic,
                tag,
                keys,
                shardingKey,
                properties,
                userProperties,
                body,
                delayLevel,
                delayTimeMillis,
                messageId,
                bornTimestamp,
                bornHost,
                transactionId,
                reconsumeTimes);
    }

    /**
     * 返回带有指定事务 ID 的新 Message 实例。
     *
     * @param transactionId 新的事务 ID
     * @return 新的 Message 实例
     */
    public Message<T> withTransactionId(String transactionId) {
        return new Message<>(
                topic,
                tag,
                keys,
                shardingKey,
                properties,
                userProperties,
                body,
                delayLevel,
                delayTimeMillis,
                messageId,
                bornTimestamp,
                bornHost,
                transactionId,
                reconsumeTimes);
    }

    /**
     * 返回带有指定系统属性的新 Message 实例（替换现有属性）。
     *
     * @param properties 新的系统属性
     * @return 新的 Message 实例
     */
    public Message<T> withProperties(Map<String, String> properties) {
        Map<String, String> copied =
                Objects.isNull(properties) ? new HashMap<>() : new LinkedHashMap<>(properties);
        return derive(tag, keys, shardingKey, copied, userProperties, body);
    }

    /**
     * 返回添加了指定系统属性的新 Message 实例。
     *
     * @param key 属性键
     * @param value 属性值
     * @return 新的 Message 实例
     */
    public Message<T> addProperty(String key, String value) {
        Objects.requireNonNull(key, "property key");
        Objects.requireNonNull(value, "property value");
        Map<String, String> copied = new HashMap<>(this.properties);
        copied.put(key, value);
        return derive(tag, keys, shardingKey, copied, userProperties, body);
    }

    /**
     * 返回带有指定用户属性的新 Message 实例（替换现有属性）。
     *
     * @param userProperties 新的用户属性
     * @return 新的 Message 实例
     */
    public Message<T> withUserProperties(Map<String, String> userProperties) {
        Map<String, String> copied =
                Objects.isNull(userProperties)
                        ? new HashMap<>()
                        : new LinkedHashMap<>(userProperties);
        return derive(tag, keys, shardingKey, properties, copied, body);
    }

    /**
     * 返回添加了指定用户属性的新 Message 实例。
     *
     * @param key 属性键
     * @param value 属性值
     * @return 新的 Message 实例
     */
    public Message<T> addUserProperty(String key, String value) {
        Objects.requireNonNull(key, "property key");
        Objects.requireNonNull(value, "userProperty value");
        Map<String, String> copied = new LinkedHashMap<>(this.userProperties);
        copied.put(key, value);
        return derive(tag, keys, shardingKey, properties, copied, body);
    }

    // ===================== 内部工具方法 =====================

    /** 以当前实例为基础派生新实例（仅变化元信息字段，其余字段原样保留）。 */
    private Message<T> derive(
            String newTag,
            String newKeys,
            String newShardingKey,
            Map<String, String> newProperties,
            Map<String, String> newUserProperties,
            T newBody) {
        return new Message<>(
                topic,
                newTag,
                newKeys,
                newShardingKey,
                newProperties,
                newUserProperties,
                newBody,
                delayLevel,
                delayTimeMillis,
                messageId,
                bornTimestamp,
                bornHost,
                transactionId,
                reconsumeTimes);
    }

    @Override
    public String toString() {
        return "Message{"
                + "topic='"
                + topic
                + '\''
                + ", tag='"
                + tag
                + '\''
                + ", keys='"
                + keys
                + '\''
                + ", shardingKey='"
                + shardingKey
                + '\''
                + ", messageId="
                + messageId
                + ", bornTimestamp="
                + bornTimestamp
                + ", reconsumeTimes="
                + reconsumeTimes
                + ", transactionId='"
                + transactionId
                + '\''
                + ", delayLevel="
                + delayLevel
                + ", delayTimeMillis="
                + delayTimeMillis
                + ", body="
                + (Objects.isNull(body) ? "null" : body.getClass().getSimpleName())
                + ", properties.size="
                + properties.size()
                + ", userProperties.size="
                + userProperties.size()
                + '}';
    }
}
