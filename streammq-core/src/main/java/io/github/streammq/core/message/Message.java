/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.message;

import io.github.streammq.core.StreamMQConstants;
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

    /**
     * 系统属性（防御性拷贝），框架使用，例如 traceId。getter 返回不可修改视图。
     *
     * <p><b>null 契约（0.1.2 统一）：</b>传入的 Map 为 {@code null} 时视为空 Map；Map 中任一 key/value 为 {@code null}
     * 立即抛 {@link NullPointerException}（与 {@code MessageBuilder} / {@link MessageMetadataBuilder}
     * 同口径）。
     */
    private final Map<String, String> properties;

    /**
     * 用户属性（防御性拷贝），用户自定义透传。getter 返回不可修改视图。
     *
     * <p><b>null 契约（0.1.2 统一）：</b>传入的 Map 为 {@code null} 时视为空 Map；Map 中任一 key/value 为 {@code null}
     * 立即抛 {@link NullPointerException}（与 {@code MessageBuilder} / {@link MessageMetadataBuilder}
     * 同口径）。
     */
    private final Map<String, String> userProperties;

    /**
     * 消息体，由序列化器决定如何转 byte[]。
     *
     * <p><b>可为 null（有意设计）：</b>{@code null} body 表示"无载荷消息"——序列化器对 {@code null} 返回 {@code
     * null}（见各内置序列化器的统一空值语义），反序列化侧亦可能得到 null body（对端发了一条无载荷 消息）。框架与消费者<b>必须容忍</b> null
     * body（如仅用于过滤/控制信号的 Topic）。
     *
     * <p>与之相对，<b>发送侧</b>的 {@link MessageBuilder#build()} 要求 body 非 null：普通业务消息缺少载荷
     * 通常是调用方笔误，应在构造期快速失败。因此"必填"约束属于<b>发送 API</b>，不属于 {@code Message} 值对象本身。
     */
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
     * @param delayTimeMillis 自定义延时毫秒数，可为 null（未设置）；非 null 时必须 &gt; 0 且 &le; 7 天
     * @param bornTimestamp 出生时间戳
     * @param bornHost 出生主机
     * @param transactionId 事务 ID
     * @param reconsumeTimes 已重试消费次数
     * @throws IllegalArgumentException 如果 delayTimeMillis 不在 {@code (0, 7 天]} 区间内（0.1.2 起校验）
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
        // 与 MessageBuilder 同规则校验：非 null、非空、不含 ':' '*' '{' '}' 或空白，并 trim 规范化
        this.topic = StringUtils.requireValidTopic(topic);
        this.tag = tag;
        this.keys = keys;
        this.shardingKey = shardingKey;
        this.properties = copyProperties(properties, false, "property");
        this.userProperties = copyProperties(userProperties, true, "userProperty");
        this.body = body;
        this.delayLevel = delayLevel;
        this.delayTimeMillis = requireValidDelayTimeMillis(delayTimeMillis);
        this.messageId = messageId;
        this.bornTimestamp = bornTimestamp;
        this.bornHost = bornHost;
        this.transactionId = transactionId;
        this.reconsumeTimes = reconsumeTimes;
    }

    /**
     * 拷贝属性 Map 并统一 null 契约（0.1.2 定稿）：{@code null} Map 视为空 Map； Map 中<b>任一 key/value 为 null 立即抛
     * {@link NullPointerException}</b>（与 {@link MessageMetadataBuilder#property(String, String)}
     * 同口径， 不把非法值推迟到属性快照/序列化时才暴露）。
     *
     * @param source 源 Map，可为 null
     * @param linked true 保留插入顺序（userProperties），false 使用普通 HashMap（properties）
     * @param field 字段前缀（用于异常信息）
     * @return 防御性拷贝（可修改副本；对外由 getter 返回不可修改视图）
     * @throws NullPointerException 如果任一 key 或 value 为 null
     */
    private static Map<String, String> copyProperties(
            Map<String, String> source, boolean linked, String field) {
        Map<String, String> copy = linked ? new LinkedHashMap<>() : new HashMap<>();
        if (Objects.isNull(source)) {
            return copy;
        }
        for (Map.Entry<String, String> entry : source.entrySet()) {
            copy.put(
                    Objects.requireNonNull(entry.getKey(), field + " key"),
                    Objects.requireNonNull(entry.getValue(), field + " value"));
        }
        return copy;
    }

    /**
     * 校验自定义延时毫秒数（构造期统一不变量）：{@code null} 表示未设置；非 null 时必须 {@code > 0} 且 {@code <= }{@link
     * StreamMQConstants#MAX_DELAY_TIME_MILLIS}（7 天）。
     *
     * <p><b>为什么收口在构造器（0.1.2）：</b>此前全参构造器与 {@link #withDelayTimeMillis(Long)} 都不校验， 而 {@code
     * MessageBuilder}/{@code MessageMetadataBuilder} 只校验 {@code > 0}，同一条消息经不同构造路径
     * 会得到不同结果（非法值直到发送时才在适配层失败）。
     *
     * @param delayTimeMillis 延时毫秒数，可为 null
     * @return 原值（null 原样返回）
     * @throws IllegalArgumentException 如果取值不在 {@code (0, 7 天]} 区间内
     */
    private static Long requireValidDelayTimeMillis(Long delayTimeMillis) {
        if (Objects.isNull(delayTimeMillis)) {
            return null;
        }
        if (delayTimeMillis <= 0) {
            throw new IllegalArgumentException(
                    "delayTimeMillis must be > 0, got: " + delayTimeMillis);
        }
        if (delayTimeMillis > StreamMQConstants.MAX_DELAY_TIME_MILLIS) {
            throw new IllegalArgumentException(
                    "delayTimeMillis must be <= "
                            + StreamMQConstants.MAX_DELAY_TIME_MILLIS
                            + " (7 days), got: "
                            + delayTimeMillis);
        }
        return delayTimeMillis;
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
     * <p>校验规则与构造器一致（非 null、非空、不含 {@code ':'}、{@code '*'}、{@code '{'}、{@code '}'} 或空白）， 由 {@code
     * Message} 私有全参构造器统一执行。
     *
     * @param topic 新的 topic
     * @return 新的 Message 实例
     */
    public Message<T> withTopic(String topic) {
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
     * <p>校验与全参构造器一致（0.1.2 起）：{@code null} 表示清除延时设置；非 null 时必须 {@code > 0} 且 {@code <=} 7 天。
     *
     * @param delayTimeMillis 新的延时毫秒数，可为 null（清除延时）
     * @return 新的 Message 实例
     * @throws IllegalArgumentException 如果 delayTimeMillis 不在 {@code (0, 7 天]} 区间内
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
     * 返回带有指定系统属性的新 Message 实例（<b>替换</b>现有属性）。
     *
     * <p><b>null 契约（0.1.2 统一）：</b>{@code properties} 为 {@code null} 时替换为空 Map；Map 中任一 key/value 为
     * {@code null} 立即抛 {@link NullPointerException}（与 {@link #addProperty(String, String)} 和 {@code
     * MessageMetadataBuilder} 同口径）。
     *
     * @param properties 新的系统属性，可为 null（表示空属性）
     * @return 新的 Message 实例
     * @throws NullPointerException 如果任一 key 或 value 为 null
     */
    public Message<T> withProperties(Map<String, String> properties) {
        return derive(
                tag,
                keys,
                shardingKey,
                copyProperties(properties, true, "property"),
                userProperties,
                body);
    }

    /**
     * 返回添加了指定系统属性的新 Message 实例。
     *
     * @param key 属性键
     * @param value 属性值
     * @return 新的 Message 实例
     * @throws NullPointerException 如果 key 或 value 为 null（null 键/null 值非法，立即失败）
     */
    public Message<T> addProperty(String key, String value) {
        Objects.requireNonNull(key, "property key");
        Objects.requireNonNull(value, "property value");
        Map<String, String> copied = new HashMap<>(this.properties);
        copied.put(key, value);
        return derive(tag, keys, shardingKey, copied, userProperties, body);
    }

    /**
     * 返回带有指定用户属性的新 Message 实例（<b>替换</b>现有属性）。
     *
     * <p><b>null 契约（0.1.2 统一）：</b>{@code userProperties} 为 {@code null} 时替换为空 Map；Map 中任一 key/value
     * 为 {@code null} 立即抛 {@link NullPointerException}（与 {@link #addUserProperty(String, String)} 和
     * {@code MessageMetadataBuilder} 同口径）。
     *
     * @param userProperties 新的用户属性，可为 null（表示空属性）
     * @return 新的 Message 实例
     * @throws NullPointerException 如果任一 key 或 value 为 null
     */
    public Message<T> withUserProperties(Map<String, String> userProperties) {
        return derive(
                tag,
                keys,
                shardingKey,
                properties,
                copyProperties(userProperties, true, "userProperty"),
                body);
    }

    /**
     * 返回添加了指定用户属性的新 Message 实例。
     *
     * @param key 属性键
     * @param value 属性值
     * @return 新的 Message 实例
     * @throws NullPointerException 如果 key 或 value 为 null（null 键/null 值非法，立即失败）
     */
    public Message<T> addUserProperty(String key, String value) {
        Objects.requireNonNull(key, "userProperty key");
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

    /**
     * 值相等语义（不可变值对象契约）：
     *
     * <ul>
     *   <li>已分配 messageId 的消息：按 {@code (topic, messageId)} 比较，与发送前后身份一致
     *   <li>发送前 messageId 为 null 的消息：退化为基于<b>用户可感知语义</b>的值比较——{@code
     *       topic/tag/keys/shardingKey/body/delayLevel/delayTimeMillis/properties/userProperties/transactionId}
     *       （{@code properties} 为内容比较），保证两个内容相同但尚未获得 ID 的消息被判定为相等
     *   <li><b>运行时元数据不参与比较</b>：{@code bornTimestamp} / {@code bornHost} / {@code reconsumeTimes}
     *       由框架或 Builder 自动填充，同一内容的两次构造必然不同，参与比较会让"内容相同即相等"不可达
     *   <li>已分配 ID 与未分配 ID 的消息始终不等（身份不同）
     * </ul>
     *
     * <p>本方法与 {@link #hashCode()} 的字段集合严格一致；<b>同一内容 + 不同出生元数据的消息可安全用于 {@code Set}/{@code Map}
     * 去重</b>。
     *
     * @param o 比较对象
     * @return true 如果语义上相等
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Message<?> other)) {
            return false;
        }
        // 身份是否分配 ID 不同 → 必然不等
        if (Objects.isNull(messageId) != Objects.isNull(other.messageId)) {
            return false;
        }
        if (Objects.nonNull(messageId)) {
            return topic.equals(other.topic) && messageId.equals(other.messageId);
        }
        // 两者 messageId 均为 null：基于用户可感知语义的值比较（不含运行时元数据）
        return Objects.equals(topic, other.topic)
                && Objects.equals(tag, other.tag)
                && Objects.equals(keys, other.keys)
                && Objects.equals(shardingKey, other.shardingKey)
                && Objects.equals(body, other.body)
                && Objects.equals(delayLevel, other.delayLevel)
                && Objects.equals(delayTimeMillis, other.delayTimeMillis)
                && Objects.equals(properties, other.properties)
                && Objects.equals(userProperties, other.userProperties)
                && Objects.equals(transactionId, other.transactionId);
    }

    /**
     * 与 {@link #equals} 一致：messageId 非 null 时为 {@code hash(topic, messageId)}， 否则为 {@code
     * hash(topic, tag, keys, shardingKey, body, delayLevel, delayTimeMillis, properties,
     * userProperties, transactionId)}（不含 bornTimestamp/bornHost/reconsumeTimes 等运行时元数据）。
     *
     * @return 哈希值
     */
    @Override
    public int hashCode() {
        if (Objects.nonNull(messageId)) {
            return Objects.hash(topic, messageId);
        }
        return Objects.hash(
                topic,
                tag,
                keys,
                shardingKey,
                body,
                delayLevel,
                delayTimeMillis,
                properties,
                userProperties,
                transactionId);
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
