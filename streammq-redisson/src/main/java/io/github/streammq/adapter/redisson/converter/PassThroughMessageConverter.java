/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.converter;

import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import java.util.*;

/**
 * 直通消息转换器，body 不经过序列化器，直接以字符串形式存取。
 *
 * <p>与 {@link DefaultMessageConverter}（序列化 + Base64 + 压缩）相比，本实现的特点：
 *
 * <ul>
 *   <li>Body 支持 String/CharSequence（原样存取）与 {@code byte[]}（Base64 存取）；其它类型 fail-fast
 *   <li>不依赖 {@link io.github.streammq.core.serializer.MessageSerializer}，无序列化开销
 *   <li>适用于 body 已经是可读字符串的场景（如 JSON 文本）
 *   <li>系统属性与用户属性合并存储为单个 {@code props} JSON 字段（与 Default 一致）
 *   <li><b>不支持消息体压缩</b>
 * </ul>
 *
 * <h3>Stream Entry 字段映射</h3>
 *
 * <table>
 *   <tr><th>字段名</th><th>常量</th><th>说明</th></tr>
 *   <tr><td>{@code body}</td><td>{@link #FIELD_BODY}</td><td>消息体（原始字符串）</td></tr>
 *   <tr><td>{@code bodyType}</td><td>{@link #FIELD_BODY_TYPE}</td><td>body 类型全限定名</td></tr>
 *   <tr><td>{@code tag}</td><td>{@link #FIELD_TAG}</td><td>标签</td></tr>
 *   <tr><td>{@code keys}</td><td>{@link #FIELD_KEYS}</td><td>业务键</td></tr>
 *   <tr><td>{@code shardingKey}</td><td>{@link #FIELD_SHARDING_KEY}</td><td>分片键</td></tr>
 *   <tr><td>{@code props}</td><td>{@link #FIELD_PROPS}</td><td>属性 JSON（sys + user 合并）</td></tr>
 *   <tr><td>{@code bornTs}</td><td>{@link #FIELD_BORN_TS}</td><td>出生时间戳（毫秒）</td></tr>
 *   <tr><td>{@code bornHost}</td><td>{@link #FIELD_BORN_HOST}</td><td>出生主机</td></tr>
 *   <tr><td>{@code retryTimes}</td><td>{@link #FIELD_RETRY_TIMES}</td><td>重试次数</td></tr>
 *   <tr><td>{@code txId}</td><td>{@link #FIELD_TX_ID}</td><td>事务 ID</td></tr>
 * </table>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class PassThroughMessageConverter extends AbstractMessageConverter {

    // ================================================================
    // 字段名常量（统一委托到 {@link MessageFields}，保证各转换器协议一致）
    // ================================================================

    /** Stream Entry 字段名：消息体（原始字符串） */
    public static final String FIELD_BODY = MessageFields.BODY;

    /** Stream Entry 字段名：消息体类型全限定名 */
    public static final String FIELD_BODY_TYPE = MessageFields.BODY_TYPE;

    /** Stream Entry 字段名：标签 */
    public static final String FIELD_TAG = MessageFields.TAG;

    /** Stream Entry 字段名：业务键 */
    public static final String FIELD_KEYS = MessageFields.KEYS;

    /** Stream Entry 字段名：分片键 */
    public static final String FIELD_SHARDING_KEY = MessageFields.SHARDING_KEY;

    /** Stream Entry 字段名：属性 JSON（sys + user 合并） */
    public static final String FIELD_PROPS = MessageFields.PROPS;

    /** Stream Entry 字段名：出生时间戳（毫秒） */
    public static final String FIELD_BORN_TS = MessageFields.BORN_TS;

    /** Stream Entry 字段名：出生主机 */
    public static final String FIELD_BORN_HOST = MessageFields.BORN_HOST;

    /** Stream Entry 字段名：事务 ID */
    public static final String FIELD_TX_ID = MessageFields.TX_ID;

    /** Stream Entry 字段名：重试次数 */
    public static final String FIELD_RETRY_TIMES = MessageFields.RETRY_TIMES;

    /** {@inheritDoc} */
    @Override
    protected String fieldBody() {
        return FIELD_BODY;
    }

    /** {@inheritDoc} */
    @Override
    protected String fieldBodyType() {
        return FIELD_BODY_TYPE;
    }

    /** {@inheritDoc} */
    @Override
    protected String fieldTag() {
        return FIELD_TAG;
    }

    /** {@inheritDoc} */
    @Override
    protected String fieldKeys() {
        return FIELD_KEYS;
    }

    /** {@inheritDoc} */
    @Override
    protected String fieldShardingKey() {
        return FIELD_SHARDING_KEY;
    }

    /** {@inheritDoc} */
    @Override
    protected String fieldBornTs() {
        return FIELD_BORN_TS;
    }

    /** {@inheritDoc} */
    @Override
    protected String fieldBornHost() {
        return FIELD_BORN_HOST;
    }

    /** {@inheritDoc} */
    @Override
    protected String fieldRetryTimes() {
        return FIELD_RETRY_TIMES;
    }

    /** {@inheritDoc} */
    @Override
    protected String fieldTxId() {
        return FIELD_TX_ID;
    }

    /** 无参构造。 */
    public PassThroughMessageConverter() {}

    // ================================================================
    // Body 编解码 —— toString / 直接 String 赋值
    // ================================================================

    /**
     * 将消息体转换为字符串写入字段。
     *
     * <p>支持类型：{@link String} / {@link CharSequence}（原样写入）、{@code byte[]}（Base64
     * 编码写入，解码时还原）。其它类型直接抛出 {@link io.github.streammq.core.exception.SerializationException} ——
     * 此前实现回退到 {@code toString()}，会把 {@code byte[]} 写成 {@code "[B@1f2a3b4c"} 之类的地址串，造成无报错的静默数据损毁。
     *
     * <p>同时写入 {@code bodyType} 字段以记录原始类型全限定名。 body 为 null 时不做任何写入。
     *
     * @param message 消息载体
     * @param fields 输出 Map
     */
    @Override
    protected void encodeBody(Message<?> message, Map<String, String> fields) {
        Object body = message.getBody();
        if (Objects.isNull(body)) {
            return;
        }
        if (body instanceof byte[] bytes) {
            fields.put(FIELD_BODY, Base64.getEncoder().encodeToString(bytes));
            fields.put(FIELD_BODY_TYPE, byte[].class.getName());
            return;
        }
        if (body instanceof CharSequence chars) {
            fields.put(FIELD_BODY, chars.toString());
            fields.put(FIELD_BODY_TYPE, body.getClass().getName());
            return;
        }
        throw new SerializationException(
                "PassThroughMessageConverter only supports String/CharSequence/byte[] bodies,"
                        + " got "
                        + body.getClass().getName()
                        + ". Use DefaultMessageConverter or CompactMessageConverter for POJOs.");
    }

    /**
     * 从字段中读取字符串并赋值为 body。
     *
     * <p>编码端为 {@code byte[]}（{@code bodyType} 为 {@code [B}）时 Base64 还原为字节数组； 其余情况直接作为 String
     * 赋值，不做进一步反序列化。声明其它目标类型的监听器将在消费侧 收到 String（与直通语义一致）。
     *
     * @param fields Stream Entry 全部字段
     * @param targetType 目标 body 类型
     * @param draft 输出草稿
     * @param bodyStr body 字段原始字符串值
     */
    @Override
    @SuppressWarnings("unchecked")
    protected <T> void decodeBody(
            Map<String, String> fields,
            Class<T> targetType,
            MessageDraft<T> draft,
            String bodyStr) {
        String bodyType = fields.get(FIELD_BODY_TYPE);
        if (byte[].class.getName().equals(bodyType)) {
            draft.body = (T) Base64.getDecoder().decode(bodyStr);
            return;
        }
        draft.body = (T) bodyStr;
    }

    // ================================================================
    // Properties 编解码 —— sys + user 合并为单个 JSON
    // ================================================================

    /**
     * 将系统属性和用户属性合并序列化为单个 JSON 字段。
     *
     * @param message 消息载体
     * @param fields 输出 Map
     */
    @Override
    protected void encodeProperties(Message<?> message, Map<String, String> fields) {
        PropsJsonCodec.write(
                fields, FIELD_PROPS, message.getProperties(), message.getUserProperties());
    }

    /**
     * 从单个 JSON 字段反序列化属性并写入草稿用户属性。
     *
     * @param draft 装配草稿
     * @param fields Stream Entry 全部字段
     */
    @Override
    protected <T> void decodeProperties(MessageDraft<T> draft, Map<String, String> fields) {
        PropsJsonCodec.read(fields, FIELD_PROPS, draft.userProperties::putAll);
    }

    /**
     * @return {@code "pass-through"}
     */
    @Override
    public String name() {
        return "pass-through";
    }

    // ================================================================
    // 静态工具
    // ================================================================

    /**
     * 为消费端还原的消息派生携带指定 Topic 的不可变新实例。
     *
     * @param message 原始消息
     * @param topic 主题名
     * @param <T> body 类型
     * @return Topic 已设置的不可变新实例
     */
    public static <T> Message<T> applyTopic(Message<T> message, String topic) {
        return message.withTopic(topic);
    }

    /**
     * 为消费端还原的消息派生携带 {@link MessageId} 的不可变新实例。
     *
     * @param message 原始消息
     * @param streamEntryId Redis Stream Entry ID
     * @param <T> body 类型
     * @return messageId 已设置的不可变新实例
     */
    public static <T> Message<T> applyMessageId(Message<T> message, String streamEntryId) {
        return message.withMessageId(MessageId.fromStreamEntry(streamEntryId));
    }
}
