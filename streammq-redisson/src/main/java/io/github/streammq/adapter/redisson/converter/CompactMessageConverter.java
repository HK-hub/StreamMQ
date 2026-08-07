package io.github.streammq.adapter.redisson.converter;

import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 紧凑型消息转换器，使用短字段名减少 Redis Stream 存储开销。
 *
 * <p>与 {@link DefaultMessageConverter}（长字段名）相比，本实现的特点：
 * <ul>
 *   <li>使用单字母/双字母字段名（{@code b} / {@code g} / {@code k} 等），降低每条 Entry 的存储体积</li>
 *   <li>适用于海量消息、对存储成本敏感的场景</li>
 *   <li>系统属性与用户属性<b>分别存储</b>为独立的 JSON 字段（{@code p} / {@code up}）</li>
 *   <li>支持延迟字段（{@code dl} / {@code dm}）写入 Stream Entry</li>
 *   <li>Topic 字段直接写入 Stream Entry（{@code t}），而非仅由 Stream Key 表示</li>
 *   <li><b>不支持消息体压缩</b>（无 {@code compressed} 字段处理）</li>
 * </ul>
 *
 * <h3>Stream Entry 字段映射</h3>
 * <table>
 *   <tr><th>字段名</th><th>常量</th><th>说明</th></tr>
 *   <tr><td>{@code t}</td><td>{@link #FIELD_TOPIC}</td><td>主题</td></tr>
 *   <tr><td>{@code g}</td><td>{@link #FIELD_TAG}</td><td>标签</td></tr>
 *   <tr><td>{@code k}</td><td>{@link #FIELD_KEYS}</td><td>业务键</td></tr>
 *   <tr><td>{@code s}</td><td>{@link #FIELD_SHARDING_KEY}</td><td>分片键</td></tr>
 *   <tr><td>{@code b}</td><td>{@link #FIELD_BODY}</td><td>消息体（Base64）</td></tr>
 *   <tr><td>{@code bt}</td><td>{@link #FIELD_BODY_TYPE}</td><td>body 类型全限定名</td></tr>
 *   <tr><td>{@code ts}</td><td>{@link #FIELD_BORN_TS}</td><td>出生时间戳（毫秒）</td></tr>
 *   <tr><td>{@code h}</td><td>{@link #FIELD_BORN_HOST}</td><td>出生主机</td></tr>
 *   <tr><td>{@code p}</td><td>{@link #FIELD_PROPS}</td><td>系统属性 JSON</td></tr>
 *   <tr><td>{@code up}</td><td>{@link #FIELD_USER_PROPS}</td><td>用户属性 JSON</td></tr>
 *   <tr><td>{@code dl}</td><td>{@link #FIELD_DELAY_LEVEL}</td><td>延时级别枚举名</td></tr>
 *   <tr><td>{@code dm}</td><td>{@link #FIELD_DELAY_TIME_MILLIS}</td><td>自定义延时毫秒数</td></tr>
 *   <tr><td>{@code tx}</td><td>{@link #FIELD_TX_ID}</td><td>事务 ID</td></tr>
 * </table>
 *
 * <p>反序列化 body 时支持两种来源：SDK 发送方（{@code bt} 非空）和跨平台发送方（{@code bt} 缺失）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class CompactMessageConverter extends AbstractMessageConverter {

    // ================================================================
    // 字段名常量
    // ================================================================

    /** 紧凑字段名：主题 */
    public static final String FIELD_TOPIC = "t";
    /** 紧凑字段名：标签 */
    public static final String FIELD_TAG = "g";
    /** 紧凑字段名：业务键 */
    public static final String FIELD_KEYS = "k";
    /** 紧凑字段名：分片键 */
    public static final String FIELD_SHARDING_KEY = "s";
    /** 紧凑字段名：消息体（Base64 编码） */
    public static final String FIELD_BODY = "b";
    /** 紧凑字段名：body 类型全限定名 */
    public static final String FIELD_BODY_TYPE = "bt";
    /** 紧凑字段名：出生时间戳（毫秒） */
    public static final String FIELD_BORN_TS = "ts";
    /** 紧凑字段名：出生主机 */
    public static final String FIELD_BORN_HOST = "h";
    /** 紧凑字段名：系统属性 JSON */
    public static final String FIELD_PROPS = "p";
    /** 紧凑字段名：用户属性 JSON */
    public static final String FIELD_USER_PROPS = "up";
    /** 紧凑字段名：延时级别枚举名 */
    public static final String FIELD_DELAY_LEVEL = "dl";
    /** 紧凑字段名：自定义延时毫秒数 */
    public static final String FIELD_DELAY_TIME_MILLIS = "dm";
    /** 紧凑字段名：事务 ID */
    public static final String FIELD_TX_ID = "tx";

    /** {@inheritDoc} */ @Override protected String fieldBody() { return FIELD_BODY; }
    /** {@inheritDoc} */ @Override protected String fieldBodyType() { return FIELD_BODY_TYPE; }
    /** {@inheritDoc} */ @Override protected String fieldTag() { return FIELD_TAG; }
    /** {@inheritDoc} */ @Override protected String fieldKeys() { return FIELD_KEYS; }
    /** {@inheritDoc} */ @Override protected String fieldShardingKey() { return FIELD_SHARDING_KEY; }
    /** {@inheritDoc} */ @Override protected String fieldBornTs() { return FIELD_BORN_TS; }
    /** {@inheritDoc} */ @Override protected String fieldBornHost() { return FIELD_BORN_HOST; }
    /** {@inheritDoc} */ @Override protected String fieldTxId() { return FIELD_TX_ID; }
    /** {@inheritDoc} */ @Override protected String fieldTopic() { return FIELD_TOPIC; }

    /** 消息体序列化器 */
    private final MessageSerializer<Object> serializer;

    /**
     * 构造紧凑转换器。
     *
     * @param serializer body 序列化器，不能为 null
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public CompactMessageConverter(MessageSerializer<?> serializer) {
        this.serializer = (MessageSerializer<Object>) Objects.requireNonNull(serializer, "serializer");
    }

    // ================================================================
    // Body 编解码
    // ================================================================

    /**
     * 将消息体序列化为 byte[] → Base64 编码 → 写入字段。
     *
     * @param message 消息载体
     * @param fields  输出 Map
     */
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected void encodeBody(Message<?> message, Map<String, String> fields) {
        Object body = message.getBody();
        if (Objects.isNull(body)) { return; }
        byte[] bodyBytes = serializer.serialize(body, (Class<Object>) (Class) body.getClass());
        fields.put(FIELD_BODY, Base64.getEncoder().encodeToString(bodyBytes));
        fields.put(FIELD_BODY_TYPE, body.getClass().getName());
    }

    /**
     * 从 Stream Entry 字段解码消息体。
     *
     * <p>支持 SDK 发送方（{@code bt} 非空 → Base64 解码 → 反序列化）
     * 和跨平台发送方（{@code bt} 缺失 → 原始字符串处理）。
     * <b>不支持压缩解压。</b>
     *
     * @param fields     Stream Entry 全部字段
     * @param targetType 目标 body 类型
     * @param message    输出消息
     * @param bodyStr    body 字段原始值
     */
    @Override
    @SuppressWarnings("unchecked")
    protected <T> void decodeBody(Map<String, String> fields, Class<T> targetType, Message<T> message, String bodyStr) {
        String bodyTypeField = fields.get(FIELD_BODY_TYPE);
        if (StringUtils.isNotEmpty(bodyTypeField)) {
            message.setBody(serializer.deserialize(Base64.getDecoder().decode(bodyStr), targetType));
            return;
        }
        if (targetType == String.class) {
            message.setBody((T) bodyStr);
            return;
        }
        message.setBody(serializer.deserialize(bodyStr.getBytes(StandardCharsets.UTF_8), targetType));
    }

    // ================================================================
    // Properties 编解码 —— 系统属性与用户属性分别存储
    // ================================================================

    /**
     * 将系统属性（{@code p}）和用户属性（{@code up}）分别序列化为独立的 JSON 字段。
     *
     * <p>与 Default 的合并存储不同，此处各自独立，消费端可分别还原。
     *
     * @param message 消息载体
     * @param fields  输出 Map
     */
    @Override
    protected void encodeProperties(Message<?> message, Map<String, String> fields) {
        Map<String, String> sysProps = message.getProperties();
        if (!sysProps.isEmpty()) { writePropsJson(fields, FIELD_PROPS, sysProps, Collections.emptyMap()); }
        Map<String, String> userProps = message.getUserProperties();
        if (!userProps.isEmpty()) { writePropsJson(fields, FIELD_USER_PROPS, Collections.emptyMap(), userProps); }
    }

    /**
     * 从两个独立 JSON 字段分别还原系统属性和用户属性。
     *
     * @param message 输出消息
     * @param fields  Stream Entry 全部字段
     */
    @Override
    protected <T> void decodeProperties(Message<T> message, Map<String, String> fields) {
        readPropsJson(fields, FIELD_PROPS, message::setProperties);
        readPropsJson(fields, FIELD_USER_PROPS, message::setUserProperties);
    }

    // ================================================================
    // Extra —— 延迟字段（仅 Compact 支持）
    // ================================================================

    /**
     * 写入延迟字段：{@code dl}（延时级别枚举名）和 {@code dm}（自定义延时毫秒数）。
     *
     * @param message 消息载体
     * @param fields  输出 Map
     */
    @Override
    protected void encodeExtra(Message<?> message, Map<String, String> fields) {
        if (Objects.nonNull(message.getDelayLevel())) {
            fields.put(FIELD_DELAY_LEVEL, message.getDelayLevel().name());
        }
        if (Objects.nonNull(message.getDelayTimeMillis())) {
            fields.put(FIELD_DELAY_TIME_MILLIS, Long.toString(message.getDelayTimeMillis()));
        }
    }

    /**
     * 读取延迟字段：将枚举名字符串还原为 {@link DelayLevel}，字符串解析为 long。
     *
     * @param message 输出消息
     * @param fields  Stream Entry 全部字段
     */
    @Override
    protected <T> void decodeExtra(Message<T> message, Map<String, String> fields) {
        String delayLevelStr = fields.get(FIELD_DELAY_LEVEL);
        if (StringUtils.isNotEmpty(delayLevelStr)) {
            try {
                message.setDelayLevel(DelayLevel.valueOf(delayLevelStr));
            } catch (IllegalArgumentException ex) {
                throw new SerializationException("Failed to parse delayLevel: " + delayLevelStr, ex);
            }
        }
        String delayTimeMillisStr = fields.get(FIELD_DELAY_TIME_MILLIS);
        if (StringUtils.isNotEmpty(delayTimeMillisStr)) {
            try {
                message.setDelayTimeMillis(Long.parseLong(delayTimeMillisStr));
            } catch (NumberFormatException ex) {
                throw new SerializationException("Failed to parse delayTimeMillis: " + delayTimeMillisStr, ex);
            }
        }
    }

    /**
     * @return {@code "compact"}
     */
    @Override
    public String name() { return "compact"; }

    // ================================================================
    // 静态工具
    // ================================================================

    /**
     * 为消费端还原的消息回填 topic 字段。
     *
     * @param message 消息载体
     * @param topic   主题名
     * @param <T>     body 类型
     */
    public static <T> void applyTopic(Message<T> message, String topic) {
        message.setTopic(topic);
    }

    /**
     * 为消费端还原的消息回填 messageId 字段。
     *
     * @param message       消息载体
     * @param streamEntryId Redis Stream Entry ID
     * @param <T>           body 类型
     */
    public static <T> void applyMessageId(Message<T> message, String streamEntryId) {
        message.setMessageId(MessageId.fromStreamEntry(streamEntryId));
    }
}
