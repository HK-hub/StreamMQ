package io.github.streammq.adapter.redisson.converter;

import io.github.streammq.core.compression.CompressionCodec;
import io.github.streammq.core.compression.CompressionCodecRegistry;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.util.StringUtils;
import lombok.Setter;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 默认 {@link AbstractMessageConverter} 实现，使用完整字段名连接 {@link Message} 与 Redis Stream Entry。
 *
 * <p>与 {@link CompactMessageConverter}（短字段名）和 {@link PassThroughMessageConverter}（直通）
 * 相比，本实现的特点：
 * <ul>
 *   <li>使用可读性强的长字段名（{@code body} / {@code tag} / {@code keys} 等）</li>
 *   <li>Body 通过注入的 {@link MessageSerializer} 序列化后 Base64 编码存储</li>
 *   <li>支持消息体压缩（GZIP 等），压缩算法名称写入 {@code compressed} 字段</li>
 *   <li>系统属性与用户属性合并存储为单个 {@code props} JSON 字段</li>
 *   <li>反序列化时支持两种来源：SDK 发送方（有 bodyType）和跨平台发送方（无 bodyType）</li>
 * </ul>
 *
 * <h3>Stream Entry 字段映射</h3>
 * <table>
 *   <tr><th>字段名</th><th>常量</th><th>类型</th><th>说明</th></tr>
 *   <tr><td>{@code body}</td><td>{@link #FIELD_BODY}</td><td>Base64</td><td>序列化后的消息体</td></tr>
 *   <tr><td>{@code bodyType}</td><td>{@link #FIELD_BODY_TYPE}</td><td>String</td><td>body 类全限定名</td></tr>
 *   <tr><td>{@code bodyTypeName}</td><td>{@link #FIELD_BODY_TYPE_NAME}</td><td>String</td><td>body 类简称</td></tr>
 *   <tr><td>{@code compressed}</td><td>{@link #FIELD_COMPRESSED}</td><td>String</td><td>压缩算法名或 "true"（旧格式）</td></tr>
 *   <tr><td>{@code tag}</td><td>{@link #FIELD_TAG}</td><td>String</td><td>标签（可选）</td></tr>
 *   <tr><td>{@code keys}</td><td>{@link #FIELD_KEYS}</td><td>String</td><td>业务键（可选）</td></tr>
 *   <tr><td>{@code shardingKey}</td><td>{@link #FIELD_SHARDING_KEY}</td><td>String</td><td>分片键（可选）</td></tr>
 *   <tr><td>{@code props}</td><td>{@link #FIELD_PROPS}</td><td>JSON</td><td>系统属性 + 用户属性合并</td></tr>
 *   <tr><td>{@code bornTs}</td><td>{@link #FIELD_BORN_TS}</td><td>long</td><td>出生时间戳（毫秒）</td></tr>
 *   <tr><td>{@code bornHost}</td><td>{@link #FIELD_BORN_HOST}</td><td>String</td><td>出生主机（可选）</td></tr>
 *   <tr><td>{@code retryTimes}</td><td>{@link #FIELD_RETRY_TIMES}</td><td>int</td><td>已重试次数（可选）</td></tr>
 *   <tr><td>{@code txId}</td><td>{@link #FIELD_TX_ID}</td><td>String</td><td>事务 ID（可选）</td></tr>
 *   <tr><td>{@code originTopic}</td><td>{@link #FIELD_ORIGIN_TOPIC}</td><td>String</td><td>原始 Topic（可选，重试/DLQ 场景）</td></tr>
 * </table>
 *
 * <p>{@code topic} 不存入 Stream Entry，由 Stream Key 本身表示。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultMessageConverter extends AbstractMessageConverter {

    // ================================================================
    // 字段名常量（public 供外部如 RedissonStreamProducer / RedissonStreamListener 引用）
    // ================================================================

    /** Stream Entry 字段名：消息体（Base64 编码） */
    public static final String FIELD_BODY = "body";
    /** Stream Entry 字段名：消息体类型全限定名 */
    public static final String FIELD_BODY_TYPE = "bodyType";
    /** Stream Entry 字段名：消息体类型简称 */
    public static final String FIELD_BODY_TYPE_NAME = "bodyTypeName";
    /** Stream Entry 字段名：标签 */
    public static final String FIELD_TAG = "tag";
    /** Stream Entry 字段名：业务键 */
    public static final String FIELD_KEYS = "keys";
    /** Stream Entry 字段名：分片键 */
    public static final String FIELD_SHARDING_KEY = "shardingKey";
    /** Stream Entry 字段名：属性 JSON（sys + user 合并） */
    public static final String FIELD_PROPS = "props";
    /** Stream Entry 字段名：出生时间戳（毫秒） */
    public static final String FIELD_BORN_TS = "bornTs";
    /** Stream Entry 字段名：出生主机 */
    public static final String FIELD_BORN_HOST = "bornHost";
    /** Stream Entry 字段名：重试次数 */
    public static final String FIELD_RETRY_TIMES = "retryTimes";
    /** Stream Entry 字段名：事务 ID */
    public static final String FIELD_TX_ID = "txId";
    /** Stream Entry 字段名：原始 Topic（重试/DLQ 场景） */
    public static final String FIELD_ORIGIN_TOPIC = "originTopic";
    /** Stream Entry 字段名：压缩算法标识（"gzip" 或旧格式 "true"） */
    public static final String FIELD_COMPRESSED = "compressed";

    /** {@inheritDoc} */ @Override protected String fieldBody() { return FIELD_BODY; }
    /** {@inheritDoc} */ @Override protected String fieldBodyType() { return FIELD_BODY_TYPE; }
    /** {@inheritDoc} */ @Override protected String fieldBodyTypeName() { return FIELD_BODY_TYPE_NAME; }
    /** {@inheritDoc} */ @Override protected String fieldTag() { return FIELD_TAG; }
    /** {@inheritDoc} */ @Override protected String fieldKeys() { return FIELD_KEYS; }
    /** {@inheritDoc} */ @Override protected String fieldShardingKey() { return FIELD_SHARDING_KEY; }
    /** {@inheritDoc} */ @Override protected String fieldBornTs() { return FIELD_BORN_TS; }
    /** {@inheritDoc} */ @Override protected String fieldBornHost() { return FIELD_BORN_HOST; }
    /** {@inheritDoc} */ @Override protected String fieldRetryTimes() { return FIELD_RETRY_TIMES; }
    /** {@inheritDoc} */ @Override protected String fieldTxId() { return FIELD_TX_ID; }
    /** {@inheritDoc} */ @Override protected String fieldOriginTopic() { return FIELD_ORIGIN_TOPIC; }

    /** 消息体序列化器 */
    private final MessageSerializer<Object> serializer;

    /**
     * 压缩编解码器（可选注入）。
     *
     * <p>用于解压旧格式 {@code compressed=true} 的消息体。
     * 新格式消息通过 {@link #compressionCodecRegistry} 按名称查找 Codec。
     */
    @Setter
    private CompressionCodec compressionCodec;

    /**
     * 压缩编解码器注册表（可选注入）。
     *
     * <p>用于按名称（如 {@code "gzip"}）查找对应的 {@link CompressionCodec} 实例解压新格式消息。
     */
    @Setter
    private CompressionCodecRegistry compressionCodecRegistry;

    /**
     * 构造默认转换器。
     *
     * @param serializer body 序列化器，不能为 null（通常为 {@code JacksonJsonSerializer}）
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public DefaultMessageConverter(MessageSerializer<?> serializer) {
        this.serializer = (MessageSerializer<Object>) Objects.requireNonNull(serializer, "serializer");
    }

    // ================================================================
    // Body 编解码 —— 序列化 + Base64 + 压缩支持
    // ================================================================

    /**
     * 将消息体序列化为 byte[] → Base64 编码 → 写入 fields。
     *
     * <p>特殊处理：若 body 已是 {@code byte[]}，则跳过序列化直接编码，避免二次编码。
     * 同时写入 {@code bodyType} 和 {@code bodyTypeName} 字段以支持消费端类型定位。
     *
     * @param message 消息载体
     * @param fields  输出 Map（已初始化，直接 put）
     */
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected void encodeBody(Message<?> message, Map<String, String> fields) {
        Object body = message.getBody();
        if (Objects.isNull(body)) { return; }
        Class<?> bodyType = body.getClass();
        byte[] bodyBytes;
        if (body instanceof byte[] bytes) {
            bodyBytes = bytes;
        } else {
            bodyBytes = serializer.serialize(body, (Class<Object>) (Class) bodyType);
        }
        fields.put(FIELD_BODY, Base64.getEncoder().encodeToString(bodyBytes));
        fields.put(FIELD_BODY_TYPE, bodyType.getName());
        fields.put(FIELD_BODY_TYPE_NAME, bodyType.getSimpleName());
    }

    /**
     * 从 Stream Entry 字段解码消息体。
     *
     * <p>支持两种来源路径：
     * <ol>
     *   <li><b>SDK 发送方</b>（{@code bodyType} 非空）：Base64 解码 → 可选的解压 → 反序列化 → 设置 body</li>
     *   <li><b>跨平台发送方</b>（{@code bodyType} 为空）：原始字符串处理 → 可选的解压 → 反序列化或直接 String</li>
     * </ol>
     *
     * @param fields     Stream Entry 全部字段
     * @param targetType 目标 body 类型
     * @param message    输出消息
     * @param bodyStr    body 字段原始值
     */
    @Override
    @SuppressWarnings("unchecked")
    protected <T> void decodeBody(Map<String, String> fields, Class<T> targetType, Message<T> message, String bodyStr) {
        String compressedFlag = fields.get(FIELD_COMPRESSED);
        String bodyTypeField = fields.get(FIELD_BODY_TYPE);
        boolean compressed = StringUtils.isNotEmpty(compressedFlag);

        if (StringUtils.isNotEmpty(bodyTypeField)) {
            byte[] bodyBytes = Base64.getDecoder().decode(bodyStr);
            if (compressed) { bodyBytes = decompressBody(bodyBytes, compressedFlag); }
            if (targetType == byte[].class || targetType == Object.class && "[B".equals(bodyTypeField)) {
                message.setBody((T) bodyBytes);
                return;
            }
            message.setBody(serializer.deserialize(bodyBytes, targetType));
            return;
        }
        if (compressed) {
            byte[] bodyBytes = Base64.getDecoder().decode(bodyStr);
            bodyBytes = decompressBody(bodyBytes, compressedFlag);
            if (targetType == String.class) {
                message.setBody((T) new String(bodyBytes, StandardCharsets.UTF_8));
                return;
            }
            message.setBody(serializer.deserialize(bodyBytes, targetType));
            return;
        }
        if (targetType == String.class) {
            message.setBody((T) bodyStr);
            return;
        }
        message.setBody(serializer.deserialize(bodyStr.getBytes(StandardCharsets.UTF_8), targetType));
    }

    // ================================================================
    // Properties 编解码 —— sys + user 合并为单个 JSON
    // ================================================================

    /**
     * 将系统属性和用户属性合并序列化为单个 JSON 字段写入。
     *
     * @param message 消息载体
     * @param fields  输出 Map
     */
    @Override
    protected void encodeProperties(Message<?> message, Map<String, String> fields) {
        writePropsJson(fields, FIELD_PROPS, message.getProperties(), message.getUserProperties());
    }

    /**
     * 从单个 JSON 字段反序列化属性（sys + user 合并 → 写入 userProperties）。
     *
     * @param message 输出消息
     * @param fields  Stream Entry 全部字段
     */
    @Override
    protected <T> void decodeProperties(Message<T> message, Map<String, String> fields) {
        readPropsJson(fields, FIELD_PROPS, message::setUserProperties);
    }

    // ================================================================
    // 压缩解压 —— 旧格式 ("true") + 新格式 (codec 名称) + 注册表
    // ================================================================

    /**
     * 按压缩标识字符串解压消息体。
     *
     * <p>三种查找路径（优先级从高到低）：
     * <ol>
     *   <li>{@code "true"}（旧格式）→ 使用注入的 {@link #compressionCodec}</li>
     *   <li>Codec 名称（如 {@code "gzip"}）→ 从 {@link #compressionCodecRegistry} 按名称查找</li>
     *   <li>未知标识 → 回退到注入的 {@link #compressionCodec}（兼容无注册表场景）</li>
     * </ol>
     *
     * @param compressedBytes 压缩后的字节
     * @param compressedFlag  {@code compressed} 字段值
     * @return 解压后的原始字节
     * @throws SerializationException 当无法找到 Codec 进行解压时
     */
    private byte[] decompressBody(byte[] compressedBytes, String compressedFlag) {
        if ("true".equals(compressedFlag)) {
            if (Objects.isNull(compressionCodec)) {
                throw new SerializationException(
                    "Message body is marked as compressed (legacy format) but no CompressionCodec is configured", null);
            }
            return compressionCodec.decompress(compressedBytes);
        }
        if (Objects.nonNull(compressionCodecRegistry)) {
            CompressionCodec codec = compressionCodecRegistry.lookup(compressedFlag);
            if (Objects.nonNull(codec)) { return codec.decompress(compressedBytes); }
            throw new SerializationException(
                "Unknown compression codec: '" + compressedFlag + "'. Available: "
                    + compressionCodecRegistry.availableCodecs(), null);
        }
        if (Objects.nonNull(compressionCodec)) { return compressionCodec.decompress(compressedBytes); }
        throw new SerializationException(
            "Message body is compressed with '" + compressedFlag
                + "' but no CompressionCodecRegistry or CompressionCodec is configured", null);
    }

    /**
     * 返回 Converter 标识名称。
     *
     * @return {@code "default"}
     */
    @Override
    public String name() { return "default"; }

    // ================================================================
    // 静态工具方法（向后兼容，供 RedissonStreamListener / TransactionScanner 等调用）
    // ================================================================

    /**
     * 为消费端还原的消息回填 topic 字段。
     *
     * <p><b>向后兼容：</b>此静态方法供外部调用方直接引用，
     * 内部等同于 {@code message.setTopic(topic)}。
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
     * <p>从 Redis Stream Entry ID 字符串构造 {@link MessageId} 并设置到消息上。
     *
     * @param message       消息载体
     * @param streamEntryId Redis Stream Entry ID（格式 {@code {timestamp}-{sequence}}）
     * @param <T>           body 类型
     */
    public static <T> void applyMessageId(Message<T> message, String streamEntryId) {
        message.setMessageId(MessageId.fromStreamEntry(streamEntryId));
    }
}
