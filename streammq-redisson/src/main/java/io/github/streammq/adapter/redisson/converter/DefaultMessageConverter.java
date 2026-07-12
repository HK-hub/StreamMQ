package io.github.streammq.adapter.redisson.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.streammq.core.compression.CompressionCodec;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.util.StringUtils;
import lombok.Setter;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 默认 {@link MessageConverter} 实现，连接 {@link Message} 与 Redis Stream Entry 字段。
 *
 * <p>Stream Entry 字段映射（对齐架构文档 §7.2）：
 * <ul>
 *   <li>{@code body} - body 序列化后的 Base64 字符串（必填）</li>
 *   <li>{@code bodyType} - body 实际类型类名（用于反序列化时校验）</li>
 *   <li>{@code tag} - 标签（可选）</li>
 *   <li>{@code keys} - 业务键（可选）</li>
 *   <li>{@code shardingKey} - 分片键（可选）</li>
 *   <li>{@code props} - 系统属性 + 用户属性合并的 JSON 字符串（可选）</li>
 *   <li>{@code bornTs} - 出生时间戳，long 字符串（必填）</li>
 *   <li>{@code bornHost} - 出生主机（可选）</li>
 *   <li>{@code retryTimes} - 已重试次数，int 字符串（可选，默认 0）</li>
 *   <li>{@code txId} - 事务 ID（可选，仅事务消息）</li>
 *   <li>{@code originTopic} - 原 topic（可选，仅重试/DLQ 转投场景）</li>
 * </ul>
 *
 * <p>body 序列化使用注入的 {@link MessageSerializer}，元信息字段（tag/keys/props 等）均为字符串，
 * 不参与序列化器处理。
 *
 * <p>{@code topic} 字段不写入 Stream Entry，因其由 Stream Key 本身表示。
 * 反序列化时由调用方（Consumer）根据读取的 Stream Key 回填 topic。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultMessageConverter implements MessageConverter {

    /** Stream Entry 字段名常量 */
    public static final String FIELD_BODY = "body";
    public static final String FIELD_BODY_TYPE = "bodyType";
    public static final String FIELD_BODY_TYPE_NAME = "bodyTypeName";
    public static final String FIELD_TAG = "tag";
    public static final String FIELD_KEYS = "keys";
    public static final String FIELD_SHARDING_KEY = "shardingKey";
    public static final String FIELD_PROPS = "props";
    public static final String FIELD_BORN_TS = "bornTs";
    public static final String FIELD_BORN_HOST = "bornHost";
    public static final String FIELD_RETRY_TIMES = "retryTimes";
    public static final String FIELD_TX_ID = "txId";
    public static final String FIELD_ORIGIN_TOPIC = "originTopic";
    public static final String FIELD_COMPRESSED = "compressed";

    private final MessageSerializer<Object> serializer;
    private final ObjectMapper propsMapper;

    /** 压缩编解码器（可选注入，用于解压 compressed=true 的消息体） */
    @Setter
    private CompressionCodec compressionCodec;

    /**
     * 构造转换器。
     *
     * @param serializer body 序列化器
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public DefaultMessageConverter(MessageSerializer<?> serializer) {
        this.serializer = (MessageSerializer<Object>) Objects.requireNonNull(serializer, "serializer");
        this.propsMapper = new ObjectMapper();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Map<String, String> toStreamFields(Message<?> message) {
        Objects.requireNonNull(message, "message");
        Map<String, String> fields = new HashMap<>(16);

        Object body = message.getBody();
        if (Objects.nonNull(body)) {
            Class<?> bodyType = body.getClass();
            byte[] bodyBytes = serializer.serialize(body, (Class<Object>) (Class) bodyType);
            fields.put(FIELD_BODY, Base64.getEncoder().encodeToString(bodyBytes));
            fields.put(FIELD_BODY_TYPE, bodyType.getName());
            fields.put(FIELD_BODY_TYPE_NAME, bodyType.getSimpleName());
        }

        if (Objects.nonNull(message.getTag())) {
            fields.put(FIELD_TAG, message.getTag());
        }
        if (Objects.nonNull(message.getKeys())) {
            fields.put(FIELD_KEYS, message.getKeys());
        }
        if (Objects.nonNull(message.getShardingKey())) {
            fields.put(FIELD_SHARDING_KEY, message.getShardingKey());
        }

        Map<String, String> sysProps = message.getProperties();
        Map<String, String> userProps = message.getUserProperties();
        if (!sysProps.isEmpty() || !userProps.isEmpty()) {
            Map<String, String> merged = new HashMap<>(sysProps.size() + userProps.size());
            merged.putAll(sysProps);
            merged.putAll(userProps);
            try {
                fields.put(FIELD_PROPS, propsMapper.writeValueAsString(merged));
            } catch (JsonProcessingException ex) {
                throw new SerializationException("Failed to serialize message properties", ex);
            }
        }

        fields.put(FIELD_BORN_TS, Long.toString(message.getBornTimestamp()));

        if (Objects.nonNull(message.getBornHost())) {
            fields.put(FIELD_BORN_HOST, message.getBornHost());
        }
        if (message.getReconsumeTimes() > 0) {
            fields.put(FIELD_RETRY_TIMES, Integer.toString(message.getReconsumeTimes()));
        }
        if (Objects.nonNull(message.getTransactionId())) {
            fields.put(FIELD_TX_ID, message.getTransactionId());
        }

        return fields;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Message<T> fromStreamFields(Map<String, String> fields, Class<T> targetType) {
        Objects.requireNonNull(fields, "fields");
        Objects.requireNonNull(targetType, "targetType");

        // topic 不在 Stream Entry 字段中（由 Stream Key 本身表示），使用无参构造 + setter 回填
        Message<T> message = new Message<>();

        String bodyStr = fields.get(FIELD_BODY);
        if (StringUtils.isNotEmpty(bodyStr)) {
            boolean compressed = "true".equals(fields.get(FIELD_COMPRESSED));
            T body = deserializeBody(bodyStr, fields.get(FIELD_BODY_TYPE), targetType, compressed);
            message.setBody(body);
        }

        if (fields.containsKey(FIELD_TAG)) {
            message.setTag(fields.get(FIELD_TAG));
        }
        if (fields.containsKey(FIELD_KEYS)) {
            message.setKeys(fields.get(FIELD_KEYS));
        }
        if (fields.containsKey(FIELD_SHARDING_KEY)) {
            message.setShardingKey(fields.get(FIELD_SHARDING_KEY));
        }
        if (fields.containsKey(FIELD_BORN_HOST)) {
            message.setBornHost(fields.get(FIELD_BORN_HOST));
        }
        if (fields.containsKey(FIELD_TX_ID)) {
            message.setTransactionId(fields.get(FIELD_TX_ID));
        }

        String propsJson = fields.get(FIELD_PROPS);
        if (StringUtils.isNotEmpty(propsJson)) {
            try {
                Map<String, String> props = propsMapper.readValue(propsJson, new TypeReference<Map<String, String>>() {
                });
                // 系统属性与用户属性合并存储，反序列化后写入 userProperties
                message.setUserProperties(props);
            } catch (JsonProcessingException ex) {
                throw new SerializationException("Failed to deserialize message properties", ex);
            }
        }

        String bornTs = fields.get(FIELD_BORN_TS);
        if (StringUtils.isNotEmpty(bornTs)) {
            try {
                message.setBornTimestamp(Long.parseLong(bornTs));
            } catch (NumberFormatException ex) {
                throw new SerializationException("Failed to parse bornTs: " + bornTs, ex);
            }
        }

        String retryTimesStr = fields.get(FIELD_RETRY_TIMES);
        if (StringUtils.isNotEmpty(retryTimesStr)) {
            try {
                message.setReconsumeTimes(Integer.parseInt(retryTimesStr));
            } catch (NumberFormatException ex) {
                throw new SerializationException("Failed to parse retryTimes: " + retryTimesStr, ex);
            }
        }

        return message;
    }

    /**
     * 反序列化 body 字段，支持两种来源：
     * <ol>
     *   <li><b>StreamMQ SDK 发送方</b>：{@code bodyTypeField} 非空，body 为 Base64 编码的序列化字节</li>
     *   <li><b>非 SDK 发送方（跨平台/跨语言）</b>：{@code bodyTypeField} 为空，body 为原始字符串
     *       （如 Go/Python 直接写 Redis Stream 的 JSON 字符串）</li>
     * </ol>
     *
     * <p>跨平台场景的回退策略：
     * <ul>
     *   <li>目标类型为 {@link String}：直接返回原始字符串（推荐用法，消费者自行反序列化）</li>
     *   <li>目标类型为其他：尝试将原始字符串的 UTF-8 字节交给序列化器反序列化
     *       （适用于 Jackson + JSON body 的组合；不兼容时抛 {@link SerializationException}）</li>
     * </ul>
     *
     * @param bodyStr body 字段值
     * @param bodyTypeField bodyType 字段值（可为 null 或空，表示非 SDK 发送方）
     * @param targetType 目标 body 类型
     * @param compressed body 是否被压缩
     * @param <T> 目标类型
     * @return 反序列化后的 body
     */
    @SuppressWarnings("unchecked")
    private <T> T deserializeBody(String bodyStr, String bodyTypeField, Class<T> targetType, boolean compressed) {
        // SDK 路径：bodyType 字段存在 → body 为 Base64 编码的序列化字节
        if (StringUtils.isNotEmpty(bodyTypeField)) {
            byte[] bodyBytes = Base64.getDecoder().decode(bodyStr);
            if (compressed) {
                bodyBytes = decompressBody(bodyBytes);
            }
            return serializer.deserialize(bodyBytes, targetType);
        }
        // 跨平台路径：bodyType 字段缺失 → body 为原始字符串
        if (compressed) {
            // 压缩消息的 body 为 Base64 编码的压缩字节，需先解码解压
            byte[] bodyBytes = Base64.getDecoder().decode(bodyStr);
            bodyBytes = decompressBody(bodyBytes);
            if (targetType == String.class) {
                return (T) new String(bodyBytes, StandardCharsets.UTF_8);
            }
            return serializer.deserialize(bodyBytes, targetType);
        }
        if (targetType == String.class) {
            return (T) bodyStr;
        }
        // 目标类型非 String：尝试将原始字符串 UTF-8 字节交给序列化器反序列化
        // 适用场景：Jackson 序列化器 + JSON 字符串 body → 反序列化为 POJO
        byte[] rawBytes = bodyStr.getBytes(StandardCharsets.UTF_8);
        return serializer.deserialize(rawBytes, targetType);
    }

    /**
     * 解压 body 字节。
     *
     * @param compressedBytes 压缩后的字节
     * @return 解压后的原始字节
     * @throws SerializationException 当未配置 CompressionCodec 时
     */
    private byte[] decompressBody(byte[] compressedBytes) {
        if (Objects.isNull(compressionCodec)) {
            throw new SerializationException(
                "Message body is marked as compressed but no CompressionCodec is configured", null);
        }
        return compressionCodec.decompress(compressedBytes);
    }

    /**
     * 为消息回填 topic（消费端从 Stream Key 解析后调用）。
     *
     * @param message 消息
     * @param topic 主题
     * @param <T> body 类型
     */
    public static <T> void applyTopic(Message<T> message, String topic) {
        message.setTopic(topic);
    }

    /**
     * 为消息回填 MessageId（消费端从 Stream Entry ID 解析后调用）。
     *
     * @param message 消息
     * @param streamEntryId Redis Stream Entry ID 字符串
     * @param <T> body 类型
     */
    public static <T> void applyMessageId(Message<T> message, String streamEntryId) {
        message.setMessageId(new MessageId(streamEntryId));
    }

    @Override
    public String name() {
        return "default";
    }
}
