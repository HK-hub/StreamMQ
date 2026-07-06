package io.github.streammq.adapter.redisson.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.serializer.MessageSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 紧凑型消息转换器，使用短字段名减少 Redis Stream 存储开销。
 *
 * <p>与 {@link DefaultMessageConverter} 的差异在于字段命名更短，从而降低单条 Stream Entry
 * 的存储体积，适用于海量消息、对存储成本敏感的场景。
 *
 * <p>Stream Entry 字段映射（短字段名）：
 * <ul>
 *   <li>{@code t} - topic（主题）</li>
 *   <li>{@code g} - tag（标签）</li>
 *   <li>{@code k} - keys（业务键）</li>
 *   <li>{@code s} - shardingKey（分片键）</li>
 *   <li>{@code b} - body（Base64 编码的序列化字节）</li>
 *   <li>{@code bt} - bodyType（body 实际类型类名）</li>
 *   <li>{@code ts} - bornTimestamp（出生时间戳，long 字符串）</li>
 *   <li>{@code h} - bornHost（出生主机）</li>
 *   <li>{@code p} - properties（系统属性 JSON 字符串）</li>
 *   <li>{@code up} - userProperties（用户属性 JSON 字符串）</li>
 *   <li>{@code dl} - delayLevel（延时级别枚举名）</li>
 *   <li>{@code dm} - delayTimeMillis（任意延时毫秒数，long 字符串）</li>
 *   <li>{@code tx} - transactionId（事务 ID）</li>
 * </ul>
 *
 * <p>body 序列化使用注入的 {@link MessageSerializer}，元信息字段均为字符串。
 * 系统属性与用户属性分别存储为独立 JSON 字段（与 {@link DefaultMessageConverter} 合并存储不同），
 * 反序列化时各自还原。
 *
 * <p>反序列化 body 时支持两种来源（与 {@link DefaultMessageConverter} 一致）：
 * <ol>
 *   <li>SDK 发送方：{@code bt} 字段非空，body 为 Base64 编码的序列化字节</li>
 *   <li>跨平台发送方：{@code bt} 字段缺失，body 为原始字符串</li>
 * </ol>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class CompactMessageConverter implements MessageConverter {

    /** 紧凑字段名常量：topic */
    public static final String FIELD_TOPIC = "t";
    /** 紧凑字段名常量：tag */
    public static final String FIELD_TAG = "g";
    /** 紧凑字段名常量：keys */
    public static final String FIELD_KEYS = "k";
    /** 紧凑字段名常量：shardingKey */
    public static final String FIELD_SHARDING_KEY = "s";
    /** 紧凑字段名常量：body */
    public static final String FIELD_BODY = "b";
    /** 紧凑字段名常量：bodyType */
    public static final String FIELD_BODY_TYPE = "bt";
    /** 紧凑字段名常量：bornTimestamp */
    public static final String FIELD_BORN_TS = "ts";
    /** 紧凑字段名常量：bornHost */
    public static final String FIELD_BORN_HOST = "h";
    /** 紧凑字段名常量：properties（系统属性） */
    public static final String FIELD_PROPS = "p";
    /** 紧凑字段名常量：userProperties（用户属性） */
    public static final String FIELD_USER_PROPS = "up";
    /** 紧凑字段名常量：delayLevel */
    public static final String FIELD_DELAY_LEVEL = "dl";
    /** 紧凑字段名常量：delayTimeMillis */
    public static final String FIELD_DELAY_TIME_MILLIS = "dm";
    /** 紧凑字段名常量：transactionId */
    public static final String FIELD_TX_ID = "tx";

    private final MessageSerializer<Object> serializer;
    private final ObjectMapper propsMapper;

    /**
     * 构造紧凑转换器。
     *
     * @param serializer body 序列化器，不能为 null
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public CompactMessageConverter(MessageSerializer<?> serializer) {
        this.serializer = (MessageSerializer<Object>) Objects.requireNonNull(serializer, "serializer");
        this.propsMapper = new ObjectMapper();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Map<String, String> toStreamFields(Message<?> message) {
        Objects.requireNonNull(message, "message");
        Map<String, String> fields = new HashMap<>(16);

        if (message.getTopic() != null) {
            fields.put(FIELD_TOPIC, message.getTopic());
        }

        Object body = message.getBody();
        if (body != null) {
            Class<?> bodyType = body.getClass();
            byte[] bodyBytes = serializer.serialize(body, (Class<Object>) (Class) bodyType);
            fields.put(FIELD_BODY, Base64.getEncoder().encodeToString(bodyBytes));
            fields.put(FIELD_BODY_TYPE, bodyType.getName());
        }

        if (message.getTag() != null) {
            fields.put(FIELD_TAG, message.getTag());
        }
        if (message.getKeys() != null) {
            fields.put(FIELD_KEYS, message.getKeys());
        }
        if (message.getShardingKey() != null) {
            fields.put(FIELD_SHARDING_KEY, message.getShardingKey());
        }

        Map<String, String> sysProps = message.getProperties();
        if (!sysProps.isEmpty()) {
            try {
                fields.put(FIELD_PROPS, propsMapper.writeValueAsString(sysProps));
            } catch (JsonProcessingException ex) {
                throw new SerializationException("Failed to serialize system properties", ex);
            }
        }

        Map<String, String> userProps = message.getUserProperties();
        if (!userProps.isEmpty()) {
            try {
                fields.put(FIELD_USER_PROPS, propsMapper.writeValueAsString(userProps));
            } catch (JsonProcessingException ex) {
                throw new SerializationException("Failed to serialize user properties", ex);
            }
        }

        fields.put(FIELD_BORN_TS, Long.toString(message.getBornTimestamp()));

        if (message.getBornHost() != null) {
            fields.put(FIELD_BORN_HOST, message.getBornHost());
        }
        if (message.getDelayLevel() != null) {
            fields.put(FIELD_DELAY_LEVEL, message.getDelayLevel().name());
        }
        if (message.getDelayTimeMillis() != null) {
            fields.put(FIELD_DELAY_TIME_MILLIS, Long.toString(message.getDelayTimeMillis()));
        }
        if (message.getTransactionId() != null) {
            fields.put(FIELD_TX_ID, message.getTransactionId());
        }

        return fields;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Message<T> fromStreamFields(Map<String, String> fields, Class<T> targetType) {
        Objects.requireNonNull(fields, "fields");
        Objects.requireNonNull(targetType, "targetType");

        Message<T> message = new Message<>();

        if (fields.containsKey(FIELD_TOPIC)) {
            message.setTopic(fields.get(FIELD_TOPIC));
        }

        String bodyStr = fields.get(FIELD_BODY);
        if (bodyStr != null && !bodyStr.isEmpty()) {
            T body = deserializeBody(bodyStr, fields.get(FIELD_BODY_TYPE), targetType);
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

        String sysPropsJson = fields.get(FIELD_PROPS);
        if (sysPropsJson != null && !sysPropsJson.isEmpty()) {
            try {
                Map<String, String> props = propsMapper.readValue(sysPropsJson,
                    new TypeReference<Map<String, String>>() {
                    });
                message.setProperties(props);
            } catch (JsonProcessingException ex) {
                throw new SerializationException("Failed to deserialize system properties", ex);
            }
        }

        String userPropsJson = fields.get(FIELD_USER_PROPS);
        if (userPropsJson != null && !userPropsJson.isEmpty()) {
            try {
                Map<String, String> props = propsMapper.readValue(userPropsJson,
                    new TypeReference<Map<String, String>>() {
                    });
                message.setUserProperties(props);
            } catch (JsonProcessingException ex) {
                throw new SerializationException("Failed to deserialize user properties", ex);
            }
        }

        String bornTs = fields.get(FIELD_BORN_TS);
        if (bornTs != null && !bornTs.isEmpty()) {
            try {
                message.setBornTimestamp(Long.parseLong(bornTs));
            } catch (NumberFormatException ex) {
                throw new SerializationException("Failed to parse bornTs: " + bornTs, ex);
            }
        }

        String delayLevelStr = fields.get(FIELD_DELAY_LEVEL);
        if (delayLevelStr != null && !delayLevelStr.isEmpty()) {
            try {
                message.setDelayLevel(DelayLevel.valueOf(delayLevelStr));
            } catch (IllegalArgumentException ex) {
                throw new SerializationException("Failed to parse delayLevel: " + delayLevelStr, ex);
            }
        }

        String delayTimeMillisStr = fields.get(FIELD_DELAY_TIME_MILLIS);
        if (delayTimeMillisStr != null && !delayTimeMillisStr.isEmpty()) {
            try {
                message.setDelayTimeMillis(Long.parseLong(delayTimeMillisStr));
            } catch (NumberFormatException ex) {
                throw new SerializationException("Failed to parse delayTimeMillis: " + delayTimeMillisStr, ex);
            }
        }

        return message;
    }

    /**
     * 反序列化 body 字段，支持 SDK 发送方与跨平台发送方两种来源。
     *
     * @param bodyStr body 字段值
     * @param bodyTypeField bodyType 字段值（可为 null 或空，表示跨平台发送方）
     * @param targetType 目标 body 类型
     * @param <T> 目标类型
     * @return 反序列化后的 body
     */
    @SuppressWarnings("unchecked")
    private <T> T deserializeBody(String bodyStr, String bodyTypeField, Class<T> targetType) {
        // SDK 路径：bodyType 字段存在 → body 为 Base64 编码的序列化字节
        if (bodyTypeField != null && !bodyTypeField.isEmpty()) {
            byte[] bodyBytes = Base64.getDecoder().decode(bodyStr);
            return serializer.deserialize(bodyBytes, targetType);
        }
        // 跨平台路径：bodyType 字段缺失 → body 为原始字符串
        if (targetType == String.class) {
            return (T) bodyStr;
        }
        // 目标类型非 String：尝试将原始字符串 UTF-8 字节交给序列化器反序列化
        byte[] rawBytes = bodyStr.getBytes(StandardCharsets.UTF_8);
        return serializer.deserialize(rawBytes, targetType);
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
        return "compact";
    }
}
