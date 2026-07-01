package io.github.streammq.adapter.redisson.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.spi.MessageConverter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 透传消息转换器。
 *
 * <p>与 {@link DefaultMessageConverter} 的差异在于 body 处理：
 * <ul>
 *   <li>{@code body} 直接以字符串形式存入 Stream Entry 的 {@code body} 字段（{@code toString()}），
 *       不经过 {@code MessageSerializer} 序列化与 Base64 编码</li>
 *   <li>反序列化时 {@code body} 字段直接作为字符串还原</li>
 * </ul>
 *
 * <p>适用于 body 本身就是字符串/JSON 的场景，避免双重序列化开销。
 *
 * <p>其他字段（tag/keys/props/bornTs 等）映射规则与 {@link DefaultMessageConverter} 一致。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class PassThroughMessageConverter implements MessageConverter {

    /** Stream Entry 字段名常量 */
    public static final String FIELD_BODY = "body";
    public static final String FIELD_BODY_TYPE = "bodyType";
    public static final String FIELD_TAG = "tag";
    public static final String FIELD_KEYS = "keys";
    public static final String FIELD_SHARDING_KEY = "shardingKey";
    public static final String FIELD_PROPS = "props";
    public static final String FIELD_BORN_TS = "bornTs";
    public static final String FIELD_BORN_HOST = "bornHost";
    public static final String FIELD_RETRY_TIMES = "retryTimes";
    public static final String FIELD_TX_ID = "txId";
    public static final String FIELD_ORIGIN_TOPIC = "originTopic";

    private final ObjectMapper propsMapper;

    /**
     * 构造透传转换器。
     */
    public PassThroughMessageConverter() {
        this.propsMapper = new ObjectMapper();
    }

    @Override
    public Map<String, String> toStreamFields(Message<?> message) {
        Objects.requireNonNull(message, "message");
        Map<String, String> fields = new HashMap<>(16);

        Object body = message.getBody();
        if (body != null) {
            // body 直接 toString()，不经过序列化器
            fields.put(FIELD_BODY, body.toString());
            fields.put(FIELD_BODY_TYPE, body.getClass().getName());
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

        if (message.getBornHost() != null) {
            fields.put(FIELD_BORN_HOST, message.getBornHost());
        }
        if (message.getReconsumeTimes() > 0) {
            fields.put(FIELD_RETRY_TIMES, Integer.toString(message.getReconsumeTimes()));
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

        // topic 不在 Stream Entry 字段中（由 Stream Key 本身表示），使用无参构造 + setter 回填
        Message<T> message = new Message<>();

        String bodyStr = fields.get(FIELD_BODY);
        if (bodyStr != null && !bodyStr.isEmpty()) {
            // body 直接取字符串，不反序列化
            message.setBody((T) bodyStr);
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
        if (propsJson != null && !propsJson.isEmpty()) {
            try {
                Map<String, String> props = propsMapper.readValue(propsJson, new TypeReference<Map<String, String>>() {
                });
                message.setUserProperties(props);
            } catch (JsonProcessingException ex) {
                throw new SerializationException("Failed to deserialize message properties", ex);
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

        String retryTimesStr = fields.get(FIELD_RETRY_TIMES);
        if (retryTimesStr != null && !retryTimesStr.isEmpty()) {
            try {
                message.setReconsumeTimes(Integer.parseInt(retryTimesStr));
            } catch (NumberFormatException ex) {
                throw new SerializationException("Failed to parse retryTimes: " + retryTimesStr, ex);
            }
        }

        return message;
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
        return "pass-through";
    }
}
