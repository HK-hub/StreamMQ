package io.github.streammq.spring.cloud.stream.binder;

import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.message.SendStatus;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.core.util.StringUtils;
import lombok.extern.slf4j.Slf4j;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ErrorMessage;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * StreamMQ 消息处理器（生产端），将 Spring Messaging 消息转换为 StreamMQ 消息并发送。
 *
 * <p>核心职责：
 * <ol>
 *   <li>从 Spring {@link Message} 中提取 payload 与 headers</li>
 *   <li>通过 {@link MessageBuilder} 构造 StreamMQ {@link io.github.streammq.core.message.Message}</li>
 *   <li>通过 {@link StreamMessageTemplate#syncSend} 同步发送</li>
 *   <li>发送失败时将异常路由到 errorChannel（若已配置）</li>
 * </ol>
 *
 * <p>消息头透传：Spring Messaging 消息头中的非框架头会被复制为 StreamMQ userProperties，
 * 消费端可通过 {@link io.github.streammq.core.message.Message#getUserProperties()} 读回。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Slf4j
public class StreamMQMessageHandler implements MessageHandler {

    /** 消息头：业务键（keys） */
    public static final String HEADER_KEYS = "streammq_keys";

    /** 消息头：分片键（shardingKey） */
    public static final String HEADER_SHARDING_KEY = "streammq_shardingKey";

    /** 消息头：消息 ID */
    public static final String HEADER_MESSAGE_ID = "streammq_messageId";

    /** 消息头：Topic */
    public static final String HEADER_TOPIC = "streammq_topic";

    /** 消息头：Tag */
    public static final String HEADER_TAG = "streammq_tag";

    /** 消息头：重试消费次数 */
    public static final String HEADER_RECONSUME_TIMES = "streammq_reconsumeTimes";

    /** 消息头：出生时间戳 */
    public static final String HEADER_BORN_TIMESTAMP = "streammq_bornTimestamp";

    /** 消息头：出生主机 */
    public static final String HEADER_BORN_HOST = "streammq_bornHost";

    private final StreamMessageTemplate template;
    private final String topic;
    private final StreamMQProducerProperties producerProperties;
    private final org.springframework.messaging.MessageChannel errorChannel;

    /**
     * 构造消息处理器。
     *
     * @param template StreamMQ 消息模板
     * @param topic 目标主题
     * @param producerProperties 生产者属性
     * @param errorChannel 错误通道（可为 null）
     */
    public StreamMQMessageHandler(StreamMessageTemplate template, String topic,
                                  StreamMQProducerProperties producerProperties,
                                  org.springframework.messaging.MessageChannel errorChannel) {
        this.template = Objects.requireNonNull(template, "template");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.producerProperties = Objects.requireNonNull(producerProperties, "producerProperties");
        this.errorChannel = errorChannel;
    }

    @Override
    public void handleMessage(Message<?> message) throws MessagingException {
        Objects.requireNonNull(message, "message");
        try {
            io.github.streammq.core.message.Message<Object> streamMessage = convert(message);
            SendResult result = template.syncSend(streamMessage,
                producerProperties.getSendTimeout(), producerProperties.getRetryTimes());
            if (Objects.nonNull(result) && result.getSendStatus() == SendStatus.SEND_OK) {
                log.debug("消息发送成功: topic={}, messageId={}", topic,
                    Objects.nonNull(result.getMessageId()) ? result.getMessageId() : "N/A");
            } else {
                String errorMsg = Objects.nonNull(result)
                    ? result.getSendStatus().name()
                    : "SendResult is null";
                log.warn("消息发送失败: topic={}, status={}", topic, errorMsg);
                sendError(message, new MessagingException(message,
                    new RuntimeException("StreamMQ send failed: " + errorMsg)));
            }
        } catch (RuntimeException ex) {
            log.error("消息发送异常: topic={}", topic, ex);
            sendError(message, new MessagingException(message, ex));
        }
    }

    /**
     * 将 Spring Messaging 消息转换为 StreamMQ 消息。
     *
     * @param springMessage Spring 消息
     * @return StreamMQ 消息
     */
    private io.github.streammq.core.message.Message<Object> convert(Message<?> springMessage) {
        Object payload = springMessage.getPayload();
        Object body = extractBody(payload);

        MessageBuilder<Object> builder = MessageBuilder.<Object>withTopic(topic)
            .body(body);

        // 设置 Tag：优先使用生产者属性中配置的 Tag
        if (StringUtils.isNotEmpty(producerProperties.getTag())) {
            builder.tag(producerProperties.getTag());
        }

        // 设置业务键：优先从消息头读取，其次使用生产者属性中配置的 keys
        Object keysHeader = springMessage.getHeaders().get(HEADER_KEYS);
        if (Objects.nonNull(keysHeader)) {
            builder.keys(keysHeader.toString());
        } else if (StringUtils.isNotEmpty(producerProperties.getKeys())) {
            builder.keys(producerProperties.getKeys());
        }

        // 设置分片键：优先从消息头读取，其次使用生产者属性中配置的 shardingKey
        String shardingKey = resolveShardingKey(springMessage);
        if (StringUtils.isNotEmpty(shardingKey)) {
            builder.shardingKey(shardingKey);
        }

        // 透传消息头为 userProperties（排除 Spring 框架内置头与 StreamMQ 专属头）
        Map<String, String> userProperties = extractUserProperties(springMessage.getHeaders());
        if (io.github.streammq.core.util.CollectionUtils.isNotEmpty(userProperties)) {
            for (Map.Entry<String, String> entry : userProperties.entrySet()) {
                builder.userProperty(entry.getKey(), entry.getValue());
            }
        }

        return builder.build();
    }

    /**
     * 提取消息体：byte[] 直接使用，String 转为 bytes，其他类型保留原值由模板序列化。
     *
     * @param payload 原始 payload
     * @return 消息体
     */
    private Object extractBody(Object payload) {
        if (Objects.isNull(payload)) {
            return new byte[0];
        }
        if (payload instanceof byte[] bytes) {
            return bytes;
        }
        if (payload instanceof String str) {
            return str.getBytes(StandardCharsets.UTF_8);
        }
        return payload;
    }

    /**
     * 解析分片键。
     *
     * <p>优先级：
     * <ol>
     *   <li>消息头 {@code streammq_shardingKey}</li>
     *   <li>生产者属性中配置的 {@code shardingKey}</li>
     * </ol>
     *
     * @param message Spring 消息
     * @return 分片键，为 null 表示不使用
     */
    private String resolveShardingKey(Message<?> message) {
        // 优先从消息头读取
        Object headerValue = message.getHeaders().get(HEADER_SHARDING_KEY);
        if (Objects.nonNull(headerValue)) {
            return headerValue.toString();
        }
        // 其次使用生产者属性中配置的 shardingKey
        if (StringUtils.isNotEmpty(producerProperties.getShardingKey())) {
            return producerProperties.getShardingKey();
        }
        return null;
    }

    /**
     * 从 Spring 消息头中提取用户自定义属性（排除框架内置头与 StreamMQ 专属头）。
     *
     * @param headers Spring 消息头
     * @return 用户属性 Map
     */
    private Map<String, String> extractUserProperties(MessageHeaders headers) {
        Map<String, String> userProperties = new HashMap<>();
        headers.forEach((key, value) -> {
            if (isUserHeader(key) && Objects.nonNull(value)) {
                userProperties.put(key, value.toString());
            }
        });
        return userProperties;
    }

    /**
     * 判断消息头是否为用户自定义头（非 Spring 框架内置头、非 StreamMQ 专属头）。
     *
     * @param headerName 头名称
     * @return true 表示为用户自定义头
     */
    private boolean isUserHeader(String headerName) {
        return !MessageHeaders.ID.equals(headerName)
            && !MessageHeaders.TIMESTAMP.equals(headerName)
            && !MessageHeaders.CONTENT_TYPE.equals(headerName)
            && !MessageHeaders.REPLY_CHANNEL.equals(headerName)
            && !MessageHeaders.ERROR_CHANNEL.equals(headerName)
            && !headerName.startsWith("streammq_");
    }

    /**
     * 将错误消息发送到错误通道（若已配置）。
     *
     * @param originalMessage 原始消息
     * @param exception 消息异常
     */
    private void sendError(Message<?> originalMessage, MessagingException exception) {
        if (Objects.nonNull(errorChannel)) {
            ErrorMessage errorMessage = new ErrorMessage(exception);
            try {
                errorChannel.send(errorMessage);
            } catch (RuntimeException ex) {
                log.error("向 errorChannel 发送错误消息失败", ex);
                throw exception;
            }
        } else {
            throw exception;
        }
    }
}
