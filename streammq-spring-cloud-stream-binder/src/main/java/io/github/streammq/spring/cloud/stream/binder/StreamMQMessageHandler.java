/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.cloud.stream.binder;

import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.message.SendStatus;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.core.util.StringUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ErrorMessage;

/**
 * StreamMQ 消息处理器（生产端），将 Spring Messaging 消息转换为 StreamMQ 消息并发送。
 *
 * <p>核心职责：
 *
 * <ol>
 *   <li>从 Spring {@link Message} 中提取 payload 与 headers
 *   <li>通过 {@link MessageBuilder} 构造 StreamMQ {@link io.github.streammq.core.message.Message}
 *   <li>通过 {@link StreamMessageTemplate#syncSend} 同步发送
 *   <li>发送失败时将异常路由到 errorChannel（若已配置）
 * </ol>
 *
 * <p>消息头透传：Spring Messaging 消息头中的非框架头会被复制为 StreamMQ userProperties， 消费端可通过 {@link
 * io.github.streammq.core.message.Message#getUserProperties()} 读回。 保留追踪头（{@code traceparent} /
 * {@code tracestate}）除外——它们由 OTel 生产者拦截器在发送期统一注入， 客户端提供的同名入站头会被丢弃，避免上下文冲突。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Slf4j
public class StreamMQMessageHandler implements MessageHandler {

    /** 消息头：业务键（keys） */
    public static final String HEADER_KEYS = StreamMQBinderConstants.HEADER_PREFIX + "keys";

    /** 消息头：分片键（shardingKey） */
    public static final String HEADER_SHARDING_KEY =
            StreamMQBinderConstants.HEADER_PREFIX + "shardingKey";

    /** 消息头：消息 ID */
    public static final String HEADER_MESSAGE_ID =
            StreamMQBinderConstants.HEADER_PREFIX + "messageId";

    /** 消息头：Topic */
    public static final String HEADER_TOPIC = StreamMQBinderConstants.HEADER_PREFIX + "topic";

    /** 消息头：Tag */
    public static final String HEADER_TAG = StreamMQBinderConstants.HEADER_PREFIX + "tag";

    /** 消息头：重试消费次数 */
    public static final String HEADER_RECONSUME_TIMES =
            StreamMQBinderConstants.HEADER_PREFIX + "reconsumeTimes";

    /** 消息头：出生时间戳 */
    public static final String HEADER_BORN_TIMESTAMP =
            StreamMQBinderConstants.HEADER_PREFIX + "bornTimestamp";

    /** 消息头：出生主机 */
    public static final String HEADER_BORN_HOST =
            StreamMQBinderConstants.HEADER_PREFIX + "bornHost";

    /** 用户属性键：Spring Messaging contentType（透传内容类型，确保消费端能正确反序列化） */
    public static final String USER_PROPERTY_CONTENT_TYPE = "contentType";

    /**
     * 保留追踪头集合：客户端提供的 {@code traceparent} / {@code tracestate} 不允许复制进 outbound userProperties。
     *
     * <p>这两个 W3C 头由 OpenTelemetry 生产者拦截器在发送期注入；若入站消息携带的同名头先行写入
     * userProperties，会与拦截器注入值冲突（后者被覆盖或产生重复键），导致下游提取到错误的父级上下文、链路断裂。
     */
    public static final java.util.Set<String> RESERVED_TRACE_HEADERS =
            java.util.Set.of("traceparent", "tracestate");

    private final StreamMessageTemplate template;
    private final String topic;
    private final StreamMQProducerProperties producerProperties;
    private final org.springframework.messaging.MessageChannel errorChannel;
    private final long sendTimeout;
    private final int retryTimes;

    /**
     * 构造消息处理器。
     *
     * @param template StreamMQ 消息模板
     * @param topic 目标主题
     * @param producerProperties 生产者扩展属性
     * @param errorChannel 错误通道（可为 null）
     * @param sendTimeout 发送超时（毫秒）
     * @param retryTimes 同步发送重试次数
     */
    public StreamMQMessageHandler(
            StreamMessageTemplate template,
            String topic,
            StreamMQProducerProperties producerProperties,
            org.springframework.messaging.MessageChannel errorChannel,
            long sendTimeout,
            int retryTimes) {
        this.template = Objects.requireNonNull(template, "template");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.producerProperties = Objects.requireNonNull(producerProperties, "producerProperties");
        this.errorChannel = errorChannel;
        this.sendTimeout = Math.max(1L, sendTimeout);
        this.retryTimes = Math.max(0, retryTimes);
    }

    @Override
    public void handleMessage(Message<?> message) throws MessagingException {
        Objects.requireNonNull(message, "message");
        try {
            io.github.streammq.core.message.Message<Object> streamMessage = convert(message);
            SendResult result =
                    template.syncSend(
                            streamMessage,
                            io.github.streammq.core.message.SendOptions.of(
                                    sendTimeout, retryTimes));
            if (Objects.nonNull(result) && result.getSendStatus() == SendStatus.SEND_OK) {
                log.debug(
                        "消息发送成功: topic={}, messageId={}",
                        topic,
                        Objects.nonNull(result.getMessageId()) ? result.getMessageId() : "N/A");
            } else {
                String errorMsg =
                        Objects.nonNull(result)
                                ? result.getSendStatus().name()
                                : "SendResult is null";
                log.warn("消息发送失败: topic={}, status={}", topic, errorMsg);
                sendError(
                        message,
                        new MessagingException(
                                message,
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

        MessageBuilder<Object> builder = MessageBuilder.<Object>withTopic(topic).body(body);

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

        // 单独保存 contentType，确保消费端能按原始内容类型反序列化。
        // 若消息未显式设置 contentType，根据 payload 类型推断默认值，避免消费端 Base64 编码无法还原。
        Object contentType = springMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE);
        if (Objects.isNull(contentType)) {
            contentType = inferContentType(payload);
        }
        if (Objects.nonNull(contentType)) {
            userProperties.put(USER_PROPERTY_CONTENT_TYPE, contentType.toString());
        }
        if (io.github.streammq.core.util.CollectionUtils.isNotEmpty(userProperties)) {
            for (Map.Entry<String, String> entry : userProperties.entrySet()) {
                builder.withUserProperty(entry.getKey(), entry.getValue());
            }
        }

        return builder.build();
    }

    /**
     * 根据 payload 类型推断默认 contentType。
     *
     * <p>推断规则：
     *
     * <ul>
     *   <li>{@code String} → {@code text/plain}
     *   <li>{@code byte[]} → {@code application/octet-stream}
     *   <li>其他类型 → {@code application/json}
     * </ul>
     *
     * @param payload 消息 payload
     * @return contentType 字符串
     */
    private String inferContentType(Object payload) {
        if (payload instanceof String) {
            return StreamMQBinderConstants.CONTENT_TYPE_TEXT_PLAIN;
        }
        if (payload instanceof byte[]) {
            return StreamMQBinderConstants.CONTENT_TYPE_OCTET_STREAM;
        }
        return StreamMQBinderConstants.CONTENT_TYPE_APPLICATION_JSON;
    }

    /**
     * 提取消息体：保留原始 payload 类型，由 {@link io.github.streammq.core.converter.MessageConverter} 统一负责序列化。
     *
     * <p>注意：不应在此处将 {@code String} 转换为 {@code byte[]}，否则 {@code DefaultMessageConverter} 会用 Jackson
     * 将 {@code byte[]} 序列化为 Base64 字符串，消费端反序列化得到的是 Base64 字符串而非原始文本。
     *
     * @param payload 原始 payload
     * @return 消息体（保持原类型）
     */
    private Object extractBody(Object payload) {
        if (Objects.isNull(payload)) {
            return new byte[0];
        }
        return payload;
    }

    /**
     * 解析分片键。
     *
     * <p>优先级：
     *
     * <ol>
     *   <li>消息头 {@code streammq_shardingKey}
     *   <li>生产者属性中配置的 {@code shardingKey}
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
     * 从 Spring 消息头中提取用户自定义属性（排除框架内置头、StreamMQ 专属头与保留追踪头）。
     *
     * @param headers Spring 消息头
     * @return 用户属性 Map
     */
    private Map<String, String> extractUserProperties(MessageHeaders headers) {
        Map<String, String> userProperties = new HashMap<>();
        headers.forEach(
                (key, value) -> {
                    if (isUserHeader(key)
                            && !RESERVED_TRACE_HEADERS.contains(
                                    key.toLowerCase(java.util.Locale.ROOT))
                            && Objects.nonNull(value)) {
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
        // 同时过滤 Spring Integration 内部头（integrationMessageHistory / ackIf / sequenceDetails 等），
        // 避免框架编排元数据泄漏进 Redis userProperties
        return !headerName.startsWith("integration")
                && !MessageHeaders.ID.equals(headerName)
                && !MessageHeaders.TIMESTAMP.equals(headerName)
                && !MessageHeaders.CONTENT_TYPE.equals(headerName)
                && !MessageHeaders.REPLY_CHANNEL.equals(headerName)
                && !MessageHeaders.ERROR_CHANNEL.equals(headerName)
                && !headerName.startsWith(StreamMQBinderConstants.HEADER_PREFIX);
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
