package io.github.streammq.spring.cloud.stream.binder;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.message.Message;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

/**
 * StreamMQ 消息生产者（消费端），注册 StreamMQ 消费者并将收到的消息转换为 Spring Integration 消息输出。
 *
 * <p>核心职责：
 *
 * <ol>
 *   <li>继承 {@link MessageProducerSupport}，作为 Spring Integration 消息源
 *   <li>实现 {@link StreamMessageConcurrentlyConsumer}，接收 StreamMQ 消息
 *   <li>将 StreamMQ {@link Message} 转换为 Spring Integration 消息并通过 {@link #sendMessage} 输出
 *   <li>在 {@link #doStart()} 时向 {@link StreamMQListenerContainer} 注册消费者
 *   <li>在 {@link #doStop()} 时由容器停止消费
 * </ol>
 *
 * <p>消费结果映射：本生产者始终返回 {@link ConsumeAction#SUCCESS}， 消费失败的重试由 Spring Cloud Stream
 * 的错误处理机制（errorChannel / DLQ）负责。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Slf4j
public class StreamMQMessageProducer extends MessageProducerSupport
        implements StreamMessageConcurrentlyConsumer<Object> {

    private final StreamMQListenerContainer listenerContainer;
    private final String topic;
    private final String group;
    private final StreamMQConsumerProperties consumerProperties;
    private final StreamMQBinderProperties binderProperties;

    /** 构造的 StreamMQConsumer 注解代理实例 */
    private StreamMQConsumer annotation;

    /**
     * 构造消息生产者。
     *
     * @param listenerContainer StreamMQ Listener 容器
     * @param topic 消费主题
     * @param group 消费者组名
     * @param consumerProperties 消费者属性
     * @param binderProperties Binder 全局属性（提供默认值）
     */
    public StreamMQMessageProducer(
            StreamMQListenerContainer listenerContainer,
            String topic,
            String group,
            StreamMQConsumerProperties consumerProperties,
            StreamMQBinderProperties binderProperties) {
        this.listenerContainer = Objects.requireNonNull(listenerContainer, "listenerContainer");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.group = Objects.requireNonNull(group, "group");
        this.consumerProperties = Objects.requireNonNull(consumerProperties, "consumerProperties");
        this.binderProperties = Objects.requireNonNull(binderProperties, "binderProperties");
    }

    @Override
    public ConsumeAction onMessage(Message<Object> message, ConsumeContext context)
            throws Exception {
        if (Objects.isNull(message)) {
            log.warn("收到空消息: topic={}, group={}", topic, group);
            return ConsumeAction.RECONSUME_LATER;
        }
        try {
            org.springframework.messaging.Message<Object> springMessage = convert(message);
            sendMessage(springMessage);
            return ConsumeAction.SUCCESS;
        } catch (RuntimeException ex) {
            log.error("消息处理失败，触发重试: topic={}, group={}", topic, group, ex);
            return ConsumeAction.RECONSUME_LATER;
        }
    }

    /**
     * 将 StreamMQ 消息转换为 Spring Integration 消息。
     *
     * @param streamMessage StreamMQ 消息
     * @return Spring Integration 消息
     */
    private org.springframework.messaging.Message<Object> convert(Message<Object> streamMessage) {
        MessageBuilder<Object> builder = MessageBuilder.withPayload(streamMessage.getBody());

        // 透传 StreamMQ 消息元数据为消息头
        if (Objects.nonNull(streamMessage.getMessageId())) {
            builder.setHeader(
                    StreamMQMessageHandler.HEADER_MESSAGE_ID,
                    streamMessage.getMessageId().toString());
        }
        builder.setHeader(StreamMQMessageHandler.HEADER_TOPIC, streamMessage.getTopic());
        if (Objects.nonNull(streamMessage.getTag())) {
            builder.setHeader(StreamMQMessageHandler.HEADER_TAG, streamMessage.getTag());
        }
        if (Objects.nonNull(streamMessage.getKeys())) {
            builder.setHeader(StreamMQMessageHandler.HEADER_KEYS, streamMessage.getKeys());
        }
        if (Objects.nonNull(streamMessage.getShardingKey())) {
            builder.setHeader(
                    StreamMQMessageHandler.HEADER_SHARDING_KEY, streamMessage.getShardingKey());
        }
        builder.setHeader(
                StreamMQMessageHandler.HEADER_RECONSUME_TIMES, streamMessage.getReconsumeTimes());
        builder.setHeader(
                StreamMQMessageHandler.HEADER_BORN_TIMESTAMP, streamMessage.getBornTimestamp());
        if (Objects.nonNull(streamMessage.getBornHost())) {
            builder.setHeader(StreamMQMessageHandler.HEADER_BORN_HOST, streamMessage.getBornHost());
        }

        // 透传用户属性（contentType 单独处理，设置为 Spring Messaging 的 contentType 头）
        Map<String, String> userProperties = streamMessage.getUserProperties();
        if (io.github.streammq.core.util.CollectionUtils.isNotEmpty(userProperties)) {
            for (Map.Entry<String, String> entry : userProperties.entrySet()) {
                if (StreamMQMessageHandler.USER_PROPERTY_CONTENT_TYPE.equals(entry.getKey())) {
                    builder.setHeader(MessageHeaders.CONTENT_TYPE, entry.getValue());
                } else {
                    builder.setHeader(entry.getKey(), entry.getValue());
                }
            }
        }

        return builder.build();
    }

    @Override
    protected void doStart() {
        Assert.notNull(this.topic, "topic must not be null");
        this.annotation = buildAnnotation();
        listenerContainer.registerConsumer(this, annotation);
        log.info(
                "StreamMQMessageProducer 已启动: topic={}, group={}, selectorExpression={},"
                        + " shardCount={}",
                topic,
                group,
                consumerProperties.getSelectorExpression(),
                consumerProperties.getShardCount());
    }

    @Override
    protected void doStop() {
        log.info("StreamMQMessageProducer 已停止: topic={}, group={}", topic, group);
    }

    /**
     * 构建 {@link StreamMQConsumer} 注解的动态代理实例，将消费者属性映射为注解属性。
     *
     * <p>由于 {@link StreamMQConsumer} 是注解类型，无法直接实例化， 此处使用 JDK 动态代理创建一个代理实例，所有方法返回值由 {@link
     * StreamMQConsumerProperties} 提供。
     *
     * @return 注解代理实例
     */
    private StreamMQConsumer buildAnnotation() {
        SelectorType selectorType = parseSelectorType(consumerProperties.getSelectorType());
        int concurrency =
                consumerProperties.getConcurrency() > 0
                        ? consumerProperties.getConcurrency()
                        : binderProperties.getConsumeThreadMin();
        int maxReconsumeTimes =
                consumerProperties.getMaxAttempts() > 0
                        ? consumerProperties.getMaxAttempts()
                        : binderProperties.getMaxReconsumeTimes();
        InvocationHandler handler =
                (proxy, method, args) -> {
                    String name = method.getName();
                    switch (name) {
                        case "topic":
                            return topic;
                        case "consumerGroup":
                            return group;
                        case "consumeMode":
                            return ConsumeMode.CLUSTERING;
                        case "messageModel":
                            return MessageModel.CONCURRENT;
                        case "consumeThreadMin":
                            return Math.max(1, concurrency);
                        case "consumeThreadMax":
                            return Math.max(concurrency, binderProperties.getConsumeThreadMax());
                        case "maxReconsumeTimes":
                            return maxReconsumeTimes;
                        case "consumeTimeout":
                            return binderProperties.getConsumeTimeout();
                        case "selectorExpression":
                            return consumerProperties.getSelectorExpression();
                        case "selectorType":
                            return selectorType;
                        case "pullBatchSize":
                            return binderProperties.getPullBatchSize();
                        case "shardCount":
                            return consumerProperties.getShardCount();
                        case "enableMsgTrace":
                            return consumerProperties.isEnableMsgTrace();
                        case "namespace":
                            return binderProperties.getNamespace();
                        case "dlqMode":
                            return false;
                        case "enable":
                            return true;
                        case "annotationType":
                            return StreamMQConsumer.class;
                        default:
                            return method.getDefaultValue();
                    }
                };
        return (StreamMQConsumer)
                Proxy.newProxyInstance(
                        StreamMQConsumer.class.getClassLoader(),
                        new Class<?>[] {StreamMQConsumer.class},
                        handler);
    }

    /**
     * 解析过滤类型字符串为枚举。
     *
     * @param value 过滤类型字符串
     * @return 枚举值，默认 TAG
     */
    private SelectorType parseSelectorType(String value) {
        if (Objects.isNull(value)) {
            return SelectorType.TAG;
        }
        try {
            return SelectorType.valueOf(value.toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            log.warn("无法识别的 selectorType={}, 使用默认值 TAG", value);
            return SelectorType.TAG;
        }
    }

    /**
     * 返回消费主题。
     *
     * @return 主题
     */
    public String getTopic() {
        return topic;
    }

    /**
     * 返回消费者组名。
     *
     * @return 消费者组名
     */
    public String getGroup() {
        return group;
    }
}
