package io.github.streammq.kafka;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * StreamMQ 兼容 Spring Kafka 风格的 KafkaTemplate。
 *
 * <p>对齐 Spring Kafka {@code KafkaTemplate<K, V>} API 风格，
 * 提供简化的 {@code send(topic, data)} / {@code send(topic, key, data)} / {@code sendDefault(data)} 方法。
 * 底层委托 {@link StreamMessageTemplate} 完成实际发送。
 *
 * <p>使用示例：
 * <pre>{@code
 * StreamMessageTemplate template = ...;
 * KafkaCompatTemplate<String, String> kafkaTemplate =
 *     new KafkaCompatTemplate<>(template, "default-topic");
 * // 发送到指定 topic
 * kafkaTemplate.send("my-topic", "hello");
 * // 发送到默认 topic
 * kafkaTemplate.sendDefault("world");
 * }</pre>
 *
 * <p><b>与 Spring KafkaTemplate 的差异</b>：
 * <ul>
 *   <li>不依赖 Spring 容器，纯 POJO 构造</li>
 *   <li>{@code send} 为同步调用，返回 {@link SendResult}</li>
 *   <li>不支持 {@code Message<?>} 转换、{@code ProducerListener} 回调等高级特性</li>
 * </ul>
 *
 * @param <K> key 类型（内部转为 String）
 * @param <V> value 类型（消息体类型）
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class KafkaCompatTemplate<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCompatTemplate.class);

    /** 底层 StreamMQ 消息模板 */
    private final StreamMessageTemplate template;

    /** 默认 topic（用于 {@link #sendDefault}） */
    private final String defaultTopic;

    /**
     * 构造 KafkaCompatTemplate，指定底层模板和默认 topic。
     *
     * @param template      StreamMQ 消息模板（必填）
     * @param defaultTopic  默认 topic，用于 {@link #sendDefault}（必填）
     * @throws NullPointerException 如果任一参数为 null
     * @throws IllegalArgumentException 如果 defaultTopic 为空
     */
    public KafkaCompatTemplate(StreamMessageTemplate template, String defaultTopic) {
        this.template = Objects.requireNonNull(template, "template");
        this.defaultTopic = validateNotEmpty(defaultTopic, "defaultTopic");
    }

    /**
     * 发送消息到指定 topic（无 key）。
     *
     * @param topic 目标 topic（必填）
     * @param data  消息体（必填）
     * @return 发送结果 {@link SendResult}
     * @throws NullPointerException 如果 topic 或 data 为 null
     * @throws io.github.streammq.core.exception.StreamMQException 发送失败
     */
    public SendResult send(String topic, V data) {
        return send(topic, null, data);
    }

    /**
     * 发送消息到指定 topic（带 key）。
     *
     * @param topic 目标 topic（必填）
     * @param key   消息键（可为 null）
     * @param data  消息体（必填）
     * @return 发送结果 {@link SendResult}
     * @throws NullPointerException 如果 topic 或 data 为 null
     * @throws io.github.streammq.core.exception.StreamMQException 发送失败
     */
    public SendResult send(String topic, K key, V data) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(data, "data");

        MessageBuilder<V> builder = MessageBuilder.<V>withTopic(topic)
            .body(data);

        if (key != null) {
            builder.keys(key.toString());
        }

        Message<V> message = builder.build();
        LOG.debug("Sending to topic={}, key={}", topic, key);
        return template.syncSend(message);
    }

    /**
     * 发送消息到构造时指定的默认 topic（无 key）。
     *
     * @param data 消息体（必填）
     * @return 发送结果 {@link SendResult}
     * @throws NullPointerException 如果 data 为 null
     * @throws io.github.streammq.core.exception.StreamMQException 发送失败
     */
    public SendResult sendDefault(V data) {
        return send(defaultTopic, null, data);
    }

    /**
     * 发送消息到构造时指定的默认 topic（带 key）。
     *
     * @param key  消息键（可为 null）
     * @param data 消息体（必填）
     * @return 发送结果 {@link SendResult}
     * @throws NullPointerException 如果 data 为 null
     * @throws io.github.streammq.core.exception.StreamMQException 发送失败
     */
    public SendResult sendDefault(K key, V data) {
        return send(defaultTopic, key, data);
    }

    /**
     * 返回默认 topic。
     *
     * @return defaultTopic
     */
    public String getDefaultTopic() {
        return defaultTopic;
    }

    /**
     * 返回底层 {@link StreamMessageTemplate}。
     *
     * @return 消息模板
     */
    public StreamMessageTemplate getTemplate() {
        return template;
    }

    // ===================== 内部方法 =====================

    /**
     * 验证字符串参数非空。
     *
     * @param value 参数值
     * @param name  参数名
     * @return value（去除首尾空白后）
     * @throws NullPointerException 如果 value 为 null
     * @throws IllegalArgumentException 如果 value 为空字符串
     */
    private static String validateNotEmpty(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value.trim();
    }
}
