package io.github.streammq.kafka;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/**
 * StreamMQ 兼容 Kafka 风格的 Producer。
 *
 * <p>对齐 Kafka {@code KafkaProducer<K, V>} API 风格，提供 {@code send} / {@code flush} / {@code close} 方法。
 * 底层委托 {@link StreamMessageTemplate} 将 {@link ProducerRecord} 转换为 StreamMQ {@link Message} 后发送。
 *
 * <p>使用示例：
 * <pre>{@code
 * StreamMessageTemplate template = ...; // 由业务层注入
 * KafkaProducer<String, String> producer = new KafkaProducer<>(template);
 * ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "hello");
 * SendResult result = producer.send(record);
 * producer.close();
 * }</pre>
 *
 * <p><b>限制说明</b>：
 * <ul>
 *   <li>{@code send} 为同步发送，返回 {@link SendResult}（非 Kafka 的 {@code Future<RecordMetadata>}）</li>
 *   <li>{@code flush} 为 no-op（StreamMQ 不缓冲消息，每次 send 即写入 Redis）</li>
 *   <li>不支持 Kafka 事务 API，事务场景请直接使用 {@code StreamMessageTemplate#executeInTransaction}</li>
 * </ul>
 *
 * @param <K> key 类型（内部转为 String）
 * @param <V> value 类型（消息体类型）
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class KafkaProducer<K, V> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer.class);

    /** 底层 StreamMQ 消息模板 */
    private final StreamMessageTemplate template;

    /** 是否已关闭 */
    private volatile boolean closed = false;

    /**
     * 构造 KafkaProducer，注入底层 {@link StreamMessageTemplate}。
     *
     * @param template StreamMQ 消息模板（必填）
     * @throws NullPointerException 如果 template 为 null
     */
    public KafkaProducer(StreamMessageTemplate template) {
        this.template = Objects.requireNonNull(template, "template");
    }

    /**
     * 同步发送一条消息。
     *
     * <p>将 {@link ProducerRecord} 转换为 StreamMQ {@link Message} 后委托
     * {@link StreamMessageTemplate#syncSend(Message)} 发送。
     *
     * @param record ProducerRecord（必填）
     * @return 发送结果 {@link SendResult}
     * @throws NullPointerException 如果 record 为 null
     * @throws io.github.streammq.core.exception.StreamMQException 发送失败时抛出
     * @throws IllegalStateException 如果 Producer 已关闭
     */
    public SendResult send(ProducerRecord<K, V> record) {
        ensureOpen();
        Objects.requireNonNull(record, "record");

        Message<V> message = toStreamMQMessage(record);
        LOG.debug("Sending message: topic={}, key={}", record.topic(), record.key());
        return template.syncSend(message);
    }

    /**
     * 刷新缓冲区（no-op）。
     *
     * <p>StreamMQ 底层不缓冲消息，每次 {@link #send} 即直接写入 Redis Stream，
     * 因此本方法为空操作，仅为 API 兼容保留。
     */
    public void flush() {
        ensureOpen();
        // StreamMQ 不缓冲消息，无需 flush
        LOG.debug("flush called (no-op for StreamMQ)");
    }

    /**
     * 关闭 Producer，释放底层资源。
     *
     * <p>注意：此方法不会关闭传入的 {@link StreamMessageTemplate}（由外部管理其生命周期）。
     * 仅标记此 Producer 实例已关闭，后续调用 {@link #send} 或 {@link #flush} 将抛出异常。
     */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            LOG.debug("KafkaProducer closed");
        }
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
     * 将 {@link ProducerRecord} 转换为 StreamMQ {@link Message}。
     *
     * @param record ProducerRecord
     * @return Message 实例
     */
    private Message<V> toStreamMQMessage(ProducerRecord<K, V> record) {
        MessageBuilder<V> builder = MessageBuilder.<V>withTopic(record.topic())
            .body(record.value());

        if (record.key() != null) {
            builder.keys(record.key().toString());
        }

        Map<String, String> headers = record.headers();
        if (!headers.isEmpty()) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                builder.userProperty(entry.getKey(), entry.getValue());
            }
        }

        return builder.build();
    }

    /**
     * 确保 Producer 未关闭。
     *
     * @throws IllegalStateException 如果已关闭
     */
    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("KafkaProducer is closed");
        }
    }
}
