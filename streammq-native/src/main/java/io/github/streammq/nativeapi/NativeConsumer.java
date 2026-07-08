package io.github.streammq.nativeapi;

import io.github.streammq.core.listener.ListenerConfig;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 原生消息消费者，封装底层 {@link StreamMQListener}，提供简化的 PULL 风格消费 API。
 *
 * <p>通过构造函数绑定 Topic、消费者组和消费者实例名，基于
 * {@link NativeStreamMQ} 的监听器工厂创建底层 Listener。
 *
 * <p>线程安全：委托给底层 {@link StreamMQListener}，其实现保证线程安全。
 * 实现 {@link AutoCloseable}，建议使用 try-with-resources 或手动调用 {@link #close()}。
 *
 * <p>使用示例：
 * <pre>{@code
 * NativeConsumer<String> consumer = new NativeConsumer<>(
 *     streamMQ, "order-topic", "my-group", "consumer-1");
 * try {
 *     while (!Thread.currentThread().isInterrupted()) {
 *         List<Message<String>> messages = consumer.poll(10, Duration.ofSeconds(5));
 *         for (Message<String> msg : messages) {
 *             process(msg.getBody());
 *             consumer.ack(msg.getMessageId());
 *         }
 *     }
 * } finally {
 *     consumer.close();
 * }
 * }</pre>
 *
 * @param <T> 消息体类型（由调用方在消费时指定）
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class NativeConsumer<T> implements AutoCloseable {

    private final StreamMQListener listener;
    private final String topic;
    private final String group;
    private final String consumerName;

    /**
     * 构造消费者实例，自动创建底层 {@link StreamMQListener} 并注册消费者组。
     *
     * <p>消费者组在首次拉取消息时懒初始化（{@code XGROUP CREATE ... MKSTREAM}），
     * 无需预先手动创建。
     *
     * @param nativeStreamMQ NativeStreamMQ 入口实例，不能为 null
     * @param topic          消费的主题，不能为 null 或空
     * @param group          消费者组名，不能为 null 或空
     * @param consumerName   消费者实例名（用于标识不同消费者实例，建议填写），
     *                       可为 null 或空（自动生成 UUID 后缀）
     * @throws NullPointerException     如果 nativeStreamMQ、topic 或 group 为 null
     * @throws IllegalArgumentException 如果 topic 或 group 为空字符串
     * @throws IllegalStateException    如果底层 ListenerFactory 已关闭
     */
    public NativeConsumer(NativeStreamMQ nativeStreamMQ, String topic, String group, String consumerName) {
        Objects.requireNonNull(nativeStreamMQ, "nativeStreamMQ");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.group = Objects.requireNonNull(group, "group");
        if (topic.isEmpty()) {
            throw new IllegalArgumentException("topic must not be empty");
        }
        if (group.isEmpty()) {
            throw new IllegalArgumentException("group must not be empty");
        }
        this.consumerName = consumerName;

        ListenerConfig config = ListenerConfig.builder()
                .topic(topic)
                .consumerGroup(group)
                .consumerName(consumerName)
                .namespace(nativeStreamMQ.getNamespace())
                .build();
        this.listener = nativeStreamMQ.getListenerFactory().createListener(config);
    }

    /**
     * 阻塞拉取一批消息。
     *
     * <p>底层调用 {@code XREADGROUP GROUP group consumer COUNT batchSize BLOCK timeoutMs}
     * 从 Redis Stream 拉取消息。超时后返回空列表（非 null）。
     * 返回的消息类型为 {@code Message<T>}，T 由泛型参数决定，
     * 需要调用方确保 Topic 中的消息体类型与 T 兼容。
     *
     * @param batchSize 批量拉取大小（1-1000）
     * @param timeout   阻塞超时时长，不能为 null
     * @return 消息列表，可能为空（超时或无可消费消息），绝不返回 null
     * @throws IllegalArgumentException 如果 batchSize 不在有效范围
     * @throws NullPointerException     如果 timeout 为 null
     */
    @SuppressWarnings("unchecked")
    public List<Message<T>> poll(int batchSize, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive: " + batchSize);
        }
        List<Message<?>> raw = listener.pullBlock(batchSize, timeout);
        if (raw.isEmpty()) {
            return Collections.emptyList();
        }
        // 类型安全由调用方保证：消费端知道 Topic 中的消息体类型
        return (List<Message<T>>) (List<?>) raw;
    }

    /**
     * 确认单条消息已消费成功。
     *
     * <p>底层调用 {@code XACK} 从 Pending Entries List (PEL) 中移除该消息。
     * 未被 ACK 的消息会在 consumer idle 超时后被其他消费者重新认领（claim）。
     *
     * @param messageId 消息 ID，不能为 null
     * @throws NullPointerException 如果 messageId 为 null
     */
    public void ack(MessageId messageId) {
        Objects.requireNonNull(messageId, "messageId");
        listener.ack(messageId);
    }

    /**
     * 关闭消费者，释放底层 Listener 资源。
     *
     * <p>关闭后不可再调用 {@link #poll} 或 {@link #ack}。
     * 建议使用 try-with-resources 自动关闭。
     */
    @Override
    public void close() {
        listener.close();
    }

    /**
     * 返回消费者绑定的 Topic。
     *
     * @return Topic 名称
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

    /**
     * 返回消费者实例名。
     *
     * @return 消费者实例名（如果构造时未指定则为 null）
     */
    public String getConsumerName() {
        return consumerName;
    }
}
