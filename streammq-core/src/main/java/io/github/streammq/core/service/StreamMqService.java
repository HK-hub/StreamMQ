package io.github.streammq.core.service;

import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.message.BatchMessage;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMqTemplate;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * MQ 消息发送服务，封装 {@link StreamMqTemplate} 提供更简洁的 API。
 *
 * <p>用户无需手动构造 {@link Message} 对象，只需传入 body 和 topic 即可发送。
 * 类似 RocketMQ 的 DefaultMQPushProducer 封装层。
 *
 * <p>使用示例：
 * <pre>{@code
 * @StreamMqProducer
 * private StreamMqService mqService;
 *
 * // 同步发送
 * mqService.send("orders", order);
 * mqService.send("orders", order, "tagA");
 * mqService.send("orders", order, "tagA", "order-key-123");
 *
 * // 异步发送
 * CompletableFuture<SendResult> future = mqService.asyncSend("orders", order);
 *
 * // 单向发送
 * mqService.sendOneway("orders", order);
 *
 * // 批量发送
 * mqService.sendBatch("orders", List.of(order1, order2, order3));
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@RequiredArgsConstructor
public class StreamMqService {

    private final StreamMqTemplate<?> template;

    /**
     * 将通配符类型的模板强转为带具体类型参数的模板，便于在泛型发送方法中调用。
     *
     * <p>由于 {@link StreamMqTemplate} 的发送方法形参为 {@code Message<T>}，
     * 持有 {@code StreamMqTemplate<?>} 时无法直接传入任意 {@code Message<T>}，
     * 需通过此处的强转解除类型捕获限制。
     *
     * @param <T> 消息体类型
     * @return 类型化后的模板
     */
    @SuppressWarnings("unchecked")
    private <T> StreamMqTemplate<T> typedTemplate() {
        return (StreamMqTemplate<T>) template;
    }

    // ===================== 同步发送 =====================

    /**
     * 同步发送消息。
     *
     * @param topic 主题
     * @param body 消息体
     * @param <T> 消息体类型
     * @return 发送结果
     */
    public <T> SendResult send(String topic, T body) {
        return this.<T>typedTemplate().syncSend(MessageBuilder.<T>withTopic(topic).body(body).build());
    }

    /**
     * 同步发送消息（带 Tag）。
     */
    public <T> SendResult send(String topic, T body, String tag) {
        return this.<T>typedTemplate().syncSend(MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build());
    }

    /**
     * 同步发送消息（带 Tag 和 Keys）。
     */
    public <T> SendResult send(String topic, T body, String tag, String keys) {
        return this.<T>typedTemplate().syncSend(MessageBuilder.<T>withTopic(topic).body(body)
            .tag(tag).keys(keys).build());
    }

    /**
     * 同步发送消息（带 Tag、Keys 和 ShardingKey）。
     */
    public <T> SendResult send(String topic, T body, String tag, String keys, String shardingKey) {
        return this.<T>typedTemplate().syncSend(MessageBuilder.<T>withTopic(topic).body(body)
            .tag(tag).keys(keys).shardingKey(shardingKey).build());
    }

    /**
     * 同步发送消息（带超时）。
     */
    public <T> SendResult send(String topic, T body, long timeoutMillis) {
        return this.<T>typedTemplate().syncSend(MessageBuilder.<T>withTopic(topic).body(body).build(), timeoutMillis);
    }

    // ===================== 异步发送 =====================

    /**
     * 异步发送消息。
     */
    public <T> CompletableFuture<SendResult> asyncSend(String topic, T body) {
        return this.<T>typedTemplate().asyncSend(MessageBuilder.<T>withTopic(topic).body(body).build());
    }

    /**
     * 异步发送消息（带 Tag）。
     */
    public <T> CompletableFuture<SendResult> asyncSend(String topic, T body, String tag) {
        return this.<T>typedTemplate().asyncSend(MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build());
    }

    // ===================== 单向发送 =====================

    /**
     * 单向发送消息（fire-and-forget）。
     */
    public <T> void sendOneway(String topic, T body) {
        this.<T>typedTemplate().sendOneway(MessageBuilder.<T>withTopic(topic).body(body).build());
    }

    /**
     * 单向发送消息（带 Tag）。
     */
    public <T> void sendOneway(String topic, T body, String tag) {
        this.<T>typedTemplate().sendOneway(MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build());
    }

    // ===================== 批量发送 =====================

    /**
     * 批量发送消息（同 Topic）。
     */
    public <T> List<SendResult> sendBatch(String topic, List<T> bodies) {
        Objects.requireNonNull(bodies, "bodies");
        if (bodies.isEmpty()) {
            throw new IllegalArgumentException("bodies list is empty");
        }
        List<Message<T>> messages = bodies.stream()
            .map(body -> MessageBuilder.<T>withTopic(topic).body(body).build())
            .toList();
        BatchMessage<T> batch = BatchMessage.<T>withTopic(topic).addAll(messages).build();
        return this.<T>typedTemplate().syncSendBatch(batch);
    }

    /**
     * 批量发送消息（同 Topic，带 Tag）。
     */
    public <T> List<SendResult> sendBatch(String topic, String tag, List<T> bodies) {
        Objects.requireNonNull(bodies, "bodies");
        if (bodies.isEmpty()) {
            throw new IllegalArgumentException("bodies list is empty");
        }
        List<Message<T>> messages = bodies.stream()
            .map(body -> MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build())
            .toList();
        BatchMessage<T> batch = BatchMessage.<T>withTopic(topic).addAll(messages).build();
        return this.<T>typedTemplate().syncSendBatch(batch);
    }

    // ===================== 延时消息 =====================

    /**
     * 发送延时消息。
     *
     * @param topic 主题
     * @param body 消息体
     * @param delayLevel 延时级别
     */
    public <T> SendResult sendDelay(String topic, T body, DelayLevel delayLevel) {
        return this.<T>typedTemplate().syncSend(MessageBuilder.<T>withTopic(topic).body(body).delayLevel(delayLevel).build());
    }

    /**
     * 发送延时消息（自定义延时时间）。
     *
     * @param topic 主题
     * @param body 消息体
     * @param delayTimeMillis 延时毫秒数
     */
    public <T> SendResult sendDelay(String topic, T body, long delayTimeMillis) {
        return this.<T>typedTemplate().syncSend(
            MessageBuilder.<T>withTopic(topic).body(body).delayTimeMillis(delayTimeMillis).build());
    }
}
