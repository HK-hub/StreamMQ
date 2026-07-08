package io.github.streammq.nativeapi;

import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.producer.StreamMessageProducer;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * 原生消息生产者，封装底层 {@link StreamMessageProducer}，提供简化的发送 API。
 *
 * <p>支持同步发送、异步发送、单向发送和延时消息发送。实例由
 * {@link NativeStreamMQ#createProducer(String)} 创建，不建议直接构造。
 *
 * <p>使用示例：
 * <pre>{@code
 * NativeProducer producer = streamMQ.createProducer("my-group");
 * SendResult result = producer.send("order-topic", orderPayload, "created");
 * }</pre>
 *
 * <p>线程安全：委托给底层 {@link StreamMessageProducer}，其实现保证线程安全。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class NativeProducer {

    private final StreamMessageProducer producer;

    /**
     * 包级私有构造，由 {@link NativeStreamMQ#createProducer(String)} 调用。
     *
     * @param producer 底层生产者实例，不能为 null
     */
    NativeProducer(StreamMessageProducer producer) {
        this.producer = Objects.requireNonNull(producer, "producer");
    }

    /**
     * 同步发送消息到指定 Topic。
     *
     * @param topic 主题（必填）
     * @param body  消息体（必填）
     * @param <T>   消息体类型
     * @return 发送结果
     * @throws NullPointerException     如果 topic 或 body 为 null
     * @throws IllegalArgumentException 如果 topic 为空字符串
     * @throws io.github.streammq.core.exception.StreamMQBrokerException 发送失败
     * @throws io.github.streammq.core.exception.ProducerTimeoutException 发送超时
     */
    public <T> SendResult send(String topic, T body) {
        Message<T> msg = buildMessage(topic, body, null);
        return producer.syncSend(msg);
    }

    /**
     * 同步发送带 Tag 的消息。
     *
     * @param topic 主题（必填）
     * @param body  消息体（必填）
     * @param tag   标签（可选，用于消费端过滤），可为 null
     * @param <T>   消息体类型
     * @return 发送结果
     * @throws NullPointerException     如果 topic 或 body 为 null
     * @throws IllegalArgumentException 如果 topic 为空字符串
     * @throws io.github.streammq.core.exception.StreamMQBrokerException 发送失败
     * @throws io.github.streammq.core.exception.ProducerTimeoutException 发送超时
     */
    public <T> SendResult send(String topic, T body, String tag) {
        Message<T> msg = buildMessage(topic, body, tag);
        return producer.syncSend(msg);
    }

    /**
     * 异步发送消息到指定 Topic。
     *
     * <p>返回的 {@link CompletableFuture} 在发送完成（成功或失败）后完成。
     * 调用方可使用 {@code future.thenAccept()} / {@code future.exceptionally()} 处理结果。
     *
     * @param topic 主题（必填）
     * @param body  消息体（必填）
     * @param <T>   消息体类型
     * @return 异步发送结果的 CompletableFuture
     * @throws NullPointerException     如果 topic 或 body 为 null
     * @throws IllegalArgumentException 如果 topic 为空字符串
     */
    public <T> CompletableFuture<SendResult> sendAsync(String topic, T body) {
        Message<T> msg = buildMessage(topic, body, null);
        return producer.asyncSend(msg);
    }

    /**
     * 单向发送消息（fire-and-forget），不等待发送结果，性能最高。
     *
     * <p>发送失败仅记录 WARN 日志，不抛出异常。适用于日志、监控等可容忍丢失的场景。
     *
     * @param topic 主题（必填）
     * @param body  消息体（必填）
     * @param <T>   消息体类型
     * @throws NullPointerException     如果 topic 或 body 为 null
     * @throws IllegalArgumentException 如果 topic 为空字符串
     */
    public <T> void sendOneway(String topic, T body) {
        Message<T> msg = buildMessage(topic, body, null);
        producer.sendOneway(msg);
    }

    /**
     * 发送延时消息，按指定延时级别在延迟时间到达后投递到目标 Topic。
     *
     * <p>底层将消息写入 Redis ZSet（延时队列）+ Hash（payload），由后台
     * {@code DelayMessageScheduler} 定期轮询到时间投递至 Stream。
     *
     * @param topic 主题（必填）
     * @param body  消息体（必填）
     * @param level 延时级别（必填），参见 {@link DelayLevel}
     * @param <T>   消息体类型
     * @return 发送结果（含合成消息 ID）
     * @throws NullPointerException     如果 topic、body 或 level 为 null
     * @throws IllegalArgumentException 如果 topic 为空字符串
     * @throws io.github.streammq.core.exception.StreamMQBrokerException 发送失败
     */
    public <T> SendResult sendDelay(String topic, T body, DelayLevel level) {
        Objects.requireNonNull(level, "delayLevel");
        Message<T> msg = MessageBuilder.<T>withPayload(body)
                .topic(topic)
                .delayLevel(level)
                .build();
        return producer.syncSend(msg);
    }

    /**
     * 构建消息对象。
     *
     * @param topic 主题
     * @param body  消息体
     * @param tag   标签，可为 null
     * @param <T>   消息体类型
     * @return 消息对象
     */
    private <T> Message<T> buildMessage(String topic, T body, String tag) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(body, "body");
        MessageBuilder<T> builder = MessageBuilder.<T>withPayload(body).topic(topic);
        if (tag != null && !tag.isEmpty()) {
            builder.tag(tag);
        }
        return builder.build();
    }
}
