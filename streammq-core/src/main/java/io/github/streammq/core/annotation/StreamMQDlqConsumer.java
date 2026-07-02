package io.github.streammq.core.annotation;

import io.github.streammq.core.consumer.StreamMessageConsumer;
import io.github.streammq.core.enums.AcknowledgeMode;
import io.github.streammq.core.spi.MessageConverter;
import io.github.streammq.core.spi.MessageSerializer;

import java.lang.annotation.*;

/**
 * DLQ（死信队列）消费者注解。
 *
 * <p>标注在 {@link StreamMessageConsumer} 实现类上，
 * 用于消费指定 (topic, consumerGroup) 的死信消息。
 *
 * <p>DLQ Stream Key 为 {@code streammq:{ns}:dlq:{topic}:{consumerGroup}}，
 * 与业务消息 Stream 完全隔离。DLQ 消费者使用独立的消费者组名（默认
 * {@code dlq-consumer-{originalGroup}}），不会影响原消费者。
 *
 * <p>使用示例：
 * <pre>{@code
 * @StreamMqDlqConsumer(topic = "order-topic", consumerGroup = "order-consumer-group")
 * public class OrderDlqHandler implements StreamMqConsumer<String> {
 *     @Override
 *     public Action onMessage(Message<String> message, ConsumerContext context) {
 *         // 处理死信消息：记录日志、告警、人工补偿等
 *         return Action.SUCCESS;
 *     }
 * }
 * }</pre>
 *
 * <p>注：DLQ 消费者消费失败后默认直接丢弃（ACK），不再进入重试/DLQ 循环，
 * 避免死信消息无限循环。如需自定义失败处理，请在 Consumer 内部捕获异常并处理。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface StreamMQDlqConsumer {

    /**
     * 原始主题（必填），DLQ Stream 的 topic 段。
     *
     * @return 原始主题
     */
    String topic();

    /**
     * 原始消费者组名（必填），DLQ Stream 的 group 段。
     *
     * @return 原始消费者组
     */
    String consumerGroup();

    /**
     * DLQ 消费者使用的消费者组名（可选，默认 {@code dlq-consumer-{consumerGroup}}）。
     *
     * @return DLQ 消费者组名
     */
    String dlqConsumerGroup() default "";

    /**
     * 命名空间（可选，默认使用全局配置）。
     *
     * @return 命名空间
     */
    String namespace() default "";

    /**
     * 消费者实例名（可选，默认自动生成）。
     *
     * @return 消费者实例名
     */
    String consumerName() default "";

    /**
     * 消费线程最小数，默认 1。
     *
     * @return 最小消费线程数
     */
    int consumeThreadMin() default 1;

    /**
     * 消费线程最大数，默认 16。
     *
     * @return 最大消费线程数
     */
    int consumeThreadMax() default 16;

    /**
     * 单条消息消费超时（毫秒），默认 30000（30 秒）。
     *
     * @return 超时毫秒数
     */
    long consumeTimeout() default 30000L;

    /**
     * 单次拉取批量大小，默认 32。
     *
     * @return 拉取批量
     */
    int pullBatchSize() default 32;

    /**
     * 最大重试次数（DLQ 消息再次失败后的处理，默认 0 表示不重试直接丢弃）。
     *
     * @return 最大重试次数
     */
    int maxReconsumeTimes() default 0;

    /**
     * 确认模式，默认 {@link AcknowledgeMode#AUTO}。
     *
     * @return ACK 模式
     */
    AcknowledgeMode acknowledgeMode() default AcknowledgeMode.AUTO;

    /**
     * 序列化器类（默认使用全局配置）。
     *
     * @return 序列化器类
     */
    Class<? extends MessageSerializer> serializer() default MessageSerializer.class;

    /**
     * 消息转换器类（默认使用全局配置）。
     *
     * @return 消息转换器类
     */
    Class<? extends MessageConverter> messageConverter() default MessageConverter.class;

    /**
     * 是否启用，默认 true。
     * 设置为 false 时仅注册但不启动 Consumer。
     *
     * @return true 启用，false 仅注册
     */
    boolean enable() default true;
}
