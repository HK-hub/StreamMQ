package io.github.streammq.core.annotation;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.enums.AcknowledgeMode;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;

import java.lang.annotation.*;

/**
 * StreamMQ 消费者注解（类级），标注在 {@code StreamMessageConcurrentlyConsumer} /
 * {@code StreamMessageOrderlyConsumer} 实现类上。
 *
 * <p>对齐 RocketMQ {@code @RocketMQMessageListener} 体验。本注解为统一入口，
 * 通过 {@link #messageModel()} 区分并发 / 顺序消费，通过 {@link #dlqConsumerGroup()}
 * 标识 DLQ 消费者。
 *
 * <p>使用示例：
 * <pre>{@code
 * // 并发消费
 * @Component
 * @StreamMQConsumer(topic = "order-topic", consumerGroup = "order-cg")
 * public class OrderConsumer implements StreamMessageConcurrentlyConsumer<Order> {
 *     @Override
 *     public ConsumeAction onMessage(Message<Order> message, ConsumeContext context) {
 *         processOrder(message.getBody());
 *         return ConsumeAction.SUCCESS;
 *     }
 * }
 *
 * // 顺序消费
 * @Component
 * @StreamMQConsumer(topic = "order-topic", consumerGroup = "order-cg",
 *                  messageModel = MessageModel.ORDERLY, shardCount = 8)
 * public class OrderOrderlyConsumer implements StreamMessageOrderlyConsumer<Order> {
 *     @Override
 *     public OrderlyAction onMessage(Message<Order> message, ConsumeOrderlyContext context) {
 *         processOrder(message.getBody());
 *         return OrderlyAction.SUCCESS;
 *     }
 * }
 *
 * // DLQ 消费
 * @Component
 * @StreamMQConsumer(topic = "order-topic", consumerGroup = "order-cg",
 *                  dlqConsumerGroup = "order-dlq-cg")
 * public class OrderDlqHandler implements StreamMessageConcurrentlyConsumer<String> {
 *     @Override
 *     public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
 *         handleDeadLetter(message);
 *         return ConsumeAction.SUCCESS;
 *     }
 * }
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface StreamMQConsumer {

    /**
     * 主题（必填）。
     *
     * @return 主题
     */
    String topic();

    /**
     * 消费者组名（必填）。
     *
     * <p>DLQ 模式下（{@link #dlqConsumerGroup()} 不为空时），本字段表示原始消费者组名，
     * 用于构造 DLQ Stream Key。
     *
     * @return 消费者组
     */
    String consumerGroup();

    /**
     * 消费模式，默认 {@link ConsumeMode#CLUSTERING}。
     *
     * @return 消费模式
     */
    ConsumeMode consumeMode() default ConsumeMode.CLUSTERING;

    /**
     * 消息模型，默认 {@link MessageModel#CONCURRENT}。
     *
     * <p>设置为 {@link MessageModel#ORDERLY} 时表示顺序消费，需实现
     * {@link io.github.streammq.core.consumer.StreamMessageOrderlyConsumer}，
     * {@link #shardCount()} 生效。
     *
     * @return 消息模型
     */
    MessageModel messageModel() default MessageModel.CONCURRENT;

    /**
     * ACK 模式，默认 {@link AcknowledgeMode#AUTO}。
     *
     * @return ACK 模式
     */
    AcknowledgeMode acknowledgeMode() default AcknowledgeMode.AUTO;

    /**
     * 最小消费线程数，默认 1。
     *
     * @return 最小消费线程数
     */
    int consumeThreadMin() default 1;

    /**
     * 最大消费线程数，默认 64。
     *
     * @return 最大消费线程数
     */
    int consumeThreadMax() default StreamMQConstants.DEFAULT_CONSUME_THREAD_MAX;

    /**
     * 最大重试次数，默认 16。
     *
     * @return 最大重试次数
     */
    int maxReconsumeTimes() default StreamMQConstants.DEFAULT_MAX_RECONSUME_TIMES;

    /**
     * 单条消息消费超时（毫秒），默认 30000（30 秒）。
     *
     * @return 超时毫秒数
     */
    long consumeTimeout() default StreamMQConstants.DEFAULT_CONSUME_TIMEOUT_MS;

    /**
     * Tag 过滤表达式（SQL92 风格子集），默认 "*" 表示全部接收。
     * 例如：{@code "tag1 || tag2"} / {@code "tag1 && tag2"}。
     *
     * @return 过滤表达式
     */
    String selectorExpression() default "*";

    /**
     * 序列化器实现类，默认使用全局配置。
     *
     * @return 序列化器类
     */
    Class<? extends MessageSerializer> serializer() default MessageSerializer.class;

    /**
     * 命名空间，默认使用全局配置。
     *
     * @return 命名空间
     */
    String namespace() default "";

    /**
     * 消息过滤类型，默认 {@link SelectorType#TAG}。
     *
     * @return 过滤类型
     */
    SelectorType selectorType() default SelectorType.TAG;

    /**
     * 单次拉取批量大小，默认 32。
     *
     * @return 拉取批量
     */
    int pullBatchSize() default StreamMQConstants.DEFAULT_CONSUME_BATCH_SIZE;

    /**
     * 每个消费者专属重试策略类，默认 {@link RetryPolicy} 表示使用全局策略。
     *
     * <p>注：使用 raw type {@code Class<? extends RetryPolicy>}，因为
     * {@code RetryPolicy.class} 返回的是 raw type，无法直接用于泛型 {@code Class<? extends RetryPolicy<?>>}。
     *
     * @return 重试策略类
     */
    Class<? extends RetryPolicy> retryPolicy() default RetryPolicy.class;

    /**
     * 是否启用消息追踪，默认 false。
     * 设置为 true 时将覆盖全局追踪开关，对该消费者单独启用追踪。
     *
     * @return true 启用追踪
     */
    boolean enableMsgTrace() default false;

    /**
     * Stream 最大长度（0=不限制，per-topic 覆盖全局配置）。
     *
     * @return Stream 最大长度
     */
    int streamMaxLen() default 0;

    /**
     * 每个消费者专属消息转换器（默认表示使用全局）。
     *
     * <p>注：使用 raw type {@code Class<? extends MessageConverter>}，因为
     * {@code MessageConverter.class} 返回的是 raw type，无法直接用于泛型
     * {@code Class<? extends MessageConverter<?>>}。
     *
     * @return 消息转换器类
     */
    Class<? extends MessageConverter> messageConverter() default MessageConverter.class;

    /**
     * 每个消费者专属重平衡策略（默认表示使用全局）。
     *
     * @return 重平衡策略类
     */
    Class<? extends RebalanceStrategy> rebalanceStrategy() default RebalanceStrategy.class;

    /**
     * 拉取间隔（毫秒，0=不间隔）。
     *
     * @return 拉取间隔毫秒
     */
    long pullInterval() default 0L;

    /**
     * 顺序消费挂起时长（毫秒）。
     *
     * @return 挂起毫秒数
     */
    long suspendCurrentQueueTimeMillis() default StreamMQConstants.DEFAULT_SUSPEND_CURRENT_QUEUE_TIME_MS;

    /**
     * 是否启用消费，默认 true。
     * 设置为 false 时仅注册但不启动 Consumer。
     *
     * @return true 启用，false 仅注册
     */
    boolean enable() default true;

    /**
     * 最大 shard 数（顺序消费分区数），默认 4。
     *
     * <p>仅当 {@link #messageModel()} = {@link MessageModel#ORDERLY} 时生效。
     *
     * @return shard 数
     */
    int shardCount() default StreamMQConstants.DEFAULT_SHARD_COUNT;

    /**
     * DLQ 消费者使用的消费者组名（可选，默认空字符串表示非 DLQ 消费者）。
     *
     * <p>当此字段不为空时，表示这是一个 DLQ（死信队列）消费者，从对应的死信 Stream 消费消息。
     * 实际 DLQ 消费者组名：若此字段为空字符串则使用默认 {@code dlq-consumer-{consumerGroup}}，
     * 但通常作为 DLQ 消费者时应显式指定此字段。
     *
     * <p>DLQ Stream Key 为 {@code streammq:{ns}:dlq:{topic}:{consumerGroup}}，
     * 其中 {@code consumerGroup} 为原消费者的组名（即 {@link #consumerGroup()}）。
     *
     * @return DLQ 消费者组名，空字符串表示非 DLQ 消费者
     */
    String dlqConsumerGroup() default "";

    /**
     * DLQ 原始消费者组（仅 DLQ 模式下使用，默认空字符串）。
     *
     * <p>用于构造 DLQ Stream Key 的 group 段。若为空，则使用 {@link #consumerGroup()} 作为原始组名。
     *
     * @return DLQ 原始消费者组名
     */
    String dlqOriginalGroup() default "";

    /**
     * 消费者实例名（可选，默认空字符串表示自动生成）。
     *
     * @return 消费者实例名
     */
    String consumerName() default "";
}
