package io.github.streammq.core.annotation;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.filter.ConsumerFilter;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;

import java.lang.annotation.*;

/**
 * StreamMQ 消费者注解（类级），标注在 {@code StreamMessageConcurrentlyConsumer} /
 * {@code StreamMessageOrderlyConsumer} 实现类上。
 *
 * <p>对齐 RocketMQ {@code @RocketMQMessageListener} 体验。本注解为统一入口，
 * 通过 {@link #messageModel()} 区分并发 / 顺序消费，通过 {@link #dlqMode()}
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
 * @StreamMQConsumer(topic = "order-topic", consumerGroup = "order-cg", dlqMode = true)
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
     * <p>DLQ 模式下（{@link #dlqMode()} 为 true 时），本字段表示原始消费者组名，
     * 用于构造 DLQ Stream Key（{@code streammq:{ns}:dlq:{consumerGroup}}）。
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
     * retry Stream 最大长度（0=不限制，per-topic 覆盖全局配置）。
     *
     * <p>仅对并发消费生效。retry Stream 是 {@code streammq:{ns}:retry:msg:{topic}:{group}}，
     * 设置上限可防止重试消息无限堆积。
     *
     * @return retry Stream 最大长度
     */
    int retryStreamMaxLen() default StreamMQConstants.DEFAULT_RETRY_STREAM_MAX_LEN;

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
     * 每个消费者专属死信消费失败策略（默认 {@link DlqFailureStrategy} 表示使用全局策略）。
     *
     * <p>仅对 {@link #dlqMode()} 为 true 的消费者生效。当死信消息消费也失败时，
     * 框架调用此策略决策 drop/retry/secondaryDlq。
     *
     * @return 死信失败策略类
     */
    Class<? extends DlqFailureStrategy> dlqFailureStrategy() default DlqFailureStrategy.class;

    /**
     * 每个消费者专属过滤器（默认 {@link ConsumerFilter} 表示使用全局过滤器）。
     *
     * <p>过滤器从 Spring 容器中获取实例，支持多个过滤器（逗号分隔）。
     * 过滤器执行顺序：先执行 {@link #selectorExpression()} 对应的内置过滤器（order = -1），
     * 再按 {@link ConsumerFilter#order()} 升序执行自定义过滤器。
     *
     * @return 过滤器类
     */
    Class<? extends ConsumerFilter>[] consumerFilter() default {};

    /**
     * 是否为 DLQ（死信队列）消费者，默认 false。
     *
     * <p>设置为 true 时，消费者从约定的死信 Stream 消费消息：
     * DLQ Stream Key 为 {@code streammq:{ns}:dlq:{consumerGroup}}（对齐 RocketMQ %DLQ%{group}）。
     * 此时 {@link #consumerGroup()} 表示原始消费者组名（用于构造 DLQ Stream Key）。
     *
     * <p>DLQ 消费者收到的消息体中携带原始 topic 字段，可从 {@link io.github.streammq.core.message.Message#getTopic()} 获取。
     *
     * @return true 表示 DLQ 消费者
     */
    boolean dlqMode() default false;

    /**
     * 消费者实例名（可选，默认空字符串表示自动生成）。
     *
     * @return 消费者实例名
     */
    String consumerName() default "";
}
