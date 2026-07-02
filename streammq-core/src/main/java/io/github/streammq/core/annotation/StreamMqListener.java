package io.github.streammq.core.annotation;

import io.github.streammq.core.StreamMqConstants;
import io.github.streammq.core.enums.AcknowledgeMode;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.spi.MessageConverter;
import io.github.streammq.core.spi.MessageSerializer;
import io.github.streammq.core.spi.RebalanceStrategy;
import io.github.streammq.core.spi.RetryPolicy;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * StreamMQ 消费监听注解（方法级），标注在 {@code StreamMqListener} / {@code StreamMqAckListener} 实现方法上。
 *
 * <p>对齐 RocketMQ {@code @RocketMQMessageListener} 体验。
 *
 * <p>使用示例：
 * <pre>{@code
 * @Component
 * @StreamMqListener(topic = "order-topic", consumerGroup = "order-consumer-group")
 * public class OrderListener implements StreamMqListener<Order> {
 *     @Override
 *     public Action onMessage(Message<Order> message, ConsumerContext context) {
 *         processOrder(message.getBody());
 *         return Action.SUCCESS;
 *     }
 * }
 * }</pre>
 *
 * <p>注：本注解为类级别注解（与 RocketMQ 一致，标注在 Listener 实现类上）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface StreamMqListener {

    /**
     * 主题（必填）。
     *
     * @return 主题
     */
    String topic();

    /**
     * 消费者组名（必填）。
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
    int consumeThreadMax() default StreamMqConstants.DEFAULT_CONSUME_THREAD_MAX;

    /**
     * 最大重试次数，默认 16。
     *
     * @return 最大重试次数
     */
    int maxReconsumeTimes() default StreamMqConstants.DEFAULT_MAX_RECONSUME_TIMES;

    /**
     * 单条消息消费超时（毫秒），默认 30000（30 秒）。
     *
     * @return 超时毫秒数
     */
    long consumeTimeout() default StreamMqConstants.DEFAULT_CONSUME_TIMEOUT_MS;

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
    int pullBatchSize() default StreamMqConstants.DEFAULT_CONSUME_BATCH_SIZE;

    /**
     * 每个监听器专属重试策略类，默认 {@link RetryPolicy} 表示使用全局策略。
     *
     * <p>注：使用 raw type {@code Class<? extends RetryPolicy>}，因为
     * {@code RetryPolicy.class} 返回的是 raw type，无法直接用于泛型 {@code Class<? extends RetryPolicy<?>>}。
     *
     * @return 重试策略类
     */
    Class<? extends RetryPolicy> retryPolicy() default RetryPolicy.class;

    /**
     * 是否启用消息追踪，默认 false。
     * 设置为 true 时将覆盖全局追踪开关，对该监听器单独启用追踪。
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
     * 每个监听器专属消息转换器（默认表示使用全局）。
     *
     * <p>注：使用 raw type {@code Class<? extends MessageConverter>}，因为
     * {@code MessageConverter.class} 返回的是 raw type，无法直接用于泛型
     * {@code Class<? extends MessageConverter<?>>}。
     *
     * @return 消息转换器类
     */
    Class<? extends MessageConverter> messageConverter() default MessageConverter.class;

    /**
     * 每个监听器专属重平衡策略（默认表示使用全局）。
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
    long suspendCurrentQueueTimeMillis() default StreamMqConstants.DEFAULT_SUSPEND_CURRENT_QUEUE_TIME_MS;

    /**
     * 是否启用消费，默认 true。
     * 设置为 false 时仅注册但不启动 Listener。
     *
     * @return true 启用，false 仅注册
     */
    boolean enable() default true;
}
