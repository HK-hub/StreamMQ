package io.github.streammq.core.annotation;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.filter.ConsumerFilter;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.serializer.MessageSerializer;
import java.lang.annotation.*;

/**
 * StreamMQ 消费者注解（类级），标注在 {@code StreamMessageConcurrentlyConsumer} /
 * {@code StreamMessageOrderlyConsumer} 实现类上。
 *
 * <p>对齐 RocketMQ {@code @RocketMQMessageListener} 体验。本注解为统一入口，
 * 通过 {@link #messageModel()} 区分并发 / 顺序消费。
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
 *     public ConsumeAction onMessage(Message<Order> message, ConsumeOrderlyContext context) {
 *         processOrder(message.getBody());
 *         return ConsumeAction.SUCCESS;
 *     }
 * }
 *
 * // DLQ 消费请使用 @StreamMQDlqConsumer + DlqMessageConsumer 接口
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
     * <p>设置为 {@link MessageModel#ORDERLY} 时表示顺序消费，需实现 {@link
     * io.github.streammq.core.consumer.StreamMessageOrderlyConsumer}， {@link #shardCount()} 生效。
     *
     * <p>顺序消费实现为「单 Stream + 分片分布式锁」：同一 {@code shardingKey} 路由到同一分片串行消费； 消费失败时在当前线程内按 {@link
     * #maxReconsumeTimes()} 重试，每次失败后按 {@link #suspendCurrentQueueTimeMillis()} 挂起， 保证同分片不越过失败消息
     * （严格有序）；重试耗尽后直接进入 DLQ。
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
     * <p>超时后框架会 ACK 当前消息并调度重试投递。由于消费线程可能仍在执行业务逻辑， 重试消费与原消费可能并发执行，因此业务层必须实现幂等性。
     *
     * <p>仅对并发消费（{@link ConsumeMode#CONCURRENTLY}）生效，顺序消费不支持超时取消。
     *
     * @return 超时毫秒数，0 表示不超时
     */
    long consumeTimeout() default StreamMQConstants.DEFAULT_CONSUME_TIMEOUT_MS;

    /**
     * Tag 过滤表达式（SQL92 风格子集），默认 "*" 表示全部接收。 例如：{@code "tag1 || tag2"} / {@code "tag1 && tag2"}。
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
     * <p>命名空间用于隔离不同环境/租户的 Stream Key，避免 Key 冲突。 命名空间会附加到所有 Redis Key 前缀：{@code streammq:{ns}:...}。
     *
     * <p><b>作用域规则：</b>
     *
     * <ul>
     *   <li>注解中的 {@code namespace} 优先级高于配置文件中的 {@code streammq.namespace}
     *   <li>不同 namespace 下的消息完全隔离（不同 Stream、不同 Consumer Group、不同 Retry ZSet）
     *   <li>namespace 会影响 Consumer Group 命名：Group Key 包含 namespace 前缀
     * </ul>
     *
     * @return 命名空间，空字符串表示使用全局配置
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
     * <p>注：使用 raw type {@code Class<? extends RetryPolicy>}，因为 {@code RetryPolicy.class} 返回的是 raw
     * type，无法直接用于泛型 {@code Class<? extends RetryPolicy<?>>}。
     *
     * @return 重试策略类
     */
    Class<? extends RetryPolicy> retryPolicy() default RetryPolicy.class;

    /**
     * 是否启用消息追踪，默认 false。 设置为 true 时将覆盖全局追踪开关，对该消费者单独启用追踪。
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
     * <p>注：使用 raw type {@code Class<? extends MessageConverter>}，因为 {@code MessageConverter.class}
     * 返回的是 raw type，无法直接用于泛型 {@code Class<? extends MessageConverter<?>>}。
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
    long suspendCurrentQueueTimeMillis() default
            StreamMQConstants.DEFAULT_SUSPEND_CURRENT_QUEUE_TIME_MS;

    /**
     * retry Stream 最大长度（0=不限制，per-topic 覆盖全局配置）。
     *
     * <p>仅对并发消费生效。retry Stream 是 {@code streammq:{ns}:retry:msg:{topic}:{group}}， 设置上限可防止重试消息无限堆积。
     *
     * @return retry Stream 最大长度
     */
    int retryStreamMaxLen() default StreamMQConstants.DEFAULT_RETRY_STREAM_MAX_LEN;

    /**
     * 是否启用消费，默认 true。 设置为 false 时仅注册但不启动 Consumer。
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
     * 每个消费者专属过滤器（默认 {@link ConsumerFilter} 表示使用全局过滤器）。
     *
     * <p>过滤器从 Spring 容器中获取实例，支持多个过滤器（逗号分隔）。 过滤器执行顺序：先执行 {@link #selectorExpression()}
     * 对应的内置过滤器（order = -1）， 再按 {@link ConsumerFilter#order()} 升序执行自定义过滤器。
     *
     * <p><b>与 selectorExpression 的关系：</b>
     *
     * <ul>
     *   <li>{@code selectorExpression} 是内置的 Tag/SQL 过滤，执行优先级最高（order = -1）
     *   <li>{@code consumerFilter} 是自定义过滤器 SPI，执行优先级低于内置过滤器
     *   <li>两者是<b>串联</b>关系：先执行 selectorExpression 过滤，再执行 consumerFilter 过滤
     *   <li>如果用户同时配置了两者，只有同时通过两种过滤的消息才会被消费
     * </ul>
     *
     * @return 过滤器类
     */
    Class<? extends ConsumerFilter>[] consumerFilter() default {};

    /**
     * 消费者实例名（可选，默认空字符串表示自动生成）。
     *
     * @return 消费者实例名
     */
    String consumerName() default "";

    /**
     * 是否为 DLQ 消费者（默认 false）。
     *
     * <p>当设置为 true 时，消费者将从 DLQ Stream 读取消息（而不是原始 Topic Stream）。 适用于希望使用统一 {@link
     * StreamMessageConcurrentlyConsumer} 接口处理 DLQ 消息的场景。
     *
     * <p>注意：更推荐使用 {@link StreamMQDlqConsumer} 注解 + {@link
     * io.github.streammq.core.consumer.DlqMessageConsumer} 接口， 这是更类型安全的方式。
     *
     * @return true 表示 DLQ 消费者
     */
    boolean dlqMode() default false;
}
