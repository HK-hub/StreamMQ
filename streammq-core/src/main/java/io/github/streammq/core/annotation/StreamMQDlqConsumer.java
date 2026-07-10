package io.github.streammq.core.annotation;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.policy.DlqFailureStrategy;

import java.lang.annotation.*;

/**
 * 死信队列（DLQ）消费者注解（类级），标注在 {@code DlqMessageConsumer} 实现类上。
 *
 * <p>与 {@link StreamMQConsumer#dlqMode()} 的区别：
 * <ul>
 *   <li>本注解专用于死信消费场景，提供丰富的 DLQ 失败处理配置</li>
 *   <li>消费者实现 {@link io.github.streammq.core.consumer.DlqMessageConsumer} 接口（非 {@code StreamMessageConcurrentlyConsumer}）</li>
 *   <li>onDlqMessage 返回 void，失败时会进入 {@code DlqFailureStrategy#decide} 决策</li>
 *   <li>支持 3 种内置策略 + 自定义策略，策略配置统一抽取</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 1. 默认策略（LogAndDrop）：DLQ 失败仅记录 ERROR 日志后丢弃
 * @StreamMQDlqConsumer(consumerGroup = "order-cg")
 * public class MyDlqConsumer extends AbstractDlqMessageConsumer<Order> { ... }
 *
 * // 2. 有限重试策略：DLQ 失败最多重试 3 次，每次间隔 10s
 * @StreamMQDlqConsumer(consumerGroup = "order-cg",
 *     failureStrategy = "io.github.streammq.adapter.redisson.dlq.LimitedRetryDlqFailureStrategy",
 *     maxDlqRetryAttempts = 3, dlqRetryDelayMs = 10_000)
 * public class MyDlqConsumer extends AbstractDlqMessageConsumer<Order> { ... }
 *
 * // 3. 二级死信策略：DLQ 失败重试 3 次后转投二级死信
 * @StreamMQDlqConsumer(consumerGroup = "order-cg",
 *     failureStrategy = "io.github.streammq.adapter.redisson.dlq.SecondaryDlqFailureStrategy",
 *     secondaryDlqEnabled = true)
 * public class MyDlqConsumer extends AbstractDlqMessageConsumer<Order> { ... }
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface StreamMQDlqConsumer {

    /** 原始消费者组名（必填），用于构造 DLQ Stream Key：{@code streammq:{ns}:dlq:{consumerGroup}} */
    String consumerGroup();

    /** 命名空间（可选，默认使用全局配置） */
    String namespace() default "";

    /** DLQ 消费失败处理策略实现类全限定名（默认 LogAndDropDlqFailureStrategy） */
    String failureStrategy() default StreamMQConstants.DEFAULT_DLQ_FAILURE_STRATEGY;

    /** DLQ 消费失败后的最大重试次数（默认 3） */
    int maxDlqRetryAttempts() default StreamMQConstants.DEFAULT_DLQ_MAX_RETRY_ATTEMPTS;

    /** DLQ 消费重试延迟（毫秒，默认 10000） */
    long dlqRetryDelayMs() default StreamMQConstants.DEFAULT_DLQ_RETRY_DELAY_MS;

    /** 是否启用二级死信队列（默认 false） */
    boolean secondaryDlqEnabled() default StreamMQConstants.DEFAULT_SECONDARY_DLQ_ENABLED;

    /** 二级死信 Stream Key 前缀段（默认 "dlq2"） */
    String secondaryDlqKeyPrefix() default StreamMQConstants.DEFAULT_SECONDARY_DLQ_KEY_PREFIX;

    /** 告警阈值：DLQ 消费失败超过此次数后触发额外告警（默认 1） */
    int dlqAlertThreshold() default StreamMQConstants.DEFAULT_DLQ_ALERT_THRESHOLD;

    /** 重试退避倍数（默认 1.0 = 固定延迟） */
    double dlqRetryBackoffMultiplier() default StreamMQConstants.DEFAULT_DLQ_RETRY_BACKOFF_MULTIPLIER;

    /** 重试延迟上限（毫秒，默认 300000 = 5 分钟） */
    long dlqRetryMaxDelayMs() default StreamMQConstants.DEFAULT_DLQ_RETRY_MAX_DELAY_MS;

    /** 是否启用消费（默认 true） */
    boolean enable() default true;
}
