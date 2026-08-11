package io.github.streammq.core.annotation;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.consumer.DlqMessageConsumer;
import io.github.streammq.core.policy.DlqFailureStrategy;
import java.lang.annotation.*;

/**
 * 死信队列（DLQ）消费者注解（类级），标注在 {@link DlqMessageConsumer} 实现类上。
 *
 * <p>与 {@link StreamMQConsumer} 完全独立，DLQ 消费者不使用 {@code dlqMode} 混用模式。 启动时框架会严格校验：标注本注解的类必须实现 {@link
 * DlqMessageConsumer}，否则启动失败。
 *
 * <p>DLQ 消费失败由 {@link #failureStrategy()} 决策：
 *
 * <ul>
 *   <li><b>drop</b>（默认）：ACK 消息并丢弃，记录 ERROR 日志
 *   <li><b>retry</b>：ACK 当前消息，重新写入 DLQ Stream（有限次重试后 drop）
 *   <li><b>secondaryDlq</b>：ACK 当前消息，转投到二级 DLQ Stream
 * </ul>
 *
 * <b>不会循环</b>：DLQ 消费失败后不会回到原始 Topic。
 *
 * <p>使用示例：
 *
 * <pre>{@code
 * @Component
 * @StreamMQDlqConsumer(consumerGroup = "order-cg")
 * public class OrderDlqConsumer implements DlqMessageConsumer<Order> {
 *     @Override
 *     public void onDlqMessage(Message<Order> msg, ConsumeContext ctx) {
 *         notifyOps("DLQ message: " + msg.getBody());
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
public @interface StreamMQDlqConsumer {

    /** 原始消费者组名（必填），用于构造 DLQ Stream Key：{@code streammq:{ns}:dlq:{consumerGroup}} */
    String consumerGroup();

    /** 命名空间（可选，默认使用全局配置） */
    String namespace() default "";

    /** DLQ 消费失败处理策略实现类（默认 LogAndDropDlqFailureStrategy） */
    Class<? extends DlqFailureStrategy> failureStrategy() default DlqFailureStrategy.class;

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
    double dlqRetryBackoffMultiplier() default
            StreamMQConstants.DEFAULT_DLQ_RETRY_BACKOFF_MULTIPLIER;

    /** 重试延迟上限（毫秒，默认 300000 = 5 分钟） */
    long dlqRetryMaxDelayMs() default StreamMQConstants.DEFAULT_DLQ_RETRY_MAX_DELAY_MS;

    /** 是否启用消费（默认 true） */
    boolean enable() default true;
}
