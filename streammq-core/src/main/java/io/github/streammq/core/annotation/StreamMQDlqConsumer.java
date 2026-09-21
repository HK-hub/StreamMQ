/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.annotation;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.consumer.DlqMessageConsumer;
import io.github.streammq.core.enums.SecondaryDlqMode;
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

    /** DLQ 消费失败处理策略实现类（默认 LogAndDropDlqFailureStrategy = 使用全局 Bean） */
    Class<? extends DlqFailureStrategy> failureStrategy() default DlqFailureStrategy.class;

    /**
     * DLQ 消费失败后的最大重试次数。
     *
     * <p>默认 {@link StreamMQConstants#ANNOTATION_UNSET_INT}（{@code -1}）= <b>未声明</b>，跟随全局配置 {@code
     * streammq.dlq.max-dlq-retry-attempts}（框架默认 3）。显式写 {@code >= 0} 时覆盖全局。
     *
     * <p><b>为什么用哨兵而不是直接写默认值 3：</b>注解默认值与"用户显式写的 3"无法区分，直接写默认值会让 注解默认值静默覆盖用户配置的全局值（配置失效）。
     */
    int maxDlqRetryAttempts() default StreamMQConstants.ANNOTATION_UNSET_INT;

    /**
     * DLQ 消费重试延迟（毫秒）。
     *
     * <p>默认 {@link StreamMQConstants#ANNOTATION_UNSET_LONG}（{@code -1}）= 未声明，跟随全局配置；显式写 {@code >=
     * 0} 时覆盖全局（框架默认 10000）。
     */
    long dlqRetryDelayMs() default StreamMQConstants.ANNOTATION_UNSET_LONG;

    /**
     * 二级死信队列开关（三态）。
     *
     * <p>默认 {@link SecondaryDlqMode#INHERIT} = 未声明，跟随全局配置 {@code
     * streammq.dlq.secondary-dlq-enabled} （框架默认关闭）。{@code ENABLED} / {@code DISABLED} 为
     * per-consumer 显式覆盖。
     */
    SecondaryDlqMode secondaryDlqMode() default SecondaryDlqMode.INHERIT;

    /**
     * 二级死信 Stream Key 前缀段。
     *
     * <p>默认空串 = 未声明，跟随全局配置（框架默认 {@code "dlq2"}）。显式写非空值时覆盖全局； 取值必须通过命名校验（拒绝 {@code :}/{@code
     * *}/{@code {}/{@code }} 等会破坏 Key 结构的字符）。
     */
    String secondaryDlqKeyPrefix() default StreamMQConstants.ANNOTATION_UNSET_STRING;

    /**
     * 告警阈值：DLQ 消费失败达到此次数后触发额外告警。
     *
     * <p>默认 {@link StreamMQConstants#ANNOTATION_UNSET_INT}（{@code -1}）= 未声明，跟随全局配置；显式写 {@code >= 1}
     * 时覆盖全局（框架默认 1）。
     */
    int dlqAlertThreshold() default StreamMQConstants.ANNOTATION_UNSET_INT;

    /**
     * 重试退避倍数。
     *
     * <p>默认 {@link StreamMQConstants#ANNOTATION_UNSET_DOUBLE}（{@code -1.0}）= 未声明，跟随全局配置；显式写 {@code
     * >= 1.0} 时覆盖全局（框架默认 1.0 = 固定延迟）。
     */
    double dlqRetryBackoffMultiplier() default StreamMQConstants.ANNOTATION_UNSET_DOUBLE;

    /**
     * 重试延迟上限（毫秒）。
     *
     * <p>默认 {@link StreamMQConstants#ANNOTATION_UNSET_LONG}（{@code -1}）= 未声明，跟随全局配置；显式写 {@code > 0}
     * 时覆盖全局（框架默认 300000 = 5 分钟）。
     */
    long dlqRetryMaxDelayMs() default StreamMQConstants.ANNOTATION_UNSET_LONG;

    /** 是否启用消费（默认 true） */
    boolean enable() default true;
}
