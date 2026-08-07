package io.github.streammq.core.policy;

import io.github.streammq.core.StreamMQConstants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DLQ 消费失败处理配置，封装所有可配置项，支持全局默认与 per-consumer 覆盖。
 *
 * <p>配置层次（优先级从高到低）：
 * <ol>
 *   <li>注解属性（{@code @StreamMQDlqConsumer} 或 {@code @StreamMQConsumer} per-consumer）</li>
 *   <li>Spring 全局配置（{@code streammq.dlq.*}）</li>
 *   <li>框架内置默认值（{@link StreamMQConstants}）</li>
 * </ol>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DlqConfig {

    /** DLQ 消费失败处理策略实现类 */
    @Builder.Default
    private Class<? extends DlqFailureStrategy> failureStrategyClass = DlqFailureStrategy.class;

    /** DLQ 消费失败后的最大重试次数（默认 3） */
    @Builder.Default
    private int maxDlqRetryAttempts = StreamMQConstants.DEFAULT_DLQ_MAX_RETRY_ATTEMPTS;

    /** DLQ 消费重试延迟（毫秒，默认 10000） */
    @Builder.Default
    private long dlqRetryDelayMs = StreamMQConstants.DEFAULT_DLQ_RETRY_DELAY_MS;

    /** 是否启用二级死信队列（默认 false） */
    @Builder.Default
    private boolean secondaryDlqEnabled = StreamMQConstants.DEFAULT_SECONDARY_DLQ_ENABLED;

    /** 二级死信 Stream Key 前缀段（默认 "dlq2"，构成 streammq:{ns}:dlq2:{group}） */
    @Builder.Default
    private String secondaryDlqKeyPrefix = StreamMQConstants.DEFAULT_SECONDARY_DLQ_KEY_PREFIX;

    /** 告警阈值：DLQ 消费失败超过此次数后触发额外告警（默认 1，即每次 DLQ 失败都告警） */
    @Builder.Default
    private int dlqAlertThreshold = StreamMQConstants.DEFAULT_DLQ_ALERT_THRESHOLD;

    /** 重试策略的退避倍数（LimitedRetryDlqFailureStrategy 使用，默认 1.0 = 固定延迟） */
    @Builder.Default
    private double dlqRetryBackoffMultiplier = StreamMQConstants.DEFAULT_DLQ_RETRY_BACKOFF_MULTIPLIER;

    /** 重试延迟上限（毫秒，默认 300000 = 5 分钟） */
    @Builder.Default
    private long dlqRetryMaxDelayMs = StreamMQConstants.DEFAULT_DLQ_RETRY_MAX_DELAY_MS;
}
