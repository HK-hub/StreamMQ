/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.policy;

import io.github.streammq.core.message.Message;
import java.time.Duration;

/**
 * 重试策略 SPI，控制消息消费失败后的重试间隔与是否停止。
 *
 * <p><b>框架调用时机（0.1.2 定稿）：</b>消息消费失败（{@code RECONSUME_LATER} / {@code DEFER}）后，框架在把消息写入 retry ZSet
 * 之前依次调用：
 *
 * <ol>
 *   <li>{@link #shouldStopRetry(int, Message)}——先判定停止条件；返回 {@code true} 时消息<b>直接路由到 DLQ</b>（进入原因为
 *       {@code MAX_RETRY}），<b>不再调用</b> {@link #nextRetryDelay(int, Message)}
 *   <li>{@link #nextRetryDelay(int, Message)}——返回 {@code null} 同样视为停止（既有契约保留），消息进 DLQ（原因 {@code
 *       MAX_RETRY}）
 *   <li>否则按返回的延迟写入 retry ZSet 重投
 * </ol>
 *
 * <p><b>重试预算唯一来源：</b>停止与否由本 SPI（配合 {@code max-reconsume-times} 全局/注解配置）决定； {@code reconsumeTimes}
 * 达到预算后，框架同样停止重试。内置策略实现不得把"延时数组长度"等内部结构尺寸当作预算上限。
 *
 * <p>默认实现：
 *
 * <ul>
 *   <li>{@code FixedArrayRetryPolicy} - 对齐 RocketMQ 16 级固定数组 {@code
 *       [10s,30s,1m,2m,3m,4m,5m,6m,7m,8m,9m,10m,20m,30m,1h,2h]}
 *   <li>{@code ExponentialBackoffRetryPolicy} - 指数退避（initial=1s, multiplier=2.0, max=2h）
 * </ul>
 *
 * <p>用户可自定义实现并通过配置注入。实现方必须保证本接口的两个方法<b>不抛出异常</b>（幂等、无副作用）， 否则会中断消费失败后的 ACK/重投路由。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface RetryPolicy {

    /**
     * 返回下一次重试的延迟时长。
     *
     * <p>仅在 {@link #shouldStopRetry(int, Message)} 返回 {@code false} 时被调用。返回 {@code null}
     * 表示不再重试（消息进入 DLQ，原因 {@code MAX_RETRY}）。
     *
     * @param reconsumeTimes 已重试次数（首次失败为 0，保证非负）
     * @param message 失败消息（非 null）
     * @return 延迟时长，{@code Duration.ZERO} 表示立即重试，{@code null} 表示不再重试
     */
    Duration nextRetryDelay(int reconsumeTimes, Message<?> message);

    /**
     * 是否应停止重试（消息将进入 DLQ，原因 {@code MAX_RETRY}）。
     *
     * <p><b>调用语义（0.1.2 定稿）：</b>框架在每次重试调度前调用本方法（早于 {@link #nextRetryDelay(int, Message)}）， 返回 {@code
     * true} 即停止重试并路由到 DLQ；用户可自定义停止条件（如按消息类型/属性/业务错误码决策）。 默认实现： {@code reconsumeTimes >=
     * maxReconsumeTimes}。
     *
     * <p>实现方应注意：{@code reconsumeTimes < 0} 由框架归一化为 0；返回 {@code false} 时框架仍可能因 {@link
     * #nextRetryDelay(int, Message)} 返回 {@code null} 或重试预算耗尽而停止。
     *
     * @param reconsumeTimes 已重试次数（保证非负）
     * @param message 失败消息（非 null）
     * @return true 停止重试（进入 DLQ，原因 {@code MAX_RETRY}）
     */
    boolean shouldStopRetry(int reconsumeTimes, Message<?> message);

    /**
     * 策略名称。
     *
     * @return 名称
     */
    default String name() {
        return getClass().getSimpleName();
    }
}
