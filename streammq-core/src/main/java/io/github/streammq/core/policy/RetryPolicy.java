package io.github.streammq.core.policy;

import io.github.streammq.core.message.Message;
import java.time.Duration;

/**
 * 重试策略 SPI，控制消息消费失败后的重试间隔与是否停止。
 *
 * <p>默认实现：
 *
 * <ul>
 *   <li>{@code FixedArrayRetryPolicy} - 对齐 RocketMQ 16 级固定数组 {@code
 *       [10s,30s,1m,2m,3m,4m,5m,6m,7m,8m,9m,10m,20m,30m,1h,2h]}
 *   <li>{@code ExponentialBackoffRetryPolicy} - 指数退避（initial=1s, multiplier=2.0, max=2h）
 * </ul>
 *
 * <p>用户可自定义实现并通过配置注入。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface RetryPolicy {

  /**
   * 返回下一次重试的延迟时长。
   *
   * @param reconsumeTimes 已重试次数（首次失败为 0）
   * @param message 失败消息
   * @return 延迟时长，{@code Duration.ZERO} 表示立即重试，{@code null} 表示不再重试
   */
  Duration nextRetryDelay(int reconsumeTimes, Message<?> message);

  /**
   * 是否应停止重试（消息将进入 DLQ）。 默认实现：{@code reconsumeTimes >= maxReconsumeTimes}，用户可自定义（如根据消息类型/属性决策）。
   *
   * @param reconsumeTimes 已重试次数
   * @param message 失败消息
   * @return true 停止重试（进入 DLQ）
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
