package io.github.streammq.diagnostics.model;

/**
 * 消息最终状态枚举，表示消息在其生命周期结束时所处的状态。
 *
 * <p>由 {@link io.github.streammq.diagnostics.MessageProfileService} 根据追踪记录推导：
 *
 * <ul>
 *   <li>{@link #SUCCESS} - 消息消费成功
 *   <li>{@link #FAILED} - 消息消费失败（未进入 DLQ）
 *   <li>{@link #DLQ} - 消息已进入死信队列
 *   <li>{@link #PROCESSING} - 消息仍在处理中
 *   <li>{@link #UNKNOWN} - 状态未知（追踪数据不足）
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
public enum MessageStatus {
  /** 消费成功 */
  SUCCESS,
  /** 消费失败 */
  FAILED,
  /** 已进入死信队列 */
  DLQ,
  /** 处理中 */
  PROCESSING,
  /** 状态未知 */
  UNKNOWN
}
