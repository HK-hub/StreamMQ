package io.github.streammq.core.policy;

import java.time.Duration;
import java.util.Objects;

/**
 * DLQ 消费失败后的决策，由 {@link DlqFailureStrategy#decide} 返回，指示框架如何处理该死信消息。
 *
 * <p>三种内置决策：
 *
 * <ul>
 *   <li>{@link #drop()} - ACK 丢弃（调用 {@link DlqFailureStrategy} 后）
 *   <li>{@link #retry(Duration)} - 按指定延迟重试（写入 retry ZSet，到期后重新投递到 DLQ Stream）
 *   <li>{@link #secondaryDlq()} - 转投到二级死信队列（实现多级 DLQ 归档）
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class DlqFailureDecision {

  /** 决策类型 */
  public enum Type {
    DROP,
    RETRY,
    SECONDARY_DLQ
  }

  private final Type type;
  private final Duration retryDelay;

  private DlqFailureDecision(Type type, Duration retryDelay) {
    this.type = type;
    this.retryDelay = retryDelay;
  }

  /** 丢弃（ACK 后由 DlqFailureStrategy 记录日志/告警） */
  public static DlqFailureDecision drop() {
    return new DlqFailureDecision(Type.DROP, null);
  }

  /** 按指定延迟重试（写入 retry ZSet，DLQ 重试计数 +1） */
  public static DlqFailureDecision retry(Duration delay) {
    Objects.requireNonNull(delay, "delay");
    if (delay.isNegative() || delay.isZero()) {
      throw new IllegalArgumentException("retry delay must be positive: " + delay);
    }
    return new DlqFailureDecision(Type.RETRY, delay);
  }

  /** 转投到二级死信队列 */
  public static DlqFailureDecision secondaryDlq() {
    return new DlqFailureDecision(Type.SECONDARY_DLQ, null);
  }

  public Type type() {
    return type;
  }

  public Duration retryDelay() {
    return retryDelay;
  }

  public boolean isDrop() {
    return type == Type.DROP;
  }

  public boolean isRetry() {
    return type == Type.RETRY;
  }

  public boolean isSecondaryDlq() {
    return type == Type.SECONDARY_DLQ;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DlqFailureDecision that)) return false;
    return type == that.type && Objects.equals(retryDelay, that.retryDelay);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, retryDelay);
  }

  @Override
  public String toString() {
    return Objects.nonNull(retryDelay)
        ? "DlqFailureDecision{" + type + ", delay=" + retryDelay + "}"
        : "DlqFailureDecision{" + type + "}";
  }
}
