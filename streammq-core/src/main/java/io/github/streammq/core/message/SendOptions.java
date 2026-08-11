package io.github.streammq.core.message;

import io.github.streammq.core.template.StreamMessageTemplate;
import java.io.Serial;
import java.io.Serializable;
import lombok.Getter;

/**
 * 消息发送选项（Builder 模式），用于统一替代 {@link StreamMessageTemplate} 中的多个参数重载方法。
 *
 * <p>使用示例：
 *
 * <pre>{@code
 * SendOptions options = SendOptions.builder()
 *     .timeoutMillis(5000)
 *     .retryTimes(3)
 *     .build();
 * template.syncSend(message, options);
 * }</pre>
 *
 * <p>所有字段均为可选，未设置时使用 {@link StreamMessageTemplate} 的默认值。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
public final class SendOptions implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  /** 默认发送超时（毫秒） */
  public static final long DEFAULT_TIMEOUT_MILLIS =
      StreamMessageTemplate.DEFAULT_SEND_TIMEOUT_MILLIS;

  /** 默认同步重试次数 */
  public static final int DEFAULT_RETRY_TIMES = StreamMessageTemplate.DEFAULT_SYNC_RETRY_TIMES;

  /** 发送超时毫秒数（<= 0 表示使用默认值） */
  private final long timeoutMillis;

  /** 同步发送重试次数（< 0 表示使用默认值） */
  private final int retryTimes;

  private SendOptions(long timeoutMillis, int retryTimes) {
    this.timeoutMillis = timeoutMillis;
    this.retryTimes = retryTimes;
  }

  /**
   * 创建 Builder。
   *
   * @return 新的 Builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * 返回实际生效的超时毫秒数（将 <= 0 的值替换为默认值）。
   *
   * @return 有效的超时毫秒数
   */
  public long effectiveTimeoutMillis() {
    return timeoutMillis > 0 ? timeoutMillis : DEFAULT_TIMEOUT_MILLIS;
  }

  /**
   * 返回实际生效的重试次数（将 < 0 的值替换为默认值）。
   *
   * @return 有效的重试次数
   */
  public int effectiveRetryTimes() {
    return retryTimes >= 0 ? retryTimes : DEFAULT_RETRY_TIMES;
  }

  @Override
  public String toString() {
    return "SendOptions{timeoutMillis=" + timeoutMillis + ", retryTimes=" + retryTimes + '}';
  }

  /** SendOptions Builder。 */
  public static final class Builder {
    private long timeoutMillis = -1;
    private int retryTimes = -1;

    private Builder() {}

    /**
     * 设置发送超时毫秒数。
     *
     * @param timeoutMillis 超时毫秒数（<= 0 表示使用默认值 3000ms）
     * @return this
     */
    public Builder timeoutMillis(long timeoutMillis) {
      this.timeoutMillis = timeoutMillis;
      return this;
    }

    /**
     * 设置同步发送重试次数。
     *
     * @param retryTimes 重试次数（< 0 表示使用默认值 2 次）
     * @return this
     */
    public Builder retryTimes(int retryTimes) {
      this.retryTimes = retryTimes;
      return this;
    }

    /**
     * 构建 SendOptions。
     *
     * @return SendOptions 实例
     */
    public SendOptions build() {
      return new SendOptions(timeoutMillis, retryTimes);
    }
  }
}
