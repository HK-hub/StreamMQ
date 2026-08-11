package io.github.streammq.core.exception;

import lombok.Getter;

/**
 * 发送超时异常：syncSend 在指定超时时间内未收到 Redis 响应。
 *
 * <p>通常可重试（Redis 临时网络抖动）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
public class ProducerTimeoutException extends StreamMQException {

  private static final long serialVersionUID = 1L;

  /** 超时阈值（毫秒） */
  private final long timeoutMillis;

  /** 目标 Topic */
  private final String topic;

  public ProducerTimeoutException(String message, String topic, long timeoutMillis) {
    super(message);
    this.topic = topic;
    this.timeoutMillis = timeoutMillis;
  }

  public ProducerTimeoutException(
      String message, String topic, long timeoutMillis, Throwable cause) {
    super(message, cause);
    this.topic = topic;
    this.timeoutMillis = timeoutMillis;
  }
}
