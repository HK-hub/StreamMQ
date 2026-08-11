package io.github.streammq.core.exception;

import lombok.Getter;

/**
 * 消费中断异常：Listener 容器在消费过程中被强制停止（如 JVM 关闭、超时）。
 *
 * <p>属于可恢复异常，重启后可继续消费 PEL 中的消息。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
public class ConsumerInterruptedException extends StreamMQException {

  private static final long serialVersionUID = 1L;

  /** 受影响的 ConsumerGroup */
  private final String consumerGroup;

  /** 受影响的 Topic */
  private final String topic;

  public ConsumerInterruptedException(String message, String topic, String consumerGroup) {
    super(message);
    this.topic = topic;
    this.consumerGroup = consumerGroup;
  }

  public ConsumerInterruptedException(
      String message, String topic, String consumerGroup, Throwable cause) {
    super(message, cause);
    this.topic = topic;
    this.consumerGroup = consumerGroup;
  }
}
