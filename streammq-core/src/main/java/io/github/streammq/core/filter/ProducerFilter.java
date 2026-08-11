package io.github.streammq.core.filter;

import io.github.streammq.core.message.Message;

/**
 * 生产者消息过滤器 SPI。
 *
 * <p>在消息发送前进行过滤，返回 false 则阻止消息发送。
 *
 * <p>典型使用场景：
 *
 * <ul>
 *   <li>按 tag 过滤消息（如阻止特定 tag 的消息发送）
 *   <li>按 body 内容过滤消息（如阻止不符合业务规则的消息发送）
 *   <li>消息内容校验（如敏感词过滤）
 * </ul>
 *
 * <p>多过滤器按 {@link #order()} 升序执行，任一过滤器返回 false 则消息被阻止。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ProducerFilter {

  /**
   * 判断消息是否应被发送。
   *
   * @param message 待过滤消息
   * @return true 不过滤（继续发送），false 过滤（阻止发送）
   */
  boolean accept(Message<?> message);

  /**
   * 过滤器名称。
   *
   * @return 名称
   */
  default String name() {
    return getClass().getSimpleName();
  }

  /**
   * 过滤器执行顺序（升序，默认 0）。
   *
   * @return 顺序值
   */
  default int order() {
    return 0;
  }
}
