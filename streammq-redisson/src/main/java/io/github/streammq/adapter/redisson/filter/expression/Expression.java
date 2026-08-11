package io.github.streammq.adapter.redisson.filter.expression;

import io.github.streammq.core.message.Message;

/**
 * 表达式节点接口。
 *
 * <p>表达式在构造时解析一次，缓存为表达式树， 每次消息处理时仅执行 evaluate 方法。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface Expression {

  /**
   * 评估表达式。
   *
   * @param message 消息
   * @return 评估结果
   */
  boolean evaluate(Message<?> message);
}
