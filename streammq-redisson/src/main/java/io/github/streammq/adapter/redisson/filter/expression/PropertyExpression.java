package io.github.streammq.adapter.redisson.filter.expression;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.util.StringUtils;
import lombok.Getter;

/**
 * 属性表达式，用于从消息中获取属性值。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
public class PropertyExpression implements Expression {

  /** RedissonConsumerGroupManager 获取属性名。 */
  private final String propertyName;

  public PropertyExpression(String propertyName) {
    this.propertyName = propertyName;
  }

  @Override
  public boolean evaluate(Message<?> message) {
    String value = message.getProperties().get(propertyName);
    return StringUtils.isNotEmpty(value);
  }

  /**
   * 获取属性值。
   *
   * @param message 消息
   * @return 属性值，可为 null
   */
  public String getValue(Message<?> message) {
    return message.getProperties().get(propertyName);
  }
}
