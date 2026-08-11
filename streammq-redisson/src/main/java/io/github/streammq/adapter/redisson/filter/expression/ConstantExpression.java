package io.github.streammq.adapter.redisson.filter.expression;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.util.StringUtils;
import java.util.Objects;

/**
 * 常量表达式。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class ConstantExpression implements Expression {

  private final String value;

  public ConstantExpression(String value) {
    this.value = Objects.nonNull(value) ? value.trim() : null;
  }

  @Override
  public boolean evaluate(Message<?> message) {
    return StringUtils.isNotEmpty(value);
  }

  /**
   * 获取常量值。
   *
   * @return 常量值
   */
  public String getValue() {
    return value;
  }

  /**
   * 获取常量值作为 long。
   *
   * @return long 值
   */
  public long getLongValue() {
    if (Objects.isNull(value)) {
      return 0L;
    }
    return Long.parseLong(value.trim());
  }

  /**
   * 获取常量值作为 double。
   *
   * @return double 值
   */
  public double getDoubleValue() {
    if (Objects.isNull(value)) {
      return 0.0;
    }
    return Double.parseDouble(value.trim());
  }
}
