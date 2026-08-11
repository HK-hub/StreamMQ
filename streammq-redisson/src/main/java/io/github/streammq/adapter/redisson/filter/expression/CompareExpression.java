package io.github.streammq.adapter.redisson.filter.expression;

import io.github.streammq.core.message.Message;
import java.util.Objects;

/**
 * 比较表达式。
 *
 * <p>支持的比较操作：
 *
 * <ul>
 *   <li>EQUAL (=)
 *   <li>NOT_EQUAL (!=)
 *   <li>GREATER_THAN (>)
 *   <li>GREATER_EQUAL (>=)
 *   <li>LESS_THAN (<)
 *   <li>LESS_EQUAL (<=)
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class CompareExpression implements Expression {

  public enum CompareType {
    EQUAL,
    NOT_EQUAL,
    GREATER_THAN,
    GREATER_EQUAL,
    LESS_THAN,
    LESS_EQUAL
  }

  private final PropertyExpression property;
  private final ConstantExpression constant;
  private final CompareType compareType;

  public CompareExpression(
      PropertyExpression property, ConstantExpression constant, CompareType compareType) {
    this.property = property;
    this.constant = constant;
    this.compareType = compareType;
  }

  @Override
  public boolean evaluate(Message<?> message) {
    String propertyValue = property.getValue(message);
    if (Objects.isNull(propertyValue)) {
      return false;
    }

    String constantValue = constant.getValue();
    if (Objects.isNull(constantValue)) {
      return false;
    }

    return switch (compareType) {
      case EQUAL -> propertyValue.equals(constantValue);
      case NOT_EQUAL -> !propertyValue.equals(constantValue);
      case GREATER_THAN -> compareNumeric(propertyValue, constantValue) > 0;
      case GREATER_EQUAL -> compareNumeric(propertyValue, constantValue) >= 0;
      case LESS_THAN -> compareNumeric(propertyValue, constantValue) < 0;
      case LESS_EQUAL -> compareNumeric(propertyValue, constantValue) <= 0;
    };
  }

  private int compareNumeric(String propertyValue, String constantValue) {
    try {
      double p = Double.parseDouble(propertyValue);
      double c = constant.getDoubleValue();
      return Double.compare(p, c);
    } catch (NumberFormatException e) {
      return propertyValue.compareTo(constantValue);
    }
  }
}
