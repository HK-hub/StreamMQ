/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
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
            case GREATER_THAN -> {
                Integer cmp = compareNumeric(propertyValue, constantValue);
                yield cmp != null && cmp > 0;
            }
            case GREATER_EQUAL -> {
                Integer cmp = compareNumeric(propertyValue, constantValue);
                yield cmp != null && cmp >= 0;
            }
            case LESS_THAN -> {
                Integer cmp = compareNumeric(propertyValue, constantValue);
                yield cmp != null && cmp < 0;
            }
            case LESS_EQUAL -> {
                Integer cmp = compareNumeric(propertyValue, constantValue);
                yield cmp != null && cmp <= 0;
            }
        };
    }

    /**
     * 数值比较：优先按 long 精确比较（覆盖 64 位 ID 场景），任一侧非整数时回退 double； 双侧含 NaN/Infinity 时视为不可比较，返回
     * null（不匹配任何区间比较）。
     *
     * @return 比较结果负/零/正；null 表示不可比较
     */
    private Integer compareNumeric(String propertyValue, String constantValue) {
        try {
            return Long.compare(
                    Long.parseLong(propertyValue.trim()), Long.parseLong(constantValue.trim()));
        } catch (NumberFormatException ignored) {
            // 至少一侧不是纯整数，回退浮点比较
        }
        double p;
        double c;
        try {
            p = Double.parseDouble(propertyValue);
            c = constant.getDoubleValue();
        } catch (NumberFormatException e) {
            return propertyValue.compareTo(constantValue);
        }
        if (Double.isNaN(p) || Double.isNaN(c) || Double.isInfinite(p) || Double.isInfinite(c)) {
            return null;
        }
        return Double.compare(p, c);
    }
}
