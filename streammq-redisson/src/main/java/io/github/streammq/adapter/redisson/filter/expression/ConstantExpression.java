package io.github.streammq.adapter.redisson.filter.expression;

import io.github.streammq.core.message.Message;

/**
 * 常量表达式。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class ConstantExpression implements Expression {

    private final String value;

    public ConstantExpression(String value) {
        this.value = value != null ? value.trim() : null;
    }

    @Override
    public boolean evaluate(Message<?> message) {
        return value != null && !value.isEmpty();
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
        if (value == null) {
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
        if (value == null) {
            return 0.0;
        }
        return Double.parseDouble(value.trim());
    }
}