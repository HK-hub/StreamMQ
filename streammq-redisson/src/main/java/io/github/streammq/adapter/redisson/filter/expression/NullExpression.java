/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.filter.expression;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.util.StringUtils;

/**
 * NULL 判断表达式。
 *
 * <p>支持：
 *
 * <ul>
 *   <li>property IS NULL
 *   <li>property IS NOT NULL
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class NullExpression implements Expression {

    private final PropertyExpression property;
    private final boolean isNull;

    public NullExpression(PropertyExpression property, boolean isNull) {
        this.property = property;
        this.isNull = isNull;
    }

    @Override
    public boolean evaluate(Message<?> message) {
        String value = property.getValue(message);
        boolean actualIsNull = StringUtils.isEmpty(value);
        return isNull == actualIsNull;
    }
}
