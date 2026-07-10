package io.github.streammq.adapter.redisson.filter.expression;

import io.github.streammq.core.message.Message;

/**
 * 逻辑表达式。
 *
 * <p>支持的逻辑操作：
 * <ul>
 *   <li>AND</li>
 *   <li>OR</li>
 *   <li>NOT</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class LogicExpression implements Expression {

    public enum LogicType {
        AND, OR, NOT
    }

    private final Expression left;
    private final Expression right;
    private final LogicType logicType;

    public LogicExpression(Expression left, Expression right, LogicType logicType) {
        this.left = left;
        this.right = right;
        this.logicType = logicType;
    }

    public LogicExpression(Expression child, LogicType logicType) {
        this.left = child;
        this.right = null;
        this.logicType = logicType;
    }

    @Override
    public boolean evaluate(Message<?> message) {
        return switch (logicType) {
            case AND -> left.evaluate(message) && (right == null || right.evaluate(message));
            case OR -> left.evaluate(message) || (right == null || right.evaluate(message));
            case NOT -> !left.evaluate(message);
        };
    }
}