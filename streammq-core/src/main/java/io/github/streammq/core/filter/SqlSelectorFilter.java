package io.github.streammq.core.filter;

import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.message.Message;

import java.util.Objects;

/**
 * SQL92 选择器过滤器抽象类。
 *
 * <p>提供 SQL92 表达式的通用解析框架，表达式在构造时编译为 AST，
 * 具体匹配逻辑由子类实现。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public abstract class SqlSelectorFilter implements ExpressionSelectorFilter {

    protected static final String WILD_CARD = "*";

    protected final String selectorExpression;

    protected SqlSelectorFilter(String selectorExpression) {
        this.selectorExpression = Objects.nonNull(selectorExpression) ? selectorExpression.trim() : WILD_CARD;
    }

    @Override
    public boolean accept(Message<?> message) {
        if (WILD_CARD.equals(selectorExpression)) {
            return true;
        }
        return evaluate(message);
    }

    /**
     * 评估 SQL92 表达式。
     *
     * @param message 消息
     * @return 是否匹配
     */
    protected abstract boolean evaluate(Message<?> message);

    @Override
    public String getSelectorExpression() {
        return selectorExpression;
    }

    @Override
    public SelectorType getSelectorType() {
        return SelectorType.SQL92;
    }

    @Override
    public String name() {
        return "sql-selector-filter";
    }

    @Override
    public int order() {
        return -1;
    }
}