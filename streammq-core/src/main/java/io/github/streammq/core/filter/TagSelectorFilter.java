package io.github.streammq.core.filter;

import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.message.Message;

/**
 * Tag 选择器过滤器抽象类。
 *
 * <p>提供 TAG 表达式的通用解析框架，具体匹配逻辑由子类实现。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public abstract class TagSelectorFilter implements ExpressionSelectorFilter {

    protected static final String WILD_CARD = "*";

    protected final String selectorExpression;

    protected TagSelectorFilter(String selectorExpression) {
        this.selectorExpression = selectorExpression != null ? selectorExpression.trim() : WILD_CARD;
    }

    @Override
    public boolean accept(Message<?> message) {
        if (WILD_CARD.equals(selectorExpression) || selectorExpression.isEmpty()) {
            return true;
        }
        if (message.getTag() == null || message.getTag().isEmpty()) {
            return false;
        }
        return matchTag(message.getTag());
    }

    /**
     * 匹配 Tag 表达式。
     *
     * @param tag 消息的 tag
     * @return 是否匹配
     */
    protected abstract boolean matchTag(String tag);

    @Override
    public String getSelectorExpression() {
        return selectorExpression;
    }

    @Override
    public SelectorType getSelectorType() {
        return SelectorType.TAG;
    }

    @Override
    public String name() {
        return "tag-selector-filter";
    }

    @Override
    public int order() {
        return -1;
    }
}