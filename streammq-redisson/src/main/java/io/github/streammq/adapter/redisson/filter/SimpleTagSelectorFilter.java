package io.github.streammq.adapter.redisson.filter;

import io.github.streammq.core.filter.TagSelectorFilter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * 简单 Tag 选择器过滤器实现。
 *
 * <p>支持的表达式：
 * <ul>
 *   <li>"tag1" - 精确匹配</li>
 *   <li>"tag1 || tag2" - 或匹配（tag1 或 tag2）</li>
 *   <li>"tag1 && tag2" - 与匹配（tag1 且 tag2）</li>
 * </ul>
 *
 * <p>表达式在构造时解析一次，缓存解析结果。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class SimpleTagSelectorFilter extends TagSelectorFilter {

    private final Set<String> orTags;
    private final Set<String> andTags;
    private final String singleTag;

    public SimpleTagSelectorFilter(String selectorExpression) {
        super(selectorExpression);
        Set<String> orSet = new HashSet<>();
        Set<String> andSet = new HashSet<>();
        String single = null;

        String expr = Objects.isNull(selectorExpression) ? "" : selectorExpression.trim();
        if (!WILD_CARD.equals(expr) && !expr.isEmpty()) {
            if (expr.contains("||")) {
                String[] parts = expr.split("\\|\\|");
                for (String part : parts) {
                    orSet.add(part.trim());
                }
            } else if (expr.contains("&&")) {
                String[] parts = expr.split("&&");
                for (String part : parts) {
                    andSet.add(part.trim());
                }
            } else {
                single = expr;
            }
        }

        this.orTags = orSet.isEmpty() ? null : orSet;
        this.andTags = andSet.isEmpty() ? null : andSet;
        this.singleTag = single;
    }

    @Override
    protected boolean matchTag(String tag) {
        String trimmedTag = tag.trim();

        if (Objects.nonNull(singleTag)) {
            return singleTag.equals(trimmedTag);
        }

        if (Objects.nonNull(orTags)) {
            return orTags.contains(trimmedTag);
        }

        if (Objects.nonNull(andTags)) {
            return andTags.size() == 1 && andTags.contains(trimmedTag);
        }

        return false;
    }

    @Override
    public String name() {
        return "SimpleTagSelectorFilter";
    }
}