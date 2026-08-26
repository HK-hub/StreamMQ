/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.filter;

import io.github.streammq.core.filter.TagSelectorFilter;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * 简单 Tag 选择器过滤器实现。
 *
 * <p>支持的表达式：
 *
 * <ul>
 *   <li>"tag1" - 精确匹配
 *   <li>"tag1 || tag2" - 或匹配（消息 tag 为其中任一）
 *   <li>"*" 或空 - 匹配全部
 * </ul>
 *
 * <p>一条消息只携带一个 tag，因此 {@code &&}（要求同时等于多个 tag）在语义上不可满足： 构造时遇到多标签 {@code &&} 表达式将直接抛出 {@link
 * IllegalArgumentException}（fail-fast）， 避免静默配置出一个永远不匹配的过滤器。
 *
 * <p>表达式在构造时解析一次，缓存解析结果。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class SimpleTagSelectorFilter extends TagSelectorFilter {

    private final Set<String> orTags;
    private final String singleTag;

    public SimpleTagSelectorFilter(String selectorExpression) {
        super(selectorExpression);
        Set<String> orSet = new HashSet<>();
        String single = null;

        String expr = Objects.isNull(selectorExpression) ? "" : selectorExpression.trim();
        if (!WILD_CARD.equals(expr) && !expr.isEmpty()) {
            if (expr.contains("&&")) {
                throw new IllegalArgumentException(
                        "Tag selector '&&' is unsatisfiable: a message carries exactly one tag,"
                                + " so it can never equal multiple tags. Use '||' for any-of"
                                + " matching, or a SQL92 selector on user properties for"
                                + " conjunctive conditions: "
                                + expr);
            }
            if (expr.contains("||")) {
                String[] parts = expr.split("\\|\\|");
                for (String part : parts) {
                    orSet.add(part.trim());
                }
            } else {
                single = expr;
            }
        }

        this.orTags = orSet.isEmpty() ? null : orSet;
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

        return false;
    }

    @Override
    public String name() {
        return "SimpleTagSelectorFilter";
    }
}
