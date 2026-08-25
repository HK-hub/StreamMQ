/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.filter;

import io.github.streammq.adapter.redisson.filter.expression.Expression;
import io.github.streammq.adapter.redisson.filter.expression.SelectorParser;
import io.github.streammq.core.filter.SqlSelectorFilter;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.util.StringUtils;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 简单 SQL92 选择器过滤器实现。
 *
 * <p>使用 {@link SelectorParser} 在构造时将表达式编译为表达式树， 每次消息处理时仅执行 evaluate 方法。
 *
 * <p>支持的表达式示例：
 *
 * <ul>
 *   <li>"a = 'hello'"
 *   <li>"b > 100"
 *   <li>"c >= 10 && c <= 100"
 *   <li>"d IS NOT NULL"
 *   <li>"e != 'world' OR f < 50"
 *   <li>"(g = 1 AND h = 2) OR i > 100"
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class SimpleSqlSelectorFilter extends SqlSelectorFilter {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleSqlSelectorFilter.class);

    private final Expression expression;

    /**
     * 构造过滤器并在订阅期完成表达式编译校验。
     *
     * <p><b>fail-fast 契约：</b>非通配符表达式若无法解析（语法错误、尾随垃圾等），构造期即抛出 {@link
     * IllegalArgumentException}。此前实现为"解析失败则放行全部消息"——静默把过滤语义 反转为全量投递，属于高危默认行为。
     *
     * @throws IllegalArgumentException 表达式无法解析
     */
    public SimpleSqlSelectorFilter(String selectorExpression) {
        super(selectorExpression);
        // 空串/通配符均表示"不过滤"（accept-all）；仅对非空且非法的表达式 fail-fast
        if (StringUtils.isEmpty(this.selectorExpression)
                || WILD_CARD.equals(this.selectorExpression)) {
            this.expression = null;
        } else {
            Expression parsed = SelectorParser.buildStrict(selectorExpression);
            this.expression =
                    java.util.Objects.requireNonNull(
                            parsed,
                            () -> "Invalid SQL92 selector expression: " + selectorExpression);
        }
    }

    @Override
    protected boolean evaluate(Message<?> message) {
        if (Objects.isNull(expression)) {
            return true;
        }
        try {
            return expression.evaluate(message);
        } catch (Exception e) {
            LOG.warn("Failed to evaluate SQL92 expression: {}", selectorExpression, e);
            return false;
        }
    }

    @Override
    public String name() {
        return "SimpleSqlSelectorFilter";
    }
}
