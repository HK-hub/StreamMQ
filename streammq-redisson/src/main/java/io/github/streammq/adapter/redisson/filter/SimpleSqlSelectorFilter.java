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

    /**
     * 求值 SQL92 表达式。
     *
     * <p><b>失败语义契约：</b>解析期错误已在构造器 fail-fast（{@link SelectorParser#buildStrict}）；
     * 求值期异常（如属性类型与字面量不兼容的类型混淆）属于<b>评估失败</b>而非「不匹配」—— 包装为 {@link IllegalStateException}
     * 上抛，由消费管线按消费者异常同路径处理（重试/DLQ）。 此前实现捕获后返回 {@code false}，求值失败被静默当作不匹配， 消息以 SUCCESS 处理被 ACK 丢弃。
     *
     * @throws IllegalStateException 表达式求值失败（原因为真实异常）
     */
    @Override
    protected boolean evaluate(Message<?> message) {
        if (Objects.isNull(expression)) {
            return true;
        }
        try {
            return expression.evaluate(message);
        } catch (RuntimeException e) {
            throw new IllegalStateException(
                    "SQL92 selector evaluation failed: " + selectorExpression, e);
        }
    }

    @Override
    public String name() {
        return "SimpleSqlSelectorFilter";
    }
}
