package io.github.streammq.adapter.redisson.filter;

import io.github.streammq.adapter.redisson.filter.expression.Expression;
import io.github.streammq.adapter.redisson.filter.expression.SelectorParser;
import io.github.streammq.core.filter.SqlSelectorFilter;
import io.github.streammq.core.message.Message;
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

    public SimpleSqlSelectorFilter(String selectorExpression) {
        super(selectorExpression);
        this.expression = SelectorParser.build(selectorExpression);
        if (Objects.isNull(expression) && !WILD_CARD.equals(selectorExpression)) {
            LOG.warn(
                    "Failed to parse SQL92 expression: {}, filter will accept all messages",
                    selectorExpression);
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
