/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.filter;

import io.github.streammq.core.enums.SelectorType;

/**
 * 表达式选择器过滤器接口。
 *
 * <p>定义基于 selectorExpression 和 selectorType 的消息过滤能力， 表达式在构造时解析一次，缓存解析结果，避免每次消息处理时重复解析。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ExpressionSelectorFilter extends ConsumerFilter {

    /**
     * 获取选择器表达式。
     *
     * @return 表达式字符串
     */
    String getSelectorExpression();

    /**
     * 获取选择器类型。
     *
     * @return 选择器类型
     */
    SelectorType getSelectorType();
}
