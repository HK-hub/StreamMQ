/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.filter.expression;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.util.StringUtils;
import lombok.Getter;

/**
 * 属性表达式，用于从消息中获取属性值。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
public class PropertyExpression implements Expression {

    /** RedissonConsumerGroupManager 获取属性名。 */
    private final String propertyName;

    public PropertyExpression(String propertyName) {
        this.propertyName = propertyName;
    }

    @Override
    public boolean evaluate(Message<?> message) {
        return StringUtils.isNotEmpty(getValue(message));
    }

    /**
     * 获取属性值。
     *
     * <p>查找顺序：系统属性 → 用户属性。SQL92 过滤的典型场景是按业务侧 {@code withUserProperty} 写入的属性过滤，因此用户属性必须参与匹配。
     *
     * @param message 消息
     * @return 属性值，可为 null
     */
    public String getValue(Message<?> message) {
        String value = message.getProperties().get(propertyName);
        if (StringUtils.isNotEmpty(value)) {
            return value;
        }
        return message.getUserProperties().get(propertyName);
    }
}
