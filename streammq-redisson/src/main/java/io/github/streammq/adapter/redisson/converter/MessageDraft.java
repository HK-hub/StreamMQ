/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.converter;

import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.util.StringUtils;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 消息装配草稿（仅限 converter 包内使用）。
 *
 * <p>{@link Message} 为不可变对象，反序列化过程中需要一个可变的中间载体逐步填充各字段； 装配完成后通过 {@link #toMessage(String)}
 * 一次性构造最终实例。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
final class MessageDraft<T> {

    String topic;
    String tag;
    String keys;
    String shardingKey;
    String bornHost;
    String transactionId;
    long bornTimestamp;
    int reconsumeTimes;
    T body;
    DelayLevel delayLevel;
    Long delayTimeMillis;

    /** 系统属性（装配期可变） */
    final Map<String, String> properties = new HashMap<>();

    /** 用户属性（装配期可变） */
    final Map<String, String> userProperties = new LinkedHashMap<>();

    /**
     * 构造最终的不可变 Message。
     *
     * @param fallbackTopic Entry 字段未携带 Topic 时使用的回填值
     * @return 完整的不可变 Message
     * @throws SerializationException 当两个来源均未提供 Topic 时
     */
    Message<T> toMessage(String fallbackTopic) {
        String resolvedTopic = StringUtils.isNotEmpty(topic) ? topic : fallbackTopic;
        if (StringUtils.isEmpty(resolvedTopic)) {
            throw new SerializationException(
                    "Stream entry is missing topic field and no fallbackTopic provided", null);
        }
        return new Message<>(
                resolvedTopic,
                tag,
                keys,
                shardingKey,
                properties,
                userProperties,
                body,
                delayLevel,
                delayTimeMillis,
                bornTimestamp,
                bornHost,
                transactionId,
                reconsumeTimes);
    }
}
