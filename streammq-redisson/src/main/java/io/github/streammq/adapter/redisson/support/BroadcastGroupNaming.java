/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.support;

import io.github.streammq.core.StreamMQConstants;

/**
 * 广播消费者组命名编解码工具。
 *
 * <p>集中维护广播实例身份与 Redis 消费者组名之间的映射，消除散落在 {@code DefaultListenerRegistrar}、 {@code
 * RedissonStreamListener}、{@code RedissonBroadcastGroupRegistry} 三处的重复字符串拼接/解析逻辑：
 *
 * <ul>
 *   <li>消费者名（Redis consumer name）：{@code {group}-{instanceId}}
 *   <li>生效组名（Redis consumer group）：{@code {group}:{group}-{instanceId}}
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
public final class BroadcastGroupNaming {

    private BroadcastGroupNaming() {}

    /** 构造 Redis 消费者名：{@code {group}-{instanceId}}。 */
    public static String consumerName(String group, String instanceId) {
        return group + "-" + instanceId;
    }

    /** 从消费者名反解实例身份；格式不匹配时返回 null。 */
    public static String instanceIdFromConsumerName(String group, String consumerName) {
        if (consumerName == null) {
            return null;
        }
        String prefix = group + "-";
        if (!consumerName.startsWith(prefix) || consumerName.length() == prefix.length()) {
            return null;
        }
        return consumerName.substring(prefix.length());
    }

    /** 从生效组名反解实例身份；格式不匹配时返回 null。 */
    public static String instanceIdFromEffectiveGroup(String group, String effectiveGroup) {
        if (effectiveGroup == null) {
            return null;
        }
        int sep = effectiveGroup.indexOf(StreamMQConstants.BROADCAST_GROUP_SEPARATOR);
        if (sep <= 0) {
            return null;
        }
        String consumerPart = effectiveGroup.substring(sep + 1);
        return instanceIdFromConsumerName(group, consumerPart);
    }
}
