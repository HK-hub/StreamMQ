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

    /**
     * 构造 Redis 消费者名：{@code {group}-{instanceId}}。
     *
     * @param group 消费者组名
     * @param instanceId 广播实例身份（必填，非空白）
     * @return 消费者名
     * @throws IllegalArgumentException 若 instanceId 为 null 或空白
     */
    public static String consumerName(String group, String instanceId) {
        validateInstanceId(instanceId);
        return group + "-" + instanceId;
    }

    /**
     * 构造生效 Redis 消费者组名：{@code {group}:{group}-{instanceId}}。
     *
     * @param group 消费者组名
     * @param instanceId 广播实例身份（必填，非空白）
     * @return 生效组名
     * @throws IllegalArgumentException 若 instanceId 为 null 或空白
     */
    public static String effectiveGroup(String group, String instanceId) {
        validateInstanceId(instanceId);
        return group
                + StreamMQConstants.BROADCAST_GROUP_SEPARATOR
                + consumerName(group, instanceId);
    }

    /**
     * Fail-fast 校验：instanceId 缺失会让字符串拼接静默产出字面量 {@code "null"}， 进而把所有未正确命名的广播实例收敛到同一个 Redis
     * 消费者组（{@code g:g-null}）—— 广播语义静默退化为集群消费，且组名指向一个不存在的实例身份，清扫任务无法回收。 相比"静默错"，这里选择启动期直接失败。
     */
    private static void validateInstanceId(String instanceId) {
        if (instanceId == null || instanceId.isBlank()) {
            throw new IllegalArgumentException(
                    "Broadcast instanceId must be non-blank; got: "
                            + instanceId
                            + ". Build the consumer name with"
                            + " BroadcastGroupNaming.consumerName(group, instanceId) so the"
                            + " broadcast consumer group can be decoded back to a stable"
                            + " identity.");
        }
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
