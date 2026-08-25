/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.enums;

/**
 * 消费模式：集群消费 vs 广播消费。
 *
 * <p>决定 ConsumerGroup 内的消息分发方式：
 *
 * <ul>
 *   <li>{@link #CLUSTERING} - 默认，同一 ConsumerGroup 内消息被其中任一 Consumer 消费一次（共享消费）
 *   <li>{@link #BROADCASTING} - 同一 Topic 的每条消息被所有订阅的 Consumer 各处理一次
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum ConsumeMode {

    /**
     * 集群消费（默认）： 同一 ConsumerGroup 下多个 Consumer 共同消费 Topic，每条消息仅被其中一个 Consumer 处理。 利用 Redis Stream 原生
     * ConsumerGroup 实现，自动负载均衡。
     */
    CLUSTERING,

    /**
     * 广播消费： 同一 Topic 的每条消息会被所有订阅的 Consumer 各处理一次。 实现机制：为每个 Consumer 实例创建独立 ConsumerGroup（基于
     * instanceId 拼接）。
     */
    BROADCASTING
}
