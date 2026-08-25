/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.tracing.model;

/**
 * 拓扑路由，表示消息从生产者节点到消费者节点的流转路径。
 *
 * <p>用于 {@link TopologyGraph} 中描述消息流向与统计指标。
 *
 * @param from 起始节点名称（生产者）
 * @param to 目标节点名称（消费者）
 * @param messageType 消息类型描述（如 Topic / Tag）
 * @param rate 消息速率（条/秒，基于追踪数据估算）
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public record TopologyRoute(String from, String to, String messageType, double rate) {}
