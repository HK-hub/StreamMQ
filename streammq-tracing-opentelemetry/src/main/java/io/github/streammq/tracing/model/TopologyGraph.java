package io.github.streammq.tracing.model;

import java.util.List;

/**
 * 拓扑图，表示一个 Topic 的完整生产-消费拓扑结构。
 *
 * <p>由 {@link StreamMQTopologyService} 构建，包含：
 *
 * <ul>
 *   <li>所有活跃的生产者节点
 *   <li>所有活跃的消费者节点
 *   <li>生产者到消费者的消息流转路由
 * </ul>
 *
 * @param topic Topic 名称
 * @param producers 生产者节点列表
 * @param consumers 消费者节点列表
 * @param routes 消息流转路由列表
 * @param lastUpdated 拓扑快照时间戳（毫秒）
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public record TopologyGraph(
        String topic,
        List<TopologyNode> producers,
        List<TopologyNode> consumers,
        List<TopologyRoute> routes,
        long lastUpdated) {}
