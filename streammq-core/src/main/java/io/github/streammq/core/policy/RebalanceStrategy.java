/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.policy;

import java.util.List;
import java.util.Map;

/**
 * 重平衡策略 SPI，控制 ConsumerGroup 内分片到 Consumer 的分配算法。
 *
 * <p>重平衡在 Consumer 实例加入/离开 ConsumerGroup 时触发，由 {@code
 * io.github.streammq.adapter.redisson.manager.RedissonConsumerGroupManager} 调用。
 * 过程中消费循环<b>不暂停</b>——消息仍由旧分配方案消费，新分配方案异步生效。
 *
 * <h3>内置实现选择指南</h3>
 *
 * <table>
 *   <tr><th>策略</th><th>适用场景</th><th>分片迁移</th></tr>
 *   <tr><td>{@code io.github.streammq.adapter.redisson.rebalance.ConsistentHashRebalanceStrategy}（默认）</td>
 *       <td>分片数多、Consumer 频繁变动的场景</td><td>最小</td></tr>
 *   <tr><td>{@code io.github.streammq.adapter.redisson.rebalance.AverageRebalanceStrategy}</td>
 *       <td>追求绝对均衡的场景</td><td>较大</td></tr>
 *   <tr><td>{@code io.github.streammq.adapter.redisson.rebalance.RangeRebalanceStrategy}</td>
 *       <td>需要连续分片范围做批量处理的场景</td><td>较大</td></tr>
 * </table>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface RebalanceStrategy {

    /**
     * 计算分片到 Consumer 的分配结果。
     *
     * @param shards 所有可分配分片
     * @param consumers 所有活跃 Consumer 实例名
     * @param consumerGroup 消费者组名
     * @return 分片到 Consumer 的映射（key=shardId, value=consumerName）
     */
    Map<Integer, String> assign(List<Integer> shards, List<String> consumers, String consumerGroup);

    /**
     * 策略名称。
     *
     * @return 名称
     */
    default String name() {
        return getClass().getSimpleName();
    }
}
