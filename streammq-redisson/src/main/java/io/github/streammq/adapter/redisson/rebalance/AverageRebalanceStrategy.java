package io.github.streammq.adapter.redisson.rebalance;

import io.github.streammq.core.policy.RebalanceStrategy;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 平均分配 Rebalance 策略，将分片均匀分配给 Consumer。
 *
 * <h3>算法</h3>
 *
 * <ul>
 *   <li>当 {@code shards >= consumers}：前 {@code shards % consumers} 个 Consumer 各分 {@code
 *       shards/consumers + 1} 个，其余各分 {@code shards/consumers} 个
 *   <li>当 {@code shards < consumers}：前 N 个 Consumer 各分 1 个，其余不分
 * </ul>
 *
 * <h3>特点</h3>
 *
 * <ul>
 *   <li><b>精确均衡</b>：各 Consumer 间分片数最多相差 1
 *   <li><b>高迁移量</b>：Consumer 数量变化后，几乎所有分片可能被重新分配（无一致性保证）
 *   <li>分片<b>交错</b>分配：同一 Consumer 获得的分片 ID 不一定连续
 * </ul>
 *
 * <p>适用场景：分片数较少（&lt; 100），Consumer 实例稳定不频繁变动的场景。 如需大规模分片 + 频繁弹性伸缩，请使用 {@link
 * ConsistentHashRebalanceStrategy}。 如需连续分片范围做批量处理，请使用 {@link RangeRebalanceStrategy}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class AverageRebalanceStrategy implements RebalanceStrategy {

    @Override
    public Map<Integer, String> assign(
            List<Integer> shards, List<String> consumers, String consumerGroup) {
        Objects.requireNonNull(shards, "shards");
        Objects.requireNonNull(consumers, "consumers");
        Objects.requireNonNull(consumerGroup, "consumerGroup");

        if (shards.isEmpty() || consumers.isEmpty()) {
            return Collections.emptyMap();
        }

        int shardCount = shards.size();
        int consumerCount = consumers.size();
        Map<Integer, String> assignment = new java.util.HashMap<>(shardCount * 2);

        if (shardCount >= consumerCount) {
            // 分片多于消费者：均分
            int base = shardCount / consumerCount;
            int remainder = shardCount % consumerCount;
            int index = 0;
            for (int c = 0; c < consumerCount; c++) {
                int count = base + (c < remainder ? 1 : 0);
                String consumer = consumers.get(c);
                for (int i = 0; i < count && index < shardCount; i++) {
                    assignment.put(shards.get(index++), consumer);
                }
            }
        } else {
            // 分片少于消费者：前 N 个消费者各分一个
            for (int i = 0; i < shardCount; i++) {
                assignment.put(shards.get(i), consumers.get(i));
            }
        }
        return assignment;
    }

    @Override
    public String name() {
        return "average";
    }
}
