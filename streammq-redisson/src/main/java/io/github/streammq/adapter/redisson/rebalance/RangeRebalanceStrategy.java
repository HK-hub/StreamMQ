package io.github.streammq.adapter.redisson.rebalance;

import io.github.streammq.core.policy.RebalanceStrategy;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 范围分配 Rebalance 策略，对齐 Kafka {@code RangeAssignor}。
 *
 * <h3>算法</h3>
 *
 * <p>与 {@link AverageRebalanceStrategy} 数量分配逻辑完全相同，差异在于分配结果—— 同一 Consumer 获得的分片 ID
 * 是<b>连续</b>的（输入列表的相邻元素）， 便于按分片范围做批量读取或范围操作。
 *
 * <h3>前提条件</h3>
 *
 * <p>要获得真正的范围连续性，调用方应先对分片 ID 列表<b>排序</b>后传入。 未排序的分片列表将得到与 {@link AverageRebalanceStrategy} 完全相同的结果。
 *
 * <h3>特点</h3>
 *
 * <ul>
 *   <li>分片<b>连续</b>分配给同一 Consumer（与 Average 的交错分配相对）
 *   <li>Consumer 变动时分片迁移量与 Average 相同（较大）
 * </ul>
 *
 * <p>适用场景：需要按连续分片范围做批量处理的场景（如按分片区间批量消费）， 或需要对齐 Kafka RangeAssignor 行为做迁移的场景。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class RangeRebalanceStrategy implements RebalanceStrategy {

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
            // 分片多于消费者：前 remainder 个消费者分到 (n/c+1) 个，其余分到 (n/c) 个
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
        return "range";
    }
}
