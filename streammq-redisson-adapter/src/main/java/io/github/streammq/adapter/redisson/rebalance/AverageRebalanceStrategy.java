package io.github.streammq.adapter.redisson.rebalance;

import io.github.streammq.core.spi.RebalanceStrategy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 平均分配 Rebalance 策略。
 *
 * <p>将分片尽可能均匀地分配给 Consumer：
 * <ul>
 *   <li>当 {@code shards.size() >= consumers.size()} 时，每个 Consumer 至少分到
 *       {@code floor(n/c)} 个分片，前 {@code n mod c} 个 Consumer 多分一个</li>
 *   <li>当 {@code shards.size() < consumers.size()} 时，前 {@code n} 个 Consumer 各分一个，
 *       其余 Consumer 不分</li>
 * </ul>
 *
 * <p>优点：精确均衡；缺点：Rebalance 时分片迁移量较大。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class AverageRebalanceStrategy implements RebalanceStrategy {

    @Override
    public Map<Integer, String> assign(List<Integer> shards, List<String> consumers, String consumerGroup) {
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
