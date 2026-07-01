package io.github.streammq.adapter.redisson.rebalance;

import io.github.streammq.core.spi.RebalanceStrategy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 范围分配 Rebalance 策略，对齐 Kafka {@code RangeAssignor}。
 *
 * <p>将分片连续分配给消费者：
 * <ul>
 *   <li>当 {@code shards.size() >= consumers.size()} 时，前 {@code n mod c} 个消费者各分到
 *       {@code floor(n/c) + 1} 个分片，其余各分到 {@code floor(n/c)} 个</li>
 *   <li>当 {@code shards.size() < consumers.size()} 时，前 {@code n} 个消费者各分一个，
 *       其余消费者不分</li>
 * </ul>
 *
 * <p>与 {@link AverageRebalanceStrategy} 在数量均衡上等价，差异在于分片连续分配
 * （同一消费者持有的分片 ID 相邻），便于按分片范围做批量处理。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class RangeRebalanceStrategy implements RebalanceStrategy {

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
