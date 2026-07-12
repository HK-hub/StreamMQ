package io.github.streammq.adapter.redisson.rebalance;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.policy.RebalanceStrategy;

import java.util.*;

/**
 * 一致性哈希 Rebalance 策略（默认实现）。
 *
 * <p>使用一致性哈希环将分片映射到 Consumer，减少 Rebalance 时的分片迁移量。
 *
 * <p>算法：
 * <ol>
 *   <li>为每个 Consumer 节点生成 {@code virtualNodes} 个虚拟节点（默认 160），
 *       哈希值 = {@code hash(consumerName#vnIndex)}</li>
 *   <li>构建 {@link TreeMap}（哈希值 → Consumer 名）作为哈希环</li>
 *   <li>对每个分片 ID 计算哈希，沿环顺时针找到第一个节点，即为其归属 Consumer</li>
 * </ol>
 *
 * <p>当 Consumer 数量变化时，仅影响相邻区段分片的归属，迁移量最小。
 *
 * <p>哈希函数使用 FNV-1a 32 位变体，分布均匀、计算快速、无依赖。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class ConsistentHashRebalanceStrategy implements RebalanceStrategy {

    /** 默认虚拟节点数 */
    public static final int DEFAULT_VIRTUAL_NODES = StreamMQConstants.DEFAULT_VIRTUAL_NODES;

    private final int virtualNodes;

    /**
     * 使用默认虚拟节点数（160）。
     */
    public ConsistentHashRebalanceStrategy() {
        this(DEFAULT_VIRTUAL_NODES);
    }

    /**
     * 自定义虚拟节点数。
     *
     * @param virtualNodes 每个真实节点的虚拟节点数，必须 > 0
     */
    public ConsistentHashRebalanceStrategy(int virtualNodes) {
        if (virtualNodes <= 0) {
            throw new IllegalArgumentException("virtualNodes must be positive: " + virtualNodes);
        }
        this.virtualNodes = virtualNodes;
    }

    @Override
    public Map<Integer, String> assign(List<Integer> shards, List<String> consumers, String consumerGroup) {
        Objects.requireNonNull(shards, "shards");
        Objects.requireNonNull(consumers, "consumers");
        Objects.requireNonNull(consumerGroup, "consumerGroup");

        if (shards.isEmpty() || consumers.isEmpty()) {
            return Collections.emptyMap();
        }

        TreeMap<Long, String> ring = buildRing(consumers);
        Map<Integer, String> assignment = new java.util.HashMap<>(shards.size() * 2);
        for (Integer shardId : shards) {
            String owner = findOwner(ring, shardId);
            assignment.put(shardId, owner);
        }
        return assignment;
    }

    private TreeMap<Long, String> buildRing(List<String> consumers) {
        TreeMap<Long, String> ring = new TreeMap<>();
        for (String consumer : consumers) {
            for (int i = 0; i < virtualNodes; i++) {
                String vn = consumer + "#" + i;
                ring.put(fnv1aHash(vn), consumer);
            }
        }
        return ring;
    }

    private String findOwner(TreeMap<Long, String> ring, int shardId) {
        if (ring.isEmpty()) {
            return null;
        }
        long hash = fnv1aHash("shard-" + shardId);
        // 顺时针查找第一个 >= hash 的节点
        Map.Entry<Long, String> entry = ring.ceilingEntry(hash);
        if (Objects.isNull(entry)) {
            // 环绕到环首
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }

    /**
     * FNV-1a 32 位哈希算法。
     *
     * @param key 输入字符串
     * @return 哈希值（无符号 32 位，存于 long）
     */
    private static long fnv1aHash(String key) {
        long hash = 0x811C9DC5L;
        for (int i = 0; i < key.length(); i++) {
            hash ^= key.charAt(i);
            hash *= 0x01000193L;
            hash &= 0xFFFFFFFFL;
        }
        return hash;
    }

    @Override
    public String name() {
        return "consistent-hash";
    }
}
