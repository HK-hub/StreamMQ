/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.rebalance;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.policy.RebalanceStrategy;
import java.util.*;

/**
 * 一致性哈希 Rebalance 策略（默认实现），最小化 Consumer 变动时的分片迁移。
 *
 * <h3>算法</h3>
 *
 * <ol>
 *   <li>为每个 Consumer 生成 {@code virtualNodes} 个虚拟节点（默认 160）， 哈希值 = {@code
 *       fnv1a(consumerName#vnIndex)}
 *   <li>构建 {@link TreeMap}（哈希值 → Consumer 名）作为哈希环
 *   <li>对每个分片计算哈希（{@code fnv1a("shard-" + shardId)}）， 沿环顺时针查找第一个节点，即为其归属 Consumer
 * </ol>
 *
 * <h3>Consumer 数量变化行为</h3>
 *
 * <ul>
 *   <li><b>添加 Consumer</b>：新节点插入哈希环，仅接管相邻区段的部分分片（约 {@code 1/(N+1)} 比例）
 *   <li><b>移除 Consumer</b>：该节点的分片被顺时针的下一个节点接管，其他分区不受影响
 *   <li>虚拟节点越多负载越均衡，但内存和计算开销越大。默认 160 是经验值
 * </ul>
 *
 * <h3>特点</h3>
 *
 * <ul>
 *   <li><b>最小迁移</b>：Consumer 变化时仅影响相邻区段的分片
 *   <li><b>近似均衡</b>：分布不完全均匀（标准差约 5-10%，取决于虚拟节点数和分片数）
 *   <li><b>哈希函数</b>：FNV-1a 32 位变体，分布均匀、计算快速、无外部依赖
 * </ul>
 *
 * <p>适用场景：分片数多（&gt; 100）、Consumer 频繁弹性伸缩的云原生场景。 此为 StreamMQ 默认策略。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class ConsistentHashRebalanceStrategy implements RebalanceStrategy {

    /** 默认虚拟节点数 */
    public static final int DEFAULT_VIRTUAL_NODES = StreamMQConstants.DEFAULT_VIRTUAL_NODES;

    /** 虚拟节点名称分隔符（改变会重新分布所有分片，需保持稳定） */
    private static final String VIRTUAL_NODE_SEPARATOR = StreamMQConstants.VIRTUAL_NODE_SEPARATOR;

    /** 分片哈希盐前缀（改变会重新分布所有分片，需保持稳定） */
    private static final String SHARD_HASH_PREFIX = "shard-";

    private final int virtualNodes;

    /** 使用默认虚拟节点数（160）。 */
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
    public Map<Integer, String> assign(
            List<Integer> shards, List<String> consumers, String consumerGroup) {
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
                String vn = consumer + VIRTUAL_NODE_SEPARATOR + i;
                ring.put(fnv1aHash(vn), consumer);
            }
        }
        return ring;
    }

    private String findOwner(TreeMap<Long, String> ring, int shardId) {
        if (ring.isEmpty()) {
            return null;
        }
        long hash = fnv1aHash(SHARD_HASH_PREFIX + shardId);
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
