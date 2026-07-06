package io.github.streammq.core.policy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 重平衡策略 SPI，控制 ConsumerGroup 内分片到 Consumer 的分配算法。
 *
 * <p>默认实现：
 * <ul>
 *   <li>{@code ConsistentHashRebalanceStrategy} - 一致性哈希（默认，减少 Rebalance 时分片迁移）</li>
 *   <li>{@code AverageRebalanceStrategy} - 平均分配（精确均衡但 Rebalance 时变动大）</li>
 * </ul>
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

    /**
     * 分片元信息。
     *
     * @param shardId 分片 ID
     * @param topic 所属 Topic
     * @param consumerGroup 所属消费者组
     */
    record ShardInfo(int shardId, String topic, String consumerGroup) {

        public ShardInfo {
            Objects.requireNonNull(topic, "topic");
            Objects.requireNonNull(consumerGroup, "consumerGroup");
        }
    }

    /**
     * Consumer 实例元信息。
     *
     * @param name 实例名
     * @param host 主机
     * @param port 端口
     * @param lastHeartbeatTimestamp 最近心跳时间戳
     */
    record ConsumerInfo(String name, String host, int port, long lastHeartbeatTimestamp) {

        public ConsumerInfo {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(host, "host");
        }

        /**
         * 返回是否活跃（30s 内有心跳）。
         *
         * @return true 活跃
         */
        public boolean isActive() {
            return System.currentTimeMillis() - lastHeartbeatTimestamp < 30_000L;
        }

        /**
         * 返回不可变空列表（用于无分片场景的便捷返回）。
         *
         * @return 空列表
         */
        public static List<ConsumerInfo> emptyList() {
            return Collections.emptyList();
        }
    }
}
