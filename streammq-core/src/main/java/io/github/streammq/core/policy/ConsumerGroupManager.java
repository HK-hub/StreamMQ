package io.github.streammq.core.policy;

import java.util.List;

/**
 * 消费者组管理器策略接口。
 *
 * <p>负责消费者实例注册、心跳维护、活跃列表查询与分片重平衡。 默认实现位于 {@code streammq-redisson-adapter} 模块，可通过容器构造器注入自定义实现。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ConsumerGroupManager {

    /** 注册当前实例到消费者组。 */
    void register();

    /** 注销当前实例。 */
    void unregister();

    /** 心跳：更新实例最后活跃时间戳。 */
    void heartbeat();

    /**
     * 获取当前活跃的消费者实例列表（剔除超时实例）。
     *
     * @return 活跃实例 ID 列表
     */
    List<String> getActiveConsumers();

    /**
     * 执行 Rebalance 分片分配。
     *
     * @param shardCount 总分片数
     * @return 当前实例分配到的分片 ID 列表
     */
    List<Integer> rebalance(int shardCount);

    /**
     * 返回当前实例是否已注册。
     *
     * @return true 如果已注册
     */
    boolean isRegistered();

    /**
     * 返回实例 ID。
     *
     * @return 实例 ID
     */
    String getInstanceId();

    /**
     * 清理旧实例残留的过期数据（如超时的心跳记录）。
     *
     * <p>广播消费模式下，每次启动时调用此方法清理旧实例的残留数据， 防止消费者组无限增长。默认实现为空操作，子类可覆盖。
     */
    default void cleanupStaleGroups() {
        // default no-op
    }
}
