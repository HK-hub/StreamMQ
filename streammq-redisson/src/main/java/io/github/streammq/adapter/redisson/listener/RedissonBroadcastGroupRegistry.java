/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.listener;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.listener.BroadcastGroupRegistry;
import java.util.Collection;
import java.util.Objects;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link BroadcastGroupRegistry} 的 Redisson 实现：广播组心跳写、僵尸组回收与统计。
 *
 * <p>广播模式下每个容器实例使用一个独立的 Redis 消费者组，且组名随容器实例标识（跨重启不保证相同） 生成。注册表以 {@code topic|effectiveGroup}
 * 为成员、心跳时间为 score，供回收任务识别已死实例的 僵尸组并 {@code XGROUP DESTROY} 释放其占用的 PEL 与元数据。
 *
 * <p><b>依赖倒置：</b>本类是适配层实现；调用方（PEL 认领调度器、运维端点）只依赖 {@link BroadcastGroupRegistry} 接口。用户可注册自定义 Bean
 * 覆盖本实现。
 *
 * <p>本类无状态（除构造注入的 {@code redisson} / {@code namespace}），线程安全，可按 Bean 单例共享。
 *
 * @author StreamMQ Contributors
 * @since 0.1.1
 */
public class RedissonBroadcastGroupRegistry implements BroadcastGroupRegistry {

    /** 广播组心跳过期 TTL（毫秒）：超过该时长无心跳的广播组视为僵尸组，由回收任务销毁 */
    public static final long BROADCAST_GROUP_STALE_TTL_MS = 10L * 60 * 1000;

    /** 单次回收扫描的最大条目数，限制单轮 Redis 往返成本 */
    private static final int DEFAULT_MAX_SWEEP = 100;

    private static final Logger LOG = LoggerFactory.getLogger(RedissonBroadcastGroupRegistry.class);

    private final RedissonClient redisson;
    private final String namespace;
    private final long staleTtlMillis;
    private final int maxSweep;

    /**
     * 使用默认参数构造：过期阈值 {@link #BROADCAST_GROUP_STALE_TTL_MS}，单次回收上限 100 条。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     */
    public RedissonBroadcastGroupRegistry(RedissonClient redisson, String namespace) {
        this(redisson, namespace, BROADCAST_GROUP_STALE_TTL_MS, DEFAULT_MAX_SWEEP);
    }

    /**
     * 全参构造。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param staleTtlMillis 心跳过期阈值（毫秒），{@code <= 0} 时回落默认值
     * @param maxSweep 单次回收扫描上限，{@code <= 0} 时回落默认值
     */
    public RedissonBroadcastGroupRegistry(
            RedissonClient redisson, String namespace, long staleTtlMillis, int maxSweep) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = Objects.isNull(namespace) ? "" : namespace;
        this.staleTtlMillis = staleTtlMillis > 0 ? staleTtlMillis : BROADCAST_GROUP_STALE_TTL_MS;
        this.maxSweep = maxSweep > 0 ? maxSweep : DEFAULT_MAX_SWEEP;
    }

    @Override
    public int sweepStaleBroadcastGroups() {
        try {
            RScoredSortedSet<String> registry =
                    redisson.<String>getScoredSortedSet(StreamMQKeys.broadcastRegistry(namespace));
            long cutoff = System.currentTimeMillis() - staleTtlMillis;
            Collection<String> staleMembers =
                    registry.valueRange(0, true, cutoff, true, 0, maxSweep - 1);
            int removed = 0;
            for (String member : staleMembers) {
                int sepIdx = member.indexOf('|');
                if (sepIdx <= 0) {
                    registry.remove(member);
                    continue;
                }
                String topic = member.substring(0, sepIdx);
                String effectiveGroup = member.substring(sepIdx + 1);
                try {
                    RStream<String, String> stream =
                            redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
                    stream.removeGroup(effectiveGroup);
                    registry.remove(member);
                    removed++;
                    LOG.info(
                            "Swept stale broadcast group: topic={}, group={}",
                            topic,
                            effectiveGroup);
                } catch (RuntimeException ex) {
                    // NOGROUP 等情况说明组已不存在，注册表条目一并清理；其他错误保留条目下轮重试
                    String msg = ex.getMessage();
                    if (Objects.nonNull(msg) && msg.contains("NOGROUP")) {
                        registry.remove(member);
                    } else {
                        LOG.debug(
                                "Sweep stale broadcast group failed: topic={}, group={}: {}",
                                topic,
                                effectiveGroup,
                                ex.getMessage());
                    }
                }
            }
            // 运维可观测性：广播消费组会随实例重启持续增长（每个容器实例一个组），
            // 清理量与残留量必须能被观测到，否则 Redis 内存只会无声上涨。
            if (removed > 0) {
                LOG.info(
                        "Swept {} stale broadcast group(s): namespace={}, remaining={}",
                        removed,
                        namespace,
                        registry.size());
            }
            return removed;
        } catch (RuntimeException ex) {
            // 回收失败不得阻塞 PEL 认领调度：下轮自动重试
            LOG.debug("Sweep stale broadcast groups failed: {}", ex.getMessage());
            return 0;
        }
    }

    @Override
    public long countBroadcastGroups() {
        RScoredSortedSet<String> registry =
                redisson.<String>getScoredSortedSet(StreamMQKeys.broadcastRegistry(namespace));
        return registry.size();
    }
}
