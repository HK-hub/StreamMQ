/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.listener;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import java.util.Collection;
import java.util.Objects;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 广播消费组注册表（ZSet）的心跳写、僵尸组回收与统计。
 *
 * <p>广播模式下每个容器实例使用一个独立的 Redis 消费者组，且组名随容器实例标识（跨重启不保证相同）
 * 生成。注册表以 {@code topic|effectiveGroup} 为成员、心跳时间为 score，供回收任务识别已死实例的
 * 僵尸组并 {@code XGROUP DESTROY} 释放其占用的 PEL 与元数据。
 *
 * <p>本类原为 {@link RedissonStreamListener} 的静态方法簇（聚合于「广播组心跳与僵尸组回收」段落），
 * 因不依赖监听器实例状态而被提取为独立组件；心跳写入仍需实例态（topic/组名/模式判定），保留在监听器上。
 *
 * @author StreamMQ Contributors
 * @since 0.1.1
 */
public final class BroadcastGroupRegistry {

    /** 广播组心跳过期 TTL（毫秒）：超过该时长无心跳的广播组视为僵尸组，由回收任务销毁 */
    public static final long BROADCAST_GROUP_STALE_TTL_MS = 10L * 60 * 1000;

    private static final Logger LOG = LoggerFactory.getLogger(BroadcastGroupRegistry.class);

    private BroadcastGroupRegistry() {}

    /**
     * 回收僵尸广播消费者组：心跳超过 {@link #BROADCAST_GROUP_STALE_TTL_MS} 的广播组， 其所属实例已确认死亡（崩溃或长时间停止），XGROUP
     * DESTROY 释放其占用的 PEL 与元数据。
     *
     * <p>由 {@code PelClaimScheduler} 周期性调用（低频）。安全性论证：
     *
     * <ul>
     *   <li>活实例每次拉取都刷新心跳，绝不会被误回收（除非暂停超过 TTL——文档已注明）；
     *   <li>被回收组的 PEL 属于已死实例；广播语义下其它实例持有各自独立副本，不造成业务丢失；
     *   <li>回收后若实例"复活"，{@code ensureGroup} 会重建组并从 0-0 开始——与旧版行为一致的最坏情况。
     * </ul>
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @return 本次回收的组数量
     */
    public static int sweepStaleBroadcastGroups(RedissonClient redisson, String namespace) {
        RScoredSortedSet<String> registry =
                redisson.<String>getScoredSortedSet(StreamMQKeys.broadcastRegistry(namespace));
        long cutoff = System.currentTimeMillis() - BROADCAST_GROUP_STALE_TTL_MS;
        Collection<String> staleMembers = registry.valueRange(0, true, cutoff, true, 0, 100);
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
                LOG.info("Swept stale broadcast group: topic={}, group={}", topic, effectiveGroup);
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
    }

    /**
     * 返回当前注册表中的广播消费组数量（含活跃与尚未被回收的僵尸组）。
     *
     * <p><b>为什么需要这个数字：</b>广播模式下每个容器实例使用一个独立的 Redis 消费者组，且组名随
     * 容器实例标识（跨重启不保证相同）生成。因此组的总数约等于「实例数 × 重启次数」在心跳超时窗口内
     * 的累积量。该数字持续增长意味着实例在崩溃循环，或心跳超时（{@link
     * #BROADCAST_GROUP_STALE_TTL_MS}）配置得过长——两者都会持续占用 Redis 内存（每个组都有自己的
     * PEL）。建议通过 {@link #sweepStaleBroadcastGroups} 的结果与该方法的返回值建立监控。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @return 注册表中的广播组条目数
     */
    public static long countBroadcastGroups(RedissonClient redisson, String namespace) {
        RScoredSortedSet<String> registry =
                redisson.<String>getScoredSortedSet(StreamMQKeys.broadcastRegistry(namespace));
        return registry.size();
    }
}
