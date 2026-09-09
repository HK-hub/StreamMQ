/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.listener;

import io.github.streammq.adapter.redisson.support.BroadcastGroupNaming;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.broadcast.BroadcastInstanceLease;
import io.github.streammq.core.broadcast.BroadcastInstanceRegistry;
import io.github.streammq.core.listener.BroadcastGroupRegistry;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
    public static final int DEFAULT_MAX_SWEEP = 100;

    private static final Logger LOG = LoggerFactory.getLogger(RedissonBroadcastGroupRegistry.class);

    private final RedissonClient redisson;
    private final String namespace;
    private final long staleTtlMillis;
    private final int maxSweep;

    /**
     * 广播实例注册中心（可选）：注入后，本回收任务会顺带清扫<b>超过回收宽限期</b>的实例身份槽位。
     *
     * <p>两者分工必须分清，否则会互相破坏：
     *
     * <ul>
     *   <li>本类按 {@code staleTtlMillis}（默认 10 分钟）销毁<b>僵尸消费者组</b>——针对身份漂移时代遗留的组；
     *   <li>实例注册中心按 {@code reclaimGraceMillis}（默认 7 天）销毁<b>身份槽位</b>——必须远长于本 TTL，
     *       否则一次稍长的重启就会永久丢掉广播消费位点。
     * </ul>
     */
    private final BroadcastInstanceRegistry instanceRegistry;

    /** 广播实例租约超时（毫秒） */
    private final long instanceLeaseTimeoutMillis;

    /** 广播实例回收宽限期（毫秒） */
    private final long instanceReclaimGraceMillis;

    /**
     * 使用默认参数构造：过期阈值 {@link #BROADCAST_GROUP_STALE_TTL_MS}，单次回收上限 100 条，不启用实例槽位清扫。
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
        this(
                redisson,
                namespace,
                staleTtlMillis,
                maxSweep,
                null,
                io.github.streammq.core.StreamMQConstants.DEFAULT_BROADCAST_LEASE_TIMEOUT_MS,
                io.github.streammq.core.StreamMQConstants.DEFAULT_BROADCAST_RECLAIM_GRACE_MS);
    }

    /**
     * 全参构造（启用广播实例身份槽位清扫）。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param staleTtlMillis 僵尸组过期阈值（毫秒），{@code <= 0} 时回落默认值
     * @param maxSweep 单次回收扫描上限，{@code <= 0} 时回落默认值
     * @param instanceRegistry 广播实例注册中心，null 表示不启用身份槽位清扫
     * @param instanceLeaseTimeoutMillis 实例租约超时（毫秒）
     * @param instanceReclaimGraceMillis 实例回收宽限期（毫秒）
     */
    public RedissonBroadcastGroupRegistry(
            RedissonClient redisson,
            String namespace,
            long staleTtlMillis,
            int maxSweep,
            BroadcastInstanceRegistry instanceRegistry,
            long instanceLeaseTimeoutMillis,
            long instanceReclaimGraceMillis) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = Objects.isNull(namespace) ? "" : namespace;
        this.staleTtlMillis = staleTtlMillis > 0 ? staleTtlMillis : BROADCAST_GROUP_STALE_TTL_MS;
        this.maxSweep = maxSweep > 0 ? maxSweep : DEFAULT_MAX_SWEEP;
        this.instanceRegistry = instanceRegistry;
        this.instanceLeaseTimeoutMillis =
                instanceLeaseTimeoutMillis > 0
                        ? instanceLeaseTimeoutMillis
                        : io.github.streammq.core.StreamMQConstants
                                .DEFAULT_BROADCAST_LEASE_TIMEOUT_MS;
        this.instanceReclaimGraceMillis =
                instanceReclaimGraceMillis > 0
                        ? instanceReclaimGraceMillis
                        : io.github.streammq.core.StreamMQConstants
                                .DEFAULT_BROADCAST_RECLAIM_GRACE_MS;
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
            // 按 group 聚合，单次拉取每个 group 的实例租约快照，避免对每个僵尸组各做一次 HGETALL
            Set<String> involvedGroups = new HashSet<>();
            for (String member : staleMembers) {
                int sepIdx = member.indexOf('|');
                if (sepIdx <= 0) {
                    continue;
                }
                String effectiveGroup = member.substring(sepIdx + 1);
                int colon = effectiveGroup.indexOf(':');
                if (colon > 0) {
                    involvedGroups.add(effectiveGroup.substring(0, colon));
                }
            }
            Map<String, Map<String, Long>> leaseByGroup = new HashMap<>();
            for (String g : involvedGroups) {
                leaseByGroup.put(g, instanceLeaseSnapshot(g));
            }
            for (String member : staleMembers) {
                int sepIdx = member.indexOf('|');
                if (sepIdx <= 0) {
                    registry.remove(member);
                    continue;
                }
                String topic = member.substring(0, sepIdx);
                String effectiveGroup = member.substring(sepIdx + 1);
                // 持久化广播身份：槽位仍在回收宽限期内时，其消费者组与 PEL 必须保留，
                // 否则"重启 10 分钟"就会把位点永久销毁，回收机制随之失去意义。
                if (isProtectedByInstanceLease(effectiveGroup, leaseByGroup)) {
                    LOG.debug(
                            "Broadcast group retained within reclaim grace window: group={}",
                            effectiveGroup);
                    continue;
                }
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
            sweepInstanceSlots(registry);
            return removed;
        } catch (RuntimeException ex) {
            // 回收失败不得阻塞 PEL 认领调度：下轮自动重试
            LOG.debug("Sweep stale broadcast groups failed: {}", ex.getMessage());
            return 0;
        }
    }

    /**
     * 判断某广播消费者组是否仍受实例身份租约保护（即其槽位还在回收宽限期内）。
     *
     * <p>组名形如 {@code {group}:{group}-{instanceId}}，按此规则反解出 group 与 instanceId， 再查预拉取的租约快照。
     *
     * @param effectiveGroup 广播消费者的实际 Redis 组名
     * @param leaseByGroup group → (instanceId → lastHeartbeatMillis) 预拉取快照
     * @return true 表示应保留该组（不得销毁）
     */
    private boolean isProtectedByInstanceLease(
            String effectiveGroup, Map<String, Map<String, Long>> leaseByGroup) {
        int colon = effectiveGroup.indexOf(':');
        if (colon <= 0) {
            return false;
        }
        String group = effectiveGroup.substring(0, colon);
        Map<String, Long> snapshot = leaseByGroup.get(group);
        if (snapshot == null) {
            return false;
        }
        String instanceId =
                BroadcastGroupNaming.instanceIdFromEffectiveGroup(group, effectiveGroup);
        if (instanceId == null || instanceId.isEmpty()) {
            return false;
        }
        Long lastHb = snapshot.get(instanceId);
        return lastHb != null && System.currentTimeMillis() - lastHb <= instanceReclaimGraceMillis;
    }

    /** 拉取某 group 下的实例租约快照：instanceId → lastHeartbeatMillis（注册中心不可用/为空时返回空 map）。 */
    private Map<String, Long> instanceLeaseSnapshot(String group) {
        BroadcastInstanceRegistry registry = instanceRegistry;
        if (registry == null) {
            return Map.of();
        }
        Map<String, Long> snapshot = new HashMap<>();
        try {
            for (BroadcastInstanceLease lease : registry.listInstances(namespace, group)) {
                snapshot.put(lease.instanceId(), lease.lastHeartbeatMillis());
            }
        } catch (RuntimeException ex) {
            LOG.debug(
                    "List broadcast instance leases failed for group={}: {}",
                    group,
                    ex.getMessage());
        }
        return snapshot;
    }

    /** 顺带清扫超过回收宽限期的实例身份槽位（按注册表内出现的 group 去重）。 */
    private void sweepInstanceSlots(RScoredSortedSet<String> registry) {
        BroadcastInstanceRegistry inst = instanceRegistry;
        if (inst == null) {
            return;
        }
        Set<String> groups = new HashSet<>();
        for (String member : registry) {
            int sepIdx = member.indexOf('|');
            if (sepIdx <= 0) {
                continue;
            }
            String effectiveGroup = member.substring(sepIdx + 1);
            int colon = effectiveGroup.indexOf(':');
            if (colon > 0) {
                groups.add(effectiveGroup.substring(0, colon));
            }
        }
        for (String group : groups) {
            try {
                inst.sweep(
                        namespace,
                        group,
                        instanceLeaseTimeoutMillis,
                        instanceReclaimGraceMillis,
                        maxSweep);
            } catch (RuntimeException ex) {
                LOG.debug("Broadcast instance slot sweep failed: {}", ex.getMessage());
            }
        }
    }

    @Override
    public long countBroadcastGroups() {
        RScoredSortedSet<String> registry =
                redisson.<String>getScoredSortedSet(StreamMQKeys.broadcastRegistry(namespace));
        return registry.size();
    }
}
