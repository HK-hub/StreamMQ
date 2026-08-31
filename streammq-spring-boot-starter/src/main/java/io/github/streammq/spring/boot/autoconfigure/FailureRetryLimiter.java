/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 管理端点的「失败重试限流」。
 *
 * <p>当一个写操作（重投 / 删除 / ACK / 重平衡 / 建删 Topic / 改配置）在 Redis 侧失败后，该操作 + 目标进入冷却期；冷却期内对相同目标的重复请求直接拒绝（返回
 * {@code rateLimited} 响应）， 不触碰 Redis。目的是避免脚本或误操作对失效目标（如 Redis 短暂不可用、目标 Stream 被误删）反复
 * 重试，放大故障期间的负载；同时给运维一个确定性的「稍后重试」信号。
 *
 * <p>实现要点：
 *
 * <ul>
 *   <li>只在失败后冷却——成功即清除，正常运行无任何额外开销（一次 {@code ConcurrentHashMap} 查询）。
 *   <li>冷却期为 0 时整体禁用（便于测试与需要高频重试的运维场景）。
 *   <li>{@link #recordFailure(String)} 在条目数超限时先清理过期条目，防止 key 空间无限增长 （恶意/异常输入构造大量不同的 key 时）。
 * </ul>
 *
 * <p>线程安全：{@link ConcurrentHashMap} + 单调时间比较，所有方法可并发调用。
 *
 * @author StreamMQ Contributors
 * @since 0.1.1
 */
public final class FailureRetryLimiter {

    /** 默认冷却期（毫秒） */
    public static final long DEFAULT_COOLDOWN_MILLIS = 5_000L;

    /** key 数量上限：超过后触发一次过期清理 */
    private static final int MAX_ENTRIES = 1_024;

    private final long cooldownMillis;

    /** key → 最近一次失败时刻（毫秒） */
    private final Map<String, Long> lastFailureAt = new ConcurrentHashMap<>();

    /**
     * 构造限流器。
     *
     * @param cooldownMillis 失败后的冷却期（毫秒）；0 表示禁用限流
     */
    public FailureRetryLimiter(long cooldownMillis) {
        if (cooldownMillis < 0) {
            throw new IllegalArgumentException(
                    "cooldownMillis must be >= 0, got: " + cooldownMillis);
        }
        this.cooldownMillis = cooldownMillis;
    }

    /** 返回冷却期（毫秒）。 */
    public long getCooldownMillis() {
        return cooldownMillis;
    }

    /**
     * 该 key 是否处于冷却期（禁止重试）。
     *
     * @param key 操作 + 目标标识
     * @return true 表示仍在冷却期
     */
    public boolean isBlocked(String key) {
        return remainingCooldownMillis(key) > 0;
    }

    /**
     * 返回距离下一次允许重试的剩余毫秒数；不在冷却期时返回 0。
     *
     * @param key 操作 + 目标标识
     * @return 剩余冷却毫秒数（0 表示可重试）
     */
    public long remainingCooldownMillis(String key) {
        if (cooldownMillis == 0) {
            return 0L;
        }
        Long since = lastFailureAt.get(key);
        if (since == null) {
            return 0L;
        }
        long elapsed = System.currentTimeMillis() - since;
        return elapsed >= cooldownMillis ? 0L : cooldownMillis - elapsed;
    }

    /** 记录一次失败，进入冷却期。 */
    public void recordFailure(String key) {
        if (cooldownMillis == 0) {
            return;
        }
        if (lastFailureAt.size() >= MAX_ENTRIES) {
            evictExpired();
        }
        lastFailureAt.put(key, System.currentTimeMillis());
    }

    /** 记录一次成功，清除该 key 的冷却状态。 */
    public void recordSuccess(String key) {
        lastFailureAt.remove(key);
    }

    /** 清除全部冷却状态（测试与运维恢复场景使用）。 */
    public void clear() {
        lastFailureAt.clear();
    }

    /** 清理已过冷却期的条目，防止 key 空间无限增长；返回清理条数。 */
    public int evictExpired() {
        if (cooldownMillis == 0 || lastFailureAt.isEmpty()) {
            return 0;
        }
        long now = System.currentTimeMillis();
        int removed = 0;
        var iterator = lastFailureAt.entrySet().iterator();
        while (iterator.hasNext()) {
            if (now - iterator.next().getValue() >= cooldownMillis) {
                iterator.remove();
                removed++;
            }
        }
        return removed;
    }
}
