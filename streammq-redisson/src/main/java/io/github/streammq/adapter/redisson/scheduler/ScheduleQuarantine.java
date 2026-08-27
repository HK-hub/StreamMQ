/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import java.util.Objects;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 调度条目隔离区登记工具（Retry / Delay 调度器共用）。
 *
 * <p><b>背景：</b>payload Hash 带 7 天 TTL 兜底回收；若调度 ZSet 条目因异常残留超过 TTL， 转投时 payload 已丢失。旧实现直接 {@code
 * ZREM + WARN}——静默删除导致消息不可观测地消失。
 *
 * <p><b>现语义：</b>先把 {@code member=msgId|kind、score=dueTime} 写入隔离区 ZSet （{@code
 * streammq:{ns}:quarantine:{kind}}），再从活跃 ZSet 移除——静默删除改为可观测的隔离登记， 运维可排查/重放；WARN 日志保留。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
final class ScheduleQuarantine {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduleQuarantine.class);

    private ScheduleQuarantine() {}

    /**
     * 将 payload 丢失的调度条目登记到隔离区后从活跃 ZSet 移除。
     *
     * <p>隔离登记失败不抛出（隔离是可观测性增强，不得阻断活跃条目清理形成永久重扫）。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param kind 调度类型（{@code retry} / {@code delay}）
     * @param activeZset 活跃调度 ZSet（条目将从此移除）
     * @param msgId 消息 ID（即调度条目 member）
     * @param contextLabel 日志上下文标签（如延时 level）
     */
    static void quarantineAndRemove(
            RedissonClient redisson,
            String namespace,
            String kind,
            RScoredSortedSet<String> activeZset,
            String msgId,
            String contextLabel) {
        try {
            Double dueTime = activeZset.getScore(msgId);
            long score =
                    Objects.nonNull(dueTime) ? dueTime.longValue() : System.currentTimeMillis();
            redisson.<String>getScoredSortedSet(StreamMQKeys.quarantineZset(namespace, kind))
                    .add(score, msgId + "|" + kind);
        } catch (RuntimeException ex) {
            LOG.debug("Failed to record quarantine entry msgId={}: {}", msgId, ex.getMessage());
        }
        boolean removed = activeZset.remove(msgId);
        LOG.warn(
                "Schedule[{}] payload lost for msgId={} (expired by payload TTL?),"
                        + " quarantined and removed from active schedule (removed={})",
                contextLabel,
                msgId,
                removed);
    }
}
