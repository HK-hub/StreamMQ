/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.scheduler.PelClaimScheduler;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.ListenerType;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link SchedulerTargetBinder} 默认实现。 */
public class DefaultSchedulerTargetBinder implements SchedulerTargetBinder {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSchedulerTargetBinder.class);

    private final RegistrationStore store;

    public DefaultSchedulerTargetBinder(RegistrationStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    @Override
    public void bindRetryTargets(RetryScheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler");
        int count = 0;
        for (ListenerRegistration<?> reg : store.registrations()) {
            if (!reg.isDlqMode()) {
                scheduler.registerRetryTarget(
                        reg.getTopic(), reg.getGroup(), reg.getMaxReconsumeTimes());
                count++;
            } else {
                scheduler.registerRetryTarget(reg.getGroup(), reg.getGroup(), 0);
                // 防御性可观测性：注册目标 max 与监听器声明值来自同一 reg，理论上一致；
                // 若未来任一侧改动导致漂移，此处 INFO 提示双真源分歧
                if (0 != reg.getMaxReconsumeTimes()) {
                    LOG.info(
                            "Retry target max (0, DLQ sentinel) differs from listener"
                                    + " maxReconsumeTimes ({}): topic={}, group={} — check"
                                    + " dual source-of-truth",
                            reg.getMaxReconsumeTimes(),
                            reg.getTopic(),
                            reg.getGroup());
                }
            }
        }
        LOG.info(
                "Registered {} retry targets to RetryScheduler ({} listeners total)",
                count,
                store.registrationCount());
    }

    @Override
    public void bindPelClaimTargets(PelClaimScheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler");
        int topicCount = 0;
        int retryCount = 0;
        int dlqCount = 0;
        for (ListenerRegistration<?> reg : store.registrations()) {
            if (reg.isDlqMode()) {
                // DLQ 流 PEL 恢复：滞留条目尾部复制重投（此前 DLQ 组被整体跳过，
                // 实例崩溃后的 DLQ pending 永久卡死）
                scheduler.registerDlqTarget(reg.getTopic(), reg.getGroup());
                dlqCount++;
                continue;
            }
            if (reg.getType() == ListenerType.ORDERLY) {
                // 顺序消费失败在分片锁内原地重试、耗尽直接转 DLQ，无 retry Stream
                scheduler.registerTarget(
                        reg.getTopic(),
                        reg.getGroup(),
                        reg.getMaxReconsumeTimes(),
                        true,
                        reg.getShardCount());
                topicCount++;
            } else if (reg.getType() == ListenerType.AUTO_ACK
                    && reg.getConsumeMode() != ConsumeMode.BROADCASTING) {
                scheduler.registerTarget(
                        reg.getTopic(), reg.getGroup(), reg.getMaxReconsumeTimes());
                topicCount++;
                // 并发集群消费的 retry Stream 同样存在 PEL（消费者名含容器随机 token，
                // 重启后自身排空读不到遗留条目），注册 RETRY 目标补齐跨重启恢复；
                // 广播模式各实例独立组、无共享 retry 流，不注册
                scheduler.registerRetryStreamTarget(
                        reg.getTopic(), reg.getGroup(), reg.getMaxReconsumeTimes());
                retryCount++;
            }
        }
        LOG.info(
                "Registered {} PelClaim targets ({} topic, {} retry-stream, {} dlq)",
                topicCount + retryCount + dlqCount,
                topicCount,
                retryCount,
                dlqCount);
    }

    @Override
    public boolean rebalanceGroup(String group) {
        for (ListenerRegistration<?> reg : store.registrations()) {
            if (reg.getType() == ListenerType.ORDERLY
                    && reg.getGroup().equals(group)
                    && reg.getShardCount() > 0) {
                var manager = store.groupManager(reg.key());
                if (Objects.nonNull(manager)) {
                    manager.rebalance(reg.getShardCount());
                    return true;
                }
            }
        }
        return false;
    }
}
