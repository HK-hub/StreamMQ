/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.converter.AbstractMessageConverter;
import io.github.streammq.adapter.redisson.scheduler.PelClaimScheduler;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.ListenerType;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SchedulerTargetBinder} 默认实现。
 *
 * <p><b>单一绑定规则源：</b>批量绑定与单注册项绑定共用 {@link #bindOneRetryTarget} / {@link
 * #bindOnePelClaimTarget}，避免两条路径各自演化出不同的目标集合（历史上运行期动态注册 只建了读循环、漏绑调度目标，导致这些消费者"能消费但失败消息永不重投"）。
 */
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
            if (bindOneRetryTarget(scheduler, reg)) {
                count++;
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
            switch (bindOnePelClaimTarget(scheduler, reg)) {
                case TOPIC -> topicCount++;
                case RETRY -> retryCount++;
                case DLQ -> dlqCount++;
                case NONE -> {
                    // 广播模式各实例独立组、无共享 retry 流：不注册任何目标
                }
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
    public void bindTargets(
            RetryScheduler retryScheduler,
            PelClaimScheduler pelClaimScheduler,
            ListenerRegistration<?> reg) {
        Objects.requireNonNull(reg, "reg");
        if (Objects.nonNull(retryScheduler)) {
            bindOneRetryTarget(retryScheduler, reg);
        }
        if (Objects.nonNull(pelClaimScheduler)) {
            bindOnePelClaimTarget(pelClaimScheduler, reg);
        }
    }

    @Override
    public void unbindTargets(
            RetryScheduler retryScheduler,
            PelClaimScheduler pelClaimScheduler,
            ListenerRegistration<?> reg) {
        Objects.requireNonNull(reg, "reg");
        if (Objects.nonNull(retryScheduler)) {
            retryScheduler.unregisterRetryTarget(
                    reg.getNamespace(), retryTopicOf(reg), reg.getGroup());
        }
        if (Objects.nonNull(pelClaimScheduler)) {
            pelClaimScheduler.unregisterTargets(reg.getNamespace(), reg.getTopic(), reg.getGroup());
        }
    }

    /** DLQ 目标的重试 ZSet 以 {@code (group, group)} 维度登记（见 {@link #bindOneRetryTarget}）。 */
    private static String retryTopicOf(ListenerRegistration<?> reg) {
        return reg.isDlqMode() ? reg.getGroup() : reg.getTopic();
    }

    /**
     * 绑定单个注册项的重试目标。
     *
     * @return true 表示按业务 (topic, group) 维度登记（DLQ 走 sentinel 维度、不计入该计数）
     */
    private static boolean bindOneRetryTarget(
            RetryScheduler scheduler, ListenerRegistration<?> reg) {
        if (!reg.isDlqMode()) {
            scheduler.registerRetryTarget(
                    reg.getNamespace(), reg.getTopic(), reg.getGroup(), reg.getMaxReconsumeTimes());
            return true;
        }
        scheduler.registerRetryTarget(reg.getNamespace(), reg.getGroup(), reg.getGroup(), 0);
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
        return false;
    }

    /** 绑定单个注册项的 PEL 认领目标，返回实际登记的种类（NONE = 该注册项无需认领目标）。 */
    private static PelClaimTargetKind bindOnePelClaimTarget(
            PelClaimScheduler scheduler, ListenerRegistration<?> reg) {
        if (reg.isDlqMode()) {
            // DLQ 流 PEL 恢复：滞留条目尾部复制重投（此前 DLQ 组被整体跳过，
            // 实例崩溃后的 DLQ pending 永久卡死）
            scheduler.registerDlqTarget(reg.getNamespace(), reg.getTopic(), reg.getGroup());
            return PelClaimTargetKind.DLQ;
        }
        if (reg.getType() == ListenerType.ORDERLY) {
            // 顺序消费失败在分片锁内原地重试、耗尽直接转 DLQ，无 retry Stream
            scheduler.registerTarget(
                    reg.getNamespace(),
                    reg.getTopic(),
                    reg.getGroup(),
                    reg.getMaxReconsumeTimes(),
                    true,
                    reg.getShardCount(),
                    shardingFieldOf(reg));
            return PelClaimTargetKind.TOPIC;
        }
        if (reg.getType() == ListenerType.AUTO_ACK
                && reg.getConsumeMode() != ConsumeMode.BROADCASTING) {
            scheduler.registerTarget(
                    reg.getNamespace(), reg.getTopic(), reg.getGroup(), reg.getMaxReconsumeTimes());
            // 并发集群消费的 retry Stream 同样存在 PEL（消费者名含容器随机 token，
            // 重启后自身排空读不到遗留条目），注册 RETRY 目标补齐跨重启恢复；
            // 广播模式各实例独立组、无共享 retry 流，不注册
            scheduler.registerRetryStreamTarget(
                    reg.getNamespace(), reg.getTopic(), reg.getGroup(), reg.getMaxReconsumeTimes());
            return PelClaimTargetKind.RETRY;
        }
        return PelClaimTargetKind.NONE;
    }

    /** 解析注册项 Converter 的分片键字段名；非 {@link AbstractMessageConverter} 实现返回 null， 由调度器回退到默认字段名。 */
    private static String shardingFieldOf(ListenerRegistration<?> reg) {
        MessageConverter converter = reg.getConverterInstance();
        if (converter instanceof AbstractMessageConverter amc) {
            return amc.shardingFieldName();
        }
        return null;
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

    /** 目标种类（用于批量绑定日志计数；NONE 表示该注册项不产生任何认领目标）。 */
    private enum PelClaimTargetKind {
        TOPIC,
        RETRY,
        DLQ,
        NONE
    }
}
