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
        int orderlyCount = 0;
        int concurrentCount = 0;
        for (ListenerRegistration<?> reg : store.registrations()) {
            if (reg.isDlqMode()) {
                continue;
            }
            if (reg.getType() == ListenerType.ORDERLY) {
                scheduler.registerTarget(
                        reg.getTopic(),
                        reg.getGroup(),
                        reg.getMaxReconsumeTimes(),
                        true,
                        reg.getShardCount());
                orderlyCount++;
            } else if (reg.getType() == ListenerType.AUTO_ACK
                    && reg.getConsumeMode() != ConsumeMode.BROADCASTING) {
                scheduler.registerTarget(
                        reg.getTopic(), reg.getGroup(), reg.getMaxReconsumeTimes());
                concurrentCount++;
            }
        }
        LOG.info(
                "Registered {} PelClaim targets ({} orderly, {} concurrent)",
                orderlyCount + concurrentCount,
                orderlyCount,
                concurrentCount);
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
