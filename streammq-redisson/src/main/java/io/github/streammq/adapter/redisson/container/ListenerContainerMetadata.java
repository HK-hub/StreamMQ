/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.scheduler.PelClaimScheduler;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.core.listener.StreamMQListenerContainer.ConsumerMetadata;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-only and bind-only metadata-facing operations for the listener container.
 *
 * <p>Extracted from {@code DefaultStreamMQListenerContainer} to reduce its public surface area; the
 * container now forwards these calls here and the heavy lifting (looking up registrations, talking
 * to schedulers) is encapsulated.
 *
 * <p>Operations:
 *
 * <ul>
 *   <li>{@link #getConsumers()} — list all registered consumer metadata
 *   <li>{@link #rebalanceGroup(String)} — trigger rebalance on a group
 *   <li>{@link #registerRetryTargets(RetryScheduler)} — bind the container's registrations to the
 *       retry scheduler
 *   <li>{@link #registerPelClaimTargets(PelClaimScheduler)} — bind to the PEL claim scheduler
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class ListenerContainerMetadata {

    private static final Logger LOG = LoggerFactory.getLogger(ListenerContainerMetadata.class);

    private final RegistrationStore store;
    private final SchedulerTargetBinder schedulerBinder;

    public ListenerContainerMetadata(
            RegistrationStore store, SchedulerTargetBinder schedulerBinder) {
        this.store = Objects.requireNonNull(store, "store");
        this.schedulerBinder = Objects.requireNonNull(schedulerBinder, "schedulerBinder");
    }

    /**
     * Snapshot all currently-registered consumer metadata.
     *
     * @return immutable list of consumer metadata
     */
    public Collection<ConsumerMetadata> getConsumers() {
        List<ConsumerMetadata> list = new ArrayList<>(store.registrationCount());
        for (var reg : store.registrations()) {
            list.add(
                    new ConsumerMetadata(
                            reg.getTopic(),
                            reg.getGroup(),
                            reg.getConsumer().getClass(),
                            reg.getTargetBodyType() != null
                                    ? reg.getTargetBodyType()
                                    : Object.class));
        }
        return Collections.unmodifiableList(list);
    }

    /**
     * Trigger rebalance for a consumer group.
     *
     * @param group group name
     * @return {@code true} if the group was found and rebalance executed; {@code false} if the
     *     group is unknown or not an orderly group
     */
    public boolean rebalanceGroup(String group) {
        boolean executed = schedulerBinder.rebalanceGroup(group);
        if (executed) {
            LOG.info("Rebalance triggered for orderly group={}", group);
        } else {
            LOG.warn("Rebalance requested for unknown/non-orderly group: {}", group);
        }
        return executed;
    }

    /** Bind all container retry targets to the given {@link RetryScheduler}. */
    public void registerRetryTargets(RetryScheduler scheduler) {
        schedulerBinder.bindRetryTargets(scheduler);
    }

    /** Bind all container PEL-claim targets to the given {@link PelClaimScheduler}. */
    public void registerPelClaimTargets(PelClaimScheduler scheduler) {
        schedulerBinder.bindPelClaimTargets(scheduler);
    }
}
