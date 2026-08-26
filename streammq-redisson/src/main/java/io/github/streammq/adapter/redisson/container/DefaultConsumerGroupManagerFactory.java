/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.manager.RedissonConsumerGroupManager;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.policy.ConsumerGroupManager;
import io.github.streammq.core.policy.RebalanceStrategy;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import org.redisson.api.RedissonClient;

/** {@link ConsumerGroupManagerFactory} 默认实现。 */
public class DefaultConsumerGroupManagerFactory implements ConsumerGroupManagerFactory {

    private final RedissonClient redisson;
    private final PerConsumerSpiResolver spiResolver;
    private final Supplier<Long> heartbeatIntervalMs;
    private final Supplier<Long> instanceTimeoutMs;

    public DefaultConsumerGroupManagerFactory(
            RedissonClient redisson,
            PerConsumerSpiResolver spiResolver,
            Supplier<Long> heartbeatIntervalMs,
            Supplier<Long> instanceTimeoutMs) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.spiResolver = Objects.requireNonNull(spiResolver, "spiResolver");
        this.heartbeatIntervalMs = Objects.requireNonNull(heartbeatIntervalMs);
        this.instanceTimeoutMs = Objects.requireNonNull(instanceTimeoutMs);
    }

    @Override
    public ConsumerGroupManager createAndRegister(ListenerRegistration<?> reg) {
        String instanceId = reg.getGroup() + "-" + UUID.randomUUID().toString().substring(0, 8);
        RebalanceStrategy rebalanceStrategy = spiResolver.resolveRebalanceStrategy(reg);
        ConsumerGroupManager manager =
                new RedissonConsumerGroupManager(
                        redisson,
                        reg.getNamespace(),
                        reg.getGroup(),
                        instanceId,
                        rebalanceStrategy,
                        heartbeatIntervalMs.get(),
                        instanceTimeoutMs.get());
        manager.register();
        manager.cleanupStaleGroups();
        return manager;
    }
}
