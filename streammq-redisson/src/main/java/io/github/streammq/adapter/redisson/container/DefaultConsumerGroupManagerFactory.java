/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.manager.RedissonConsumerGroupManager;
import io.github.streammq.adapter.redisson.support.BroadcastGroupNaming;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.policy.ConsumerGroupManager;
import io.github.streammq.core.policy.RebalanceStrategy;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link ConsumerGroupManagerFactory} 默认实现。 */
public class DefaultConsumerGroupManagerFactory implements ConsumerGroupManagerFactory {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultConsumerGroupManagerFactory.class);

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
        String instanceId = resolveInstanceId(reg);
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

    /**
     * 实例身份必须与 Redis 消费者名内嵌的实例标识一致。
     *
     * <p>PEL 认领的存活判定（{@code PelClaimScheduler#isOwnerConsumerAlive}）依赖「instances Hash 的 key
     * 是消费者名的实例后缀」 这一不变式：两个身份若各自随机生成，判活将永远失败——活跃慢消费者仍会被复制重投并误入 DLQ。 因此这里从注册项的消费者名反解实例标识，保证两处身份同源。
     */
    private static String resolveInstanceId(ListenerRegistration<?> reg) {
        String token =
                BroadcastGroupNaming.instanceIdFromConsumerName(
                        reg.getGroup(), reg.getConsumerName());
        if (token != null && !token.isBlank()) {
            return token;
        }
        LOG.warn(
                "Registration has no decodable consumer name; PEL liveness cross-check cannot"
                        + " match live consumers. group={}, consumerName={}",
                reg.getGroup(),
                reg.getConsumerName());
        return reg.getGroup() + "-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
