/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.github.streammq.adapter.redisson.filter.DefaultConsumerFilterChain;
import io.github.streammq.adapter.redisson.filter.ReflectiveConsumerFilterResolver;
import io.github.streammq.adapter.redisson.interceptor.DefaultConsumerInterceptorChain;
import io.github.streammq.adapter.redisson.rebalance.AverageRebalanceStrategy;
import io.github.streammq.adapter.redisson.rebalance.ConsistentHashRebalanceStrategy;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.listener.DefaultListenerRegistration;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

/**
 * 验证 per-consumer 未指定 rebalanceStrategy 时回退到全局默认（来自 streammq.rebalance.strategy 配置）。
 *
 * <p>此前实现硬编码回退到 {@link AverageRebalanceStrategy}，与 {@code streammq.rebalance.strategy}
 * 配置不一致；本测试在 0.1.0 后修复为优先使用全局配置。
 */
@DisplayName("DefaultPerConsumerSpiResolver 全局 RebalanceStrategy 回退")
class DefaultPerConsumerSpiResolverRebalanceTest {

    @Test
    @DisplayName("未设置全局默认时回退到 AverageRebalanceStrategy（向后兼容）")
    void noGlobalDefault_fallsBackToAverage() {
        DefaultPerConsumerSpiResolver resolver = newResolver(null, 160);
        RebalanceStrategy strategy = resolver.resolveRebalanceStrategy(emptyRegistration());
        assertThat(strategy).isInstanceOf(AverageRebalanceStrategy.class);
    }

    @Test
    @DisplayName("全局默认 = ConsistentHashRebalanceStrategy 时使用一致性哈希（携带虚拟节点数）")
    void globalConsistentHash_usedWhenSet() {
        DefaultPerConsumerSpiResolver resolver = newResolver(ConsistentHashRebalanceStrategy.class, 64);
        RebalanceStrategy strategy = resolver.resolveRebalanceStrategy(emptyRegistration());
        assertThat(strategy).isInstanceOf(ConsistentHashRebalanceStrategy.class);
    }

    @Test
    @DisplayName("per-consumer 显式指定时优先级高于全局配置")
    @SuppressWarnings({"unchecked", "rawtypes"})
    void perConsumerOverridesGlobal() {
        // 全局配置为 Average，但 per-consumer 注解指定 ConsistentHash
        DefaultPerConsumerSpiResolver resolver = newResolver(AverageRebalanceStrategy.class, 64);
        DefaultListenerRegistration.Builder builder = new DefaultListenerRegistration.Builder();
        DefaultListenerRegistration<?> reg =
                (DefaultListenerRegistration<?>)
                        builder.topic("t")
                                .group("g")
                                .consumer(
                                        (io.github.streammq.core.consumer
                                                        .StreamMessageConcurrentlyConsumer)
                                                (m, c) -> ConsumeAction.SUCCESS)
                                .rebalanceStrategy(ConsistentHashRebalanceStrategy.class)
                                .build();
        RebalanceStrategy strategy = resolver.resolveRebalanceStrategy(reg);
        assertThat(strategy).isInstanceOf(ConsistentHashRebalanceStrategy.class);
    }

    // ---- 构造辅助 ----

    private static DefaultPerConsumerSpiResolver newResolver(
            Class<? extends RebalanceStrategy> globalDefault, int virtualNodes) {
        return new DefaultPerConsumerSpiResolver(
                mock(RedissonClient.class),
                mock(MessageConverter.class),
                mock(RetryPolicy.class),
                mock(DlqFailureStrategy.class),
                DlqConfig.builder().build(),
                new DefaultConsumerInterceptorChain(),
                new DefaultConsumerFilterChain(),
                () -> new ReflectiveConsumerFilterResolver(),
                () -> virtualNodes,
                () -> null,
                globalDefault,
                true);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static DefaultListenerRegistration<?> emptyRegistration() {
        DefaultListenerRegistration.Builder builder = new DefaultListenerRegistration.Builder();
        return (DefaultListenerRegistration<?>)
                builder.topic("t")
                        .group("g")
                        .consumer(
                                (io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer)
                                        (m, c) -> ConsumeAction.SUCCESS)
                        .build();
    }
}
