/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.filter.SimpleSqlSelectorFilter;
import io.github.streammq.adapter.redisson.filter.SimpleTagSelectorFilter;
import io.github.streammq.adapter.redisson.handler.DefaultRetryAndDlqHandler;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.filter.ConsumerFilter;
import io.github.streammq.core.filter.ConsumerFilterChain;
import io.github.streammq.core.filter.ConsumerFilterResolver;
import io.github.streammq.core.filter.ExpressionSelectorFilter;
import io.github.streammq.core.interceptor.ConsumerInterceptorChain;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.metrics.StreamMQMetrics;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryAndDlqHandler;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.util.SpiResolver;
import io.github.streammq.core.util.StringUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PerConsumerSpiResolver} 默认实现。
 *
 * <p>解析结果（路由处理器、过滤器链、转换器实例）写回 {@link RegistrationStore} 与注册模型；重平衡策略一致性哈希支持虚拟节点数配置，失败回退平均策略。
 */
public class DefaultPerConsumerSpiResolver implements PerConsumerSpiResolver {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultPerConsumerSpiResolver.class);

    private final RedissonClient redisson;
    private final MessageConverter globalConverter;
    private final RetryPolicy globalRetryPolicy;
    private final DlqFailureStrategy globalDlqFailureStrategy;
    private final DlqConfig dlqConfig;
    private final ConsumerInterceptorChain interceptorChain;
    private final ConsumerFilterChain globalFilterChain;
    private final Supplier<ConsumerFilterResolver> filterResolverSupplier;
    private final Supplier<Integer> virtualNodesSupplier;
    private final Supplier<StreamMQMetrics> metricsSupplier;
    private final boolean enabled;

    public DefaultPerConsumerSpiResolver(
            RedissonClient redisson,
            MessageConverter globalConverter,
            RetryPolicy globalRetryPolicy,
            DlqFailureStrategy globalDlqFailureStrategy,
            DlqConfig dlqConfig,
            ConsumerInterceptorChain interceptorChain,
            ConsumerFilterChain globalFilterChain,
            Supplier<ConsumerFilterResolver> filterResolverSupplier,
            Supplier<Integer> virtualNodesSupplier,
            Supplier<StreamMQMetrics> metricsSupplier,
            boolean enabled) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.globalConverter = Objects.requireNonNull(globalConverter, "globalConverter");
        this.globalRetryPolicy = Objects.requireNonNull(globalRetryPolicy, "globalRetryPolicy");
        this.globalDlqFailureStrategy =
                Objects.requireNonNull(globalDlqFailureStrategy, "globalDlqFailureStrategy");
        this.dlqConfig = Objects.requireNonNull(dlqConfig, "dlqConfig");
        this.interceptorChain = Objects.requireNonNull(interceptorChain, "interceptorChain");
        this.globalFilterChain = Objects.requireNonNull(globalFilterChain, "globalFilterChain");
        this.filterResolverSupplier =
                Objects.requireNonNull(filterResolverSupplier, "filterResolverSupplier");
        this.virtualNodesSupplier = Objects.requireNonNull(virtualNodesSupplier);
        this.metricsSupplier = Objects.requireNonNull(metricsSupplier, "metricsSupplier");
        this.enabled = enabled;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void resolveInto(ListenerRegistration<?> reg, RegistrationStore store) {
        if (!enabled) {
            return;
        }
        // 1. per-consumer 消息转换器（含 per-consumer 序列化器）——直接回填到注册模型
        MessageConverter converter = resolveConverter(reg);
        reg.setConverterInstance(converter);

        // 2. per-consumer 重试策略
        RetryPolicy policy =
                SpiResolver.resolveOrInstantiate(
                        reg.getRetryPolicy(), RetryPolicy.class, globalRetryPolicy);

        // 3. per-consumer 死信失败策略
        DlqFailureStrategy dlqStrategy =
                SpiResolver.resolveOrInstantiate(
                        reg.getDlqFailureStrategy(),
                        DlqFailureStrategy.class,
                        globalDlqFailureStrategy);

        // 4. per-consumer 路由处理器
        RetryAndDlqHandler handler =
                new DefaultRetryAndDlqHandler(
                        redisson, converter, policy, interceptorChain, dlqStrategy, dlqConfig);
        StreamMQMetrics metrics = metricsSupplier.get();
        if (Objects.nonNull(metrics) && handler instanceof DefaultRetryAndDlqHandler drh) {
            drh.setMetrics(metrics);
        }
        store.putHandler(reg.key(), handler);

        // 5. per-consumer 重平衡策略（实例化校验）
        precheckRebalanceStrategy(reg);

        // 6. per-consumer 过滤器链（预构建并缓存）
        List<ConsumerFilter> filters = buildFilters(reg);
        store.putFilters(reg.key(), filters);

        LOG.debug(
                "Resolved per-consumer SPI: key={}, retryPolicy={}, converter={},"
                        + " dlqFailureStrategy={}, filters={}",
                reg.key(),
                policy.name(),
                converter.getClass().getSimpleName(),
                dlqStrategy.name(),
                filters.stream().map(ConsumerFilter::name).toList());
    }

    @Override
    public void rebuildFilters(ListenerRegistration<?> reg, RegistrationStore store) {
        store.putFilters(reg.key(), buildFilters(reg));
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public RebalanceStrategy resolveRebalanceStrategy(ListenerRegistration<?> reg) {
        Class<? extends RebalanceStrategy> rebalanceClass = reg.getRebalanceStrategy();
        if (Objects.isNull(rebalanceClass) || rebalanceClass == RebalanceStrategy.class) {
            return new io.github.streammq.adapter.redisson.rebalance.AverageRebalanceStrategy();
        }
        if (rebalanceClass
                == io.github.streammq.adapter.redisson.rebalance.ConsistentHashRebalanceStrategy
                        .class) {
            return new io.github.streammq.adapter.redisson.rebalance
                    .ConsistentHashRebalanceStrategy(virtualNodesSupplier.get());
        }
        try {
            return SpiResolver.resolveOrInstantiate(
                    (Class) rebalanceClass, RebalanceStrategy.class, null);
        } catch (RuntimeException ex) {
            LOG.warn(
                    "Failed to instantiate rebalanceStrategy for {}, using default: {}",
                    reg.key(),
                    ex.getMessage());
            return new io.github.streammq.adapter.redisson.rebalance.AverageRebalanceStrategy();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void precheckRebalanceStrategy(ListenerRegistration<?> reg) {
        Class<? extends RebalanceStrategy> rebalanceClass = reg.getRebalanceStrategy();
        if (Objects.isNull(rebalanceClass) || rebalanceClass == RebalanceStrategy.class) {
            return;
        }
        try {
            SpiResolver.resolveOrInstantiate((Class) rebalanceClass, RebalanceStrategy.class, null);
        } catch (RuntimeException ex) {
            LOG.warn(
                    "Failed to pre-instantiate rebalanceStrategy for {} ({}): {}",
                    reg.key(),
                    rebalanceClass.getName(),
                    ex.getMessage());
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private MessageConverter resolveConverter(ListenerRegistration<?> reg) {
        Class<? extends MessageConverter> converterClass = reg.getMessageConverter();
        Class<? extends MessageSerializer> serializerClass = reg.getSerializer();
        if (Objects.nonNull(converterClass) && converterClass != MessageConverter.class) {
            return SpiResolver.resolveOrInstantiate(
                    (Class) converterClass, MessageConverter.class, globalConverter);
        }
        if (Objects.nonNull(serializerClass) && serializerClass != MessageSerializer.class) {
            try {
                MessageSerializer<?> serializer =
                        serializerClass.getDeclaredConstructor().newInstance();
                return new DefaultMessageConverter(serializer);
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException(
                        "Failed to instantiate serializer "
                                + serializerClass.getName()
                                + " (requires public no-arg constructor)",
                        e);
            }
        }
        return globalConverter;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<ConsumerFilter> buildFilters(ListenerRegistration<?> reg) {
        List<ConsumerFilter> allFilters = new ArrayList<>();

        String selectorExpression = reg.getSelectorExpression();
        if (StringUtils.isNotEmpty(selectorExpression)
                && !StreamMQConstants.SELECTOR_WILDCARD.equals(selectorExpression)) {
            SelectorType selectorType = reg.getSelectorType();
            ExpressionSelectorFilter selectorFilter =
                    switch (selectorType) {
                        case TAG -> new SimpleTagSelectorFilter(selectorExpression);
                        case SQL92 -> new SimpleSqlSelectorFilter(selectorExpression);
                    };
            allFilters.add(selectorFilter);
        }

        allFilters.addAll(globalFilterChain.getFilters());

        Class<? extends ConsumerFilter>[] perConsumerClasses =
                (Class<? extends ConsumerFilter>[]) reg.getConsumerFilter();
        if (Objects.nonNull(perConsumerClasses) && perConsumerClasses.length > 0) {
            for (Class<? extends ConsumerFilter> filterClass : perConsumerClasses) {
                ConsumerFilter filter = resolveFilter(filterClass);
                if (Objects.nonNull(filter)) {
                    allFilters.add(filter);
                }
            }
        }

        allFilters.sort(Comparator.comparingInt(ConsumerFilter::order));
        return Collections.unmodifiableList(allFilters);
    }

    private ConsumerFilter resolveFilter(Class<? extends ConsumerFilter> filterClass) {
        if (Objects.isNull(filterClass) || filterClass == ConsumerFilter.class) {
            return null;
        }
        ConsumerFilterResolver resolver = filterResolverSupplier.get();
        if (Objects.nonNull(resolver)) {
            ConsumerFilter filter = resolver.resolve(filterClass);
            if (Objects.nonNull(filter)) {
                return filter;
            }
        }
        try {
            return filterClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            LOG.warn(
                    "Failed to instantiate per-consumer filter {}: {}",
                    filterClass.getName(),
                    e.getMessage());
            return null;
        }
    }
}
