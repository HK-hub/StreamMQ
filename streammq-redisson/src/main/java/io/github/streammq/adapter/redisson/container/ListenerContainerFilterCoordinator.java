/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.filter.ReflectiveConsumerFilterResolver;
import io.github.streammq.core.filter.ConsumerFilter;
import io.github.streammq.core.filter.ConsumerFilterChain;
import io.github.streammq.core.filter.ConsumerFilterResolver;
import io.github.streammq.core.interceptor.ConsumerInterceptor;
import io.github.streammq.core.interceptor.ConsumerInterceptorChain;
import java.util.Collection;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinator for global filter and interceptor chains on the listener container.
 *
 * <p>Extracted from {@code DefaultStreamMQListenerContainer} to reduce its surface area; the container
 * delegates "add a filter / interceptor" calls to this class and the actual filter rebuild is
 * driven lazily via {@link #rebuildFilters(RegistrationStore, PerConsumerSpiResolver)}.
 *
 * <p><b>Why this class exists</b>:
 *
 * <ul>
 *   <li>Single responsibility: own the global filter chain and the per-consumer resolver state.
 *   <li>Replaceable: tests / advanced users can supply an alternative coordinator.
 *   <li>Testable in isolation without spinning up the full container.
 * </ul>
 *
 * <p>Thread safety: all delegated collections are themselves thread-safe; the methods are safe to
 * call from any thread as long as the container is in {@code INIT} or {@code RUNNING} state.
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class ListenerContainerFilterCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(ListenerContainerFilterCoordinator.class);

    private final ConsumerFilterChain consumerFilterChain;
    private final ConsumerInterceptorChain interceptorChain;
    private volatile ConsumerFilterResolver filterResolver =
            new ReflectiveConsumerFilterResolver();

    public ListenerContainerFilterCoordinator(
            ConsumerFilterChain consumerFilterChain,
            ConsumerInterceptorChain interceptorChain) {
        this.consumerFilterChain =
                Objects.requireNonNull(consumerFilterChain, "consumerFilterChain");
        this.interceptorChain = Objects.requireNonNull(interceptorChain, "interceptorChain");
    }

    /** Replace the per-consumer filter resolver. */
    public void setFilterResolver(ConsumerFilterResolver resolver) {
        this.filterResolver = Objects.requireNonNull(resolver, "filterResolver");
    }

    public ConsumerFilterResolver filterResolver() {
        return filterResolver;
    }

    // ===================== Filter chain (consumer-side) =====================

    /** Add a single consumer filter. Triggers filter cache rebuild. */
    public void addFilter(ConsumerFilter filter, RegistrationStore store,
                          PerConsumerSpiResolver spiResolver) {
        consumerFilterChain.addFilter(filter);
        rebuildFilters(store, spiResolver);
    }

    /** Add multiple consumer filters. Triggers filter cache rebuild. */
    public void addFilters(Collection<ConsumerFilter> filters, RegistrationStore store,
                           PerConsumerSpiResolver spiResolver) {
        consumerFilterChain.addFilters(filters);
        rebuildFilters(store, spiResolver);
    }

    // ===================== Interceptor chain (consumer-side) =====================

    /** Add a single consumer interceptor. */
    public void addInterceptor(ConsumerInterceptor interceptor) {
        interceptorChain.addInterceptor(interceptor);
    }

    /** Add multiple consumer interceptors. */
    public void addInterceptors(Collection<ConsumerInterceptor> interceptors) {
        interceptorChain.addInterceptors(interceptors);
    }

    public ConsumerInterceptorChain interceptorChain() {
        return interceptorChain;
    }

    // ===================== Internal =====================

    /** Rebuild per-consumer filter cache across all current registrations. */
    public void rebuildFilters(RegistrationStore store, PerConsumerSpiResolver spiResolver) {
        if (Objects.isNull(spiResolver) || Objects.isNull(store)) {
            return;
        }
        for (var reg : store.registrations()) {
            spiResolver.rebuildFilters(reg, store);
        }
        LOG.debug("Rebuilt consumer filter cache for {} registrations", store.registrationCount());
    }
}
