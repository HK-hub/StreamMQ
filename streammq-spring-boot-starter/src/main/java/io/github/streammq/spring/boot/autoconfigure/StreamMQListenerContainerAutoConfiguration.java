/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.scheduler.PelClaimScheduler;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.filter.ConsumerFilter;
import io.github.streammq.core.interceptor.ConsumerInterceptor;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.metrics.StreamMQMetrics;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.spring.boot.StreamMQSpringConstants;
import io.github.streammq.spring.boot.properties.StreamMQProperties;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Listener 容器自动装配：注册 {@link DefaultStreamMQListenerContainer}、 {@link StreamMQListenerRegistrar} 与
 * {@link StreamMQListenerContainerLifecycle}。
 *
 * <p>装配条件：
 *
 * <ul>
 *   <li>{@code streammq.enabled=true}
 *   <li>classpath 存在 {@link DefaultStreamMQListenerContainer} 与 {@link RedissonClient}
 *   <li>存在已注册的 {@link StreamMQListenerFactory} Bean
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@AutoConfigureAfter(StreamMQCoreAutoConfiguration.class)
@ConditionalOnProperty(
        prefix = StreamMQSpringConstants.PROP_PREFIX,
        name = StreamMQSpringConstants.PROP_NAME_ENABLED,
        havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE,
        matchIfMissing = true)
@ConditionalOnClass({DefaultStreamMQListenerContainer.class, RedissonClient.class})
@ConditionalOnBean(StreamMQListenerFactory.class)
public class StreamMQListenerContainerAutoConfiguration {

    private static final Logger LOG =
            LoggerFactory.getLogger(StreamMQListenerContainerAutoConfiguration.class);

    /**
     * 默认 Listener 容器。
     *
     * @param redisson Redisson 客户端
     * @param consumerFactory 消费者工厂
     * @param messageConverter 消息转换器
     * @param retryPolicy 重试策略
     * @param properties 配置
     * @return Listener 容器
     */
    @Bean
    @ConditionalOnMissingBean(DefaultStreamMQListenerContainer.class)
    public DefaultStreamMQListenerContainer streamMQListenerContainer(
            RedissonClient redisson,
            StreamMQListenerFactory consumerFactory,
            MessageConverter messageConverter,
            RetryPolicy retryPolicy,
            DlqFailureStrategy dlqFailureStrategy,
            DlqConfig dlqConfig,
            StreamMQProperties properties,
            ObjectProvider<ConsumerInterceptor> consumerInterceptorProvider,
            ObjectProvider<ConsumerFilter> consumerFilterProvider,
            ObjectProvider<StreamMQMetrics> metricsProvider,
            ObjectProvider<PelClaimScheduler> pelClaimSchedulerProvider,
            @org.springframework.beans.factory.annotation.Qualifier("streammqExecutor")
                    org.springframework.beans.factory.ObjectProvider<
                                    java.util.concurrent.ExecutorService>
                            executorProvider,
            ApplicationContext applicationContext) {
        String namespace = properties.getNamespace();
        LOG.info(
                "Creating DefaultStreamMQListenerContainer: namespace={}, dlqFailureStrategy={}",
                namespace,
                dlqFailureStrategy.name());
        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson,
                        consumerFactory,
                        messageConverter,
                        retryPolicy,
                        dlqFailureStrategy,
                        dlqConfig,
                        namespace);

        // 消费者全局默认配置：注解未显式指定时生效（streammq.consumer.* / streammq.rebalance.*）
        container.setDefaultPullBatchSize(properties.getConsumer().getBatchSize());
        container.setDefaultPullBlockTimeoutMillis(
                properties.getConsumer().getPollTimeout().toMillis());
        container.setDefaultPullIntervalMillis(properties.getConsumer().getPullInterval());
        container.setMaxBatchSizeLimit(properties.getConsumer().getMaxBatchSizeLimit());
        container.setInflightCapacity(properties.getConsumer().getInflightCapacity());
        container.setDefaultVirtualNodes(properties.getRebalance().getVirtualNodes());
        // 统一线程模型：容器消费循环复用 streammqExecutor（仅识别该名称的 Bean，用户可同名覆盖自定义）
        java.util.concurrent.ExecutorService executor = executorProvider.getIfAvailable();
        if (executor != null) {
            container.setConsumeExecutor(executor);
            LOG.info("Injected streammqExecutor into ListenerContainer");
        }
        // 消费超时取消宽限期与消费者组心跳/实例超时（streammq.consumer.* / streammq.group.*）
        container.setTimeoutCancelGraceMillis(
                properties.getConsumer().getTimeoutCancelGraceMillis());
        container.setHeartbeatIntervalMs(properties.getGroup().getHeartbeatIntervalMs());
        container.setInstanceTimeoutMs(properties.getGroup().getInstanceTimeoutMs());
        LOG.info(
                "ListenerContainer defaults: pullBatchSize={}, pullBlockTimeout={}ms,"
                        + " pullInterval={}ms, maxBatchSizeLimit={}, virtualNodes={}",
                properties.getConsumer().getBatchSize(),
                properties.getConsumer().getPollTimeout().toMillis(),
                properties.getConsumer().getPullInterval(),
                properties.getConsumer().getMaxBatchSizeLimit(),
                properties.getRebalance().getVirtualNodes());

        container.setFilterResolver(
                filterClass -> {
                    try {
                        return applicationContext.getBean(filterClass);
                    } catch (org.springframework.beans.factory.NoSuchBeanDefinitionException e) {
                        return null;
                    }
                });

        // 注入指标收集器：消费指标记录在容器，重试 / 死信指标传播到内部 DefaultRetryAndDlqHandler
        StreamMQMetrics metrics = metricsProvider.getIfAvailable();
        if (metrics != null) {
            container.setMetrics(metrics);
            container.setHandlerMetrics(metrics);
            LOG.info(
                    "StreamMQMetrics injected into DefaultStreamMQListenerContainer:"
                            + " consume/retry/dlq metrics enabled");
        }

        java.util.List<ConsumerInterceptor> interceptors =
                consumerInterceptorProvider.stream().toList();
        if (!interceptors.isEmpty()) {
            LOG.info(
                    "Registering {} ConsumerInterceptor(s): {}",
                    interceptors.size(),
                    interceptors.stream().map(ConsumerInterceptor::name).toList());
            container.addConsumerInterceptors(interceptors);
        }

        java.util.List<ConsumerFilter> filters = consumerFilterProvider.stream().toList();
        if (!filters.isEmpty()) {
            LOG.info(
                    "Registering {} ConsumerFilter(s): {}",
                    filters.size(),
                    filters.stream().map(ConsumerFilter::name).toList());
            container.addConsumerFilters(filters);
        }

        // 注入顺序消费 PEL 认领调度器（可选）：容器启动时注册 ORDERLY 消费目标
        PelClaimScheduler pelClaimScheduler = pelClaimSchedulerProvider.getIfAvailable();
        if (pelClaimScheduler != null) {
            container.setPelClaimScheduler(pelClaimScheduler);
            LOG.info("PelClaimScheduler injected into DefaultStreamMQListenerContainer");
        }

        return container;
    }

    /**
     * Listener 注册器：在所有单例 Bean 初始化后扫描注解并注册 Listener。
     *
     * @param listenerContainer Listener 容器
     * @return 注册器
     */
    @Bean
    @ConditionalOnMissingBean(StreamMQListenerRegistrar.class)
    public StreamMQListenerRegistrar streamMQListenerRegistrar(
            DefaultStreamMQListenerContainer listenerContainer) {
        return new StreamMQListenerRegistrar(listenerContainer);
    }

    /**
     * 容器生命周期：通过 SmartLifecycle 启动与停止 Listener 容器。
     *
     * @param listenerContainer Listener 容器
     * @return 生命周期包装
     */
    @Bean
    @ConditionalOnMissingBean(name = StreamMQSpringConstants.BEAN_LISTENER_CONTAINER_LIFECYCLE)
    public StreamMQListenerContainerLifecycle streamMQListenerContainerLifecycle(
            DefaultStreamMQListenerContainer listenerContainer) {
        return new StreamMQListenerContainerLifecycle(listenerContainer);
    }
}
