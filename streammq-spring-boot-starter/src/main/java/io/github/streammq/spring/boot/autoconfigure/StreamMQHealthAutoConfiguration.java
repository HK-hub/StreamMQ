/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.security.DenyAllAuthenticator;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.listener.BroadcastGroupRegistry;
import io.github.streammq.core.policy.ManagementAuthenticator;
import io.github.streammq.core.policy.RateLimitedAuthenticator;
import io.github.streammq.spring.boot.StreamMQSpringConstants;
import java.util.Map;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 健康检查自动装配（条件装配：仅当 Actuator 在 classpath 时生效）。
 *
 * <p>注册 {@link StreamMQHealthIndicator}，对外暴露：
 *
 * <ul>
 *   <li>Redis 连接状态（基于 {@code RedissonClient} ping）
 *   <li>Listener 容器运行状态
 * </ul>
 *
 * <p>禁用方式：{@code streammq.enabled=false}（整体关闭）或 {@code streammq.health.enabled=false} （仅关闭健康检查）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({HealthIndicator.class, org.springframework.boot.actuate.health.Health.class})
@ConditionalOnExpression(
        "${"
                + StreamMQSpringConstants.PROP_PREFIX
                + "."
                + StreamMQSpringConstants.PROP_NAME_ENABLED
                + ":true}")
@ConditionalOnProperty(
        prefix = StreamMQSpringConstants.PROP_PREFIX_HEALTH,
        name = StreamMQSpringConstants.PROP_NAME_ENABLED,
        havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE,
        matchIfMissing = true)
public class StreamMQHealthAutoConfiguration {

    private static final Logger LOG =
            LoggerFactory.getLogger(StreamMQHealthAutoConfiguration.class);

    /**
     * StreamMQ 健康检查器：综合检查 Redis 连通性 + Listener 容器状态 + 调度器启动状态。
     *
     * @param redisson Redis 客户端
     * @param listenerContainerProvider Listener 容器（可选）
     * @param schedulerLifecycleProvider 调度器生命周期（可选，用于暴露部分启动失败）
     * @return HealthIndicator
     */
    @Bean
    @ConditionalOnMissingBean(name = StreamMQSpringConstants.BEAN_HEALTH_INDICATOR)
    public HealthIndicator streamMQHealthIndicator(
            RedissonClient redisson,
            org.springframework.beans.factory.ObjectProvider<DefaultStreamMQListenerContainer>
                    listenerContainerProvider,
            org.springframework.beans.factory.ObjectProvider<StreamMQSchedulerLifecycle>
                    schedulerLifecycleProvider) {
        LOG.debug("Creating StreamMQHealthIndicator");
        return new StreamMQHealthIndicator(
                redisson, listenerContainerProvider.getIfAvailable(), schedulerLifecycleProvider);
    }

    /** 管理端点的后端逻辑 Bean（供 StreamMQActuatorEndpoint 使用）。 */
    @Bean
    @ConditionalOnMissingBean(name = StreamMQSpringConstants.BEAN_ADMIN_ENDPOINT)
    @ConditionalOnProperty(
            prefix = "streammq.admin",
            name = StreamMQSpringConstants.PROP_NAME_ENABLED,
            havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE,
            matchIfMissing = true)
    public StreamMQAdminEndpoint streamMQAdminEndpoint(
            RedissonClient redisson,
            org.springframework.beans.factory.ObjectProvider<DefaultStreamMQListenerContainer>
                    listenerContainerProvider,
            io.github.streammq.spring.boot.properties.StreamMQProperties properties,
            org.springframework.beans.factory.ObjectProvider<BroadcastGroupRegistry>
                    registryProvider) {
        LOG.debug("Creating StreamMQAdminEndpoint");
        StreamMQAdminEndpoint adminEndpoint =
                new StreamMQAdminEndpoint(
                        redisson,
                        listenerContainerProvider.getIfAvailable(),
                        properties.getNamespace(),
                        properties.getAdmin().getFailureRetryCooldownMillis(),
                        registryProvider.getIfAvailable());
        adminEndpoint.setMaxPendingQuerySize(properties.getAdmin().getMaxPendingQuerySize());
        return adminEndpoint;
    }

    /**
     * Actuator 端点 Bean（注册到 /actuator/streammq）。
     *
     * <p>注入 {@link StreamMQHealthIndicator} 而非泛型 {@link HealthIndicator}， 避免当容器中存在多个 {@code
     * HealthIndicator} Bean 时触发 {@code NoUniqueBeanDefinitionException}。
     *
     * <p>{@link ManagementAuthenticator} 通过 {@link ObjectProvider} 防御性注入：当核心装配因 {@code
     * streammq.enabled=false} 回退、容器中不存在鉴权器 Bean 时， 使用 {@link DenyAllAuthenticator} 兜底，避免启动期 {@code
     * UnsatisfiedDependencyException}。
     */
    @Bean
    @ConditionalOnMissingBean(name = StreamMQSpringConstants.BEAN_ACTUATOR_ENDPOINT)
    @ConditionalOnClass(org.springframework.boot.actuate.endpoint.annotation.Endpoint.class)
    @ConditionalOnProperty(
            prefix = "streammq.admin",
            name = StreamMQSpringConstants.PROP_NAME_ENABLED,
            havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE,
            matchIfMissing = true)
    public StreamMQActuatorEndpoint streamMQActuatorEndpoint(
            StreamMQAdminEndpoint adminEndpoint,
            org.springframework.beans.factory.ObjectProvider<StreamMQHealthIndicator>
                    healthIndicatorProvider,
            ObjectProvider<ManagementAuthenticator> authenticatorProvider,
            io.github.streammq.spring.boot.properties.StreamMQProperties properties) {
        LOG.debug("Creating StreamMQActuatorEndpoint");
        ManagementAuthenticator authenticator =
                authenticatorProvider.getIfAvailable(DenyAllAuthenticator::new);
        // 包一层失败限流：即使启用 Basic/Token 弱凭据，也能抵御针对管理端点的暴力破解
        ManagementAuthenticator rateLimited = new RateLimitedAuthenticator(authenticator);
        StreamMQActuatorEndpoint endpoint =
                new StreamMQActuatorEndpoint(
                        adminEndpoint, healthIndicatorProvider.getIfAvailable(), rateLimited);
        endpoint.setListPageSize(properties.getAdmin().getListPageSize());
        return endpoint;
    }

    /** StreamMQ 健康检查实现。 */
    public static class StreamMQHealthIndicator implements HealthIndicator {

        private final RedissonClient redisson;
        private final DefaultStreamMQListenerContainer listenerContainer;
        private final org.springframework.beans.factory.ObjectProvider<StreamMQSchedulerLifecycle>
                schedulerLifecycleProvider;

        /**
         * 构造健康检查器。
         *
         * @param redisson Redis 客户端
         * @param listenerContainer Listener 容器（可为 null，表示未装配）
         * @param schedulerLifecycleProvider 调度器生命周期提供者（可为 null，表示未装配）
         */
        public StreamMQHealthIndicator(
                RedissonClient redisson,
                DefaultStreamMQListenerContainer listenerContainer,
                org.springframework.beans.factory.ObjectProvider<StreamMQSchedulerLifecycle>
                        schedulerLifecycleProvider) {
            this.redisson = redisson;
            this.listenerContainer = listenerContainer;
            this.schedulerLifecycleProvider = schedulerLifecycleProvider;
        }

        @Override
        public Health health() {
            try {
                // Redis 连通性检查：GET 只读命令验证真实链路（兼容单机/集群/哨兵，且无弃用 API）
                long start = System.currentTimeMillis();
                long val = redisson.getAtomicLong(StreamMQConstants.HEALTH_CHECK_KEY).get();
                long elapsed = System.currentTimeMillis() - start;
                boolean schedulersHealthy = isSchedulersHealthy();
                Health.Builder builder =
                        isListenerContainerHealthy() && schedulersHealthy
                                ? Health.up()
                                : Health.down();
                builder.withDetail(StreamMQSpringConstants.HEALTH_DETAIL_PING_LATENCY, elapsed);
                builder.withDetail(StreamMQSpringConstants.HEALTH_DETAIL_HEALTH_VALUE, val);
                buildListenerContainerDetails(builder);
                buildSchedulerDetails(builder);
                return builder.build();
            } catch (RuntimeException ex) {
                return Health.down(ex)
                        .withDetail(
                                StreamMQSpringConstants.HEALTH_DETAIL_ERROR,
                                "Redis ping failed: " + ex.getMessage())
                        .build();
            }
        }

        /**
         * 容器已装配但未运行时视为不健康（与 Binder 健康指标行为对齐）。
         *
         * <p>此外，任何消费循环启动失败同样视为不健康：这些监听器在注册表中可见但永远不会消费， 属于"静默故障"——如果健康检查仍报 UP，运维几乎不可能从指标上发现。
         */
        private boolean isListenerContainerHealthy() {
            if (listenerContainer == null) {
                return true;
            }
            return listenerContainer.isRunning() && listenerContainer.isConsumeLoopsHealthy();
        }

        /** 调度器存在部分启动失败时视为不健康：调度器未装配（provider 为空或无 Bean）不影响整体状态。 */
        private boolean isSchedulersHealthy() {
            if (schedulerLifecycleProvider == null) {
                return true;
            }
            StreamMQSchedulerLifecycle lifecycle = schedulerLifecycleProvider.getIfAvailable();
            if (lifecycle == null) {
                return true;
            }
            return lifecycle.getSchedulerStatuses().values().stream()
                    .noneMatch(
                            status ->
                                    status.startsWith(
                                            StreamMQSchedulerLifecycle.STATUS_FAILED_PREFIX));
        }

        private void buildSchedulerDetails(Health.Builder builder) {
            if (schedulerLifecycleProvider == null) {
                return;
            }
            StreamMQSchedulerLifecycle lifecycle = schedulerLifecycleProvider.getIfAvailable();
            if (lifecycle != null) {
                builder.withDetail(
                        StreamMQSpringConstants.HEALTH_DETAIL_SCHEDULER_STATUSES,
                        Map.copyOf(lifecycle.getSchedulerStatuses()));
            }
        }

        private void buildListenerContainerDetails(Health.Builder builder) {
            if (listenerContainer != null) {
                builder.withDetail(
                        StreamMQSpringConstants.HEALTH_DETAIL_LC_STATE,
                        listenerContainer.getState().name());
                builder.withDetail(
                        StreamMQSpringConstants.HEALTH_DETAIL_LC_RUNNING,
                        listenerContainer.isRunning());
                builder.withDetail(
                        StreamMQSpringConstants.HEALTH_DETAIL_LC_COUNT,
                        listenerContainer.getConsumers().size());
                java.util.Map<String, String> loopFailures =
                        listenerContainer.getConsumeLoopFailures();
                if (!loopFailures.isEmpty()) {
                    builder.withDetail(
                            StreamMQSpringConstants.HEALTH_DETAIL_LC_LOOP_FAILURES,
                            Map.copyOf(loopFailures));
                }
            } else {
                builder.withDetail(
                        StreamMQSpringConstants.HEALTH_DETAIL_LC_STATE,
                        StreamMQSpringConstants.HEALTH_VALUE_NOT_CONFIGURED);
            }
        }
    }
}
