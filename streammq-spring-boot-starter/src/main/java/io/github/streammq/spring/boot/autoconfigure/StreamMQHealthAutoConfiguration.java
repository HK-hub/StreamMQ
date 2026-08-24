package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.policy.ManagementAuthenticator;
import io.github.streammq.spring.boot.StreamMQSpringConstants;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
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
 * <p>禁用方式：{@code streammq.health.enabled=false}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({HealthIndicator.class, org.springframework.boot.actuate.health.Health.class})
@ConditionalOnProperty(
        prefix = StreamMQSpringConstants.PROP_PREFIX_HEALTH,
        name = StreamMQSpringConstants.PROP_NAME_ENABLED,
        havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE,
        matchIfMissing = true)
public class StreamMQHealthAutoConfiguration {

    private static final Logger LOG =
            LoggerFactory.getLogger(StreamMQHealthAutoConfiguration.class);

    /**
     * StreamMQ 健康检查器：综合检查 Redis 连通性 + Listener 容器状态。
     *
     * @param redisson Redisson 客户端
     * @param listenerContainerProvider Listener 容器（可选）
     * @return HealthIndicator
     */
    @Bean
    @ConditionalOnMissingBean(name = StreamMQSpringConstants.BEAN_HEALTH_INDICATOR)
    public HealthIndicator streamMQHealthIndicator(
            RedissonClient redisson,
            org.springframework.beans.factory.ObjectProvider<DefaultStreamMQListenerContainer>
                    listenerContainerProvider) {
        LOG.debug("Creating StreamMQHealthIndicator");
        return new StreamMQHealthIndicator(redisson, listenerContainerProvider.getIfAvailable());
    }

    /** 管理端点的后端逻辑 Bean（供 StreamMQActuatorEndpoint 使用）。 */
    @Bean
    @ConditionalOnMissingBean(name = StreamMQSpringConstants.BEAN_ADMIN_ENDPOINT)
    public StreamMQAdminEndpoint streamMQAdminEndpoint(
            RedissonClient redisson,
            org.springframework.beans.factory.ObjectProvider<DefaultStreamMQListenerContainer>
                    listenerContainerProvider,
            io.github.streammq.spring.boot.properties.StreamMQProperties properties) {
        LOG.debug("Creating StreamMQAdminEndpoint");
        StreamMQAdminEndpoint adminEndpoint =
                new StreamMQAdminEndpoint(
                        redisson,
                        listenerContainerProvider.getIfAvailable(),
                        properties.getNamespace());
        adminEndpoint.setMaxPendingQuerySize(properties.getAdmin().getMaxPendingQuerySize());
        return adminEndpoint;
    }

    /**
     * Actuator 端点 Bean（注册到 /actuator/streammq）。
     *
     * <p>注入 {@link StreamMQHealthIndicator} 而非泛型 {@link HealthIndicator}， 避免当容器中存在多个 {@code
     * HealthIndicator} Bean 时触发 {@code NoUniqueBeanDefinitionException}。
     */
    @Bean
    @ConditionalOnMissingBean(name = StreamMQSpringConstants.BEAN_ACTUATOR_ENDPOINT)
    @ConditionalOnClass(org.springframework.boot.actuate.endpoint.annotation.Endpoint.class)
    public StreamMQActuatorEndpoint streamMQActuatorEndpoint(
            StreamMQAdminEndpoint adminEndpoint,
            org.springframework.beans.factory.ObjectProvider<StreamMQHealthIndicator>
                    healthIndicatorProvider,
            ManagementAuthenticator authenticator,
            io.github.streammq.spring.boot.properties.StreamMQProperties properties) {
        LOG.debug("Creating StreamMQActuatorEndpoint");
        StreamMQActuatorEndpoint endpoint =
                new StreamMQActuatorEndpoint(
                        adminEndpoint,
                        healthIndicatorProvider.getIfAvailable(),
                        authenticator);
        endpoint.setListPageSize(properties.getAdmin().getListPageSize());
        return endpoint;
    }

    /** StreamMQ 健康检查实现。 */
    public static class StreamMQHealthIndicator implements HealthIndicator {

        private final RedissonClient redisson;
        private final DefaultStreamMQListenerContainer listenerContainer;

        /**
         * 构造健康检查器。
         *
         * @param redisson Redisson 客户端
         * @param listenerContainer Listener 容器（可为 null，表示未装配）
         */
        public StreamMQHealthIndicator(
                RedissonClient redisson, DefaultStreamMQListenerContainer listenerContainer) {
            this.redisson = redisson;
            this.listenerContainer = listenerContainer;
        }

        @Override
        public Health health() {
            Health.Builder builder = Health.up();
            try {
                // Redis 连通性检查（通过 GET 命令验证真实连通性）
                long start = System.currentTimeMillis();
                long val = redisson.getAtomicLong(StreamMQConstants.HEALTH_CHECK_KEY).get();
                long elapsed = System.currentTimeMillis() - start;
                builder.withDetail(StreamMQSpringConstants.HEALTH_DETAIL_PING_LATENCY, elapsed);
                builder.withDetail(StreamMQSpringConstants.HEALTH_DETAIL_HEALTH_VALUE, val);
            } catch (RuntimeException ex) {
                return Health.down(ex)
                        .withDetail(
                                StreamMQSpringConstants.HEALTH_DETAIL_ERROR,
                                "Redis ping failed: " + ex.getMessage())
                        .build();
            }
            // Listener 容器状态
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
            } else {
                builder.withDetail(
                        StreamMQSpringConstants.HEALTH_DETAIL_LC_STATE,
                        StreamMQSpringConstants.HEALTH_VALUE_NOT_CONFIGURED);
            }
            return builder.build();
        }
    }
}
