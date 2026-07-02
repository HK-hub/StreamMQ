package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
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
 * <p>注册 {@link StreamMqHealthIndicator}，对外暴露：
 * <ul>
 *   <li>Redis 连接状态（基于 {@code RedissonClient} ping）</li>
 *   <li>Listener 容器运行状态</li>
 * </ul>
 *
 * <p>禁用方式：{@code streammq.health.enabled=false}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({HealthIndicator.class, org.springframework.boot.actuate.health.Health.class})
@ConditionalOnProperty(prefix = "streammq.health", name = "enabled", havingValue = "true", matchIfMissing = true)
public class StreamMQHealthAutoConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMQHealthAutoConfiguration.class);

    /** Redis 健康探测使用的 AtomicLong Key */
    private static final String HEALTH_CHECK_KEY = "streammq:health-check";

    /**
     * StreamMQ 健康检查器：综合检查 Redis 连通性 + Listener 容器状态。
     *
     * @param redisson Redisson 客户端
     * @param listenerContainerProvider Listener 容器（可选）
     * @return HealthIndicator
     */
    @Bean
    @ConditionalOnMissingBean(name = "streamMqHealthIndicator")
    public HealthIndicator streamMqHealthIndicator(RedissonClient redisson,
            org.springframework.beans.factory.ObjectProvider<DefaultStreamMQListenerContainer> listenerContainerProvider) {
        LOG.info("Creating StreamMqHealthIndicator");
        return new StreamMqHealthIndicator(redisson, listenerContainerProvider.getIfAvailable());
    }

    /**
     * StreamMQ 健康检查实现。
     */
    public static class StreamMqHealthIndicator implements HealthIndicator {

        private final RedissonClient redisson;
        private final DefaultStreamMQListenerContainer listenerContainer;

        /**
         * 构造健康检查器。
         *
         * @param redisson Redisson 客户端
         * @param listenerContainer Listener 容器（可为 null，表示未装配）
         */
        public StreamMqHealthIndicator(RedissonClient redisson,
                                       DefaultStreamMQListenerContainer listenerContainer) {
            this.redisson = redisson;
            this.listenerContainer = listenerContainer;
        }

        @Override
        public Health health() {
            Health.Builder builder = Health.up();
            try {
                // Redis 连通性检查（通过读取一个不存在的 key 来触发网络请求）
                long start = System.currentTimeMillis();
                long val = redisson.getAtomicLong(HEALTH_CHECK_KEY).get();
                long elapsed = System.currentTimeMillis() - start;
                builder.withDetail("redis.ping.latencyMs", elapsed);
                builder.withDetail("redis.health.value", val);
            } catch (RuntimeException ex) {
                return Health.down(ex)
                    .withDetail("error", "Redis ping failed: " + ex.getMessage())
                    .build();
            }
            // Listener 容器状态
            if (listenerContainer != null) {
                builder.withDetail("listenerContainer.state",
                    listenerContainer.getState().name());
                builder.withDetail("listenerContainer.running",
                    listenerContainer.isRunning());
                builder.withDetail("listenerContainer.listenerCount",
                    listenerContainer.getConsumers().size());
            } else {
                builder.withDetail("listenerContainer.state", "NOT_CONFIGURED");
            }
            return builder.build();
        }
    }
}
