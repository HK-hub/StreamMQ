package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.metrics.StreamMQMetrics;
import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * StreamMQ Micrometer 指标自动装配。
 *
 * <p>当 classpath 中存在 {@link MeterRegistry} 且用户已引入 Actuator 时，
 * 自动注册 {@link StreamMQMetrics} Bean，供容器 / 模板 / 调度器记录指标。
 *
 * <p>禁用方式：{@code streammq.enabled=false}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(MeterRegistry.class)
@ConditionalOnProperty(prefix = "streammq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class StreamMQMetricsAutoConfiguration {

    /**
     * 注册 StreamMQ 指标收集器。
     *
     * @param meterRegistry Micrometer 注册表（由 Spring Boot Actuator 自动提供）
     * @return StreamMqMetrics 实例
     */
    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean(StreamMQMetrics.class)
    public StreamMQMetrics streamMqMetrics(MeterRegistry meterRegistry) {
        return new StreamMQMetrics(meterRegistry);
    }
}
