/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.metrics.MicrometerStreamMQMetrics;
import io.github.streammq.core.metrics.StreamMQMetrics;
import io.github.streammq.spring.boot.StreamMQSpringConstants;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * StreamMQ Micrometer 指标自动装配。
 *
 * <p>当 classpath 中存在 {@link MeterRegistry} 且用户已引入 Actuator 时， 自动注册 {@link StreamMQMetrics} Bean，供容器
 * / 模板 / 调度器记录指标。
 *
 * <p>通过 {@link AutoConfigureAfter} 确保在 Spring Boot 的 {@code MetricsAutoConfiguration} 与 {@code
 * CompositeMeterRegistryAutoConfiguration}（注册 {@code MeterRegistry}）之后装配， 避免
 * {@code @ConditionalOnBean(MeterRegistry)} 因装配顺序而静默失效。本配置类必须同时登记在 {@code
 * AutoConfiguration.imports} 文件中，排序提示才会生效（{@code @Import} 引入的配置类不参与 .imports 排序）。
 *
 * <p>禁用方式：{@code streammq.enabled=false}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@AutoConfigureAfter(
        name = {
            "org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration",
            "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration"
        })
@ConditionalOnClass(MeterRegistry.class)
@ConditionalOnProperty(
        prefix = StreamMQSpringConstants.PROP_PREFIX,
        name = StreamMQSpringConstants.PROP_NAME_ENABLED,
        havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE,
        matchIfMissing = true)
public class StreamMQMetricsAutoConfiguration {

    /**
     * 注册 StreamMQ 指标收集器。
     *
     * @param meterRegistry Micrometer 注册表（由 Spring Boot Actuator 自动提供）
     * @return StreamMQMetrics 实例
     */
    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean(StreamMQMetrics.class)
    public StreamMQMetrics streamMQMetrics(MeterRegistry meterRegistry) {
        return new MicrometerStreamMQMetrics(meterRegistry);
    }
}
