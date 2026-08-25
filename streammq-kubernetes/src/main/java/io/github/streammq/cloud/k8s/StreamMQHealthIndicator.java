/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * StreamMQ 健康指标，集成 Spring Boot Actuator 暴露健康状态。
 *
 * <p>由 {@link CloudK8sAutoConfiguration} 通过 {@code @Import} 装配， 启用条件由 AutoConfiguration 的
 * {@code @ConditionalOnClass(StreamMQListenerContainer.class)} 统一控制。通过 {@code ObjectProvider}
 * 可选注入容器，避免对 {@code streammq-redisson} 的强依赖。
 *
 * <p>暴露的健康信息：
 *
 * <ul>
 *   <li>状态：容器运行中为 UP，否则为 DOWN；纯生产者应用（无容器）视为 UP
 *   <li>activeConsumers：当前已注册的消费者数量
 *   <li>running：容器是否正在运行
 * </ul>
 *
 * <p>当 {@link StreamMQListenerContainer} 未装配时（纯生产者应用）报告 UP 并标注 producer-only。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMQHealthIndicator implements HealthIndicator {

    /** 健康详情 key：原因 */
    private static final String DETAIL_REASON = "reason";

    /** 健康详情 key：活跃消费者数量 */
    private static final String DETAIL_ACTIVE_CONSUMERS = "activeConsumers";

    /** 健康详情 key：运行标志 */
    private static final String DETAIL_RUNNING = "running";

    private final ObjectProvider<StreamMQListenerContainer> containerProvider;

    /**
     * 构造健康指标。
     *
     * @param containerProvider 监听器容器的可选注入提供者
     */
    public StreamMQHealthIndicator(ObjectProvider<StreamMQListenerContainer> containerProvider) {
        this.containerProvider = Objects.requireNonNull(containerProvider, "containerProvider");
    }

    @Override
    public Health health() {
        StreamMQListenerContainer container = containerProvider.getIfAvailable();
        if (Objects.isNull(container)) {
            // 纯生产者应用（未装配 Listener 容器）是合法形态：不应把整体健康拖为 DOWN，
            // 否则 K8s 会重启健康的生产者 Pod
            return Health.up()
                    .withDetail(DETAIL_REASON, "producer-only (no listener container)")
                    .build();
        }
        boolean running = container.isRunning();
        int activeConsumers = container.getConsumers().size();
        Map<String, Object> details = new LinkedHashMap<>();
        details.put(DETAIL_ACTIVE_CONSUMERS, activeConsumers);
        details.put(DETAIL_RUNNING, running);
        if (running) {
            return Health.up().withDetails(details).build();
        }
        return Health.down().withDetails(details).build();
    }
}
