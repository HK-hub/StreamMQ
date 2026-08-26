/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s;

import io.github.streammq.cloud.k8s.autoscaler.HpaAutoScaler;
import io.github.streammq.cloud.k8s.config.ConfigMapConfigRefresher;
import io.github.streammq.cloud.k8s.operator.StreamMQClusterController;
import io.github.streammq.core.listener.StreamMQListenerContainer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * StreamMQ cloud native K8s enhancement module auto-configuration.
 *
 * <p><b>EXPERIMENTAL (实验性)：</b>本模块为 CRD 控制器 / HPA / 热更新的早期预览实现， 默认不启用，需显式配置 {@code
 * streammq.cloud.k8s.enabled=true}。
 *
 * <p>Enable conditions:
 *
 * <ul>
 *   <li>Classpath contains {@link StreamMQListenerContainer}
 *   <li>Property {@code streammq.cloud.k8s.enabled=true}（默认 OFF，避免引入 jar 即产生副作用）
 *   <li>{@code streammq.cloud.k8s.operator.enabled}（默认 true，总开关开启时生效）
 * </ul>
 *
 * <p>Components registered:
 *
 * <ul>
 *   <li>{@link StreamMQHealthIndicator} - Spring Boot Actuator health indicator
 *   <li>{@link StreamMQHealthController} - K8s liveness and readiness probe REST endpoints
 *   <li>{@link NoopConfigRefresher} - Config refresh no-op default (user can override)
 *   <li>{@link GracefulShutdownHandler} - Graceful shutdown handler
 *   <li>{@link HpaMetricsProvider} - HPA metrics provider
 *   <li>{@link StreamMQClusterController} - StreamMQCluster CRD controller
 *   <li>{@link HpaAutoScaler} - HPA auto-scaling controller
 *   <li>{@link ConfigMapConfigRefresher} - ConfigMap watch config refresher (wraps user refresher)
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@AutoConfiguration
@ConditionalOnClass(StreamMQListenerContainer.class)
@ConditionalOnProperty(
        prefix = CloudK8sProperties.PROP_PREFIX,
        name = CloudK8sProperties.PROP_NAME_ENABLED,
        havingValue = CloudK8sProperties.PROP_VALUE_TRUE,
        matchIfMissing = false)
@EnableConfigurationProperties(CloudK8sProperties.class)
public class CloudK8sAutoConfiguration {

    /** Operator/HPA/热更新子开关：总开关开启后，仍可通过 operator.enabled=false 只用健康检查等轻量能力。 */
    @ConditionalOnProperty(
            prefix = CloudK8sProperties.PROP_PREFIX,
            name = "operator.enabled",
            havingValue = "true",
            matchIfMissing = true)
    @ConditionalOnClass(
            io.fabric8.kubernetes.client.KubernetesClient
                    .class) // 模块依赖为 provided：classpath 缺失 fabric8 时优雅跳过而非 NoClassDefFoundError
    static class OperatorConfiguration {

        @Bean
        public StreamMQClusterController streamMQClusterController(CloudK8sProperties properties) {
            StreamMQClusterController controller = new StreamMQClusterController();
            controller.setReconcileIntervalSeconds(properties.getReconcileIntervalSeconds());
            return controller;
        }

        @Bean
        public HpaAutoScaler hpaAutoScaler(CloudK8sProperties properties) {
            HpaAutoScaler scaler = new HpaAutoScaler();
            scaler.setSyncIntervalSeconds(properties.getHpaSyncIntervalSeconds());
            scaler.setDefaultTargetLag(properties.getHpaDefaultTargetLag());
            scaler.setScaleUpThreshold(properties.getHpaScaleUpThreshold());
            scaler.setScaleDownThreshold(properties.getHpaScaleDownThreshold());
            return scaler;
        }

        /**
         * ConfigMap 配置热更新：包装用户提供的 {@link StreamMQConfigRefresher}（或内部 Noop）， 由其 SmartLifecycle
         * 生命周期启动/停止 watch。唯一入口，避免多 Bean 注入歧义。
         */
        @Bean(destroyMethod = "stop")
        public ConfigMapConfigRefresher configMapConfigRefresher(
                CloudK8sProperties properties,
                org.springframework.beans.factory.ObjectProvider<StreamMQConfigRefresher>
                        userRefresher) {
            StreamMQConfigRefresher delegate =
                    userRefresher.getIfAvailable(NoopConfigRefresher::new);
            ConfigMapConfigRefresher refresher = new ConfigMapConfigRefresher(delegate);
            refresher.setWatchNamespaces(properties.getConfigWatchNamespaces());
            return refresher;
        }
    }
}
