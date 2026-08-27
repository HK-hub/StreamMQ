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
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
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
 *   <li>{@link StreamMQHealthController} - K8s liveness and readiness probe REST endpoints（仅
 *       Servlet Web 环境且 {@code health-endpoint-enabled=true} 时注册）
 *   <li>{@link GracefulShutdownHandler} - Graceful shutdown handler
 *   <li>{@link NoopConfigRefresher} - Config refresh no-op default (user can override)
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

    /**
     * 健康探针与优雅关闭（轻量能力，不依赖 fabric8）。
     *
     * <p>受 {@code streammq.cloud.k8s.health-endpoint-enabled} 控制（默认开启）； 探针 REST 控制器仅在 Servlet Web
     * 环境注册。
     */
    @ConditionalOnClass(HealthIndicator.class)
    static class HealthConfiguration {

        /** K8s 健康指标：容器运行中为 UP，纯生产者应用视为 UP。 */
        @Bean
        @ConditionalOnMissingBean(StreamMQHealthIndicator.class)
        public StreamMQHealthIndicator streamMQK8sHealthIndicator(
                ObjectProvider<StreamMQListenerContainer> containerProvider) {
            return new StreamMQHealthIndicator(containerProvider);
        }

        /**
         * K8s 存活 / 就绪探针 REST 端点（{@code GET /streammq/health/liveness|readiness}）。
         *
         * <p>需要 Servlet Web 栈提供请求映射；非 Web 环境跳过而非报错。
         */
        @Bean
        @ConditionalOnMissingBean(StreamMQHealthController.class)
        @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
        @ConditionalOnProperty(
                prefix = CloudK8sProperties.PROP_PREFIX,
                name = "health-endpoint-enabled",
                havingValue = CloudK8sProperties.PROP_VALUE_TRUE,
                matchIfMissing = true)
        public StreamMQHealthController streamMQHealthController(
                ObjectProvider<StreamMQListenerContainer> containerProvider) {
            return new StreamMQHealthController(containerProvider);
        }

        /**
         * 优雅关闭处理器：容器关闭时暂停拉取、等待 in-flight 消息完成、停止容器。
         *
         * <p>实现 {@link org.springframework.beans.factory.DisposableBean}，由 Spring 容器在关闭期回调。
         */
        @Bean
        @ConditionalOnMissingBean(GracefulShutdownHandler.class)
        public GracefulShutdownHandler gracefulShutdownHandler(
                ObjectProvider<StreamMQListenerContainer> containerProvider,
                CloudK8sProperties properties) {
            return new GracefulShutdownHandler(containerProvider, properties);
        }
    }

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
            // watch 范围：默认全命名空间（需 ClusterRole RBAC），可收敛为指定列表
            controller.setWatchAllNamespaces(properties.isOperatorWatchAllNamespaces());
            controller.setWatchNamespaces(properties.getOperatorWatchNamespaces());
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

        /** HPA 指标提供者：内存态默认实现，用户可注册自定义 Bean（如接入真实指标源）覆盖。 */
        @Bean
        @ConditionalOnMissingBean(HpaMetricsProvider.class)
        public HpaMetricsProvider hpaMetricsProvider() {
            return new HpaMetricsProvider();
        }

        /**
         * ConfigMap 配置热更新：包装用户提供的 {@link StreamMQConfigRefresher}（或内部 Noop）， 由其 SmartLifecycle
         * 生命周期启动/停止 watch。唯一入口，避免多 Bean 注入歧义。
         */
        @Bean(destroyMethod = "stop")
        public ConfigMapConfigRefresher configMapConfigRefresher(
                CloudK8sProperties properties,
                ObjectProvider<StreamMQConfigRefresher> userRefresher) {
            // 关键：不得在创建期 getIfAvailable() 解析——ConfigMapConfigRefresher 自身实现了
            // StreamMQConfigRefresher，会把创建中的自身当作候选，触发
            // "Requested bean is currently in creation" 循环引用启动失败（红队 F-05）。
            // 传入 ObjectProvider 延迟到首次 refresh 回调时解析。
            ConfigMapConfigRefresher refresher = new ConfigMapConfigRefresher(userRefresher);
            refresher.setWatchNamespaces(properties.getConfigWatchNamespaces());
            return refresher;
        }
    }
}
