package io.github.streammq.cloud.k8s;

import io.github.streammq.cloud.k8s.autoscaler.HpaAutoScaler;
import io.github.streammq.cloud.k8s.config.ConfigMapConfigRefresher;
import io.github.streammq.cloud.k8s.operator.StreamMQClusterController;
import io.github.streammq.core.listener.StreamMQListenerContainer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/**
 * StreamMQ cloud native K8s enhancement module auto-configuration.
 *
 * <p>Enable conditions:
 *
 * <ul>
 *   <li>Classpath contains {@link StreamMQListenerContainer}
 *   <li>Property {@code streammq.cloud.k8s.enabled=true} (default: true)
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
 *   <li>{@link StreamMQClusterController} - StreamMQCluster CRD Operator controller
 *   <li>{@link HpaAutoScaler} - HPA auto-scaling controller
 *   <li>{@link ConfigMapConfigRefresher} - ConfigMap watch config refresher
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
        matchIfMissing = true)
@EnableConfigurationProperties(CloudK8sProperties.class)
@Import({
    StreamMQHealthIndicator.class,
    StreamMQHealthController.class,
    GracefulShutdownHandler.class,
    HpaMetricsProvider.class
})
public class CloudK8sAutoConfiguration {

    /**
     * Default no-op config refresher, registered only when no other {@link StreamMQConfigRefresher}
     * bean is present.
     *
     * @return no-op config refresher instance
     */
    @Bean
    @ConditionalOnMissingBean(StreamMQConfigRefresher.class)
    public StreamMQConfigRefresher noopConfigRefresher() {
        return new NoopConfigRefresher();
    }

    /**
     * StreamMQCluster CRD Operator controller，注入调和间隔配置。
     *
     * @param properties K8s 云原生增强配置
     * @return Operator 控制器实例
     */
    @Bean
    public StreamMQClusterController streamMQClusterController(CloudK8sProperties properties) {
        StreamMQClusterController controller = new StreamMQClusterController();
        controller.setReconcileIntervalSeconds(properties.getReconcileIntervalSeconds());
        return controller;
    }

    /**
     * HPA 自动扩缩容控制器，注入扫描间隔与扩缩阈值默认值。
     *
     * <p>{@code metricsProvider} 通过 {@code @Autowired} 字段注入，由 Spring 生命周期自动完成。
     *
     * @param properties K8s 云原生增强配置
     * @return HPA 控制器实例
     */
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
     * ConfigMap 配置热更新实现（支持 label 过滤与多命名空间监听）。
     *
     * @return ConfigMap 配置热更新实例
     */
    @Bean
    public ConfigMapConfigRefresher configMapConfigRefresher() {
        return new ConfigMapConfigRefresher();
    }
}
