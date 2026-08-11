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
 * @since 2.0.0
 */
@AutoConfiguration
@ConditionalOnClass(StreamMQListenerContainer.class)
@ConditionalOnProperty(
        prefix = "streammq.cloud.k8s",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
@EnableConfigurationProperties(CloudK8sProperties.class)
@Import({
    StreamMQHealthIndicator.class,
    StreamMQHealthController.class,
    GracefulShutdownHandler.class,
    HpaMetricsProvider.class,
    StreamMQClusterController.class,
    HpaAutoScaler.class,
    ConfigMapConfigRefresher.class
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
}
