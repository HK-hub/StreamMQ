package io.github.streammq.cloud.k8s;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/**
 * StreamMQ 云原生 K8s 增强模块自动装配配置。
 *
 * <p>启用条件：
 * <ul>
 *   <li>类路径存在 {@link StreamMQListenerContainer}</li>
 *   <li>属性 {@code streammq.cloud.k8s.enabled=true}（默认开启）</li>
 * </ul>
 *
 * <p>装配的组件：
 * <ul>
 *   <li>{@link StreamMQHealthIndicator} - Spring Boot Actuator 健康指标</li>
 *   <li>{@link StreamMQHealthController} - K8s 存活与就绪探针 REST 端点</li>
 *   <li>{@link NoopConfigRefresher} - 配置热更新空操作默认实现（可被用户覆盖）</li>
 *   <li>{@link GracefulShutdownHandler} - 优雅关闭处理器</li>
 *   <li>{@link HpaMetricsProvider} - HPA 指标提供者</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * # application.yml
 * streammq:
 *   cloud:
 *     k8s:
 *       enabled: true
 *       graceful-shutdown-timeout-ms: 30000
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 2.0.0
 */
@AutoConfiguration
@ConditionalOnClass(StreamMQListenerContainer.class)
@ConditionalOnProperty(prefix = "streammq.cloud.k8s", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(CloudK8sProperties.class)
@Import({
    StreamMQHealthIndicator.class,
    StreamMQHealthController.class,
    GracefulShutdownHandler.class,
    HpaMetricsProvider.class
})
public class CloudK8sAutoConfiguration {

    /**
     * 装配默认的空操作配置刷新器（仅当容器中不存在其它 {@link StreamMQConfigRefresher} 时生效）。
     *
     * @return 空操作配置刷新器实例
     */
    @Bean
    @ConditionalOnMissingBean(StreamMQConfigRefresher.class)
    public StreamMQConfigRefresher noopConfigRefresher() {
        return new NoopConfigRefresher();
    }
}
