package io.github.streammq.cloud.k8s;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * StreamMQ 云原生 K8s 增强模块配置属性。
 *
 * <p>通过 {@code streammq.cloud.k8s.*} 前缀配置 K8s 环境下的增强行为。
 *
 * <p>典型配置示例：
 *
 * <pre>{@code
 * streammq:
 *   cloud:
 *     k8s:
 *       enabled: true
 *       graceful-shutdown-timeout-ms: 30000
 *       health-endpoint-enabled: true
 *       config-refresh-enabled: false
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 2.0.0
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "streammq.cloud.k8s")
public class CloudK8sProperties {

    /** 是否启用 K8s 云原生增强模块，默认开启 */
    private boolean enabled = true;

    /** 优雅关闭等待处理中消息完成的最长时间（毫秒），默认 30 秒 */
    private long gracefulShutdownTimeoutMs = 30000L;

    /** 是否启用健康探针端点，默认开启 */
    private boolean healthEndpointEnabled = true;

    /** 是否启用配置热更新能力，默认关闭 */
    private boolean configRefreshEnabled = false;
}
