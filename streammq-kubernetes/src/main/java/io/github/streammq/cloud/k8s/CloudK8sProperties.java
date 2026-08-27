/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * StreamMQ 云原生 K8s 增强模块配置属性。
 *
 * <p>通过 {@code streammq.cloud.k8s.*} 前缀配置 K8s 环境下的增强行为。 模块<b>默认关闭</b>（{@code enabled=false}），
 * 与自动装配的 {@code matchIfMissing=false} 语义一致， 需显式配置 {@code enabled=true} 开启。
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
 * @since 0.1.0
 */
@Getter
@Setter
@ConfigurationProperties(prefix = CloudK8sProperties.PROP_PREFIX)
public class CloudK8sProperties {

    /** 配置属性前缀：streammq.cloud.k8s */
    public static final String PROP_PREFIX = "streammq.cloud.k8s";

    /** 开关属性名：enabled */
    public static final String PROP_NAME_ENABLED = "enabled";

    /** 开关属性值：true */
    public static final String PROP_VALUE_TRUE = "true";

    /** 默认优雅关闭超时（毫秒，30 秒） */
    public static final long DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_MS = 30_000L;

    /** 默认调和间隔（秒） */
    public static final int DEFAULT_RECONCILE_INTERVAL_SECONDS =
            io.github.streammq.cloud.k8s.operator.StreamMQK8sDefaults
                    .DEFAULT_RECONCILE_INTERVAL_SECONDS;

    /** 是否启用 K8s 云原生增强模块，默认关闭（需显式配置 enabled=true 开启） */
    private boolean enabled = false;

    /** 优雅关闭等待处理中消息完成的最长时间（毫秒） */
    private long gracefulShutdownTimeoutMs = DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_MS;

    /** 是否启用健康探针端点，默认开启 */
    private boolean healthEndpointEnabled = true;

    /** 是否启用配置热更新能力，默认关闭 */
    private boolean configRefreshEnabled = false;

    /** CRD 调和间隔（秒），Operator 周期性对账的周期 */
    private int reconcileIntervalSeconds = DEFAULT_RECONCILE_INTERVAL_SECONDS;

    /** HPA 扫描同步间隔（秒） */
    private int hpaSyncIntervalSeconds = DEFAULT_RECONCILE_INTERVAL_SECONDS;

    /** HPA 默认目标消费积压（每实例消息数） */
    private long hpaDefaultTargetLag =
            io.github.streammq.cloud.k8s.operator.StreamMQK8sDefaults.AUTOSCALE_TARGET_LAG;

    /** HPA 默认扩容阈值百分比 */
    private int hpaScaleUpThreshold =
            io.github.streammq.cloud.k8s.operator.StreamMQK8sDefaults.AUTOSCALE_SCALE_UP_THRESHOLD;

    /** HPA 默认缩容阈值百分比 */
    private int hpaScaleDownThreshold =
            io.github.streammq.cloud.k8s.operator.StreamMQK8sDefaults
                    .AUTOSCALE_SCALE_DOWN_THRESHOLD;

    /** ConfigMap 热更新 watch 命名空间列表（默认 default） */
    private java.util.List<String> configWatchNamespaces;

    /** Operator 是否监听全部命名空间（默认 true；为 false 时使用 {@link #operatorWatchNamespaces}） */
    private boolean operatorWatchAllNamespaces = true;

    /**
     * Operator 监听的命名空间列表（仅当 {@code operator.watch-all-namespaces=false} 时生效）。
     *
     * <p>注意：收敛到指定命名空间时，部署仍需对应命名空间的读权限，但不再要求 ClusterRole 全局 watch 权限。
     */
    private java.util.List<String> operatorWatchNamespaces;

    public java.util.List<String> getConfigWatchNamespaces() {
        return configWatchNamespaces;
    }

    public void setConfigWatchNamespaces(java.util.List<String> namespaces) {
        this.configWatchNamespaces = namespaces;
    }

    public boolean isOperatorWatchAllNamespaces() {
        return operatorWatchAllNamespaces;
    }

    public void setOperatorWatchAllNamespaces(boolean watchAllNamespaces) {
        this.operatorWatchAllNamespaces = watchAllNamespaces;
    }

    public java.util.List<String> getOperatorWatchNamespaces() {
        return operatorWatchNamespaces;
    }

    public void setOperatorWatchNamespaces(java.util.List<String> namespaces) {
        this.operatorWatchNamespaces = namespaces;
    }
}
