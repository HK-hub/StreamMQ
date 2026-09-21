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

    /**
     * Operator 子配置（嵌套绑定）。
     *
     * <p><b>item-7 修复：</b>文档、CRD yaml 与运行时 WARN 文案统一使用点号写法（{@code
     * streammq.cloud.k8s.operator.watch-all-namespaces} / {@code operator.watch-namespaces}），但扁平的
     * {@code operatorWatchAllNamespaces} 字段只能被 kebab 写法（{@code
     * operator-watch-all-namespaces}）绑定——按文档配置时两个开关会被 静默忽略，收敛模式失效、部署仍需 ClusterRole 全局 watch
     * 权限。此处把开关收进嵌套的 {@link Operator} 子对象： 点号写法生效；同时保留扁平访问器（委托到子对象）以兼容既有 kebab 写法与自动装配调用点。
     */
    private final Operator operator = new Operator();

    /** Operator 的 watch 范围开关（嵌套绑定，点号与 kebab 写法均可生效）。 */
    @Getter
    @Setter
    public static class Operator {

        /** 是否监听全部命名空间（默认 true；为 false 时使用 {@link #watchNamespaces}） */
        private boolean watchAllNamespaces = true;

        /**
         * 监听的命名空间列表（仅当 {@code watch-all-namespaces=false} 时生效）。
         *
         * <p>注意：收敛到指定命名空间时，部署仍需对应命名空间的读权限，但不再要求 ClusterRole 全局 watch 权限。
         */
        private java.util.List<String> watchNamespaces;
    }

    public java.util.List<String> getConfigWatchNamespaces() {
        return configWatchNamespaces;
    }

    public void setConfigWatchNamespaces(java.util.List<String> namespaces) {
        this.configWatchNamespaces = namespaces;
    }

    /** 嵌套子配置访问器（供 Spring Boot 绑定 {@code operator.*}）。 */
    public Operator getOperator() {
        return operator;
    }

    public boolean isOperatorWatchAllNamespaces() {
        return operator.isWatchAllNamespaces();
    }

    public void setOperatorWatchAllNamespaces(boolean watchAllNamespaces) {
        operator.setWatchAllNamespaces(watchAllNamespaces);
    }

    public java.util.List<String> getOperatorWatchNamespaces() {
        return operator.getWatchNamespaces();
    }

    public void setOperatorWatchNamespaces(java.util.List<String> namespaces) {
        operator.setWatchNamespaces(namespaces);
    }

    /**
     * 启动期配置校验（fail-fast）。
     *
     * <p><b>为什么必须有：</b>本模块此前对全部数值配置零校验，非法值以两种不一致的方式暴露 —— {@code hpa-sync-interval-seconds = 0} 会让
     * {@code ScheduledThreadPoolExecutor.scheduleAtFixedRate} 在 {@code afterPropertiesSet}
     * 阶段抛<b>不带任何配置键信息</b>的 {@link IllegalArgumentException} （启动失败但无法定位），而 {@code
     * reconcile-interval-seconds} 的非法值却被<b>静默忽略</b>回落默认值。 同一模块内两种口径让用户无法预测行为。此处统一为 fail-fast —— 后者的
     * setter 也已改为 fail-fast。
     *
     * @throws IllegalArgumentException 任一取值非法
     */
    public void validate() {
        // gracefulShutdownTimeoutMs 允许负值：其语义是"不做优雅等待"，由 GracefulShutdownHandler
        // 夹取为 0 处理（不是配置错误）——该契约由 CloudK8sPropertiesWiringTest 锁定。
        requirePositive("streammq.cloud.k8s.reconcile-interval-seconds", reconcileIntervalSeconds);
        requirePositive("streammq.cloud.k8s.hpa-sync-interval-seconds", hpaSyncIntervalSeconds);
        requirePositive("streammq.cloud.k8s.hpa-default-target-lag", hpaDefaultTargetLag);
        if (hpaScaleUpThreshold <= 0) {
            throw new IllegalArgumentException(
                    "streammq.cloud.k8s.hpa-scale-up-threshold must be > 0 (percent), got: "
                            + hpaScaleUpThreshold);
        }
        if (hpaScaleDownThreshold < 0) {
            throw new IllegalArgumentException(
                    "streammq.cloud.k8s.hpa-scale-down-threshold must be >= 0 (percent), got: "
                            + hpaScaleDownThreshold);
        }
        if (hpaScaleDownThreshold >= hpaScaleUpThreshold) {
            // 缩容阈值不低于扩容阈值时，扩缩判定会互相矛盾并形成持续抖动（反复扩容-缩容）
            throw new IllegalArgumentException(
                    "streammq.cloud.k8s.hpa-scale-down-threshold ("
                            + hpaScaleDownThreshold
                            + ") must be < hpa-scale-up-threshold ("
                            + hpaScaleUpThreshold
                            + "), otherwise the autoscaler oscillates between scale-up and"
                            + " scale-down.");
        }
        validateNamespaces("streammq.cloud.k8s.config-watch-namespaces", configWatchNamespaces);
        validateNamespaces(
                "streammq.cloud.k8s.operator.watch-namespaces", operator.getWatchNamespaces());
    }

    private static void requirePositive(String key, long value) {
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be > 0, got: " + value);
        }
    }

    private static void validateNamespaces(String key, java.util.List<String> namespaces) {
        if (namespaces == null) {
            return;
        }
        for (String namespace : namespaces) {
            if (namespace == null || namespace.isBlank()) {
                throw new IllegalArgumentException(
                        key + " must not contain null or blank namespace entries");
            }
        }
    }
}
