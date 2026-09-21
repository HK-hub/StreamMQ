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
public class CloudK8sAutoConfiguration
        implements org.springframework.beans.factory.InitializingBean {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(CloudK8sAutoConfiguration.class);

    private final CloudK8sProperties properties;

    private final org.springframework.core.env.Environment environment;

    /**
     * 构造注入配置属性与环境（用于启动期校验与"配了但不生效"提示）。
     *
     * @param properties K8s 增强模块配置
     * @param environment Spring 环境（读取 {@code operator.enabled} 这类由条件注解消费的键）
     */
    public CloudK8sAutoConfiguration(
            CloudK8sProperties properties, org.springframework.core.env.Environment environment) {
        this.properties = properties;
        this.environment = environment;
    }

    /**
     * 启动期校验配置并提示"配了但不生效"的组合。
     *
     * <p>{@link CloudK8sProperties#validate()} 把 {@code hpa-sync-interval-seconds=0} 这类此前会抛
     * <b>无配置键信息</b>异常、以及会被静默忽略的非法间隔统一为可定位的 fail-fast。
     *
     * <p>另：{@code config-refresh-enabled=true} 而 {@code operator.enabled=false} 时， {@code
     * ConfigMapConfigRefresher} 位于 Operator 子配置内、不会被注册 —— 用户按包文档"关掉 Operator 只用热更新"
     * 会得到"配了但什么都没发生"。这里显式 WARN，绝不静默。
     */
    @Override
    public void afterPropertiesSet() {
        properties.validate();
        if (properties.isConfigRefreshEnabled() && !isOperatorEnabled()) {
            LOG.warn(
                    "streammq.cloud.k8s.config-refresh-enabled=true has NO effect while"
                        + " streammq.cloud.k8s.operator.enabled=false: the ConfigMap watcher lives"
                        + " in the operator configuration. Set operator.enabled=true, or remove"
                        + " config-refresh-enabled.");
        }
    }

    /**
     * 读取 {@code streammq.cloud.k8s.operator.enabled}（由 {@link
     * org.springframework.boot.autoconfigure.condition.ConditionalOnProperty} 消费，不是 {@link
     * CloudK8sProperties} 的字段），默认 true。
     *
     * <p>用宽松解析（{@code Boolean.parseBoolean} 语义）而非类型转换：非法取值不得让启动失败——
     * 条件注解本身会对无法识别的值按"不匹配"处理，这里只用于告警判定。
     */
    private boolean isOperatorEnabled() {
        String raw = environment.getProperty("streammq.cloud.k8s.operator.enabled");
        return raw == null || !"false".equalsIgnoreCase(raw.trim());
    }

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
    }

    /**
     * 优雅关闭：容器关闭时暂停拉取、等待 in-flight 消息完成、停止容器。
     *
     * <p><b>为什么必须是独立的嵌套配置类：</b>此前本 Bean 与健康探针同处 {@code @ConditionalOnClass(HealthIndicator.class)}
     * 的 {@code HealthConfiguration} 内—— 而 {@code spring-boot-starter-actuator} 在本模块是 {@code
     * provided}，未引入 Actuator 的应用 <b>不会</b>注册优雅关闭处理器（{@code pause → 等在途 → stop} 整条链路静默失效），K8s
     * 滚动发布/驱逐时 在途消息被中断、重复投递概率升高，且没有任何提示。优雅关闭与 Actuator 毫无关系，条件必须解耦。
     */
    @ConditionalOnClass(StreamMQListenerContainer.class)
    static class GracefulShutdownConfiguration {

        /**
         * 优雅关闭处理器。
         *
         * <p>实现 {@link org.springframework.context.SmartLifecycle}（phase = {@code Integer.MAX_VALUE
         * - 150}），由 Spring 在停止阶段先行回调（早于 starter 的容器生命周期 phase=MAX_VALUE-200）， 保证 pause 有效；{@link
         * org.springframework.beans.factory.DisposableBean} 仅作幂等兜底。
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

        /**
         * HPA 自动扩缩器。
         *
         * <p>本方法（自动装配）是 {@link HpaAutoScaler} 的<b>唯一装配真源</b>：该类自身的 {@code @Component}
         * 已移除。此前两者并存时，若用户把 {@code io.github.streammq.cloud.k8s} 包纳入组件扫描，
         * 组件扫描会先注册一个<b>未经属性注入</b>的实例（全部参数回落硬编码默认值，用户配置静默失效）， 且不受 {@code
         * streammq.cloud.k8s.enabled=false} 约束也会启动调度线程。 {@code @ConditionalOnMissingBean}
         * 保留，用于让用户自定义 Bean 覆盖。
         */
        @Bean
        @ConditionalOnMissingBean(HpaAutoScaler.class)
        public HpaAutoScaler hpaAutoScaler(CloudK8sProperties properties) {
            HpaAutoScaler scaler = new HpaAutoScaler();
            scaler.setSyncIntervalSeconds(properties.getHpaSyncIntervalSeconds());
            scaler.setDefaultTargetLag(properties.getHpaDefaultTargetLag());
            scaler.setScaleUpThreshold(properties.getHpaScaleUpThreshold());
            scaler.setScaleDownThreshold(properties.getHpaScaleDownThreshold());
            // 扫描范围与 operator watch 语义一致（K9）：默认全命名空间，可收敛为指定列表
            scaler.setWatchAllNamespaces(properties.isOperatorWatchAllNamespaces());
            scaler.setWatchNamespaces(properties.getOperatorWatchNamespaces());
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
         *
         * <p>受 {@code streammq.cloud.k8s.config-refresh-enabled} 控制（默认 false）： 显式开启时才注册 ConfigMap
         * watcher，避免默认装配即在 K8s 集群中建立 informers 占用 ApiServer 配额；显式关闭时整套热更新链路（watch + 调度）整体下线。
         */
        @Bean(destroyMethod = "stop")
        @ConditionalOnProperty(
                prefix = CloudK8sProperties.PROP_PREFIX,
                name = "config-refresh-enabled",
                havingValue = CloudK8sProperties.PROP_VALUE_TRUE,
                matchIfMissing = false)
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
