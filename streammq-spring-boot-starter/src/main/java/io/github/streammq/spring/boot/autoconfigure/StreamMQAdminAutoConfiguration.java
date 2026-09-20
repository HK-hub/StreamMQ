/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.security.AllowAllAuthenticator;
import io.github.streammq.adapter.redisson.security.DenyAllAuthenticator;
import io.github.streammq.core.listener.BroadcastGroupRegistry;
import io.github.streammq.core.policy.ManagementAuthenticator;
import io.github.streammq.core.policy.RateLimitedAuthenticator;
import io.github.streammq.spring.boot.StreamMQSpringConstants;
import io.github.streammq.spring.boot.properties.StreamMQProperties;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * 管理面自动装配：管理端点后端（{@link StreamMQAdminEndpoint}）、Actuator 端点（{@link
 * StreamMQActuatorEndpoint}）以及两个启动期安全提醒组件。
 *
 * <p><b>为什么独立成类（R6-S2）：</b>此前这些 Bean 定义在 {@link StreamMQHealthAutoConfiguration} 内部， 被类级 {@code
 * streammq.health.enabled} 条件包住——用户按文档关闭健康组件（{@code streammq.health.enabled=false}）后，{@code
 * /actuator/streammq/**} 会连带 404 且没有任何日志。健康检查与 管理端点是两个独立开关：
 *
 * <ul>
 *   <li>{@code streammq.health.enabled} 只门控 {@code StreamMQHealthIndicator}
 *   <li>{@code streammq.admin.enabled}（默认 true）只门控本类的管理端点与提醒组件
 * </ul>
 *
 * <p><b>启动提醒组件为何是 Bean（R6-S1）：</b>{@link AdminEndpointExposureStartupWarner} 与 {@link
 * AuthenticatorStartupLogger} 此前标注 {@code @Component}，但位于自动装配包、不在用户组件扫描范围内，也不在 {@code .imports} /
 * {@code @Import} 中——是彻底的死代码：{@code AllowAllAuthenticator} 的 SECURITY ALERT 与文档承诺的 {@code
 * streammq.admin.startup-warn} 开关都不生效。现改为在此处显式注册 {@code @Bean}（{@code @EventListener} 对
 * {@code @Bean} 实例同样生效），并保留运行时直读 {@code streammq.admin.startup-warn} 的语义。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@Configuration(proxyBeanMethods = false)
@AutoConfigureAfter(StreamMQCoreAutoConfiguration.class)
@ConditionalOnExpression(
        "${"
                + StreamMQSpringConstants.PROP_PREFIX
                + "."
                + StreamMQSpringConstants.PROP_NAME_ENABLED
                + ":true}")
@ConditionalOnClass({RedissonClient.class, StreamMQActuatorEndpoint.class})
@ConditionalOnProperty(
        prefix = "streammq.admin",
        name = StreamMQSpringConstants.PROP_NAME_ENABLED,
        havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE,
        matchIfMissing = true)
public class StreamMQAdminAutoConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMQAdminAutoConfiguration.class);

    /**
     * 管理端点的后端逻辑 Bean（供 {@link StreamMQActuatorEndpoint} 使用）。
     *
     * <p>仅在 {@code streammq.admin.enabled=true}（默认）时注册；与 {@code streammq.health.enabled} 完全解耦。
     *
     * @param redisson Redis 客户端
     * @param listenerContainerProvider Listener 容器（可选）
     * @param properties 配置属性
     * @param registryProvider 广播组注册表（可选）
     * @return 管理端点后端实例
     */
    @Bean
    @ConditionalOnMissingBean(name = StreamMQSpringConstants.BEAN_ADMIN_ENDPOINT)
    public StreamMQAdminEndpoint streamMQAdminEndpoint(
            RedissonClient redisson,
            ObjectProvider<DefaultStreamMQListenerContainer> listenerContainerProvider,
            StreamMQProperties properties,
            ObjectProvider<BroadcastGroupRegistry> registryProvider) {
        LOG.debug("Creating StreamMQAdminEndpoint");
        StreamMQAdminEndpoint adminEndpoint =
                new StreamMQAdminEndpoint(
                        redisson,
                        StreamMQBeanResolution.uniqueOrNull(
                                listenerContainerProvider, "DefaultStreamMQListenerContainer"),
                        properties.getNamespace(),
                        properties.getAdmin().getFailureRetryCooldownMillis(),
                        StreamMQBeanResolution.uniqueOrNull(
                                registryProvider, "BroadcastGroupRegistry"));
        adminEndpoint.setMaxPendingQuerySize(properties.getAdmin().getMaxPendingQuerySize());
        return adminEndpoint;
    }

    /**
     * Actuator 端点 Bean（注册到 /actuator/streammq）。
     *
     * <p>注入 {@link StreamMQHealthAutoConfiguration.StreamMQHealthIndicator} 而非泛型 {@code
     * HealthIndicator}， 避免当容器中存在多个 {@code HealthIndicator} Bean 时触发 {@code
     * NoUniqueBeanDefinitionException}。
     *
     * <p>{@link ManagementAuthenticator} 通过 {@link ObjectProvider} 防御性注入：当核心装配因 {@code
     * streammq.enabled=false} 回退、容器中不存在鉴权器 Bean 时， 使用 {@link DenyAllAuthenticator} 兜底，避免启动期 {@code
     * UnsatisfiedDependencyException}。
     *
     * @param adminEndpoint 管理端点后端
     * @param healthIndicatorProvider 健康指示器（可选，{@code streammq.health.enabled=false} 时不存在）
     * @param authenticatorProvider 管理鉴权器（可选，默认 DenyAll）
     * @param clientAddressPolicy 客户端地址可信策略（限流来源聚合）
     * @param properties 配置属性
     * @return Actuator 端点实例
     */
    @Bean
    @ConditionalOnMissingBean(name = StreamMQSpringConstants.BEAN_ACTUATOR_ENDPOINT)
    @ConditionalOnClass(org.springframework.boot.actuate.endpoint.annotation.Endpoint.class)
    public StreamMQActuatorEndpoint streamMQActuatorEndpoint(
            StreamMQAdminEndpoint adminEndpoint,
            ObjectProvider<StreamMQHealthAutoConfiguration.StreamMQHealthIndicator>
                    healthIndicatorProvider,
            ObjectProvider<ManagementAuthenticator> authenticatorProvider,
            io.github.streammq.core.util.WebRequestAuthSupport.ClientAddressPolicy
                    clientAddressPolicy,
            StreamMQProperties properties) {
        LOG.debug("Creating StreamMQActuatorEndpoint");
        ManagementAuthenticator authenticator =
                StreamMQBeanResolution.uniqueOrNull(
                        authenticatorProvider,
                        "ManagementAuthenticator",
                        new DenyAllAuthenticator());
        // 包一层失败限流：即使启用 Basic/Token 弱凭据，也能抵御针对管理端点的暴力破解
        ManagementAuthenticator rateLimited =
                new RateLimitedAuthenticator(authenticator, clientAddressPolicy);
        StreamMQActuatorEndpoint endpoint =
                new StreamMQActuatorEndpoint(
                        adminEndpoint,
                        StreamMQBeanResolution.uniqueOrNull(
                                healthIndicatorProvider, "StreamMQHealthIndicator"),
                        rateLimited);
        endpoint.setListPageSize(properties.getAdmin().getListPageSize());
        // 发布前修复 P2-5：标记是否使用了 AllowAll 鉴权器，供启动告警针对最危险场景发出强提示
        endpoint.setAllowAll(authenticator instanceof AllowAllAuthenticator);
        return endpoint;
    }

    /**
     * 启动期管理端点暴露面提醒（{@code SECURITY ALERT} / 主端口 WARN）。
     *
     * <p>条件：{@code streammq.admin.enabled=true}（类级）且 {@code streammq.admin.startup-warn} 未显式关闭
     * （本方法级，{@code matchIfMissing=true}）。{@code startup-warn} 保持文档承诺的<b>直读精确键</b>语义： 只识别 {@code
     * streammq.admin.startup-warn} 的确切写法（{@code -D} 或 yml 均可）， 组件内部仍会再读一次以兼容"被用户自行 {@code @Import}
     * 注册、未走条件"的场景。
     *
     * @param endpointProvider Actuator 端点（可选）
     * @param environment 环境（读取管理端口与开关）
     * @return 告警组件实例
     */
    @Bean
    @ConditionalOnMissingBean(AdminEndpointExposureStartupWarner.class)
    @ConditionalOnProperty(
            prefix = "streammq.admin",
            name = "startup-warn",
            havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE,
            matchIfMissing = true)
    public AdminEndpointExposureStartupWarner streamMQAdminEndpointExposureStartupWarner(
            ObjectProvider<StreamMQActuatorEndpoint> endpointProvider, Environment environment) {
        LOG.debug("Registering AdminEndpointExposureStartupWarner");
        return new AdminEndpointExposureStartupWarner(endpointProvider, environment);
    }

    /**
     * 启动期鉴权器姿态日志（默认 {@link DenyAllAuthenticator} 时提示如何开放访问）。
     *
     * @param authenticatorProvider 管理鉴权器（可选）
     * @param environment 环境（读取开关）
     * @return 日志组件实例
     */
    @Bean
    @ConditionalOnMissingBean(AuthenticatorStartupLogger.class)
    @ConditionalOnProperty(
            prefix = "streammq.admin",
            name = "startup-warn",
            havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE,
            matchIfMissing = true)
    public AuthenticatorStartupLogger streamMQAuthenticatorStartupLogger(
            ObjectProvider<ManagementAuthenticator> authenticatorProvider,
            Environment environment) {
        LOG.debug("Registering AuthenticatorStartupLogger");
        return new AuthenticatorStartupLogger(authenticatorProvider, environment);
    }
}
