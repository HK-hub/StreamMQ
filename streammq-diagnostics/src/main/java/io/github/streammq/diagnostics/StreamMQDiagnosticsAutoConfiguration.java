/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.policy.ManagementAuthenticator;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.diagnostics.endpoint.StreamMQDiagnosticsEndpoint;
import io.github.streammq.diagnostics.spi.BacklogProbe;
import io.github.streammq.diagnostics.support.RedisBacklogProbe;
import org.redisson.api.RedissonClient;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * StreamMQ 诊断模块自动装配配置。
 *
 * <p>启用条件：
 *
 * <ul>
 *   <li>类路径存在 {@link StreamMQTraceService}
 *   <li>属性 {@code streammq.diagnostics.enabled=true}（默认关闭）
 * </ul>
 *
 * <p>装配的 Bean：
 *
 * <ul>
 *   <li>{@link MessageProfileService} - 当 {@link StreamMQTraceService} Bean 存在时
 *   <li>{@link StreamMQDiagnosticsService} - 当 {@link StreamMQTraceService} 与 {@link
 *       StreamMQListenerContainer} Bean 均存在时
 *   <li>{@link StreamMQDiagnosticsEndpoint} - 当为 Web 应用时
 * </ul>
 *
 * <p>使用示例：
 *
 * <pre>{@code
 * # application.yml
 * streammq:
 *   diagnostics:
 *     enabled: true
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@AutoConfiguration
@AutoConfigureAfter(
        name = {
            StreamMQDiagnosticsDefaults.AUTO_CONFIGURE_AFTER_TRACE,
            StreamMQDiagnosticsDefaults.AUTO_CONFIGURE_AFTER_LISTENER_CONTAINER
        })
@ConditionalOnClass(StreamMQTraceService.class)
@ConditionalOnProperty(
        prefix = StreamMQDiagnosticsDefaults.PROP_PREFIX,
        name = StreamMQDiagnosticsDefaults.PROP_NAME_ENABLED,
        havingValue = StreamMQDiagnosticsDefaults.PROP_VALUE_TRUE,
        matchIfMissing = false)
@EnableConfigurationProperties(StreamMQDiagnosticsProperties.class)
public class StreamMQDiagnosticsAutoConfiguration {

    /**
     * 装配消息画像服务。
     *
     * @param traceService 追踪查询服务
     * @return 消息画像服务实例
     */
    @Bean
    @ConditionalOnBean(StreamMQTraceService.class)
    public MessageProfileService messageProfileService(
            StreamMQTraceService traceService, StreamMQDiagnosticsProperties properties) {
        MessageProfileService service = new MessageProfileService(traceService);
        service.setMaxProfileQuerySize(properties.getMaxProfileQuerySize());
        return service;
    }

    /**
     * 装配诊断服务（Facade）。
     *
     * <p>由 {@code @Component} + 构造注入装配 3 个 analyzer + 容器 + 属性 + 可选积压探针。 探针通过 {@link
     * ObjectProvider} 注入以兼容 Redisson 缺席场景。
     *
     * @return 诊断服务实例
     */
    @Bean
    @ConditionalOnBean({StreamMQTraceService.class, StreamMQListenerContainer.class})
    public StreamMQDiagnosticsService streamMQDiagnosticsService(
            SlowConsumeAnalyzer slowConsumeAnalyzer,
            BacklogAnalyzer backlogAnalyzer,
            DlqAnalyzer dlqAnalyzer,
            StreamMQListenerContainer listenerContainer,
            StreamMQDiagnosticsProperties properties) {
        return new StreamMQDiagnosticsService(
                slowConsumeAnalyzer,
                backlogAnalyzer,
                dlqAnalyzer,
                listenerContainer,
                properties);
    }

    /**
     * 基于 Redisson 的积压探针：当存在 {@link RedissonClient} 时装配，提供真实 XLEN/XPENDING 积压数据。
     *
     * @param redisson Redisson 客户端
     * @param properties 诊断配置属性
     * @return 积压探针
     */
    @Bean
    @ConditionalOnMissingBean(BacklogProbe.class)
    @ConditionalOnBean(RedissonClient.class)
    public BacklogProbe redisBacklogProbe(
            RedissonClient redisson, StreamMQDiagnosticsProperties properties) {
        return new RedisBacklogProbe(redisson, properties.getNamespace());
    }

    /**
     * 默认管理鉴权器：始终拒绝（安全兜底）。
     *
     * <p>与 {@code streammq-spring-boot-starter} 的默认策略一致，诊断端点默认拒绝所有访问； 用户注册自定义 {@link
     * ManagementAuthenticator} Bean 后开放。
     *
     * @return 拒绝一切访问的鉴权器
     */
    @Bean
    @ConditionalOnMissingBean(ManagementAuthenticator.class)
    public ManagementAuthenticator diagnosticsDenyAllAuthenticator() {
        return (username, password, resource) -> false;
    }

    /**
     * 装配诊断 REST 端点（仅 Web 应用）。
     *
     * <p>仅当诊断服务与画像服务均就绪时才注册端点，避免因依赖未就绪导致上下文启动失败。 所有端点均通过 {@link ManagementAuthenticator} 鉴权，默认拒绝（返回
     * 401）。
     *
     * @param diagnosticsService 诊断服务
     * @param profileService 消息画像服务
     * @param authenticator 管理鉴权器
     * @return 诊断端点实例
     */
    @Bean
    @ConditionalOnWebApplication
    @ConditionalOnBean({StreamMQDiagnosticsService.class, MessageProfileService.class})
    public StreamMQDiagnosticsEndpoint streamMQDiagnosticsEndpoint(
            StreamMQDiagnosticsService diagnosticsService,
            MessageProfileService profileService,
            ManagementAuthenticator authenticator) {
        return new StreamMQDiagnosticsEndpoint(diagnosticsService, profileService, authenticator);
    }
}
