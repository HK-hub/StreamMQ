/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.policy.ManagementAuthenticator;
import io.github.streammq.core.policy.RateLimitedAuthenticator;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.diagnostics.endpoint.StreamMQDiagnosticsEndpoint;
import io.github.streammq.diagnostics.spi.BacklogProbe;
import io.github.streammq.diagnostics.support.RedisBacklogProbe;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.ObjectProvider;
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
public class StreamMQDiagnosticsAutoConfiguration
        implements org.springframework.beans.factory.InitializingBean {

    private final StreamMQDiagnosticsProperties properties;

    /**
     * 构造注入诊断属性（用于启动期校验）。
     *
     * @param properties 诊断配置属性
     */
    public StreamMQDiagnosticsAutoConfiguration(StreamMQDiagnosticsProperties properties) {
        this.properties = properties;
    }

    /**
     * 启动期校验诊断配置（阈值倒挂 / 零窗口 / 负值一律 fail-fast）。
     *
     * <p>此前这些键零校验：{@code backlog-warning-threshold > backlog-critical-threshold} 会让 {@code /health}
     * 常态返回 DOWN，{@code recent-window-ms = 0} 会产出 {@code Infinity} 速率。
     *
     * @throws IllegalArgumentException 配置非法
     */
    @Override
    public void afterPropertiesSet() {
        properties.validate();
    }

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
     * 装配慢消费分析器。
     *
     * <p>三个 analyzer 均为 {@code @Component}，但类位于 {@code io.github.streammq.diagnostics} 包—— 普通应用（不额外
     * {@code @ComponentScan} 该包）永远扫不到。因此本自动装配显式以 {@code @Bean} 注册并供 {@link
     * StreamMQDiagnosticsService} 使用；若应用自行扫描了该包（{@code @Component}
     * 已注册同类型），{@code @ConditionalOnMissingBean} 会跳过，避免重复实例。
     *
     * @param traceService 追踪查询服务
     * @param listenerContainer 监听容器
     * @param properties 诊断属性
     * @return 慢消费分析器
     */
    @Bean
    @ConditionalOnMissingBean(SlowConsumeAnalyzer.class)
    // 必须同时具备 TraceService 与 ListenerContainer Bean：两者的注入都是硬依赖，
    // 缺少任一 Bean 时若仍装配本方法，上下文会以 UnsatisfiedDependencyException 启动失败——
    // 而文档承诺的是"缺前置即不装配、优雅降级"。类级 @ConditionalOnClass 只检查**类路径**，挡不住这种情况。
    @ConditionalOnBean({StreamMQTraceService.class, StreamMQListenerContainer.class})
    public SlowConsumeAnalyzer slowConsumeAnalyzer(
            StreamMQTraceService traceService,
            StreamMQListenerContainer listenerContainer,
            StreamMQDiagnosticsProperties properties) {
        return new SlowConsumeAnalyzer(traceService, listenerContainer, properties);
    }

    /**
     * 装配积压分析器。
     *
     * <p>积压探针为可选依赖（Redisson 缺席时回退到追踪窗口估算），以 {@link ObjectProvider} 注入。
     *
     * @param traceService 追踪查询服务
     * @param properties 诊断属性
     * @param backlogProbeProvider 积压探针（可为空）
     * @return 积压分析器
     */
    @Bean
    @ConditionalOnMissingBean(BacklogAnalyzer.class)
    @ConditionalOnBean(StreamMQTraceService.class)
    public BacklogAnalyzer backlogAnalyzer(
            StreamMQTraceService traceService,
            StreamMQDiagnosticsProperties properties,
            ObjectProvider<BacklogProbe> backlogProbeProvider) {
        return new BacklogAnalyzer(traceService, properties, backlogProbeProvider.getIfAvailable());
    }

    /**
     * 装配死信分析器。
     *
     * @param traceService 追踪查询服务
     * @param properties 诊断属性
     * @return 死信分析器
     */
    @Bean
    @ConditionalOnMissingBean(DlqAnalyzer.class)
    @ConditionalOnBean(StreamMQTraceService.class)
    public DlqAnalyzer dlqAnalyzer(
            StreamMQTraceService traceService, StreamMQDiagnosticsProperties properties) {
        return new DlqAnalyzer(traceService, properties);
    }

    /**
     * 装配诊断服务（Facade）。
     *
     * <p>由三个 analyzer {@code @Bean}（见上方同名方法）+ 容器 + 属性装配；可选积压探针通过 {@link ObjectProvider} 注入以兼容
     * Redisson 缺席场景。
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
                slowConsumeAnalyzer, backlogAnalyzer, dlqAnalyzer, listenerContainer, properties);
    }

    /**
     * 基于 Redisson 的积压探针：当存在 {@link RedissonClient} 时装配，提供真实 XLEN/XPENDING 积压数据。
     *
     * @param redisson Redisson 客户端
     * @param properties 诊断配置属性
     * @return 积压探针
     */
    @Bean
    @ConditionalOnClass(RedissonClient.class)
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
    // 端点使用 Spring MVC 注解（@RequestMapping/@GetMapping），必须限定 SERVLET 应用：
    // WebFlux 下 Bean 会被创建但没有 handler adapter，端点静默 404（与 CloudK8sAutoConfiguration 口径一致）。
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    @ConditionalOnBean({StreamMQDiagnosticsService.class, MessageProfileService.class})
    public StreamMQDiagnosticsEndpoint streamMQDiagnosticsEndpoint(
            StreamMQDiagnosticsService diagnosticsService,
            MessageProfileService profileService,
            ManagementAuthenticator authenticator,
            org.springframework.beans.factory.ObjectProvider<
                            io.github.streammq.core.util.WebRequestAuthSupport.ClientAddressPolicy>
                    clientAddressPolicyProvider) {
        // 包一层失败限流：即使启用 Basic/Token 弱凭据，也能抵御针对诊断端点的暴力破解
        // 地址可信策略来自上下文 Bean（由 starter 提供）；未装配时退化为安全默认（不信任 XFF）
        ManagementAuthenticator rateLimited =
                new RateLimitedAuthenticator(
                        authenticator,
                        clientAddressPolicyProvider.getIfAvailable(
                                () ->
                                        io.github.streammq.core.util.WebRequestAuthSupport
                                                .ClientAddressPolicy.DEFAULT));
        return new StreamMQDiagnosticsEndpoint(diagnosticsService, profileService, rateLimited);
    }
}
