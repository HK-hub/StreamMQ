package io.github.streammq.diagnostics;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.diagnostics.endpoint.StreamMQDiagnosticsEndpoint;
import java.util.Objects;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
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
            "io.github.streammq.spring.boot.autoconfigure.StreamMQTraceAutoConfiguration",
            "io.github.streammq.spring.boot.autoconfigure.StreamMQListenerContainerAutoConfiguration"
        })
@ConditionalOnClass(StreamMQTraceService.class)
@ConditionalOnProperty(
        prefix = "streammq.diagnostics",
        name = "enabled",
        havingValue = "true",
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
    public MessageProfileService messageProfileService(StreamMQTraceService traceService) {
        return new MessageProfileService(traceService);
    }

    /**
     * 装配诊断服务。
     *
     * @param traceService 追踪查询服务
     * @param listenerContainer 监听器容器
     * @param properties 诊断配置属性
     * @return 诊断服务实例
     */
    @Bean
    @ConditionalOnBean({StreamMQTraceService.class, StreamMQListenerContainer.class})
    public StreamMQDiagnosticsService streamMQDiagnosticsService(
            StreamMQTraceService traceService,
            StreamMQListenerContainer listenerContainer,
            StreamMQDiagnosticsProperties properties) {
        Objects.requireNonNull(traceService, "traceService");
        Objects.requireNonNull(listenerContainer, "listenerContainer");
        Objects.requireNonNull(properties, "properties");
        return new StreamMQDiagnosticsService(traceService, listenerContainer, properties);
    }

    /**
     * 装配诊断 REST 端点（仅 Web 应用）。
     *
     * <p>仅当诊断服务与画像服务均就绪时才注册端点，避免因依赖未就绪导致上下文启动失败。
     *
     * @param diagnosticsService 诊断服务
     * @param profileService 消息画像服务
     * @return 诊断端点实例
     */
    @Bean
    @ConditionalOnWebApplication
    @ConditionalOnBean({StreamMQDiagnosticsService.class, MessageProfileService.class})
    public StreamMQDiagnosticsEndpoint streamMQDiagnosticsEndpoint(
            StreamMQDiagnosticsService diagnosticsService, MessageProfileService profileService) {
        return new StreamMQDiagnosticsEndpoint(diagnosticsService, profileService);
    }
}
