package io.github.streammq.diagnostics;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.diagnostics.endpoint.StreamMQDiagnosticsEndpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.Objects;

/**
 * StreamMQ 诊断模块自动装配配置。
 *
 * <p>启用条件：
 * <ul>
 *   <li>类路径存在 {@link StreamMQTraceService}</li>
 *   <li>属性 {@code streammq.diagnostics.enabled=true}（默认关闭）</li>
 * </ul>
 *
 * <p>装配的 Bean：
 * <ul>
 *   <li>{@link MessageProfileService} - 当 {@link StreamMQTraceService} Bean 存在时</li>
 *   <li>{@link StreamMQDiagnosticsService} - 当 {@link StreamMQTraceService} 与
 *       {@link StreamMQListenerContainer} Bean 均存在时</li>
 *   <li>{@link StreamMQDiagnosticsEndpoint} - 当为 Web 应用时</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * # application.yml
 * streammq:
 *   diagnostics:
 *     enabled: true
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
@AutoConfiguration
@ConditionalOnClass(StreamMQTraceService.class)
@ConditionalOnProperty(prefix = "streammq.diagnostics", name = "enabled", havingValue = "true", matchIfMissing = false)
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
     * @return 诊断服务实例
     */
    @Bean
    @ConditionalOnBean({StreamMQTraceService.class, StreamMQListenerContainer.class})
    public StreamMQDiagnosticsService streamMQDiagnosticsService(StreamMQTraceService traceService,
                                                               StreamMQListenerContainer listenerContainer) {
        Objects.requireNonNull(traceService, "traceService");
        Objects.requireNonNull(listenerContainer, "listenerContainer");
        return new StreamMQDiagnosticsService(traceService, listenerContainer);
    }

    /**
     * 装配诊断 REST 端点（仅 Web 应用）。
     *
     * @param diagnosticsService 诊断服务
     * @param profileService 消息画像服务
     * @return 诊断端点实例
     */
    @Bean
    @ConditionalOnWebApplication
    public StreamMQDiagnosticsEndpoint streamMQDiagnosticsEndpoint(
            StreamMQDiagnosticsService diagnosticsService,
            MessageProfileService profileService) {
        return new StreamMQDiagnosticsEndpoint(diagnosticsService, profileService);
    }
}
