package io.github.streammq.tracing;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.opentelemetry.api.OpenTelemetry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * StreamMQ OpenTelemetry 追踪自动装配。
 *
 * <p>触发条件：
 *
 * <ul>
 *   <li>classpath 中存在 {@link StreamMessageTemplate}
 *   <li>属性 {@code streammq.tracing.otel.enabled=true}
 * </ul>
 *
 * <p>装配内容：
 *
 * <ul>
 *   <li>{@link OpenTelemetry}：若上下文不存在则创建 no-op 默认实例（建议用户自行注入真实 SDK 实例以启用导出）
 *   <li>{@link StreamMQTracing}：追踪核心门面
 *   <li>{@link OpenTelemetryProducerInterceptor} / {@link OpenTelemetryConsumerInterceptor}：追踪拦截器
 *   <li>{@link StreamMQTopologyService}：当存在 {@link StreamMQTraceService} 与 {@link
 *       StreamMQListenerContainer} 时装配
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(StreamMessageTemplate.class)
@ConditionalOnProperty(prefix = "streammq.tracing.otel", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(StreamMQTracingProperties.class)
public class StreamMQTracingAutoConfiguration {

    /**
     * 默认 OpenTelemetry 实例（no-op）。
     *
     * <p>当用户未提供自定义 {@link OpenTelemetry} Bean 时使用 no-op 实例，追踪操作将优雅降级为空操作。 建议用户通过 OTel Spring Boot
     * Starter 或 Agent 注入真实 SDK 实例以启用数据导出。
     *
     * @return OpenTelemetry 实例
     */
    @Bean
    @ConditionalOnMissingBean
    public OpenTelemetry streamMQOpenTelemetry(StreamMQTracingProperties properties) {
        log.info(
                "StreamMQ OpenTelemetry 追踪已启用，未检测到自定义 OpenTelemetry Bean，使用 no-op 默认实例；"
                        + "serviceName={}, otlpEndpoint={}",
                properties.getServiceName(),
                properties.getOtlpEndpoint());
        return OpenTelemetry.noop();
    }

    /**
     * 追踪核心门面 Bean。
     *
     * @param openTelemetry OpenTelemetry 实例
     * @return StreamMQTracing 实例
     */
    @Bean
    @ConditionalOnMissingBean
    public StreamMQTracing streamMQTracing(OpenTelemetry openTelemetry) {
        return new StreamMQTracing(openTelemetry);
    }

    /**
     * 生产者追踪拦截器 Bean。
     *
     * <p>用户可通过提供同名 Bean 覆盖默认实现。
     *
     * @param tracing 追踪门面
     * @return 拦截器实例
     */
    @Bean
    @ConditionalOnMissingBean
    public OpenTelemetryProducerInterceptor openTelemetryProducerInterceptor(
            StreamMQTracing tracing) {
        return new OpenTelemetryProducerInterceptor(tracing);
    }

    /**
     * 消费者追踪拦截器 Bean。
     *
     * <p>用户可通过提供同名 Bean 覆盖默认实现。
     *
     * @param tracing 追踪门面
     * @return 拦截器实例
     */
    @Bean
    @ConditionalOnMissingBean
    public OpenTelemetryConsumerInterceptor openTelemetryConsumerInterceptor(
            StreamMQTracing tracing) {
        return new OpenTelemetryConsumerInterceptor(tracing);
    }

    /**
     * 拓扑服务 Bean，当存在追踪查询服务与监听器容器时装配。
     *
     * @param traceService 追踪查询服务
     * @param listenerContainer 监听器容器
     * @return 拓扑服务实例
     */
    @Bean
    @ConditionalOnBean({StreamMQTraceService.class, StreamMQListenerContainer.class})
    public StreamMQTopologyService streamMQTopologyService(
            StreamMQTraceService traceService, StreamMQListenerContainer listenerContainer) {
        return new StreamMQTopologyService(traceService, listenerContainer);
    }
}
