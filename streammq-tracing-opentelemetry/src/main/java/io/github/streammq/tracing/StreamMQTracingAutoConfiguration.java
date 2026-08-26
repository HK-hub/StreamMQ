/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.tracing;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

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
@org.springframework.boot.autoconfigure.AutoConfiguration
@ConditionalOnClass(StreamMessageTemplate.class)
@ConditionalOnProperty(
        prefix = StreamMQTracingProperties.PROP_PREFIX,
        name = StreamMQTracingProperties.PROP_NAME_ENABLED,
        havingValue = StreamMQTracingProperties.PROP_VALUE_TRUE)
@EnableConfigurationProperties(StreamMQTracingProperties.class)
public class StreamMQTracingAutoConfiguration {

    /**
     * 默认 OpenTelemetry 实例。
     *
     * <p>行为由配置决定：
     *
     * <ul>
     *   <li>配置 {@code streammq.tracing.otel.otlp-endpoint} 时：构建完整 SDK（BatchSpanProcessor + OTLP
     *       gRPC Exporter），Span 按配置间隔批量导出到指定端点
     *   <li>未配置端点且用户未提供自定义 Bean 时：no-op 实例，优雅降级为空操作；建议通过 OTel Spring Boot Starter / Agent 注入真实 SDK
     *       以启用导出
     * </ul>
     *
     * @param properties 追踪配置
     * @return OpenTelemetry 实例
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public OpenTelemetry streamMQOpenTelemetry(StreamMQTracingProperties properties) {
        String otlpEndpoint = properties.getOtlpEndpoint();
        if (otlpEndpoint == null || otlpEndpoint.isBlank()) {
            log.info(
                    "StreamMQ OpenTelemetry 追踪已启用，未配置 otlp-endpoint，使用 no-op 默认实例"
                            + "（不会导出任何数据）；serviceName={}",
                    properties.getServiceName());
            // OpenTelemetry.noop() 返回 DefaultOpenTelemetry，并非 OpenTelemetrySdk 的实例；
            // 此前的强转在运行期必然抛 ClassCastException，导致无端点场景上下文启动失败
            return OpenTelemetry.noop();
        }
        // 真实 SDK + OTLP 导出
        // service.name 使用原生 AttributeKey，避免依赖 -alpha 语义约定构件
        Resource resource =
                Resource.getDefault()
                        .merge(
                                Resource.create(
                                        Attributes.of(
                                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                                        "service.name"),
                                                properties.getServiceName())));
        OtlpGrpcSpanExporter exporter =
                OtlpGrpcSpanExporter.builder()
                        .setEndpoint(otlpEndpoint)
                        .setTimeout(java.time.Duration.ofSeconds(10))
                        .build();
        BatchSpanProcessor spanProcessor =
                BatchSpanProcessor.builder(exporter)
                        .setScheduleDelay(
                                java.time.Duration.ofMillis(properties.getExporterIntervalMs()))
                        .build();
        SdkTracerProvider tracerProvider =
                SdkTracerProvider.builder()
                        .setResource(resource)
                        .addSpanProcessor(spanProcessor)
                        .build();
        log.info(
                "StreamMQ OpenTelemetry SDK 已启用，OTLP gRPC 导出至 {}，serviceName={}，批次间隔={}ms",
                otlpEndpoint,
                properties.getServiceName(),
                properties.getExporterIntervalMs());
        return OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();
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
