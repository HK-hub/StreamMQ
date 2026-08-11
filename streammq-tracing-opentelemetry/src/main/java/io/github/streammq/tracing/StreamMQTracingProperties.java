package io.github.streammq.tracing;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * StreamMQ OpenTelemetry 追踪配置属性，绑定前缀 {@code streammq.tracing.otel}。
 *
 * <p>典型 {@code application.yml} 示例：
 *
 * <pre>{@code
 * streammq:
 *   tracing:
 *     otel:
 *       enabled: true
 *       service-name: streammq
 *       otlp-endpoint: http://localhost:4317
 *       exporter-interval-ms: 5000
 * }</pre>
 *
 * <p>当未提供自定义 {@link io.opentelemetry.api.OpenTelemetry} Bean 时， 自动装配将基于 {@code serviceName} 创建
 * no-op 实例；若用户自行提供 OTel SDK Bean， 则以下字段可作为参考配置使用。
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
@ConfigurationProperties(prefix = "streammq.tracing.otel")
@Getter
@Setter
public class StreamMQTracingProperties {

  /** 是否启用 OpenTelemetry 追踪自动装配，默认 false */
  private boolean enabled = false;

  /** OTLP 导出端点（如 {@code http://localhost:4317}），未配置时默认 OpenTelemetry 为 no-op */
  private String otlpEndpoint;

  /** 服务名称，用于标识遥测数据来源，默认 {@code streammq} */
  private String serviceName = "streammq";

  /** Span 导出间隔（毫秒），默认 5000 */
  private long exporterIntervalMs = 5000L;
}
