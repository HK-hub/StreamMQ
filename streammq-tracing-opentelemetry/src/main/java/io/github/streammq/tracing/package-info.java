/**
 * StreamMQ OpenTelemetry 追踪模块，提供原生 OpenTelemetry 集成、W3C TraceContext 上下文传播与消息拓扑可视化。
 *
 * <p>核心组件：
 *
 * <ul>
 *   <li>{@link io.github.streammq.tracing.StreamMQTracing} - 追踪核心门面，管理 OpenTelemetry /
 *       Tracer，提供上下文注入与提取
 *   <li>{@link io.github.streammq.tracing.OpenTelemetryProducerInterceptor} / {@link
 *       io.github.streammq.tracing.OpenTelemetryConsumerInterceptor} - 生产者 / 消费者追踪拦截器
 *   <li>{@link io.github.streammq.tracing.StreamMQTopologyService} - 消息拓扑构建与链路查询服务
 *   <li>{@link io.github.streammq.tracing.StreamMQTracingAutoConfiguration} - Spring Boot 自动装配
 *   <li>{@link io.github.streammq.tracing.model} - 拓扑与链路数据模型
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
package io.github.streammq.tracing;
