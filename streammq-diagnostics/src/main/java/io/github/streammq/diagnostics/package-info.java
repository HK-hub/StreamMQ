/**
 * StreamMQ 消息生命周期可视化与异常诊断模块。
 *
 * <p>本模块基于 {@link io.github.streammq.core.trace.StreamMQTraceService} 提供的追踪数据，
 * 构建消息完整生命周期画像，并提供慢消费、消息积压、死信队列等异常的自动诊断能力。
 *
 * <p>核心能力：
 * <ul>
 *   <li>消息画像：{@link io.github.streammq.diagnostics.MessageProfileService} 构建单条消息的完整生命周期画像</li>
 *   <li>异常诊断：{@link io.github.streammq.diagnostics.StreamMQDiagnosticsService} 自动诊断慢消费、积压、DLQ 问题</li>
 *   <li>REST 端点：{@link io.github.streammq.diagnostics.endpoint.StreamMQDiagnosticsEndpoint} 暴露 JSON 接口供仪表盘集成</li>
 *   <li>数据模型：{@link io.github.streammq.diagnostics.model} 包含画像与诊断报告的全部数据结构</li>
 * </ul>
 *
 * <p>启用方式：在 {@code application.yml} 中配置：
 * <pre>{@code
 * streammq:
 *   diagnostics:
 *     enabled: true
 * }</pre>
 *
 * <p>当追踪服务不可用时，所有诊断方法将以无数据方式优雅降级，不抛出异常。
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
package io.github.streammq.diagnostics;
