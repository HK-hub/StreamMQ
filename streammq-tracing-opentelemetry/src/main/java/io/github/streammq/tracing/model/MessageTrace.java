package io.github.streammq.tracing.model;

import java.util.List;

/**
 * 消息完整链路追踪，表示一条消息从发送到消费（含重试、死信）的完整生命周期。
 *
 * <p>由 {@link StreamMQTopologyService} 根据 {@link
 * io.github.streammq.core.trace.StreamMQTraceService#queryByMessageId(String)} 返回的 {@link
 * io.github.streammq.core.trace.TraceRecord} 列表构建，用于消息问题排查与链路可视化。
 *
 * @param messageId 消息 ID
 * @param topic Topic 名称
 * @param events 事件列表，按时间升序排列
 * @param totalDurationMillis 总耗时（毫秒，从首个事件到末尾事件）
 * @param routePath 消息流转路径（节点描述列表， 如 {@code ["Producer", "Topic:order-topic", "Group:order-group",
 *     "Consumer:OrderConsumer"]}）
 * @param finalStatus 最终状态：{@code SUCCESS} / {@code FAILED} / {@code DLQ} / {@code PROCESSING}
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public record MessageTrace(
        String messageId,
        String topic,
        List<TraceEvent> events,
        long totalDurationMillis,
        List<String> routePath,
        String finalStatus) {}
