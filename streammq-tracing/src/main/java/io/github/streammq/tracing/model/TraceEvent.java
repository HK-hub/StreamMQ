package io.github.streammq.tracing.model;

import java.util.Map;

/**
 * 追踪事件，表示消息生命周期中的一个关键事件。
 *
 * <p>由 {@link StreamMQTopologyService} 从 {@link io.github.streammq.core.trace.TraceRecord}
 * 转换而来，用于构建 {@link MessageTrace} 的完整事件链。
 *
 * @param type           事件类型
 * @param timestamp      事件时间戳（毫秒）
 * @param durationMillis 耗时（毫秒）
 * @param success        是否成功
 * @param detail         事件详情描述
 * @param attributes     扩展属性
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
public record TraceEvent(
    TraceEventType type,
    long timestamp,
    long durationMillis,
    boolean success,
    String detail,
    Map<String, String> attributes
) {
}
