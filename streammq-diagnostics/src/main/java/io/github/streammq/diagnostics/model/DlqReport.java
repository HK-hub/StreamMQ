package io.github.streammq.diagnostics.model;

import java.util.List;

/**
 * 死信队列诊断报告，反映指定消费者组的死信状况与失败原因分布。
 *
 * <p>由 {@link io.github.streammq.diagnostics.StreamMQDiagnosticsService#diagnoseDlq(String)} 生成，
 * 包含死信总数、Top 失败原因、Top 失败主题、最早死信消息时间戳与优化建议。
 *
 * @param group 消费者组
 * @param totalDlqCount 死信队列消息总数
 * @param topFailureReasons Top 失败原因列表（按出现次数降序）
 * @param topFailedTopics Top 失败主题列表（按失败次数降序）
 * @param oldestDlqMessageTimestamp 最早进入死信队列的消息时间戳（毫秒）
 * @param recommendation 优化建议
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
public record DlqReport(
    String group,
    long totalDlqCount,
    List<FailureReason> topFailureReasons,
    List<TopicFailureCount> topFailedTopics,
    long oldestDlqMessageTimestamp,
    String recommendation
) {
}
