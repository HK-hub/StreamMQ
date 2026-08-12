package io.github.streammq.diagnostics.model;

/**
 * 失败原因统计，表示某一类失败原因的出现次数与占比。
 *
 * <p>由 {@link io.github.streammq.diagnostics.StreamMQDiagnosticsService#diagnoseDlq(String)} 聚合生成，
 * 用于识别最高频的失败原因。
 *
 * @param reason 失败原因描述（异常类名或摘要信息）
 * @param count 出现次数
 * @param percentage 占比（0.0 ~ 100.0）
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public record FailureReason(String reason, long count, double percentage) {}
