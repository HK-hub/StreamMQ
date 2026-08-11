package io.github.streammq.diagnostics.model;

/**
 * 消息积压诊断报告，反映指定主题+消费者组的积压状况与清积压预估。
 *
 * <p>由 {@link io.github.streammq.diagnostics.StreamMQDiagnosticsService#diagnoseBacklog(String,
 * String)} 生成， 包含当前积压量、增长速率、清积压预估时间、严重级别与优化建议。
 *
 * <p>严重级别判定规则：
 *
 * <ul>
 *   <li>{@link Severity#INFO} - 积压低于警告阈值（backlogWarningThreshold，默认 1000）
 *   <li>{@link Severity#WARNING} - 积压介于警告阈值与严重阈值之间（backlogCriticalThreshold，默认 10000）
 *   <li>{@link Severity#CRITICAL} - 积压超过严重阈值
 * </ul>
 *
 * @param topic 主题
 * @param group 消费者组
 * @param currentBacklog 当前积压消息数
 * @param growthRate 积压增长率（条/秒，正数表示积压增加，负数表示积压减少）
 * @param estimatedClearTimeMinutes 预计清空积压时间（分钟，-1 表示无法估算或积压在增长）
 * @param produceRate 生产速率（条/秒）
 * @param consumeRate 消费速率（条/秒）
 * @param recommendation 优化建议
 * @param severity 严重级别
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
public record BacklogReport(
    String topic,
    String group,
    long currentBacklog,
    double growthRate,
    long estimatedClearTimeMinutes,
    double produceRate,
    double consumeRate,
    String recommendation,
    Severity severity) {}
