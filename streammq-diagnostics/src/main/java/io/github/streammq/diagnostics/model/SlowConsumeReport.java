package io.github.streammq.diagnostics.model;

/**
 * 慢消费诊断报告，反映指定主题+消费者组的消费性能状况。
 *
 * <p>由 {@link io.github.streammq.diagnostics.StreamMQDiagnosticsService#diagnoseSlowConsume(String,
 * String)} 生成， 包含消费速率、耗时统计、线程池状态、瓶颈分析与优化建议。
 *
 * @param topic 主题
 * @param group 消费者组
 * @param consumeRate 消费速率（条/秒）
 * @param produceRate 生产速率（条/秒）
 * @param avgConsumeTimeMillis 平均消费耗时（毫秒）
 * @param maxConsumeTimeMillis 最大消费耗时（毫秒）
 * @param p99ConsumeTimeMillis P99 消费耗时（毫秒）
 * @param threadPoolActive 线程池活跃线程数
 * @param threadPoolMax 线程池最大线程数
 * @param bottleneck 瓶颈分析描述
 * @param recommendation 优化建议
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
public record SlowConsumeReport(
    String topic,
    String group,
    double consumeRate,
    double produceRate,
    double avgConsumeTimeMillis,
    long maxConsumeTimeMillis,
    long p99ConsumeTimeMillis,
    int threadPoolActive,
    int threadPoolMax,
    String bottleneck,
    String recommendation) {}
