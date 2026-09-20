/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
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
 * @param consumerInstances 当前该 consumer group 的消费者实例数（可观测事实）。
 *     <p><b>为什么不再报告"线程池活跃/最大线程数"：</b>真实 executor 指标尚未接入本模块，此前把该字段直接 填成消费者实例数（两个字段同为实例数，且空报告里填
 *     {@code 0} / {@code max(instances,1)}），会让运维误读为"线程池已 100% 打满"。宁可不给，也不给假数据。
 * @param bottleneck 瓶颈分析描述
 * @param recommendation 优化建议
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public record SlowConsumeReport(
        String topic,
        String group,
        double consumeRate,
        double produceRate,
        double avgConsumeTimeMillis,
        long maxConsumeTimeMillis,
        long p99ConsumeTimeMillis,
        int consumerInstances,
        String bottleneck,
        String recommendation,
        String code) {}
