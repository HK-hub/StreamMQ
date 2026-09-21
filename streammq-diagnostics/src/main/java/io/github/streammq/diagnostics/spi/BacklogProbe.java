/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics.spi;

/**
 * 积压探针 SPI：提供基于真实 Redis 数据的积压统计，供 {@code StreamMQDiagnosticsService} 使用。
 *
 * <p>默认实现 {@code RedisBacklogProbe} 基于 {@code XLEN} / {@code XPENDING} 计算； 用户可实现本接口注册自定义
 * 探针（如基于监控系统、大屏指标）以替换默认实现。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface BacklogProbe {

    /**
     * 探测指定主题 + 消费者组的实时积压统计。
     *
     * @param topic 主题
     * @param group 消费者组
     * @return 积压统计结果；无法计算时返回 {@code null}
     */
    Result probe(String topic, String group);

    /**
     * 积压统计结果。
     *
     * <p><b>为什么同时给出三个量：</b>"积压"在两类故障下由不同信号表达，只看其中之一必然漏判——
     *
     * <ul>
     *   <li><b>消费者跟不上</b>（读得慢/处理慢）：{@code pendingCount}（XPENDING）增长， 此时 {@code streamSize}
     *       可能因历史条目未裁剪而长期偏大、不反映增量
     *   <li><b>消费者进程全挂</b>：{@code pendingCount ≈ 0}（没人读自然没有未确认）， 但 {@code streamSize} 持续增长；只看
     *       XPENDING 会判定"无积压"而<b>永不扩容</b>
     * </ul>
     *
     * <p>因此 {@code consumerCount} 是必要的判别维度：调用方在"无活跃消费者"时应以 {@code streamSize} 作为积压信号。
     *
     * @param streamSize Stream 当前条目总数（XLEN）
     * @param pendingCount 消费者组 PEL 未确认消息数（XPENDING）
     * @param consumerCount 消费者组当前活跃消费者数（XINFO GROUPS → consumers 长度；组不存在时为 0）
     */
    record Result(long streamSize, long pendingCount, int consumerCount) {}
}
