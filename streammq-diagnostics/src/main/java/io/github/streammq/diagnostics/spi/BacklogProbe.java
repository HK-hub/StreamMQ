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
     * @param streamSize Stream 当前条目总数（XLEN）
     * @param pendingCount 消费者组 PEL 未确认消息数（XPENDING）
     */
    record Result(long streamSize, long pendingCount) {}
}
