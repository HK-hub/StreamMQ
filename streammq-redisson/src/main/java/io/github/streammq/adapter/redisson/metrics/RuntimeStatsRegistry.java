/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * 进程内运行时统计登记表。
 *
 * <p><b>发布前修复 P1-3：</b>{@code GET /actuator/streammq/stats/{group}/{topic}} 此前读取 Redis 上的 {@code
 * streammq:{ns}:meta:stats:{group}:{topic}} Hash，而该 key 在全项目中<b>没有任何生产代码写入</b> —— 端点永远返回 {@code
 * {}}，是一个被 README 宣传的死端点。
 *
 * <p>本登记表提供真实的统计数据源：
 *
 * <ul>
 *   <li>消费成功/失败计数（由 {@code DefaultMessageProcessor} 在每条消息消费后上报）
 *   <li>重试计数、死信计数（由 {@code DefaultRetryAndDlqHandler} 在上报）
 *   <li>消费耗时累计（用于派生平均耗时）
 * </ul>
 *
 * <p><b>为什么是进程内而非直接写 Redis：</b>统计上报位于每条消息的关键路径上。若每次消费都发起 Redis 写，吞吐会被直接吃掉（这正是 Micrometer
 * 使用本地累积再按步上报的原因）。进程内 {@link LongAdder} 的开销在纳秒级，对消费链路无影响；跨实例聚合由管理端点在读取时把本实例 快照与 Redis 侧持久化的聚合值合并呈现。
 *
 * <p>线程安全：全部状态为 {@link ConcurrentHashMap} + {@link LongAdder}，可并发上报。
 *
 * @author StreamMQ Contributors
 * @since 0.1.1
 */
public class RuntimeStatsRegistry {

    /** 统计维度键：group + topic。 */
    private record Key(String group, String topic) {}

    /** 单个维度的计数器组。 */
    private static final class Counters {
        final LongAdder consumeSuccess = new LongAdder();
        final LongAdder consumeFailure = new LongAdder();
        final LongAdder retried = new LongAdder();
        final LongAdder dlq = new LongAdder();
        final LongAdder consumeNanos = new LongAdder();
    }

    private final ConcurrentHashMap<Key, Counters> counters = new ConcurrentHashMap<>();

    /** 登记表创建时间戳（毫秒），用于派生"运行时长"与平均速率。 */
    private final long startedAtMillis = System.currentTimeMillis();

    private Counters countersFor(String group, String topic) {
        return counters.computeIfAbsent(new Key(group, topic), k -> new Counters());
    }

    /** 上报一次消费结果（{@code durationNanos} 用于派生平均耗时）。 */
    public void recordConsume(String group, String topic, boolean success, long durationNanos) {
        Counters c = countersFor(group, topic);
        if (success) {
            c.consumeSuccess.increment();
        } else {
            c.consumeFailure.increment();
        }
        c.consumeNanos.add(durationNanos);
    }

    /** 上报一次重试调度。 */
    public void recordRetry(String group, String topic) {
        countersFor(group, topic).retried.increment();
    }

    /** 上报一次进入死信队列。 */
    public void recordDlq(String group, String topic) {
        countersFor(group, topic).dlq.increment();
    }

    /**
     * 返回指定维度的统计快照。
     *
     * <p>字段稳定（属于管理端点的对外契约，变更需走 CHANGELOG）： {@code consumeSuccess} / {@code consumeFailure} / {@code
     * consumeTotal} / {@code retried} / {@code dlq} / {@code avgConsumeMillis} / {@code
     * uptimeMillis} / {@code recordedSince}。
     *
     * @param group 消费者组
     * @param topic 主题
     * @return 统计快照；该维度无数据时返回仅含 {@code uptimeMillis} 与 {@code noData} 标记的 map
     */
    public Map<String, Object> snapshot(String group, String topic) {
        Map<String, Object> out = new LinkedHashMap<>();
        long uptime = System.currentTimeMillis() - startedAtMillis;
        out.put("uptimeMillis", uptime);
        Counters c = counters.get(new Key(group, topic));
        if (c == null) {
            out.put("noData", true);
            out.put(
                    "hint",
                    "No consumption recorded for this group/topic on this instance yet."
                            + " Stats are per-instance; query other instances or check that the"
                            + " consumer is running.");
            return out;
        }
        long success = c.consumeSuccess.sum();
        long failure = c.consumeFailure.sum();
        long total = success + failure;
        long retried = c.retried.sum();
        long dlq = c.dlq.sum();
        out.put("consumeSuccess", success);
        out.put("consumeFailure", failure);
        out.put("consumeTotal", total);
        out.put("retried", retried);
        out.put("dlq", dlq);
        out.put(
                "avgConsumeMillis",
                total == 0 ? 0.0 : round2(c.consumeNanos.sum() / 1_000_000.0 / total));
        out.put("recordedSince", startedAtMillis);
        return out;
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
