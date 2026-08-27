/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

/**
 * PelClaim 扫描目标种类。
 *
 * <p>同一调度器需要恢复三类 Stream 的 PEL 滞留消息，各类别的流 Key、认领后处理方式不同：
 *
 * <ul>
 *   <li>{@link #TOPIC} —— 业务消息流 {@code streammq:{ns}:msg:{topic}}：维持既有语义 （分片锁保护、超限转 DLQ、递增
 *       retryTimes 重投）
 *   <li>{@link #RETRY} —— 重试流 {@code streammq:{ns}:retry:msg:{topic}:{group}}： 超限转 DLQ，否则原样复制到流尾 +
 *       ACK 旧条目 （消费者经 {@code >} 读新 ID 继续处理，重试计数字段随行）
 *   <li>{@link #DLQ} —— 死信流 {@code streammq:{ns}:dlq:{group}}：原样复制到流尾 + ACK 旧条目 （DLQ
 *       消费者重新处理，其失败策略字段随行约束循环）
 * </ul>
 *
 * <p>种类参与目标去重键（{@code KIND:topic:group}），避免不同类别在相同 topic/group 维度上互相覆盖。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum PelClaimTargetKind {

    /** 业务消息流 */
    TOPIC,

    /** 重试消息流（对齐 RocketMQ %RETRY%{group}%） */
    RETRY,

    /** 死信流（对齐 RocketMQ %DLQ%{group}%） */
    DLQ
}
