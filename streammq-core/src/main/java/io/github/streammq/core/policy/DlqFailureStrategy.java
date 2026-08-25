/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.policy;

import io.github.streammq.core.message.Message;

/**
 * DLQ 消费失败处理策略 SPI。
 *
 * <p>当 {@code dlqMode=true} 的消费者消费死信消息失败（抛出异常或返回非 SUCCESS）时， 框架构造 {@link DlqFailureContext} 并调用本接口的
 * {@link #decide} 方法。 策略根据上下文信息返回 {@link DlqFailureDecision}：
 *
 * <ul>
 *   <li>{@link DlqFailureDecision#drop()} - 丢弃消息（框架 ACK 后由策略记录日志/告警）
 *   <li>{@link DlqFailureDecision#retry(java.time.Duration)} - 按指定延迟重试本 DLQ 消息
 *   <li>{@link DlqFailureDecision#secondaryDlq()} - 转投到二级死信队列
 * </ul>
 *
 * <p>内置策略（参见 {@code streammq-redisson-adapter} 模块）：
 *
 * <ul>
 *   <li>{@code LogAndDropDlqFailureStrategy} - 始终丢弃（仅记录日志）
 *   <li>{@code LimitedRetryDlqFailureStrategy} - 有限次重试后丢弃
 *   <li>{@code SecondaryDlqFailureStrategy} - 有限次重试后转投二级死信
 * </ul>
 *
 * <p>用户可实现本接口并注入 Spring Bean 或通过注解 {@code dlqFailureStrategy} 指定实现类。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface DlqFailureStrategy {

    /**
     * 根据上下文决定 DLQ 消息失败后的处理动作。
     *
     * @param message 消费失败的死信消息
     * @param context DLQ 失败上下文（含重试次数、原因等）
     * @return 决策（永不返回 null；返回 null 时框架视为 {@link DlqFailureDecision#drop()}）
     */
    DlqFailureDecision decide(Message<?> message, DlqFailureContext context);

    /** 策略名称 */
    default String name() {
        return getClass().getSimpleName();
    }
}
