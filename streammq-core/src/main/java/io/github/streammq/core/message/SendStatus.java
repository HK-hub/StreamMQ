/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.message;

/**
 * 发送结果状态（0.1.2 定稿语义，发布即冻结）。
 *
 * <p><b>{@link #SEND_OK} 是"已受理"而非"已投递到目标 Stream"的泛化语义</b>，按发送路径分别解读：
 *
 * <ul>
 *   <li><b>普通发送</b>：Redis 已确认 XADD，消息已写入目标 Stream（持久化级别取决于 AOF，见 {@link SendResult}）
 *   <li><b>延时消息</b>（{@code delayLevel} / {@code delayTimeMillis}）：<b>已登记延时投递</b>——payload 与调度条目已
 *       原子写入，尚未写入目标 Stream（真实 Entry ID 在到期转投时才生成，此前的 {@code messageId} 为占位 ID，见 {@link
 *       MessageId#pending()}）
 *   <li><b>事务消息</b>：仅 {@code COMMIT_MESSAGE} 时为 {@code SEND_OK}；{@code ROLLBACK_MESSAGE} / {@code
 *       UNKNOWN} 等非提交状态一律为 {@link #SEND_FAILED}，调用方须结合 {@link SendResult#getTransactionState()}
 *       区分"未知 （待回查）"与"硬失败"
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum SendStatus {

    /** 已受理（普通=已写入目标 Stream；延时=已登记延时投递；事务=仅 COMMIT_MESSAGE），详见类型 javadoc。 */
    SEND_OK,

    /** 发送失败（异常、超时、过滤器/拦截器拒绝、事务非提交状态等） */
    SEND_FAILED
}
