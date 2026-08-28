/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.enums;

/**
 * 本地事务状态：事务消息回查的返回值。
 *
 * <p>对齐 RocketMQ 事务消息语义：
 *
 * <ul>
 *   <li>{@link #COMMIT_MESSAGE} - 本地事务执行成功，提交半消息使其可见
 *   <li>{@link #ROLLBACK_MESSAGE} - 本地事务执行失败，回滚半消息并删除
 *   <li>{@link #UNKNOWN} - 本地事务状态未知，等待后续回查
 * </ul>
 *
 * <p>{@link #UNKNOWN} 在连续多次回查仍无明确结果后，框架将强制回滚。
 *
 * <p><b>历史命名修正：</b>0.1.0 起标准名为 {@code UNKNOWN}（拼写正确）。为兼容 0.0.x 早期用户，
 * 仍保留 {@link #UNKNOW} 作为 {@code @Deprecated} 别名，0.3.0 计划移除。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum LocalTransactionState {

    /** 提交消息：本地事务执行成功，半消息将被提交，对消费者可见。 */
    COMMIT_MESSAGE,

    /** 回滚消息：本地事务执行失败，半消息将被回滚并删除。 */
    ROLLBACK_MESSAGE,

    /**
     * 未知状态：本地事务状态不确定，等待事务回查任务稍后再次检查。 连续 {@code check-max-times} 次仍为 UNKNOWN，框架强制 ROLLBACK_MESSAGE。
     */
    UNKNOWN,

    /**
     * @deprecated 拼写错误；请使用 {@link #UNKNOWN}。保留仅为兼容 0.0.x 早期用户，0.3.0 计划移除。
     */
    @Deprecated
    UNKNOW
}
