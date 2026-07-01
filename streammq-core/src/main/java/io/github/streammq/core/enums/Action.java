package io.github.streammq.core.enums;

/**
 * 消费回调返回动作。
 *
 * <p>Listener 在 {@code onMessage} 中返回此枚举值控制后续流程：
 * <ul>
 *   <li>{@link #SUCCESS} - 消费成功，自动 ACK，从 PEL 移除</li>
 *   <li>{@link #RECONSUME_LATER} - 消费失败，稍后重试（写入 retry ZSet）</li>
 *   <li>{@link #SUSPEND_CURRENT_QUEUE_A_MOMENT} - 顺序消费专用，暂停当前 shard 一小段时间</li>
 *   <li>{@link #COMMIT} - 事务消息提交</li>
 *   <li>{@link #ROLLBACK} - 事务消息回滚</li>
 * </ul>
 *
 * <p>当 Listener 抛出 {@link RuntimeException} 时，框架将其视为 {@link #RECONSUME_LATER}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum Action {

    /**
     * 消费成功。框架将自动 ACK 该消息，从 PEL 中移除。
     */
    SUCCESS,

    /**
     * 消费失败，需稍后重试。框架将消息写入 retry ZSet，按 {@code RetryPolicy} 调度重投。
     */
    RECONSUME_LATER,

    /**
     * 顺序消费专用：暂停当前 shard 一小段时间后重新消费该消息。
     * 用于顺序消费场景下避免消息丢失，但又不希望立即重试导致雪崩。
     */
    SUSPEND_CURRENT_QUEUE_A_MOMENT,

    /**
     * 事务消息提交：通知框架提交半消息。
     */
    COMMIT,

    /**
     * 事务消息回滚：通知框架回滚半消息。
     */
    ROLLBACK
}
