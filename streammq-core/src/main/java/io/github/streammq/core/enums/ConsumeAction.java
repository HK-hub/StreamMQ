package io.github.streammq.core.enums;

/**
 * 并发消费回调返回动作。
 *
 * <p>Listener 在 {@code onMessage} 中返回此枚举值控制后续流程：
 * <ul>
 *   <li>{@link #SUCCESS} - 消费成功，自动 ACK，从 PEL 移除</li>
 *   <li>{@link #RECONSUME_LATER} - 消费失败，稍后重试（写入 retry ZSet）</li>
 * </ul>
 *
 * <p>当 Listener 抛出 {@link RuntimeException} 时，框架将其视为 {@link #RECONSUME_LATER}。
 *
 * <p>在 {@code AcknowledgeMode.MANUAL} 模式下，本返回值被忽略，
 * 由 Consumer 通过 {@code context.acknowledge()} 显式控制 ACK。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum ConsumeAction {

    /**
     * 消费成功。框架将自动 ACK 该消息，从 PEL 中移除。
     */
    SUCCESS,

    /**
     * 消费失败，需稍后重试。框架将消息写入 retry ZSet，按 {@code RetryPolicy} 调度重投。
     */
    RECONSUME_LATER
}
