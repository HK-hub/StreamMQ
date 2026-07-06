package io.github.streammq.core.enums;

/**
 * 顺序消费回调返回动作。
 *
 * <p>顺序消费 Listener 在 {@code onMessage} 中返回此枚举值控制后续流程：
 * <ul>
 *   <li>{@link #SUCCESS} - 消费成功，自动 ACK，下一条继续</li>
 *   <li>{@link #SUSPEND_CURRENT_QUEUE_A_MOMENT} - 暂停当前 shard 一小段时间（默认 10ms），然后重新消费同一消息</li>
 * </ul>
 *
 * <p>当 Listener 抛出 {@link RuntimeException} 时，框架将其视为
 * {@link #SUSPEND_CURRENT_QUEUE_A_MOMENT}，避免顺序消息丢失。
 *
 * <p>顺序消费以返回值为唯一标准（与并发消费一致），框架不提供手动 ACK 调用。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum OrderlyAction {

    /**
     * 消费成功。框架将自动 ACK 该消息，从 PEL 中移除，下一条继续。
     */
    SUCCESS,

    /**
     * 顺序消费专用：暂停当前 shard 一小段时间后重新消费该消息。
     * 用于顺序消费场景下避免消息丢失，但又不希望立即重试导致雪崩。
     */
    SUSPEND_CURRENT_QUEUE_A_MOMENT
}
