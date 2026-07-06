package io.github.streammq.core.enums;

/**
 * ACK 模式：自动 vs 手动。
 *
 * <p>决定 Consumer 内是否需要显式调用 {@code Acknowledgment.acknowledge()}：
 * <ul>
 *   <li>{@link #AUTO} - 默认，Consumer 返回 {@link ConsumeAction#SUCCESS} / {@link OrderlyAction#SUCCESS}
 *       即视为成功并 ACK</li>
 *   <li>{@link #MANUAL} - 需通过 {@code ConsumeContext.acknowledge()} 显式 ACK，
 *       {@code onMessage} 返回值被忽略</li>
 * </ul>
 *
 * <p>MANUAL 模式下，用户实现 {@link io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer}
 * 或 {@link io.github.streammq.core.consumer.StreamMessageOrderlyConsumer}，
 * 通过 {@link io.github.streammq.core.consumer.ConsumeContext#acknowledge()} 控制 ACK。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum AcknowledgeMode {

    /**
     * 自动 ACK：Consumer 返回 {@link ConsumeAction#SUCCESS} / {@link OrderlyAction#SUCCESS} 后框架自动调用 XACK。
     * 适用于幂等性强、处理快速的场景。
     */
    AUTO,

    /**
     * 手动 ACK：Consumer 通过 {@code ConsumeContext.acknowledge()} 显式 ACK。
     * {@code onMessage} 返回值被忽略；若退出时未 ACK，框架视为失败进入重试。
     * 适用于业务需要异步处理或自定义成功条件的场景。
     */
    MANUAL
}
