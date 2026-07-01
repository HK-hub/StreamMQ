package io.github.streammq.core.enums;

/**
 * ACK 模式：自动 vs 手动。
 *
 * <p>决定 Listener 内是否需要显式调用 {@code Acknowledgment.acknowledge()}：
 * <ul>
 *   <li>{@link #AUTO} - 默认，Listener 返回 {@link Action#SUCCESS} 即视为成功并 ACK</li>
 *   <li>{@link #MANUAL} - 需通过 {@code ConsumerContext.acknowledge()} 显式 ACK</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum AcknowledgeMode {

    /**
     * 自动 ACK：Listener 返回 {@link Action#SUCCESS} 后框架自动调用 XACK。
     * 适用于幂等性强、处理快速的场景。
     */
    AUTO,

    /**
     * 手动 ACK：Listener 通过 {@code ConsumerContext.acknowledge()} 显式 ACK。
     * 适用于业务需要异步处理或自定义成功条件的场景。
     */
    MANUAL
}
