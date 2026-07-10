package io.github.streammq.core.filter;

import io.github.streammq.core.message.Message;

/**
 * 消费者消息过滤器 SPI。
 *
 * <p>在消息消费前进行过滤，返回 false 则跳过该消息（自动 ACK）。
 * 支持全局维度（配置文件）和 per-consumer 维度（注解）的过滤器。
 *
 * <p>典型使用场景：
 * <ul>
 *   <li>按 tag 过滤消息（如只消费特定 tag 的消息）</li>
 *   <li>按 keys 过滤消息（如只消费特定分片的消息）</li>
 *   <li>按 userProperties 过滤消息（如只处理特定业务类型的消息）</li>
 *   <li>按 body 内容过滤消息（如只处理符合特定条件的业务数据）</li>
 * </ul>
 *
 * <p>多过滤器按 {@link #order()} 升序执行，任一过滤器返回 false 则消息被跳过。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ConsumerFilter {

    /**
     * 判断消息是否应被消费。
     *
     * @param message 待过滤消息
     * @return true 不过滤（继续消费），false 过滤（跳过，自动 ACK）
     */
    boolean accept(Message<?> message);

    /**
     * 过滤器名称。
     *
     * @return 名称
     */
    default String name() {
        return getClass().getSimpleName();
    }

    /**
     * 过滤器执行顺序（升序，默认 0）。
     *
     * <p>selectorExpression 对应的过滤器默认 order = -1，优先执行。
     *
     * @return 顺序值
     */
    default int order() {
        return 0;
    }
}