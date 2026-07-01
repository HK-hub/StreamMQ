package io.github.streammq.core.spi;

import io.github.streammq.core.enums.Action;
import io.github.streammq.core.message.Message;

/**
 * 消费者拦截器 SPI，对齐 RocketMQ {@code ConsumerInterceptor} 体验。
 *
 * <p>在消费前后被调用，可用于：
 * <ul>
 *   <li>追踪埋点</li>
 *   <li>消费审计日志</li>
 *   <li>消息预处理（解密、解压）</li>
 *   <li>限流（beforeConsume 返回 false 中止本次消费，视为 RECONSUME_LATER）</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ConsumerInterceptor {

    /**
     * 消费前回调。
     *
     * @param message 待消费消息（可修改）
     * @return true 继续消费，false 中止（视为消费失败）
     */
    boolean beforeConsume(Message<?> message);

    /**
     * 消费后回调。
     *
     * @param message 已消费消息
     * @param action 消费动作（SUCCESS / RECONSUME_LATER / SUSPEND / ...）
     */
    void afterConsume(Message<?> message, Action action);

    /**
     * 拦截器名称。
     *
     * @return 名称
     */
    default String name() {
        return getClass().getSimpleName();
    }

    /**
     * 拦截器执行顺序（升序，默认 0）。
     *
     * @return 顺序值
     */
    default int order() {
        return 0;
    }
}
