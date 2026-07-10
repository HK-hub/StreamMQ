package io.github.streammq.core.interceptor;

import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.consumer.ConsumeContext;

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
     * @param context 消费上下文（含 topic、consumerGroup、reconsumeTimes 等）
     * @return true 继续消费，false 中止（视为消费失败）
     */
    boolean beforeConsume(Message<?> message, ConsumeContext context);

    /**
     * 消费后回调。
     *
     * @param message 已消费消息
     * @param action 消费动作（SUCCESS / RECONSUME_LATER）；顺序消费的 SUSPEND 会映射为 RECONSUME_LATER
     * @param context 消费上下文
     */
    void afterConsume(Message<?> message, ConsumeAction action, ConsumeContext context);

    /**
     * 消费过程中发生异常时调用。
     *
     * @param message 消息
     * @param exception 异常
     * @param timing 触发时机（BEFORE/EXECUTING/AFTER）
     * @param context 消费上下文
     */
    default void onException(Message<?> message, Exception exception, InvokeTiming timing, ConsumeContext context) {
        // 默认空实现，子类按需覆盖
    }

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
