package io.github.streammq.core.spi;

import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;

/**
 * 生产者拦截器 SPI，对齐 RocketMQ {@code ProducerInterceptor} 体验。
 *
 * <p>在发送前后被调用，可用于：
 * <ul>
 *   <li>添加追踪属性（traceId / spanId）</li>
 *   <li>审计日志记录</li>
 *   <li>消息预处理（加密、压缩、字段补全）</li>
 *   <li>限流（beforeSend 返回 false 中止发送）</li>
 * </ul>
 *
 * <p>多拦截器按 {@link #order()} 升序执行。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ProducerInterceptor {

    /**
     * 发送前回调。
     *
     * @param message 待发送消息（可修改）
     * @return true 继续发送（含后续拦截器），false 中止发送（返回 SEND_FAILED）
     */
    boolean beforeSend(Message<?> message);

    /**
     * 发送后回调。
     *
     * @param message 已发送消息
     * @param result 发送结果
     */
    void afterSend(Message<?> message, SendResult result);

    /**
     * 发送过程中发生异常时调用。
     *
     * @param message 消息
     * @param exception 异常
     * @param timing 触发时机（BEFORE/EXECUTING/AFTER）
     */
    default void onException(Message<?> message, Exception exception, InvokeTiming timing) {
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
