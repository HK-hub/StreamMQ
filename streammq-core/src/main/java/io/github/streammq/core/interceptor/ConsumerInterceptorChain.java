package io.github.streammq.core.interceptor;

import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.message.Message;

import java.util.Collection;
import java.util.List;

/**
 * 消费者拦截器链策略接口。
 *
 * <p>管理全局 {@link ConsumerInterceptor} 列表，并按 {@link ConsumerInterceptor#order()} 升序执行。
 * 封装三阶段拦截逻辑：
 * <ul>
 *   <li>{@link #applyBefore} - 消费前拦截，任一拦截器返回 false 则中止消费</li>
 *   <li>{@link #applyAfter} - 消费后拦截，传入最终 {@link ConsumeAction}</li>
 *   <li>{@link #notifyException} - 异常通知，传入 {@link InvokeTiming} 触发时机</li>
 * </ul>
 *
 * <p>实现可通过 {@code DefaultStreamMQListenerContainer} 构造器注入，以自定义拦截链行为。
 * 默认实现位于 {@code streammq-redisson-adapter} 模块。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ConsumerInterceptorChain {

    /**
     * 添加单个拦截器（按 {@link ConsumerInterceptor#order()} 升序插入）。
     *
     * @param interceptor 拦截器实例
     */
    void addInterceptor(ConsumerInterceptor interceptor);

    /**
     * 批量添加拦截器。
     *
     * @param interceptors 拦截器集合
     */
    void addInterceptors(Collection<ConsumerInterceptor> interceptors);

    /**
     * 执行 beforeConsume 拦截器链。
     *
     * @param message 待消费消息
     * @return true 全部通过，false 任一拦截器拒绝
     */
    boolean applyBefore(Message<?> message);

    /**
     * 执行 afterConsume 拦截器链。
     *
     * @param message 已消费消息
     * @param action 消费动作
     */
    void applyAfter(Message<?> message, ConsumeAction action);

    /**
     * 通知所有拦截器发生异常。
     *
     * @param message 消息
     * @param ex 异常
     * @param timing 触发时机（BEFORE/EXECUTING/AFTER）
     */
    void notifyException(Message<?> message, Exception ex, InvokeTiming timing);

    /**
     * 返回当前已注册的拦截器列表（按 order 升序）。
     *
     * @return 拦截器列表
     */
    List<ConsumerInterceptor> getInterceptors();
}
