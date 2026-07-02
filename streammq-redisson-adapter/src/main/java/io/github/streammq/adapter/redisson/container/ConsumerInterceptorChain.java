package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.enums.Action;
import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.spi.ConsumerInterceptor;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 消费者拦截器链（策略类）。
 *
 * <p>管理全局 {@link ConsumerInterceptor} 列表，并按 {@link ConsumerInterceptor#order()} 升序执行。
 * 封装三阶段拦截逻辑：
 * <ul>
 *   <li>{@link #applyBefore} - 消费前拦截，任一拦截器返回 false 则中止消费</li>
 *   <li>{@link #applyAfter} - 消费后拦截，传入最终 {@link Action}</li>
 *   <li>{@link #notifyException} - 异常通知，传入 {@link InvokeTiming} 触发时机</li>
 * </ul>
 *
 * <p>线程安全：使用 {@link CopyOnWriteArrayList}，支持运行时动态添加拦截器。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class ConsumerInterceptorChain {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerInterceptorChain.class);

    @Getter
    private final List<ConsumerInterceptor> interceptors = new CopyOnWriteArrayList<>();

    /**
     * 添加单个拦截器（按 {@link ConsumerInterceptor#order()} 升序插入）。
     *
     * @param interceptor 拦截器实例
     */
    public void addInterceptor(ConsumerInterceptor interceptor) {
        Objects.requireNonNull(interceptor, "interceptor");
        int insertIndex = 0;
        for (ConsumerInterceptor existing : interceptors) {
            if (existing.order() <= interceptor.order()) {
                insertIndex++;
            } else {
                break;
            }
        }
        interceptors.add(insertIndex, interceptor);
    }

    /**
     * 批量添加拦截器。
     *
     * @param interceptors 拦截器集合
     */
    public void addInterceptors(Collection<ConsumerInterceptor> interceptors) {
        if (interceptors != null) {
            for (ConsumerInterceptor interceptor : interceptors) {
                addInterceptor(interceptor);
            }
        }
    }

    /**
     * 执行 beforeConsume 拦截器链。
     *
     * @param message 待消费消息
     * @return true 全部通过，false 任一拦截器拒绝
     */
    public boolean applyBefore(Message<?> message) {
        for (ConsumerInterceptor interceptor : interceptors) {
            try {
                if (!interceptor.beforeConsume(message)) {
                    LOG.debug("ConsumerInterceptor {} aborted consume: topic={}",
                        interceptor.name(), message.getTopic());
                    return false;
                }
            } catch (RuntimeException ex) {
                LOG.warn("ConsumerInterceptor {} beforeConsume threw exception: {}",
                    interceptor.name(), ex.getMessage(), ex);
                notifyException(message, ex, InvokeTiming.BEFORE);
            }
        }
        return true;
    }

    /**
     * 执行 afterConsume 拦截器链。
     *
     * @param message 已消费消息
     * @param action 消费动作
     */
    public void applyAfter(Message<?> message, Action action) {
        for (ConsumerInterceptor interceptor : interceptors) {
            try {
                interceptor.afterConsume(message, action);
            } catch (RuntimeException ex) {
                LOG.warn("ConsumerInterceptor {} afterConsume threw exception: {}",
                    interceptor.name(), ex.getMessage(), ex);
                notifyException(message, ex, InvokeTiming.AFTER);
            }
        }
    }

    /**
     * 通知所有拦截器发生异常。
     *
     * @param message 消息
     * @param ex 异常
     * @param timing 触发时机（BEFORE/EXECUTING/AFTER）
     */
    public void notifyException(Message<?> message, Exception ex, InvokeTiming timing) {
        for (ConsumerInterceptor interceptor : interceptors) {
            try {
                interceptor.onException(message, ex, timing);
            } catch (Exception ignored) {
                // 拦截器异常不应影响主流程
            }
        }
    }
}
