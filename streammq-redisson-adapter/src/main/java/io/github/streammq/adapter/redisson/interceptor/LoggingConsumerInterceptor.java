package io.github.streammq.adapter.redisson.interceptor;

import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.interceptor.ConsumerInterceptor;
import io.github.streammq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 消费者日志拦截器。
 *
 * <p>在消费前后记录 INFO 级别日志，消费异常时记录 ERROR 级别日志，
 * 适用于消息消费的审计与问题排查场景。
 *
 * <p>日志格式：
 * <ul>
 *   <li>{@code beforeConsume}: {@code [ConsumerLog] beforeConsume topic={}, keys={}}</li>
 *   <li>{@code afterConsume}: {@code [ConsumerLog] afterConsume topic={}, action={}}</li>
 *   <li>{@code onException}: ERROR 级别，含异常堆栈</li>
 * </ul>
 *
 * <p>执行顺序为 1000（低优先级，最后执行），确保日志记录不干扰其他拦截器的主流程。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class LoggingConsumerInterceptor implements ConsumerInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingConsumerInterceptor.class);

    @Override
    public boolean beforeConsume(Message<?> message) {
        Objects.requireNonNull(message, "message");
        LOG.info("[ConsumerLog] beforeConsume topic={}, keys={}", message.getTopic(), message.getKeys());
        return true;
    }

    @Override
    public void afterConsume(Message<?> message, ConsumeAction action) {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(action, "action");
        LOG.info("[ConsumerLog] afterConsume topic={}, action={}", message.getTopic(), action);
    }

    @Override
    public void onException(Message<?> message, Exception exception, InvokeTiming timing) {
        LOG.error("[ConsumerLog] onException topic={}, keys={}, timing={}",
            message != null ? message.getTopic() : null,
            message != null ? message.getKeys() : null,
            timing, exception);
    }

    @Override
    public int order() {
        return 1000;
    }

    @Override
    public String name() {
        return "logging-consumer";
    }
}
