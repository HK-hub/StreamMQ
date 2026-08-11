package io.github.streammq.adapter.redisson.interceptor;

import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.interceptor.ConsumerInterceptor;
import io.github.streammq.core.message.Message;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消费者日志拦截器。
 *
 * <p>在消费前后记录 INFO 级别日志，消费异常时记录 ERROR 级别日志， 适用于消息消费的审计与问题排查场景。
 *
 * <p>日志格式：
 *
 * <ul>
 *   <li>{@code beforeConsume}: {@code [ConsumerLog] beforeConsume topic={}, keys={}}
 *   <li>{@code afterConsume}: {@code [ConsumerLog] afterConsume topic={}, action={}}
 *   <li>{@code onException}: ERROR 级别，含异常堆栈
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
    public boolean beforeConsume(Message<?> message, ConsumeContext context) {
        Objects.requireNonNull(message, "message");
        LOG.info(
                "[ConsumerLog] beforeConsume topic={}, keys={}, group={}",
                message.getTopic(),
                message.getKeys(),
                Objects.nonNull(context) ? context.consumerGroup() : null);
        return true;
    }

    @Override
    public void afterConsume(Message<?> message, ConsumeAction action, ConsumeContext context) {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(action, "action");
        LOG.info(
                "[ConsumerLog] afterConsume topic={}, action={}, group={}, reconsumeTimes={}",
                message.getTopic(),
                action,
                Objects.nonNull(context) ? context.consumerGroup() : null,
                Objects.nonNull(context) ? context.reconsumeTimes() : 0);
    }

    @Override
    public void onException(
            Message<?> message, Exception exception, InvokeTiming timing, ConsumeContext context) {
        LOG.error(
                "[ConsumerLog] onException topic={}, keys={}, timing={}, group={}",
                Objects.nonNull(message) ? message.getTopic() : null,
                Objects.nonNull(message) ? message.getKeys() : null,
                timing,
                Objects.nonNull(context) ? context.consumerGroup() : null,
                exception);
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
