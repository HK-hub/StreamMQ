package io.github.streammq.adapter.redisson.interceptor;

import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.interceptor.ProducerInterceptor;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 生产者日志拦截器。
 *
 * <p>在发送前后记录 INFO 级别日志，发送异常时记录 ERROR 级别日志，
 * 适用于消息发送的审计与问题排查场景。
 *
 * <p>日志格式：
 * <ul>
 *   <li>{@code beforeSend}: {@code [ProducerLog] beforeSend topic={}, keys={}}</li>
 *   <li>{@code afterSend}: {@code [ProducerLog] afterSend topic={}, msgId={}, status={}}</li>
 *   <li>{@code onException}: ERROR 级别，含异常堆栈</li>
 * </ul>
 *
 * <p>执行顺序为 1000（低优先级，最后执行），确保日志记录不干扰其他拦截器的主流程。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class LoggingProducerInterceptor implements ProducerInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingProducerInterceptor.class);

    @Override
    public boolean beforeSend(Message<?> message) {
        Objects.requireNonNull(message, "message");
        LOG.info("[ProducerLog] beforeSend topic={}, keys={}", message.getTopic(), message.getKeys());
        return true;
    }

    @Override
    public void afterSend(Message<?> message, SendResult result) {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(result, "result");
        LOG.info("[ProducerLog] afterSend topic={}, msgId={}, status={}",
            message.getTopic(), result.getMessageId(), result.getSendStatus());
    }

    @Override
    public void onException(Message<?> message, Exception exception, InvokeTiming timing) {
        LOG.error("[ProducerLog] onException topic={}, keys={}, timing={}",
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
        return "logging-producer";
    }
}
