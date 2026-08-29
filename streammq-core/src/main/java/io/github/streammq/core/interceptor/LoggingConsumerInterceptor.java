/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.interceptor;

import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.message.Message;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 日志消费者拦截器（默认实现）：在消费前/后/异常时输出 INFO/WARN/ERROR 日志， 全部放行（{@code beforeConsume} 始终返回 {@code
 * true}），仅用于审计与问题排查。
 *
 * <p>本类是 {@link ConsumerInterceptor} SPI 的默认空操作+日志记录实现， 业务方可注册自定义 {@link ConsumerInterceptor} Bean
 * 覆盖本默认（例如注入 traceId、限流、解密等）。 若同时存在多个同类型 Bean，Spring 依赖 {@link
 * org.springframework.core.annotation.Order} / {@link ConsumerInterceptor#order()} 决定执行顺序。
 *
 * <p>日志格式：
 *
 * <ul>
 *   <li>{@code beforeConsume}：{@code [ConsumerLog] beforeConsume topic={}, keys={}, group={}}
 *   <li>{@code afterConsume}：{@code [ConsumerLog] afterConsume topic={}, action={}, group={},
 *       reconsumeTimes={}}
 *   <li>{@code onException}：ERROR 级别，含异常堆栈
 * </ul>
 *
 * <p>执行顺序为 1000（低优先级，最后执行），确保日志记录不干扰其他拦截器的主流程。
 *
 * <p>线程安全：本类为无状态单例，可安全在多线程间共享。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
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
