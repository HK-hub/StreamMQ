/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.interceptor;

import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 日志生产者拦截器（默认实现）：在发送前/后/异常时输出 INFO/WARN 日志。
 *
 * <p>v0.1.0 起作为 {@link ProducerInterceptor} SPI 的默认实现存在，使得未配置任何 {@code ProducerInterceptor} Bean
 * 时拦截器链仍有可观测输出。业务方可注册自定义 Bean（如分布式追踪上下文注入）覆盖本默认。
 *
 * <p>本类为单例，线程安全；日志通过 SLF4J 输出。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class LoggingProducerInterceptor implements ProducerInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingProducerInterceptor.class);

    /** 共享单例。 */
    public static final LoggingProducerInterceptor INSTANCE = new LoggingProducerInterceptor();

    private LoggingProducerInterceptor() {}

    @Override
    public Message<?> beforeSend(Message<?> message) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "beforeSend: topic={}, tag={}, keys={}, bornHost={}",
                    message.getTopic(),
                    message.getTag(),
                    message.getKeys(),
                    message.getBornHost());
        }
        return message;
    }

    @Override
    public void afterSend(Message<?> message, SendResult result) {
        if (result.isSuccess()) {
            LOG.info(
                    "send success: topic={}, messageId={}",
                    message.getTopic(),
                    result.getMessageId());
        } else {
            LOG.warn(
                    "send failed: topic={}, messageId={}, error={}",
                    message.getTopic(),
                    result.getMessageId(),
                    result.getErrorMessage());
        }
    }

    @Override
    public void onException(Message<?> message, Exception exception, InvokeTiming timing) {
        LOG.error(
                "send exception: topic={}, keys={}, timing={}, error={}",
                message.getTopic(),
                message.getKeys(),
                timing,
                exception.getMessage(),
                exception);
    }

    @Override
    public String name() {
        return "logging";
    }
}
