/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.interceptor;

import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.interceptor.ConsumerInterceptor;
import io.github.streammq.core.message.Message;
import java.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 消费者审计拦截器示例。
 *
 * <p>演示在消费后记录审计日志，包括消息内容、消费时间、结果等。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class AuditConsumerInterceptor implements ConsumerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(AuditConsumerInterceptor.class);

    @Override
    public boolean beforeConsume(Message<?> message, ConsumeContext context) {
        return true;
    }

    @Override
    public void afterConsume(Message<?> message, ConsumeAction action, ConsumeContext context) {
        String auditLog =
                String.format(
                        "[AUDIT] topic=%s, tag=%s, keys=%s, body=%s, action=%s, group=%s,"
                                + " reconsumeTimes=%d, time=%s",
                        message.getTopic(),
                        message.getTag(),
                        message.getKeys(),
                        message.getBody(),
                        action,
                        context != null ? context.consumerGroup() : null,
                        context != null ? context.reconsumeTimes() : 0,
                        LocalDateTime.now());
        log.info(auditLog);
    }

    @Override
    public int order() {
        return 2;
    }
}
