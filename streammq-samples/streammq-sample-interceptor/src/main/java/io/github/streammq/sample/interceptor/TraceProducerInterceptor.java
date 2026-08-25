/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.interceptor;

import io.github.streammq.core.interceptor.ProducerInterceptor;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 生产者追踪拦截器示例。
 *
 * <p>演示在发送前自动注入 traceId，用于全链路追踪。{@link Message} 为不可变对象， 拦截器通过返回派生实例完成属性注入。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class TraceProducerInterceptor implements ProducerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(TraceProducerInterceptor.class);

    /** 示例用固定 spanId 值 */
    private static final String SPAN_ID_VALUE = "1";

    @Override
    public Message<?> beforeSend(Message<?> message) {
        String traceId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        Message<?> enriched =
                message.addUserProperty(SampleConstants.PROP_TRACE_ID, traceId)
                        .addUserProperty(SampleConstants.PROP_SPAN_ID, SPAN_ID_VALUE);
        log.debug(
                "Trace producer interceptor injected: traceId={}, topic={}",
                traceId,
                message.getTopic());
        return enriched;
    }

    @Override
    public void afterSend(Message<?> message, SendResult result) {
        String traceId = message.getUserProperties().get(SampleConstants.PROP_TRACE_ID);
        log.info(
                "Trace producer interceptor afterSend: traceId={}, topic={}, success={}",
                traceId,
                message.getTopic(),
                result.isSuccess());
    }

    @Override
    public int order() {
        return 1;
    }
}
