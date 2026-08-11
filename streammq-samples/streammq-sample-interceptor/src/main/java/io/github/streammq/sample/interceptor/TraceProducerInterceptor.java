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
 * <p>演示在发送前自动注入 traceId，用于全链路追踪。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class TraceProducerInterceptor implements ProducerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(TraceProducerInterceptor.class);

    @Override
    public boolean beforeSend(Message<?> message) {
        String traceId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        message.putUserProperty("traceId", traceId);
        message.putUserProperty("spanId", "1");
        log.debug(
                "Trace producer interceptor injected: traceId={}, topic={}",
                traceId,
                message.getTopic());
        return true;
    }

    @Override
    public void afterSend(Message<?> message, SendResult result) {
        String traceId = message.getUserProperties().get("traceId");
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
