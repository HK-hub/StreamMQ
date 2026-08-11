package io.github.streammq.sample.interceptor;

import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.interceptor.ConsumerInterceptor;
import io.github.streammq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 消费者追踪拦截器示例。
 *
 * <p>演示在消费前后记录追踪日志，与生产者拦截器配合实现全链路追踪。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class TraceConsumerInterceptor implements ConsumerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(TraceConsumerInterceptor.class);

    @Override
    public boolean beforeConsume(Message<?> message, ConsumeContext context) {
        String traceId = message.getUserProperties().get("traceId");
        log.info(
                "Trace consumer interceptor beforeConsume: traceId={}, topic={}, tag={}, keys={},"
                        + " group={}",
                traceId,
                message.getTopic(),
                message.getTag(),
                message.getKeys(),
                context != null ? context.consumerGroup() : null);
        return true;
    }

    @Override
    public void afterConsume(Message<?> message, ConsumeAction action, ConsumeContext context) {
        String traceId = message.getUserProperties().get("traceId");
        log.info(
                "Trace consumer interceptor afterConsume: traceId={}, topic={}, action={},"
                        + " group={}, reconsumeTimes={}",
                traceId,
                message.getTopic(),
                action,
                context != null ? context.consumerGroup() : null,
                context != null ? context.reconsumeTimes() : 0);
    }

    @Override
    public int order() {
        return 1;
    }
}
