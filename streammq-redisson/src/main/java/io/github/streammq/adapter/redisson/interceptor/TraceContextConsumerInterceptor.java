package io.github.streammq.adapter.redisson.interceptor;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.interceptor.ConsumerInterceptor;
import io.github.streammq.core.interceptor.TraceCollector;
import io.github.streammq.core.message.Message;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * 追踪上下文消费者拦截器。
 *
 * <p>在消费前后处理追踪上下文：
 *
 * <ul>
 *   <li>{@link #beforeConsume}: 从消息 userProperties 读取 traceId，放入 MDC
 *   <li>{@link #afterConsume}: 从 MDC 移除 traceId，记录消费结果到 {@link TraceCollector}
 * </ul>
 *
 * <p>执行顺序为 0（最先执行），确保消费逻辑可在 MDC 中读取 traceId 输出结构化日志。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class TraceContextConsumerInterceptor implements ConsumerInterceptor {

    private static final Logger LOG =
            LoggerFactory.getLogger(TraceContextConsumerInterceptor.class);

    /** userProperties 中 traceId 的键名 */
    public static final String TRACE_ID_KEY = StreamMQConstants.TRACE_ATTR_TRACE_ID;

    /** MDC 中 traceId 的键名 */
    public static final String MDC_TRACE_ID_KEY = StreamMQConstants.TRACE_ATTR_TRACE_ID;

    /** 线程局部消费起始时间戳，用于计算消费耗时 */
    private final ThreadLocal<Long> consumeStartTimestamp = new ThreadLocal<>();

    private final TraceCollector traceCollector;

    /**
     * 构造拦截器。
     *
     * @param traceCollector 追踪收集器
     */
    public TraceContextConsumerInterceptor(TraceCollector traceCollector) {
        this.traceCollector = Objects.requireNonNull(traceCollector, "traceCollector");
    }

    @Override
    public boolean beforeConsume(Message<?> message, ConsumeContext context) {
        Objects.requireNonNull(message, "message");
        String traceId = message.getUserProperties().get(TRACE_ID_KEY);
        if (Objects.nonNull(traceId)) {
            MDC.put(MDC_TRACE_ID_KEY, traceId);
        }
        consumeStartTimestamp.set(System.currentTimeMillis());
        return true;
    }

    @Override
    public void afterConsume(Message<?> message, ConsumeAction action, ConsumeContext context) {
        Long start = consumeStartTimestamp.get();
        consumeStartTimestamp.remove();
        String traceId = MDC.get(MDC_TRACE_ID_KEY);
        // 无论后续是否成功上报，先移除 MDC 中的 traceId
        MDC.remove(MDC_TRACE_ID_KEY);

        if (!traceCollector.isEnabled()) {
            return;
        }
        long duration = Objects.nonNull(start) ? System.currentTimeMillis() - start : 0L;
        boolean success = action == ConsumeAction.SUCCESS;
        Map<String, String> attributes = new HashMap<>(2);
        attributes.put(StreamMQConstants.TRACE_ATTR_ACTION, action.name());

        try {
            TraceCollector.ConsumeTraceContext ctx =
                    new TraceCollector.ConsumeTraceContext(
                            message.getTopic(),
                            message.getTag(),
                            message.getMessageId(),
                            Objects.nonNull(context) ? context.consumerGroup() : null,
                            Objects.nonNull(context) ? context.consumerName() : null,
                            Objects.nonNull(context)
                                    ? context.reconsumeTimes()
                                    : message.getReconsumeTimes(),
                            success,
                            duration,
                            traceId,
                            attributes);
            traceCollector.recordConsume(ctx);
        } catch (Exception ex) {
            // 追踪上报失败不影响主流程
            LOG.warn(
                    "记录消费追踪失败: topic={}, messageId={}",
                    message.getTopic(),
                    message.getMessageId(),
                    ex);
        }
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public String name() {
        return "trace-context-consumer";
    }
}
