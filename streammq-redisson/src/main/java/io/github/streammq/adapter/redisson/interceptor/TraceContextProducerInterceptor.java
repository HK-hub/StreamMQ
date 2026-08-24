package io.github.streammq.adapter.redisson.interceptor;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.interceptor.ProducerInterceptor;
import io.github.streammq.core.interceptor.TraceCollector;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 追踪上下文生产者拦截器。
 *
 * <p>在发送前后注入/上报追踪上下文：
 *
 * <ul>
 *   <li>{@link #beforeSend}: 如果消息没有 traceId，生成 UUID 作为 traceId 放入 userProperties
 *   <li>{@link #afterSend}: 记录发送结果到 {@link TraceCollector}（如果启用）
 * </ul>
 *
 * <p>执行顺序为 0（最先执行），确保后续拦截器可读取到 traceId。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class TraceContextProducerInterceptor implements ProducerInterceptor {

    private static final Logger LOG =
            LoggerFactory.getLogger(TraceContextProducerInterceptor.class);

    /** userProperties 中 traceId 的键名 */
    public static final String TRACE_ID_KEY = StreamMQConstants.TRACE_ATTR_TRACE_ID;

    private final TraceCollector traceCollector;

    /** 线程局部发送起始时间戳，用于计算发送耗时 */
    private final ThreadLocal<Long> sendStartTimestamp = new ThreadLocal<>();

    /**
     * 构造拦截器。
     *
     * @param traceCollector 追踪收集器
     */
    public TraceContextProducerInterceptor(TraceCollector traceCollector) {
        this.traceCollector = Objects.requireNonNull(traceCollector, "traceCollector");
    }

    @Override
    public boolean beforeSend(Message<?> message) {
        Objects.requireNonNull(message, "message");
        // 如果消息没有 traceId，生成 UUID 作为 traceId
        Map<String, String> userProps = message.getUserProperties();
        if (Objects.isNull(userProps.get(TRACE_ID_KEY))) {
            message.putUserProperty(TRACE_ID_KEY, UUID.randomUUID().toString());
        }
        sendStartTimestamp.set(System.currentTimeMillis());
        return true;
    }

    @Override
    public void afterSend(Message<?> message, SendResult result) {
        Long start = sendStartTimestamp.get();
        sendStartTimestamp.remove();
        if (!traceCollector.isEnabled()) {
            return;
        }
        long duration = Objects.nonNull(start) ? System.currentTimeMillis() - start : 0L;
        String traceId = message.getUserProperties().get(TRACE_ID_KEY);
        Map<String, String> attributes = new HashMap<>(4);
        if (Objects.nonNull(result.getErrorMessage())) {
            attributes.put("errorMessage", result.getErrorMessage());
        }
        if (Objects.nonNull(result.getRegionId())) {
            attributes.put("regionId", result.getRegionId());
        }
        if (Objects.nonNull(message.getKeys())) {
            attributes.put("keys", message.getKeys());
        }
        try {
            TraceCollector.SendTraceContext ctx =
                    new TraceCollector.SendTraceContext(
                            message.getTopic(),
                            message.getTag(),
                            result.getMessageId(),
                            null,
                            message.getBornTimestamp(),
                            result.isSuccess(),
                            duration,
                            traceId,
                            attributes);
            traceCollector.recordSend(ctx);
        } catch (Exception ex) {
            // 追踪上报失败不影响主流程
            LOG.warn(
                    "记录发送追踪失败: topic={}, messageId={}",
                    message.getTopic(),
                    result.getMessageId(),
                    ex);
        }
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public String name() {
        return "trace-context-producer";
    }
}
