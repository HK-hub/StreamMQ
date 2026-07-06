package io.github.streammq.adapter.redisson.trace;

import io.github.streammq.core.interceptor.TraceCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 SLF4J 的链路追踪收集器。
 *
 * <p>通过 SLF4J 日志门面输出追踪信息：
 * <ul>
 *   <li>成功事件使用 DEBUG 级别输出详细追踪信息</li>
 *   <li>失败事件使用 INFO 级别输出（确保可见性）</li>
 * </ul>
 *
 * <p>适用于开发调试与轻量级生产追踪场景。如需对接 APM 系统，
 * 请实现自定义 {@link TraceCollector}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class Slf4jTraceCollector implements TraceCollector {

    private static final Logger LOG = LoggerFactory.getLogger(Slf4jTraceCollector.class);

    /** 用户属性中的 traceId 键名 */
    public static final String TRACE_ID_KEY = "traceId";

    @Override
    public void recordSend(SendTraceContext context) {
        if (context == null) {
            return;
        }
        if (context.success()) {
            LOG.debug("发送追踪: topic={}, messageId={}, producerGroup={}, success={}, durationMs={}, traceId={}",
                    context.topic(), context.messageId(), context.producerGroup(),
                    context.success(), context.durationMillis(), context.traceId());
        } else {
            LOG.info("发送追踪(失败): topic={}, messageId={}, producerGroup={}, success={}, durationMs={}, traceId={}",
                    context.topic(), context.messageId(), context.producerGroup(),
                    context.success(), context.durationMillis(), context.traceId());
        }
    }

    @Override
    public void recordConsume(ConsumeTraceContext context) {
        if (context == null) {
            return;
        }
        if (context.success()) {
            LOG.debug("消费追踪: topic={}, messageId={}, consumerGroup={}, reconsumeTimes={}, success={}, durationMs={}, traceId={}",
                    context.topic(), context.messageId(), context.consumerGroup(),
                    context.reconsumeTimes(), context.success(), context.durationMillis(), context.traceId());
        } else {
            LOG.info("消费追踪(失败): topic={}, messageId={}, consumerGroup={}, reconsumeTimes={}, success={}, durationMs={}, traceId={}",
                    context.topic(), context.messageId(), context.consumerGroup(),
                    context.reconsumeTimes(), context.success(), context.durationMillis(), context.traceId());
        }
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public String name() {
        return "slf4j";
    }
}
