/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.tracing;

import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.util.StringUtils;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.TraceStateBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamMQ 追踪核心门面，提供 OpenTelemetry 原生集成与 W3C TraceContext 上下文传播。
 *
 * <p>核心能力：
 *
 * <ul>
 *   <li>生产者 Span 创建与上下文注入：{@link #injectProducerSpan(Message)} 创建名为 {@value #SPAN_PRODUCER_SEND} 的
 *       PRODUCER Span，并将 W3C {@code traceparent} 头 注入消息系统属性
 *   <li>消费者 Span 创建与上下文提取：{@link #startConsumerSpan(Message, ConsumeContext)} 从消息属性提取 {@code
 *       traceparent}，创建名为 {@value #SPAN_CONSUMER_CONSUME} 的 CONSUMER Span 并建立远程父级关系
 *   <li>Span 结束与状态记录：{@link #endSpan(Span, boolean)} / {@link #endSpan(Span, boolean, String)}
 * </ul>
 *
 * <p>上下文传播遵循 W3C TraceContext 标准（<a href="https://www.w3.org/TR/trace-context/">W3C
 * TraceContext</a>）， 通过消息系统属性 {@code traceparent} / {@code tracestate} 在生产者与消费者之间串联链路。 当
 * OpenTelemetry 为 no-op 实现时（如未配置 SDK），Span 上下文无效，注入将自动跳过，实现优雅降级。
 *
 * <p>Span 命名约定：
 *
 * <ul>
 *   <li>生产者发送：{@value #SPAN_PRODUCER_SEND}
 *   <li>消费者消费：{@value #SPAN_CONSUMER_CONSUME}
 *   <li>重试调度：{@value #SPAN_SCHEDULER_RETRY}
 *   <li>延时调度：{@value #SPAN_SCHEDULER_DELAY}
 *   <li>死信路由：{@value #SPAN_DLQ_ROUTE}
 * </ul>
 *
 * <p>线程安全：{@link Tracer} 与 {@link OpenTelemetry} 均为线程安全；生产者 Span 通过 ThreadLocal 存储，
 * 适配单线程顺序发送场景。跨线程发送应由调用方在外部管理 Span 生命周期。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMQTracing {

    private static final Logger log = LoggerFactory.getLogger(StreamMQTracing.class);

    /** Tracer Instrumentation Scope 名称 */
    private static final String INSTRUMENTATION_SCOPE = "io.github.streammq";

    /** 生产者发送 Span 名称 */
    public static final String SPAN_PRODUCER_SEND = "streammq.producer.send";

    /** Span 注册表 / 启动时间戳映射的容量上限（超出按插入序淘汰，防止泄漏） */
    public static final int SPAN_REGISTRY_CAPACITY = 4_096;

    /** 消费者消费 Span 名称 */
    public static final String SPAN_CONSUMER_CONSUME = "streammq.consumer.consume";

    /** 重试调度 Span 名称 */
    public static final String SPAN_SCHEDULER_RETRY = "streammq.scheduler.retry";

    /** 延时调度 Span 名称 */
    public static final String SPAN_SCHEDULER_DELAY = "streammq.scheduler.delay";

    /** 死信路由 Span 名称 */
    public static final String SPAN_DLQ_ROUTE = "streammq.dlq.route";

    /** W3C traceparent 属性键 */
    public static final String TRACEPARENT_KEY = "traceparent";

    /** W3C tracestate 属性键 */
    public static final String TRACESTATE_KEY = "tracestate";

    /** Span 属性键 - Topic */
    public static final String ATTR_TOPIC = "streammq.topic";

    /** Span 属性键 - Tag */
    public static final String ATTR_TAG = "streammq.tag";

    /** Span 属性键 - 消息 ID */
    public static final String ATTR_MESSAGE_ID = "streammq.message.id";

    /** Span 属性键 - 是否成功 */
    public static final String ATTR_SUCCESS = "streammq.success";

    /** Span 属性键 - 耗时（毫秒） */
    public static final String ATTR_DURATION = "streammq.duration";

    /** Span 属性键 - 重试次数 */
    public static final String ATTR_RECONSUME_TIMES = "streammq.reconsume.times";

    /** Span 属性键 - 消费者组 */
    public static final String ATTR_CONSUMER_GROUP = "streammq.consumer.group";

    /** OpenTelemetry 实例 */
    private final OpenTelemetry openTelemetry;

    /** Tracer 实例 */
    private final Tracer tracer;

    /** 生产者 Span 注册表：按消息引用配对，跨线程安全，容量有界防泄漏 */
    private final BoundedSpanRegistry producerSpans =
            new BoundedSpanRegistry(SPAN_REGISTRY_CAPACITY);

    /** Span 启动纳秒时间戳，用于计算 duration 属性（容量有界，超限按插入序淘汰） */
    private final Map<Span, Long> spanStartNanos =
            Collections.synchronizedMap(new IdentityHashMap<>());

    /**
     * 使用指定 OpenTelemetry 实例构造。
     *
     * @param openTelemetry OpenTelemetry 实例，为 null 时回退到 no-op 实例
     */
    public StreamMQTracing(OpenTelemetry openTelemetry) {
        this.openTelemetry = Objects.isNull(openTelemetry) ? OpenTelemetry.noop() : openTelemetry;
        this.tracer = this.openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
        log.debug("StreamMQTracing 已初始化，tracer scope={}", INSTRUMENTATION_SCOPE);
    }

    // ===================== 生产者 Span =====================

    /**
     * 创建生产者 Span，将 W3C TraceContext 注入消息属性，并按消息引用登记 Span。
     *
     * <p>在 {@code ProducerInterceptor.beforeSend} 中调用。创建 PRODUCER 类型的 Span （名称 {@value
     * #SPAN_PRODUCER_SEND}），设置 Topic / Tag / 消息 ID 等属性， 并将 W3C {@code traceparent} / {@code
     * tracestate} 写入派生消息的用户属性。
     *
     * <p><b>跨线程配对：</b>Span 以返回的派生消息实例为键存入有界注册表；发送完成后在 {@code afterSend} / {@code onException}
     * 回调（可能运行于其他线程）中通过 {@link #endProducerSpan(Message, boolean, String)} 按同一消息引用结束。异步发送场景下
     * ThreadLocal 配对会失效，因此不使用 ThreadLocal。
     *
     * @param message 待发送消息
     * @return 携带追踪上下文的派生消息（注入失败时返回原消息）
     */
    public Message<?> injectProducerSpan(Message<?> message) {
        if (Objects.isNull(message)) {
            return message;
        }
        try {
            Span span =
                    tracer.spanBuilder(SPAN_PRODUCER_SEND)
                            .setSpanKind(SpanKind.PRODUCER)
                            .startSpan();
            recordStart(span);
            setProducerAttributes(span, message);
            Message<?> enriched = injectTraceContext(span, message);
            producerSpans.track(enriched, span);
            return enriched;
        } catch (Exception ex) {
            log.warn("注入生产者 Span 失败，降级跳过追踪: {}", ex.getMessage());
            return message;
        }
    }

    /**
     * 结束与指定消息配对的生产者 Span（按消息引用查找，跨线程安全）。
     *
     * <p>在 {@code ProducerInterceptor.afterSend} / {@code onException} 中调用。未找到配对 Span 时
     * 静默跳过；无论成功与否都会从注册表移除条目，保证幂等且不泄漏。
     *
     * @param message beforeSend 返回的派生消息引用
     * @param success 是否发送成功
     * @param errorMessage 失败时的错误描述，成功时可为 null
     */
    public void endProducerSpan(Message<?> message, boolean success, String errorMessage) {
        if (Objects.isNull(message)) {
            return;
        }
        Span span = producerSpans.remove(message);
        if (Objects.nonNull(span)) {
            endSpan(span, success, errorMessage);
        }
    }

    /** 便捷重载：以默认错误信息结束与消息配对的生产者 Span。 */
    public void endProducerSpan(Message<?> message, boolean success) {
        endProducerSpan(message, success, null);
    }

    // ===================== 消费者 Span =====================

    /**
     * 从消息属性提取追踪上下文并创建消费者 Span。
     *
     * <p>在 {@code ConsumerInterceptor.beforeConsume} 中调用。解析消息属性中的 W3C {@code traceparent}，创建
     * CONSUMER 类型的 Span（名称 {@value #SPAN_CONSUMER_CONSUME}）， 以提取的远程上下文作为父级，设置 Topic / 消费者组 /
     * 重试次数等属性。
     *
     * @param message 待消费消息
     * @param context 消费上下文
     * @return 已启动的 Span，需在消费完成后调用 {@link #endSpan(Span, boolean)} 结束
     */
    public Span startConsumerSpan(Message<?> message, ConsumeContext context) {
        if (Objects.isNull(message)) {
            return Span.getInvalid();
        }
        try {
            Context parentContext = extractTraceContext(message);
            Span span =
                    tracer.spanBuilder(SPAN_CONSUMER_CONSUME)
                            .setParent(parentContext)
                            .setSpanKind(SpanKind.CONSUMER)
                            .startSpan();
            recordStart(span);
            setConsumerAttributes(span, message, context);
            return span;
        } catch (Exception ex) {
            log.warn("启动消费者 Span 失败，降级返回无效 Span: {}", ex.getMessage());
            return Span.getInvalid();
        }
    }

    // ===================== Span 结束 =====================

    /**
     * 结束 Span 并记录成功/失败状态。
     *
     * @param span 待结束的 Span，为 null 时安全跳过
     * @param success 是否成功
     */
    public void endSpan(Span span, boolean success) {
        endSpan(span, success, null);
    }

    /**
     * 结束 Span 并记录成功/失败状态与错误信息。
     *
     * @param span 待结束的 Span，为 null 时安全跳过
     * @param success 是否成功
     * @param errorMessage 失败时的错误描述，成功时可为 null
     */
    public void endSpan(Span span, boolean success, String errorMessage) {
        if (Objects.isNull(span)) {
            return;
        }
        try {
            span.setAttribute(ATTR_SUCCESS, success);
            recordDuration(span);
            if (success) {
                span.setStatus(StatusCode.OK);
            } else {
                span.setStatus(
                        StatusCode.ERROR,
                        StringUtils.isNotEmpty(errorMessage) ? errorMessage : "执行失败");
            }
            span.end();
        } catch (Exception ex) {
            log.warn("结束 Span 失败: {}", ex.getMessage());
        } finally {
            spanStartNanos.remove(span);
        }
    }

    // ===================== 上下文注入 / 提取 =====================

    /**
     * 将 Span 的 W3C TraceContext 注入消息用户属性（返回派生的不可变实例）。
     *
     * <p>使用 {@code addUserProperty} 而非系统属性，因为 {@link
     * io.github.streammq.core.converter.MessageConverter} 在 Redis Stream 往返时 会将属性合并存储，反序列化后统一写入
     * {@code userProperties}。
     *
     * @param span 生产者 Span
     * @param message 消息载体
     * @return 注入追踪上下文后的派生消息
     */
    private Message<?> injectTraceContext(Span span, Message<?> message) {
        SpanContext ctx = span.getSpanContext();
        if (!ctx.isValid()) {
            return message;
        }
        String traceparent =
                "00-"
                        + ctx.getTraceId()
                        + "-"
                        + ctx.getSpanId()
                        + "-"
                        + ctx.getTraceFlags().asHex();
        Message<?> enriched = message.addUserProperty(TRACEPARENT_KEY, traceparent);
        TraceState traceState = ctx.getTraceState();
        if (Objects.nonNull(traceState) && !traceState.isEmpty()) {
            enriched = enriched.addUserProperty(TRACESTATE_KEY, serializeTraceState(traceState));
        }
        return enriched;
    }

    /**
     * 从消息属性提取 W3C TraceContext 并构造父级 Context。
     *
     * <p>查找顺序：先查用户属性（{@code getUserProperties}，Redis Stream 往返后的存储位置）， 再查系统属性（{@code
     * getProperties}，未经序列化的内存直通场景）。
     *
     * @param message 消息载体
     * @return 包含远程父级 Span 的 Context，无追踪属性时返回当前 Context
     */
    private Context extractTraceContext(Message<?> message) {
        String traceparent = message.getUserProperties().get(TRACEPARENT_KEY);
        String tracestate = message.getUserProperties().get(TRACESTATE_KEY);
        if (StringUtils.isEmpty(traceparent)) {
            traceparent = message.getProperties().get(TRACEPARENT_KEY);
            tracestate = message.getProperties().get(TRACESTATE_KEY);
        }
        if (StringUtils.isEmpty(traceparent)) {
            return Context.current();
        }
        SpanContext remoteParent = parseTraceparent(traceparent, tracestate);
        if (Objects.isNull(remoteParent) || !remoteParent.isValid()) {
            return Context.current();
        }
        return Span.wrap(remoteParent).storeInContext(Context.current());
    }

    /**
     * 解析 W3C traceparent 字符串为远程 SpanContext。
     *
     * @param traceparent traceparent 字符串，格式 {@code version-traceId-spanId-flags}
     * @param tracestate tracestate 字符串，可为 null
     * @return 远程 SpanContext，格式不合法时返回 null
     */
    private static SpanContext parseTraceparent(String traceparent, String tracestate) {
        String[] parts = traceparent.split("-", 4);
        if (parts.length != 4) {
            return null;
        }
        String traceId = parts[1];
        String spanId = parts[2];
        String flags = parts[3];
        if (traceId.length() != 32 || spanId.length() != 16 || flags.length() != 2) {
            return null;
        }
        TraceFlags traceFlags = TraceFlags.fromHex(flags, 0);
        TraceState state =
                StringUtils.isEmpty(tracestate)
                        ? TraceState.getDefault()
                        : parseTraceState(tracestate);
        return SpanContext.createFromRemoteParent(traceId, spanId, traceFlags, state);
    }

    /**
     * 将 {@link TraceState} 序列化为 W3C tracestate 字符串（格式 {@code key1=value1,key2=value2}）。
     *
     * <p>OpenTelemetry 1.40.0 的 {@link TraceState} 接口未直接提供序列化方法， 此处通过 {@link TraceState#asMap()}
     * 获取键值对后手动拼接。
     *
     * @param traceState 追踪状态
     * @return 序列化字符串，为空时返回空字符串
     */
    private static String serializeTraceState(TraceState traceState) {
        if (traceState.isEmpty()) {
            return "";
        }
        Map<String, String> entries = traceState.asMap();
        if (entries.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            if (!sb.isEmpty()) {
                sb.append(',');
            }
            sb.append(entry.getKey()).append('=').append(entry.getValue());
        }
        return sb.toString();
    }

    /**
     * 解析 W3C tracestate 字符串为 {@link TraceState}。
     *
     * <p>OpenTelemetry 1.40.0 的 {@link TraceState} 接口未提供静态 {@code parse} 方法， 此处通过 {@link
     * TraceStateBuilder} 手动构建。格式不合法的条目将被跳过。
     *
     * @param tracestate tracestate 字符串，格式 {@code key1=value1,key2=value2}
     * @return 解析后的 TraceState，无合法条目时返回空 TraceState
     */
    private static TraceState parseTraceState(String tracestate) {
        TraceStateBuilder builder = TraceState.builder();
        String[] entries = tracestate.split(",");
        for (String entry : entries) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int idx = trimmed.indexOf('=');
            if (idx <= 0 || idx == trimmed.length() - 1) {
                continue;
            }
            builder.put(trimmed.substring(0, idx), trimmed.substring(idx + 1));
        }
        return builder.build();
    }

    // ===================== 访问器 =====================

    /**
     * 返回当前 OpenTelemetry 实例。
     *
     * @return OpenTelemetry 实例
     */
    public OpenTelemetry getOpenTelemetry() {
        return openTelemetry;
    }

    /**
     * 返回当前 Tracer 实例。
     *
     * @return Tracer 实例
     */
    public Tracer getTracer() {
        return tracer;
    }

    // ===================== 内部工具 =====================

    /** 记录 Span 启动纳秒时间。 */
    private void recordStart(Span span) {
        synchronized (spanStartNanos) {
            if (spanStartNanos.size() >= SPAN_REGISTRY_CAPACITY) {
                var it = spanStartNanos.keySet().iterator();
                if (it.hasNext()) {
                    it.next();
                    it.remove();
                }
            }
            spanStartNanos.put(span, System.nanoTime());
        }
    }

    /** 计算 Span 耗时并设置 duration 属性。 */
    private void recordDuration(Span span) {
        Long start = spanStartNanos.get(span);
        if (Objects.isNull(start)) {
            return;
        }
        long durationMillis = Math.max(0L, (System.nanoTime() - start) / 1_000_000L);
        span.setAttribute(ATTR_DURATION, durationMillis);
    }

    /** 设置生产者 Span 属性。 */
    private void setProducerAttributes(Span span, Message<?> message) {
        span.setAttribute(ATTR_TOPIC, orDefault(message.getTopic(), ""));
        if (StringUtils.isNotEmpty(message.getTag())) {
            span.setAttribute(ATTR_TAG, message.getTag());
        }
        if (Objects.nonNull(message.getMessageId())) {
            span.setAttribute(ATTR_MESSAGE_ID, message.getMessageId().toString());
        }
    }

    /** 设置消费者 Span 属性。 */
    private void setConsumerAttributes(Span span, Message<?> message, ConsumeContext context) {
        span.setAttribute(ATTR_TOPIC, orDefault(message.getTopic(), ""));
        if (StringUtils.isNotEmpty(message.getTag())) {
            span.setAttribute(ATTR_TAG, message.getTag());
        }
        if (Objects.nonNull(message.getMessageId())) {
            span.setAttribute(ATTR_MESSAGE_ID, message.getMessageId().toString());
        }
        if (Objects.nonNull(context)) {
            if (StringUtils.isNotEmpty(context.consumerGroup())) {
                span.setAttribute(ATTR_CONSUMER_GROUP, context.consumerGroup());
            }
            span.setAttribute(ATTR_RECONSUME_TIMES, context.reconsumeTimes());
        } else if (message.getReconsumeTimes() > 0) {
            span.setAttribute(ATTR_RECONSUME_TIMES, message.getReconsumeTimes());
        }
    }

    /**
     * 字符串为空时返回默认值。
     *
     * @param str 原始字符串
     * @param defaultStr 默认值
     * @return 非空则返回原值，否则返回默认值
     */
    private static String orDefault(String str, String defaultStr) {
        return StringUtils.isNotEmpty(str) ? str : defaultStr;
    }
}
