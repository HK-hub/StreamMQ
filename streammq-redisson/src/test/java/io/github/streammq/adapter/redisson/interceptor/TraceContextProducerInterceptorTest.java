package io.github.streammq.adapter.redisson.interceptor;

import io.github.streammq.adapter.redisson.trace.NoopTraceCollector;
import io.github.streammq.adapter.redisson.trace.Slf4jTraceCollector;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.message.SendStatus;
import io.github.streammq.core.interceptor.TraceCollector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link TraceContextProducerInterceptor} 单元测试，覆盖 traceId 生成、
 * 追踪上报、isEnabled 跳过、order/name 与异常场景。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("TraceContextProducerInterceptor 追踪上下文生产者拦截器测试")
class TraceContextProducerInterceptorTest {

    @Test
    @DisplayName("beforeSend 消息无 traceId 时生成 UUID 写入 userProperties")
    void beforeSendGeneratesTraceId() {
        TraceCollector collector = new NoopTraceCollector();
        TraceContextProducerInterceptor interceptor = new TraceContextProducerInterceptor(collector);

        Message<String> msg = new Message<>();
        msg.setBody("hello");

        boolean result = interceptor.beforeSend(msg);

        assertThat(result).isTrue();
        String traceId = msg.getUserProperties().get(TraceContextProducerInterceptor.TRACE_ID_KEY);
        assertThat(traceId).isNotNull().isNotEmpty();
        // 应为合法 UUID
        assertThat(UUID.fromString(traceId)).isNotNull();
    }

    @Test
    @DisplayName("beforeSend 已有 traceId 时保留原值")
    void beforeSendKeepsExistingTraceId() {
        TraceCollector collector = new NoopTraceCollector();
        TraceContextProducerInterceptor interceptor = new TraceContextProducerInterceptor(collector);

        Message<String> msg = new Message<>();
        msg.setBody("hello");
        msg.putUserProperty(TraceContextProducerInterceptor.TRACE_ID_KEY, "existing-trace-id");

        interceptor.beforeSend(msg);

        assertThat(msg.getUserProperties().get(TraceContextProducerInterceptor.TRACE_ID_KEY))
                .isEqualTo("existing-trace-id");
    }

    @Test
    @DisplayName("beforeSend message 为 null 抛出 NullPointerException")
    void beforeSendNullMessage() {
        TraceContextProducerInterceptor interceptor =
                new TraceContextProducerInterceptor(new NoopTraceCollector());
        assertThatThrownBy(() -> interceptor.beforeSend(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("message");
    }

    @Test
    @DisplayName("afterSend 启用 TraceCollector 时调用 recordSend")
    void afterSendRecordsWhenEnabled() {
        TraceCollector collector = mock(TraceCollector.class);
        when(collector.isEnabled()).thenReturn(true);
        TraceContextProducerInterceptor interceptor = new TraceContextProducerInterceptor(collector);

        Message<String> msg = new Message<>();
        msg.setTopic("topic-1");
        msg.setBody("hello");
        msg.putUserProperty(TraceContextProducerInterceptor.TRACE_ID_KEY, "trace-1");
        interceptor.beforeSend(msg);

        SendResult result = new SendResult(new MessageId("1-0"), "topic-1", "tag-1",
                SendStatus.SEND_OK, System.currentTimeMillis(), null, null);

        interceptor.afterSend(msg, result);

        ArgumentCaptor<TraceCollector.SendTraceContext> captor =
                ArgumentCaptor.forClass(TraceCollector.SendTraceContext.class);
        verify(collector, times(1)).recordSend(captor.capture());
        TraceCollector.SendTraceContext ctx = captor.getValue();
        assertThat(ctx.topic()).isEqualTo("topic-1");
        assertThat(ctx.messageId()).isEqualTo(new MessageId("1-0"));
        assertThat(ctx.success()).isTrue();
        assertThat(ctx.traceId()).isEqualTo("trace-1");
        assertThat(ctx.durationMillis()).isGreaterThanOrEqualTo(0L);
    }

    @Test
    @DisplayName("afterSend 未启用 TraceCollector 时不调用 recordSend")
    void afterSendSkipsWhenDisabled() {
        TraceCollector collector = mock(TraceCollector.class);
        when(collector.isEnabled()).thenReturn(false);
        TraceContextProducerInterceptor interceptor = new TraceContextProducerInterceptor(collector);

        Message<String> msg = new Message<>();
        msg.setBody("hello");
        interceptor.beforeSend(msg);

        SendResult result = new SendResult(new MessageId("1-0"), "topic-1", null,
                SendStatus.SEND_OK, System.currentTimeMillis(), null, null);
        interceptor.afterSend(msg, result);

        verify(collector, never()).recordSend(any(TraceCollector.SendTraceContext.class));
    }

    @Test
    @DisplayName("afterSend 失败结果包含 errorMessage 属性")
    void afterSendFailureIncludesError() {
        TraceCollector collector = mock(TraceCollector.class);
        when(collector.isEnabled()).thenReturn(true);
        TraceContextProducerInterceptor interceptor = new TraceContextProducerInterceptor(collector);

        Message<String> msg = new Message<>();
        msg.setTopic("topic-1");
        msg.setBody("hello");
        interceptor.beforeSend(msg);

        SendResult result = new SendResult(new MessageId("1-0"), "topic-1", null,
                SendStatus.SEND_FAILED, System.currentTimeMillis(), "region-1", "boom");
        interceptor.afterSend(msg, result);

        ArgumentCaptor<TraceCollector.SendTraceContext> captor =
                ArgumentCaptor.forClass(TraceCollector.SendTraceContext.class);
        verify(collector).recordSend(captor.capture());
        assertThat(captor.getValue().success()).isFalse();
        assertThat(captor.getValue().attributes())
                .containsEntry("errorMessage", "boom")
                .containsEntry("regionId", "region-1");
    }

    @Test
    @DisplayName("afterSend TraceCollector 抛异常时不传播")
    void afterSendToleratesCollectorException() {
        TraceCollector collector = mock(TraceCollector.class);
        when(collector.isEnabled()).thenReturn(true);
        doThrow(new RuntimeException("boom")).when(collector).recordSend(any(TraceCollector.SendTraceContext.class));
        TraceContextProducerInterceptor interceptor = new TraceContextProducerInterceptor(collector);

        Message<String> msg = new Message<>();
        msg.setBody("hello");
        interceptor.beforeSend(msg);

        SendResult result = new SendResult(new MessageId("1-0"), "topic-1", null,
                SendStatus.SEND_OK, System.currentTimeMillis(), null, null);
        // 不应抛异常
        interceptor.afterSend(msg, result);
    }

    @Test
    @DisplayName("未调用 beforeSend 直接 afterSend 不抛异常（duration=0）")
    void afterSendWithoutBeforeSend() {
        TraceCollector collector = mock(TraceCollector.class);
        when(collector.isEnabled()).thenReturn(true);
        TraceContextProducerInterceptor interceptor = new TraceContextProducerInterceptor(collector);

        Message<String> msg = new Message<>();
        msg.setTopic("topic-1");
        msg.setBody("hello");
        msg.putUserProperty(TraceContextProducerInterceptor.TRACE_ID_KEY, "trace-1");

        SendResult result = new SendResult(new MessageId("1-0"), "topic-1", null,
                SendStatus.SEND_OK, System.currentTimeMillis(), null, null);
        interceptor.afterSend(msg, result);

        ArgumentCaptor<TraceCollector.SendTraceContext> captor =
                ArgumentCaptor.forClass(TraceCollector.SendTraceContext.class);
        verify(collector).recordSend(captor.capture());
        assertThat(captor.getValue().durationMillis()).isZero();
    }

    @Test
    @DisplayName("构造 TraceCollector 为 null 抛出 NullPointerException")
    void constructNullCollector() {
        assertThatThrownBy(() -> new TraceContextProducerInterceptor(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("traceCollector");
    }

    @Test
    @DisplayName("order 返回 0")
    void order() {
        TraceContextProducerInterceptor interceptor =
                new TraceContextProducerInterceptor(new Slf4jTraceCollector());
        assertThat(interceptor.order()).isZero();
    }

    @Test
    @DisplayName("name 返回 trace-context-producer")
    void name() {
        TraceContextProducerInterceptor interceptor =
                new TraceContextProducerInterceptor(new NoopTraceCollector());
        assertThat(interceptor.name()).isEqualTo("trace-context-producer");
    }
}
