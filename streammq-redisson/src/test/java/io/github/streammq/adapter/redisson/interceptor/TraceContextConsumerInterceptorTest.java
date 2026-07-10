package io.github.streammq.adapter.redisson.interceptor;

import io.github.streammq.adapter.redisson.trace.NoopTraceCollector;
import io.github.streammq.adapter.redisson.trace.Slf4jTraceCollector;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.interceptor.TraceCollector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.MDC;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("TraceContextConsumerInterceptor 追踪上下文消费者拦截器测试")
class TraceContextConsumerInterceptorTest {

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    @DisplayName("beforeConsume 将 traceId 从 userProperties 写入 MDC")
    void beforeConsumePutsTraceIdIntoMdc() {
        TraceContextConsumerInterceptor interceptor =
                new TraceContextConsumerInterceptor(new NoopTraceCollector());

        Message<String> msg = new Message<>();
        msg.setBody("hello");
        msg.putUserProperty(TraceContextConsumerInterceptor.TRACE_ID_KEY, "trace-abc");

        boolean result = interceptor.beforeConsume(msg, null);

        assertThat(result).isTrue();
        assertThat(MDC.get(TraceContextConsumerInterceptor.MDC_TRACE_ID_KEY)).isEqualTo("trace-abc");
    }

    @Test
    @DisplayName("beforeConsume 消息无 traceId 时 MDC 不写入")
    void beforeConsumeNoTraceId() {
        TraceContextConsumerInterceptor interceptor =
                new TraceContextConsumerInterceptor(new NoopTraceCollector());

        Message<String> msg = new Message<>();
        msg.setBody("hello");

        interceptor.beforeConsume(msg, null);

        assertThat(MDC.get(TraceContextConsumerInterceptor.MDC_TRACE_ID_KEY)).isNull();
    }

    @Test
    @DisplayName("beforeConsume message 为 null 抛出 NullPointerException")
    void beforeConsumeNullMessage() {
        TraceContextConsumerInterceptor interceptor =
                new TraceContextConsumerInterceptor(new NoopTraceCollector());
        assertThatThrownBy(() -> interceptor.beforeConsume(null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("message");
    }

    @Test
    @DisplayName("afterConsume SUCCESS 时从 MDC 移除 traceId 并上报成功事件")
    void afterConsumeSuccess() {
        TraceCollector collector = mock(TraceCollector.class);
        when(collector.isEnabled()).thenReturn(true);
        TraceContextConsumerInterceptor interceptor = new TraceContextConsumerInterceptor(collector);

        Message<String> msg = new Message<>();
        msg.setTopic("topic-1");
        msg.setMessageId(new MessageId("1-0"));
        msg.setBody("hello");
        msg.putUserProperty(TraceContextConsumerInterceptor.TRACE_ID_KEY, "trace-1");
        msg.setReconsumeTimes(2);

        interceptor.beforeConsume(msg, null);
        assertThat(MDC.get(TraceContextConsumerInterceptor.MDC_TRACE_ID_KEY)).isEqualTo("trace-1");

        interceptor.afterConsume(msg, ConsumeAction.SUCCESS, null);

        assertThat(MDC.get(TraceContextConsumerInterceptor.MDC_TRACE_ID_KEY)).isNull();

        ArgumentCaptor<TraceCollector.ConsumeTraceContext> captor =
                ArgumentCaptor.forClass(TraceCollector.ConsumeTraceContext.class);
        verify(collector, times(1)).recordConsume(captor.capture());
        TraceCollector.ConsumeTraceContext ctx = captor.getValue();
        assertThat(ctx.topic()).isEqualTo("topic-1");
        assertThat(ctx.messageId()).isEqualTo(new MessageId("1-0"));
        assertThat(ctx.reconsumeTimes()).isEqualTo(2);
        assertThat(ctx.success()).isTrue();
        assertThat(ctx.traceId()).isEqualTo("trace-1");
        assertThat(ctx.durationMillis()).isGreaterThanOrEqualTo(0L);
        assertThat(ctx.attributes()).containsEntry("action", "SUCCESS");
    }

    @Test
    @DisplayName("afterConsume RECONSUME_LATER 上报失败事件")
    void afterConsumeReconsumeLater() {
        TraceCollector collector = mock(TraceCollector.class);
        when(collector.isEnabled()).thenReturn(true);
        TraceContextConsumerInterceptor interceptor = new TraceContextConsumerInterceptor(collector);

        Message<String> msg = new Message<>();
        msg.setTopic("topic-1");
        msg.setMessageId(new MessageId("1-0"));
        msg.putUserProperty(TraceContextConsumerInterceptor.TRACE_ID_KEY, "trace-1");

        interceptor.beforeConsume(msg, null);
        interceptor.afterConsume(msg, ConsumeAction.RECONSUME_LATER, null);

        ArgumentCaptor<TraceCollector.ConsumeTraceContext> captor =
                ArgumentCaptor.forClass(TraceCollector.ConsumeTraceContext.class);
        verify(collector).recordConsume(captor.capture());
        assertThat(captor.getValue().success()).isFalse();
        assertThat(captor.getValue().attributes()).containsEntry("action", "RECONSUME_LATER");
    }

    @Test
    @DisplayName("afterConsume 未启用 TraceCollector 时不调用 recordConsume")
    void afterConsumeSkipsWhenDisabled() {
        TraceCollector collector = mock(TraceCollector.class);
        when(collector.isEnabled()).thenReturn(false);
        TraceContextConsumerInterceptor interceptor = new TraceContextConsumerInterceptor(collector);

        Message<String> msg = new Message<>();
        msg.putUserProperty(TraceContextConsumerInterceptor.TRACE_ID_KEY, "trace-1");
        interceptor.beforeConsume(msg, null);
        interceptor.afterConsume(msg, ConsumeAction.SUCCESS, null);

        verify(collector, never()).recordConsume(any(TraceCollector.ConsumeTraceContext.class));
        assertThat(MDC.get(TraceContextConsumerInterceptor.MDC_TRACE_ID_KEY)).isNull();
    }

    @Test
    @DisplayName("afterConsume TraceCollector 抛异常时不传播且仍清理 MDC")
    void afterConsumeToleratesCollectorException() {
        TraceCollector collector = mock(TraceCollector.class);
        when(collector.isEnabled()).thenReturn(true);
        doThrow(new RuntimeException("boom")).when(collector).recordConsume(any(TraceCollector.ConsumeTraceContext.class));
        TraceContextConsumerInterceptor interceptor = new TraceContextConsumerInterceptor(collector);

        Message<String> msg = new Message<>();
        msg.putUserProperty(TraceContextConsumerInterceptor.TRACE_ID_KEY, "trace-1");
        interceptor.beforeConsume(msg, null);
        interceptor.afterConsume(msg, ConsumeAction.SUCCESS, null);
        assertThat(MDC.get(TraceContextConsumerInterceptor.MDC_TRACE_ID_KEY)).isNull();
    }

    @Test
    @DisplayName("未调用 beforeConsume 直接 afterConsume 不抛异常（traceId=null, duration=0）")
    void afterConsumeWithoutBeforeConsume() {
        TraceCollector collector = mock(TraceCollector.class);
        when(collector.isEnabled()).thenReturn(true);
        TraceContextConsumerInterceptor interceptor = new TraceContextConsumerInterceptor(collector);

        Message<String> msg = new Message<>();
        msg.setTopic("topic-1");
        interceptor.afterConsume(msg, ConsumeAction.SUCCESS, null);

        ArgumentCaptor<TraceCollector.ConsumeTraceContext> captor =
                ArgumentCaptor.forClass(TraceCollector.ConsumeTraceContext.class);
        verify(collector).recordConsume(captor.capture());
        assertThat(captor.getValue().traceId()).isNull();
        assertThat(captor.getValue().durationMillis()).isZero();
    }

    @Test
    @DisplayName("构造 TraceCollector 为 null 抛出 NullPointerException")
    void constructNullCollector() {
        assertThatThrownBy(() -> new TraceContextConsumerInterceptor(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("traceCollector");
    }

    @Test
    @DisplayName("order 返回 0")
    void order() {
        TraceContextConsumerInterceptor interceptor =
                new TraceContextConsumerInterceptor(new Slf4jTraceCollector());
        assertThat(interceptor.order()).isZero();
    }

    @Test
    @DisplayName("name 返回 trace-context-consumer")
    void name() {
        TraceContextConsumerInterceptor interceptor =
                new TraceContextConsumerInterceptor(new NoopTraceCollector());
        assertThat(interceptor.name()).isEqualTo("trace-context-consumer");
    }
}