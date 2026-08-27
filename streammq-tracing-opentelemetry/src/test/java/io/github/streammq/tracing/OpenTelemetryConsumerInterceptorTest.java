/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.MessageId;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link OpenTelemetryConsumerInterceptor} 消费者 Span 跨线程配对测试。
 *
 * <p>回归背景：此前使用 ThreadLocal 配对 before / after 回调，容器在消费超时路径上把业务回调调度到 独立线程，ThreadLocal 配对失效并泄漏未结束的
 * Span。现以消息 ID 为键经有界注册表配对， 本测试模拟「before 与 after 运行在不同线程」验证配对仍然成立。
 */
@DisplayName("消费者拦截器跨线程 Span 配对测试")
class OpenTelemetryConsumerInterceptorTest {

    private StreamMQTracing tracing;
    private Scope scope;
    private List<Span> endedSpans;
    private OpenTelemetryConsumerInterceptor interceptor;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        tracing = mock(StreamMQTracing.class);
        scope = mock(Scope.class);
        endedSpans = new CopyOnWriteArrayList<>();
        interceptor = new OpenTelemetryConsumerInterceptor(tracing);

        // 记录被结束的 Span（endSpan 是真实结束入口）
        doAnswer(
                        invocation -> {
                            endedSpans.add(invocation.getArgument(0));
                            return null;
                        })
                .when(tracing)
                .endSpan(any(Span.class), anyBoolean());
    }

    private Message<String> buildMessage(String messageId) {
        return (Message<String>)
                MessageBuilder.<String>withTopic("order-topic")
                        .body("payload")
                        .build()
                        .withMessageId(new MessageId(messageId));
    }

    @Test
    @DisplayName("afterConsume 在另一线程执行时仍能按消息 ID 配对并结束 Span")
    void pairingSurvivesThreadHop() throws Exception {
        Span span = mock(Span.class);
        when(span.makeCurrent()).thenReturn(scope);
        when(tracing.startConsumerSpan(any(), any())).thenReturn(span);

        Message<String> message = buildMessage("100-1");
        interceptor.beforeConsume(message, null);

        CountDownLatch done = new CountDownLatch(1);
        Thread consumer =
                new Thread(
                        () -> {
                            interceptor.afterConsume(message, ConsumeAction.SUCCESS, null);
                            done.countDown();
                        },
                        "simulated-worker");
        consumer.start();
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();

        verify(span).makeCurrent();
        verify(scope).close();
        verify(tracing).endSpan(span, true);
        assertThat(endedSpans).containsExactly(span);
    }

    @Test
    @DisplayName("onException 以失败状态结束对应 Span")
    void exceptionPathEndsSpanAsFailure() {
        Span span = mock(Span.class);
        when(span.makeCurrent()).thenReturn(scope);
        when(tracing.startConsumerSpan(any(), any())).thenReturn(span);

        Message<String> message = buildMessage("200-2");
        interceptor.beforeConsume(message, null);
        interceptor.onException(message, new IllegalStateException("biz boom"), null, null);

        verify(tracing).endSpan(span, false, "biz boom");
        verify(scope).close();
    }

    @Test
    @DisplayName("重复消费同 ID 消息时旧 Span 被注册表淘汰结束，不发生泄漏")
    void duplicateMessageId_evictsAndEndsOldSpan() {
        Span first = mock(Span.class);
        Span second = mock(Span.class);
        when(first.makeCurrent()).thenReturn(scope);
        when(second.makeCurrent()).thenReturn(scope);
        when(tracing.startConsumerSpan(any(), any())).thenReturn(first, second);

        Message<String> message = buildMessage("300-3");
        // 同一消息两次投递（重试场景）：before 各登记一次
        interceptor.beforeConsume(message, null);
        interceptor.beforeConsume(message, null);

        // 第一次投递的 Span 已在覆盖时被注册表直接 end()（淘汰路径不经过 tracing.endSpan，
        // 注册表与追踪门面解耦；endSpan 仅用于正常 after/onException 收尾）
        verify(first).end();
        assertThat(endedSpans).isEmpty();

        // after 只应结束第二次投递的 Span，且只关闭其作用域一次
        interceptor.afterConsume(message, ConsumeAction.RECONSUME_LATER, null);
        verify(tracing).endSpan(second, false);
        verify(tracing, never()).endSpan(first, true);
    }
}
