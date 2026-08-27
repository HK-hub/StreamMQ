/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.trace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.streammq.core.interceptor.TraceCollector;
import io.github.streammq.core.message.MessageId;
import java.util.HashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamAddArgs;

/**
 * {@link RedisTraceCollector} 单元测试，覆盖启用状态、name、记录方法、 null 入参、异常容忍与构造参数校验。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("RedisTraceCollector Redis 追踪收集器测试")
class RedisTraceCollectorTest {

    private RedissonClient redisson;
    private RStream<String, String> stream;
    private RedisTraceCollector collector;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisson = mock(RedissonClient.class);
        stream = mock(RStream.class);
        doReturn(stream).when(redisson).getStream(anyString());
        collector = new RedisTraceCollector(redisson, "ns");
    }

    @Test
    @DisplayName("isEnabled 返回 true")
    void isEnabled() {
        assertThat(collector.isEnabled()).isTrue();
    }

    @Test
    @DisplayName("name 返回 redis")
    void name() {
        assertThat(collector.name()).isEqualTo("redis");
    }

    @Test
    @DisplayName("recordSend 成功事件写入 trace Stream")
    void recordSendSuccess() {
        TraceCollector.SendTraceContext ctx =
                new TraceCollector.SendTraceContext(
                        "topic-1",
                        "tag-1",
                        new MessageId("1-0"),
                        "pg",
                        System.currentTimeMillis(),
                        true,
                        12L,
                        "trace-1",
                        new HashMap<>());

        collector.recordSend(ctx);

        verify(redisson).getStream(contains(":trace:"));
        verify(stream, times(1)).add(any());
    }

    @Test
    @DisplayName("recordSend 失败事件写入 trace Stream")
    void recordSendFailure() {
        TraceCollector.SendTraceContext ctx =
                new TraceCollector.SendTraceContext(
                        "topic-1",
                        null,
                        new MessageId("2-0"),
                        "pg",
                        System.currentTimeMillis(),
                        false,
                        30L,
                        "trace-2",
                        new HashMap<>());

        collector.recordSend(ctx);

        verify(stream, times(1)).add(any());
    }

    @Test
    @DisplayName("recordSend 带 attributes 时写入 trace Stream")
    void recordSendWithAttributes() {
        HashMap<String, String> attrs = new HashMap<>();
        attrs.put("region", "us-east-1");
        TraceCollector.SendTraceContext ctx =
                new TraceCollector.SendTraceContext(
                        "topic-1",
                        "tag-1",
                        new MessageId("3-0"),
                        "pg",
                        System.currentTimeMillis(),
                        true,
                        5L,
                        "trace-3",
                        attrs);

        collector.recordSend(ctx);

        verify(stream, times(1)).add(any());
    }

    @Test
    @DisplayName("recordSend null 入参不抛异常且不写入 Stream")
    void recordSendNull() {
        collector.recordSend(null);

        verify(stream, never()).add(any());
    }

    @Test
    @DisplayName("recordSend messageId 为 null 时不抛异常")
    void recordSendNullMessageId() {
        TraceCollector.SendTraceContext ctx =
                new TraceCollector.SendTraceContext(
                        "topic-1",
                        "tag-1",
                        null,
                        "pg",
                        System.currentTimeMillis(),
                        true,
                        10L,
                        "trace-1",
                        new HashMap<>());

        collector.recordSend(ctx);

        verify(stream, times(1)).add(any());
    }

    @Test
    @DisplayName("recordConsume 成功事件写入 trace Stream")
    void recordConsumeSuccess() {
        TraceCollector.ConsumeTraceContext ctx =
                new TraceCollector.ConsumeTraceContext(
                        "topic-1",
                        "tag-1",
                        new MessageId("1-0"),
                        "cg",
                        "c1",
                        0,
                        true,
                        5L,
                        "trace-1",
                        new HashMap<>());

        collector.recordConsume(ctx);

        verify(redisson).getStream(contains(":trace:"));
        verify(stream, times(1)).add(any());
    }

    @Test
    @DisplayName("recordConsume 失败事件写入 trace Stream")
    void recordConsumeFailure() {
        TraceCollector.ConsumeTraceContext ctx =
                new TraceCollector.ConsumeTraceContext(
                        "topic-1",
                        null,
                        new MessageId("2-0"),
                        "cg",
                        "c2",
                        3,
                        false,
                        50L,
                        "trace-2",
                        new HashMap<>());

        collector.recordConsume(ctx);

        verify(stream, times(1)).add(any());
    }

    @Test
    @DisplayName("recordConsume null 入参不抛异常且不写入 Stream")
    void recordConsumeNull() {
        collector.recordConsume(null);

        verify(stream, never()).add(any());
    }

    @Test
    @DisplayName("recordConsume messageId 为 null 时不抛异常")
    void recordConsumeNullMessageId() {
        TraceCollector.ConsumeTraceContext ctx =
                new TraceCollector.ConsumeTraceContext(
                        "topic-1",
                        "tag-1",
                        null,
                        "cg",
                        "c1",
                        0,
                        true,
                        5L,
                        "trace-1",
                        new HashMap<>());

        collector.recordConsume(ctx);

        verify(stream, times(1)).add(any());
    }

    @Test
    @DisplayName("recordSend Stream 写入异常时不传播")
    void recordSendToleratesStreamException() {
        doThrow(new RuntimeException("redis down")).when(stream).add(any());

        TraceCollector.SendTraceContext ctx =
                new TraceCollector.SendTraceContext(
                        "topic-1",
                        "tag-1",
                        new MessageId("1-0"),
                        "pg",
                        System.currentTimeMillis(),
                        true,
                        10L,
                        "trace-1",
                        new HashMap<>());

        // 不应抛异常
        collector.recordSend(ctx);
    }

    @Test
    @DisplayName("recordConsume Stream 写入异常时不传播")
    void recordConsumeToleratesStreamException() {
        doThrow(new RuntimeException("redis down")).when(stream).add(any());

        TraceCollector.ConsumeTraceContext ctx =
                new TraceCollector.ConsumeTraceContext(
                        "topic-1",
                        "tag-1",
                        new MessageId("1-0"),
                        "cg",
                        "c1",
                        0,
                        true,
                        5L,
                        "trace-1",
                        new HashMap<>());

        // 不应抛异常
        collector.recordConsume(ctx);
    }

    @Test
    @DisplayName("namespace 为 null 时正常工作")
    void nullNamespace() {
        RedisTraceCollector nullNsCollector = new RedisTraceCollector(redisson, null);

        TraceCollector.SendTraceContext ctx =
                new TraceCollector.SendTraceContext(
                        "topic-1",
                        "tag-1",
                        new MessageId("1-0"),
                        "pg",
                        System.currentTimeMillis(),
                        true,
                        10L,
                        "trace-1",
                        new HashMap<>());

        nullNsCollector.recordSend(ctx);

        verify(redisson).getStream(contains(":trace:"));
        verify(stream, times(1)).add(any());
    }

    @Test
    @DisplayName("构造 redisson 为 null 抛出 NullPointerException")
    void constructNullRedisson() {
        assertThatThrownBy(() -> new RedisTraceCollector(null, "ns"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("redisson");
    }

    @Test
    @DisplayName("trace Key 包含命名空间段")
    void traceKeyContainsNamespace() {
        TraceCollector.SendTraceContext ctx =
                new TraceCollector.SendTraceContext(
                        "topic-1",
                        "tag-1",
                        new MessageId("1-0"),
                        "pg",
                        System.currentTimeMillis(),
                        true,
                        10L,
                        "trace-1",
                        new HashMap<>());

        collector.recordSend(ctx);

        verify(redisson).getStream(contains("streammq:ns:trace:"));
    }

    // ===================== MAXLEN / EXPIRE 兜底（无界增长修复） =====================

    private TraceCollector.SendTraceContext sendCtx(String traceId) {
        return new TraceCollector.SendTraceContext(
                "topic-1",
                "tag-1",
                new MessageId("9-0"),
                "pg",
                System.currentTimeMillis(),
                true,
                10L,
                traceId,
                new HashMap<>());
    }

    @SuppressWarnings("unchecked")
    private org.redisson.api.stream.StreamAddParams<String, String> capturedAddArgs() {
        org.mockito.ArgumentCaptor<StreamAddArgs<String, String>> captor =
                org.mockito.ArgumentCaptor.forClass(
                        (Class<StreamAddArgs<String, String>>) (Class<?>) StreamAddArgs.class);
        verify(stream).add(captor.capture());
        return (org.redisson.api.stream.StreamAddParams<String, String>) captor.getValue();
    }

    @Test
    @DisplayName("XADD 默认附加近似 MAXLEN=100000，并对日期 Key 施加 7 天 EXPIRE")
    void writeAppliesDefaultMaxlenAndTtl() {
        collector.recordSend(sendCtx("t-maxlen-default"));

        org.redisson.api.stream.StreamAddParams<String, String> args = capturedAddArgs();
        assertThat(args.getMaxLen()).isEqualTo(RedisTraceCollector.DEFAULT_MAX_STREAM_LEN);
        assertThat(args.isTrimStrict()).isFalse();
        verify(stream).expire(java.time.Duration.ofDays(7));
    }

    @Test
    @DisplayName("setter 注入小 MAXLEN 后 XADD 携带该上限")
    void writeAppliesInjectedMaxlen() {
        collector.setMaxStreamLen(5);
        collector.recordSend(sendCtx("t-maxlen-small"));

        assertThat(capturedAddArgs().getMaxLen()).isEqualTo(5);
        verify(stream).expire(java.time.Duration.ofDays(7));
    }

    @Test
    @DisplayName("maxStreamLen<=0 时 XADD 不附加截断")
    void writeWithoutTrimWhenDisabled() {
        collector.setMaxStreamLen(0);
        collector.recordSend(sendCtx("t-maxlen-disabled"));

        org.redisson.api.stream.StreamAddParams<String, String> args = capturedAddArgs();
        assertThat(args.getMaxLen()).isZero();
        verify(stream).expire(java.time.Duration.ofDays(7));
    }
}
