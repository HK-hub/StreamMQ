/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.trace;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.core.interceptor.TraceCollector;
import io.github.streammq.core.message.MessageId;
import java.util.HashMap;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link Slf4jTraceCollector} 单元测试，覆盖启用状态、name 与记录方法不抛异常。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("Slf4jTraceCollector SLF4J 追踪收集器测试")
class Slf4jTraceCollectorTest {

    private final Slf4jTraceCollector collector = new Slf4jTraceCollector();

    @Test
    @DisplayName("isEnabled 返回 true")
    void isEnabled() {
        assertThat(collector.isEnabled()).isTrue();
    }

    @Test
    @DisplayName("name 返回 slf4j")
    void name() {
        assertThat(collector.name()).isEqualTo("slf4j");
    }

    @Test
    @DisplayName("recordSend 成功事件不抛异常")
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
    }

    @Test
    @DisplayName("recordSend 失败事件不抛异常")
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
    }

    @Test
    @DisplayName("recordSend null 入参不抛异常")
    void recordSendNull() {
        collector.recordSend(null);
    }

    @Test
    @DisplayName("recordConsume 成功事件不抛异常")
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
    }

    @Test
    @DisplayName("recordConsume 失败事件不抛异常")
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
    }

    @Test
    @DisplayName("recordConsume null 入参不抛异常")
    void recordConsumeNull() {
        collector.recordConsume(null);
    }
}
