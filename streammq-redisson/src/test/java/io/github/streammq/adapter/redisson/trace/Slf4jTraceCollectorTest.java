/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.trace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.streammq.core.interceptor.TraceCollector;
import io.github.streammq.core.message.MessageId;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * {@link Slf4jTraceCollector} 单元测试，覆盖启用状态、name 与各记录分支的日志产出。
 *
 * <p>零断言写法（"调用即通过"）无法区分"确实记录了追踪"与"静默失败"， 因此每个记录分支都以 logback {@code ListAppender}
 * 捕获真实日志事件并断言级别与关键字段（成功=DEBUG， 失败=INFO，null 入参=不产生任何事件）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("Slf4jTraceCollector SLF4J 追踪收集器测试")
class Slf4jTraceCollectorTest {

    private final Slf4jTraceCollector collector = new Slf4jTraceCollector();

    private Logger collectorLogger;
    private Level previousLevel;
    private ListAppender<ILoggingEvent> logAppender;

    @BeforeEach
    void attachLogCapture() {
        collectorLogger = (Logger) LoggerFactory.getLogger(Slf4jTraceCollector.class);
        previousLevel = collectorLogger.getLevel();
        collectorLogger.setLevel(Level.DEBUG);
        logAppender = new ListAppender<>();
        logAppender.start();
        collectorLogger.addAppender(logAppender);
    }

    @AfterEach
    void detachLogCapture() {
        if (collectorLogger != null && logAppender != null) {
            collectorLogger.detachAppender(logAppender);
            logAppender.stop();
            collectorLogger.setLevel(previousLevel);
        }
    }

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
    @DisplayName("recordSend 成功事件：不抛异常且产出 DEBUG 追踪日志")
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

        assertThatCode(() -> collector.recordSend(ctx)).doesNotThrowAnyException();

        List<ILoggingEvent> debugEvents = eventsOf(Level.DEBUG);
        assertThat(debugEvents).hasSize(1);
        assertThat(debugEvents.get(0).getFormattedMessage())
                .contains("topic=topic-1")
                .contains("traceId=trace-1");
    }

    @Test
    @DisplayName("recordSend 失败事件：不抛异常且产出 INFO 追踪日志")
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

        assertThatCode(() -> collector.recordSend(ctx)).doesNotThrowAnyException();

        List<ILoggingEvent> infoEvents = eventsOf(Level.INFO);
        assertThat(infoEvents).hasSize(1);
        assertThat(infoEvents.get(0).getFormattedMessage())
                .contains("topic=topic-1")
                .contains("success=false");
    }

    @Test
    @DisplayName("recordSend null 入参：不抛异常且不产生日志事件")
    void recordSendNull() {
        assertThatCode(() -> collector.recordSend(null)).doesNotThrowAnyException();

        assertThat(logAppender.list).isEmpty();
    }

    @Test
    @DisplayName("recordConsume 成功事件：不抛异常且产出 DEBUG 追踪日志")
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

        assertThatCode(() -> collector.recordConsume(ctx)).doesNotThrowAnyException();

        List<ILoggingEvent> debugEvents = eventsOf(Level.DEBUG);
        assertThat(debugEvents).hasSize(1);
        assertThat(debugEvents.get(0).getFormattedMessage())
                .contains("topic=topic-1")
                .contains("traceId=trace-1");
    }

    @Test
    @DisplayName("recordConsume 失败事件：不抛异常且产出 INFO 追踪日志")
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

        assertThatCode(() -> collector.recordConsume(ctx)).doesNotThrowAnyException();

        List<ILoggingEvent> infoEvents = eventsOf(Level.INFO);
        assertThat(infoEvents).hasSize(1);
        assertThat(infoEvents.get(0).getFormattedMessage())
                .contains("topic=topic-1")
                .contains("success=false");
    }

    @Test
    @DisplayName("recordConsume null 入参：不抛异常且不产生日志事件")
    void recordConsumeNull() {
        assertThatCode(() -> collector.recordConsume(null)).doesNotThrowAnyException();

        assertThat(logAppender.list).isEmpty();
    }

    private List<ILoggingEvent> eventsOf(Level level) {
        return logAppender.list.stream().filter(event -> level.equals(event.getLevel())).toList();
    }
}
