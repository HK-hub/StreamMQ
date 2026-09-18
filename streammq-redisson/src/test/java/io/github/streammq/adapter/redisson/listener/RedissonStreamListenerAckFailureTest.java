/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.exception.StreamMQBrokerException;
import io.github.streammq.core.message.MessageId;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RFuture;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.slf4j.LoggerFactory;

/**
 * {@link RedissonStreamListener} 异步 ACK 失败分支单元测试（红队 F-02 / U-01 缺口）。
 *
 * <p>覆盖此前零覆盖的三条降级路径：
 *
 * <ul>
 *   <li>{@code ackAsync} 返回失败 future：{@code ack()} 不抛异常，ACK 窗口许可必须释放
 *   <li>许可不释放的后果：窗口在若干次失败后永久耗尽，消费循环整体卡死
 *   <li>ERROR 日志必须明示「消息仍在 PEL、消费端需幂等」，不得吞掉未 ACK 事实
 *   <li>{@code ackAsync} 同步抛异常：包装为 {@link StreamMQBrokerException} 且许可不泄漏
 *   <li>停机排空遇到永不完成的 ACK：必须在有界窗口内返回并告警，绝不无限阻塞停机
 * </ul>
 *
 * <p>本测试只使用 mockito 替换 Redisson 客户端，不依赖真实 Redis。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("RedissonStreamListener 异步 ACK 失败分支测试")
class RedissonStreamListenerAckFailureTest {

    private static final String NAMESPACE = "ack-failure-ns";
    private static final String TOPIC = "ack-failure-topic";
    private static final String GROUP = "ack-failure-group";
    private static final String CONSUMER_NAME = "ack-failure-consumer";

    private RedissonClient redisson;
    private RStream<String, String> stream;
    private RedissonStreamListener listener;
    private Logger listenerLogger;
    private ListAppender<ILoggingEvent> logAppender;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisson = mock(RedissonClient.class);
        stream = mock(RStream.class);
        doReturn(stream).when(redisson).getStream(anyString(), any());
        listener =
                new RedissonStreamListener(
                        redisson,
                        NAMESPACE,
                        TOPIC,
                        GROUP,
                        CONSUMER_NAME,
                        mock(MessageConverter.class));
        listenerLogger = (Logger) LoggerFactory.getLogger(RedissonStreamListener.class);
        logAppender = new ListAppender<>();
        logAppender.start();
        listenerLogger.addAppender(logAppender);
    }

    @AfterEach
    void tearDown() {
        if (listenerLogger != null && logAppender != null) {
            listenerLogger.detachAppender(logAppender);
            logAppender.stop();
        }
    }

    @Test
    @DisplayName("ackAsync 异步失败：ack 不抛异常且 ERROR 日志保留未 ACK 事实")
    void ack_asyncAckFails_doesNotThrowAndLogsError() {
        stubAckFuture(failedAckFuture(new RuntimeException("boom")));

        assertThatCode(() -> listener.ack(MessageId.of(1L, 0L))).doesNotThrowAnyException();

        List<ILoggingEvent> errors = errorEvents();
        assertThat(errors).hasSize(1);
        assertThat(errors.get(0).getFormattedMessage())
                .contains("Async ACK failed")
                .contains("messageId=1-0")
                .contains("consumers must be idempotent")
                .contains("cause=boom");
    }

    @Test
    @DisplayName("ackAsync 异步失败：窗口许可被释放，连续 ack 超过窗口容量仍不阻塞")
    void ack_asyncAckFails_releasesWindowPermitBeyondCapacity() throws Exception {
        stubAckFuture(failedAckFuture(new RuntimeException("boom")));
        int attempts = RedissonStreamListener.ACK_PIPELINE_WINDOW + 8;

        // 若失败分支泄漏许可，第 ACK_PIPELINE_WINDOW+1 次 acquire 将永久阻塞、该任务无法在超时内完成
        runInBoundedTime(
                () -> {
                    for (int i = 0; i < attempts; i++) {
                        listener.ack(MessageId.of(i, 0L));
                    }
                },
                Duration.ofSeconds(5));

        assertThat(errorEvents()).hasSize(attempts);
    }

    @Test
    @DisplayName("ackAsync 同步抛出：包装为 StreamMQBrokerException 且许可不泄漏")
    void ack_ackAsyncThrowsSynchronously_wrapsAndReleasesPermit() throws Exception {
        when(stream.ackAsync(eq(GROUP), any(StreamMessageId.class)))
                .thenThrow(new RuntimeException("redis down"))
                .thenReturn(failedAckFuture(new RuntimeException("boom")));

        assertThatThrownBy(() -> listener.ack(MessageId.of(2L, 0L)))
                .isInstanceOf(StreamMQBrokerException.class)
                .hasMessageContaining("ack failed for topic " + TOPIC)
                .hasRootCauseMessage("redis down");

        // 同步失败占用的许可若不释放，窗口只剩 255 个许可，恰好 ACK_PIPELINE_WINDOW 次 ack 会阻塞
        runInBoundedTime(
                () -> {
                    for (int i = 0; i < RedissonStreamListener.ACK_PIPELINE_WINDOW; i++) {
                        listener.ack(MessageId.of(3L, i));
                    }
                },
                Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("停机排空：ACK future 永不完成时有界返回并告警，不无限阻塞")
    void close_withNeverCompletingAck_returnsWithinDrainTimeout() {
        // whenComplete 不被回调 → 许可永不释放，模拟 ACK 请求悬挂在网络上
        stubAckFuture(neverCompletingAckFuture());

        listener.ack(MessageId.of(4L, 0L));

        Instant start = Instant.now();
        assertThatCode(listener::close).doesNotThrowAnyException();
        Duration elapsed = Duration.between(start, Instant.now());

        // 排空上限为 5s：必须等满（证明确实等待了悬挂 ACK）而不越过上界（证明未无限阻塞）
        assertThat(elapsed)
                .isGreaterThanOrEqualTo(Duration.ofSeconds(3))
                .isLessThan(Duration.ofSeconds(15));
        assertThat(listener.isRunning()).isFalse();
        assertThat(warnEvents())
                .anySatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .contains("Timed out waiting for outstanding ACKs")
                                        .contains("stays in PEL"));
    }

    // ===================== 夹具 =====================

    private void stubAckFuture(RFuture<Long> future) {
        when(stream.ackAsync(eq(GROUP), any(StreamMessageId.class))).thenReturn(future);
    }

    /**
     * 构造"已完成但失败"的 {@link RFuture}：{@code whenComplete} 在调用线程上同步回调失败原因。
     *
     * <p>mock 该接口即可精确复现"ACK 命令返回失败"分支，无需真实 Redis。
     */
    @SuppressWarnings("unchecked")
    private static RFuture<Long> failedAckFuture(Throwable cause) {
        RFuture<Long> future = mock(RFuture.class);
        doAnswer(
                        invocation -> {
                            BiConsumer<Long, Throwable> action = invocation.getArgument(0);
                            action.accept(null, cause);
                            return null;
                        })
                .when(future)
                .whenComplete(any());
        return future;
    }

    /** 构造一个永不完成的 {@link RFuture}：模拟 ACK 请求悬挂（既不成功也不失败）。 */
    @SuppressWarnings("unchecked")
    private static RFuture<Long> neverCompletingAckFuture() {
        return mock(RFuture.class);
    }

    /** 在守护线程上执行任务并断言其在给定时间内完成（阻塞即视为失败，不会挂死测试 JVM）。 */
    private static void runInBoundedTime(Runnable task, Duration timeout) throws Exception {
        ExecutorService worker =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "ack-failure-test-worker");
                            thread.setDaemon(true);
                            return thread;
                        });
        try {
            Future<?> done = worker.submit(task);
            assertThatCode(() -> done.get(timeout.toMillis(), TimeUnit.MILLISECONDS))
                    .doesNotThrowAnyException();
        } finally {
            worker.shutdownNow();
        }
    }

    private List<ILoggingEvent> errorEvents() {
        return logAppender.list.stream()
                .filter(event -> Level.ERROR.equals(event.getLevel()))
                .toList();
    }

    private List<ILoggingEvent> warnEvents() {
        return logAppender.list.stream()
                .filter(event -> Level.WARN.equals(event.getLevel()))
                .toList();
    }
}
