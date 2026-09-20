/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListener;
import io.github.streammq.core.consumer.StreamMessageConsumer;
import io.github.streammq.core.exception.StreamMQBrokerException;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.ListenerType;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

/**
 * {@link ConsumeLoopTask} 运行期故障可见性 + 运行期配置生效性回归测试。
 *
 * <p>覆盖发布前修复 P1-6 的两个闭环：
 *
 * <ul>
 *   <li>持续失败：连续 {@link ConsumeLoopTask#RUNTIME_FAILURE_REPORT_THRESHOLD} 次可恢复异常后，通过 {@code
 *       LoopFailureReporter} 上报健康信号——否则消费线程静默消失、健康检查仍 UP；
 *   <li>恢复清除：任一成功拉取即复位连续失败计数并调用 {@code LoopFailureCleaner}，实现 "持续失败 → DOWN、 恢复 → UP"。
 * </ul>
 *
 * <p>红队第六轮补充：
 *
 * <ul>
 *   <li>R1-5：暂停期心跳按 {@code heartbeatIntervalMillis} 节流（此前按 100ms 暂停间隔刷 ZSet，≈20 写/秒）；
 *   <li>R1-6：{@code pausedSleepMillis} / {@code brokerErrorBackoffMillis} 每轮读取（supplier），运行期 setter
 *       生效；
 *   <li>R1-8：{@code drainPendingOnce} 返回 null（未实现契约）时 WARN 一次并跳过，不 NPE。
 * </ul>
 */
@DisplayName("消费循环运行期故障上报与运行期配置生效性测试")
class ConsumeLoopTaskTest {

    private static final String TOPIC = "order-topic";
    private static final String GROUP = "default-group";
    private static final String EXPECTED_PUMP_KEY = TOPIC + ":" + GROUP + "#0";

    @Test
    @DisplayName("连续失败达到阈值后上报健康信号")
    void continuousFailures_reportAfterThreshold() throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicReference<String> reportedKey = new AtomicReference<>();
        AtomicReference<Throwable> reportedCause = new AtomicReference<>();

        StreamMQListener listener = mock(StreamMQListener.class);
        when(listener.pullBlock(anyInt(), any(Duration.class)))
                .thenThrow(new StreamMQBrokerException("broker unavailable"));

        ConsumeLoopTask task =
                newTask(
                        listener,
                        running,
                        (key, cause) -> {
                            reportedKey.set(key);
                            reportedCause.set(cause);
                        },
                        key -> {});

        Thread loop = new Thread(task, "consume-loop-test");
        loop.start();
        try {
            org.awaitility.Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(reportedKey.get()).isNotNull());
            assertThat(reportedKey.get()).isEqualTo(EXPECTED_PUMP_KEY);
            assertThat(reportedCause.get())
                    .isInstanceOf(StreamMQBrokerException.class)
                    .hasMessage("broker unavailable");
        } finally {
            running.set(false);
            loop.join(5_000);
            assertThat(loop.isAlive()).isFalse();
        }
    }

    @Test
    @DisplayName("失败后成功拉取复位并清除健康条目")
    void successfulPull_clearsReportedFailure() throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger pullCalls = new AtomicInteger();
        AtomicInteger cleared = new AtomicInteger();

        StreamMQListener listener = mock(StreamMQListener.class);
        when(listener.pullBlock(anyInt(), any(Duration.class)))
                .thenAnswer(
                        (Answer<List<Message<?>>>)
                                inv -> {
                                    if (pullCalls.incrementAndGet() == 1) {
                                        throw new StreamMQBrokerException("transient glitch");
                                    }
                                    return List.of();
                                });

        ConsumeLoopTask task =
                newTask(listener, running, (key, cause) -> {}, key -> cleared.incrementAndGet());

        Thread loop = new Thread(task, "consume-loop-test");
        loop.start();
        try {
            // 首次失败后应发生成功拉取 → 清除被调用
            org.awaitility.Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(cleared.get()).isEqualTo(1));
        } finally {
            running.set(false);
            loop.join(5_000);
            assertThat(loop.isAlive()).isFalse();
        }
    }

    // ===================== R1-5：暂停期心跳节流 =====================

    @Test
    @DisplayName("R1-5：暂停期心跳按配置间隔节流（不随 100ms 暂停间隔放大 20 倍），恢复后立即补一次")
    void pauseHeartbeat_isThrottledAndResumesImmediately() throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicBoolean paused = new AtomicBoolean(true);
        AtomicInteger heartbeats = new AtomicInteger();

        RedissonStreamListener broadcastListener = mock(RedissonStreamListener.class);
        doAnswer(inv -> heartbeats.incrementAndGet())
                .when(broadcastListener)
                .heartbeatBroadcastRegistry();
        when(broadcastListener.pullBlock(anyInt(), any(Duration.class))).thenReturn(List.of());

        ConsumeLoopTask task =
                newTask(
                        broadcastListener,
                        running,
                        paused::get,
                        () -> 10L /* pausedSleep：旧实现每 10ms 一次心跳 */,
                        () -> 0L,
                        () -> 300L /* heartbeatInterval */,
                        false,
                        (key, cause) -> {},
                        key -> {});

        Thread loop = new Thread(task, "consume-loop-pause-heartbeat");
        loop.start();
        try {
            Thread.sleep(1_200L);
            int duringPause = heartbeats.get();
            assertThat(duringPause).as("暂停 1.2s、心跳间隔 300ms → 心跳次数应有上界（旧实现约 120 次）").isBetween(1, 8);

            // 恢复后立即补一次心跳
            paused.set(false);
            org.awaitility.Awaitility.await()
                    .atMost(2, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(heartbeats.get()).isGreaterThan(duringPause));
        } finally {
            running.set(false);
            loop.join(5_000);
            assertThat(loop.isAlive()).isFalse();
        }
    }

    // ===================== R1-6：运行期 setter 每轮生效 =====================

    @Test
    @DisplayName("R1-6：pausedSleepMillis 每轮读取——运行期改大后循环节拍立即变慢")
    void pauseSleep_isReadEveryIteration() throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger pauseChecks = new AtomicInteger();
        AtomicLong pauseSleep = new AtomicLong(10L);
        BooleanSupplier paused =
                () -> {
                    pauseChecks.incrementAndGet();
                    return true;
                };

        ConsumeLoopTask task =
                newTask(
                        mock(StreamMQListener.class),
                        running,
                        paused,
                        pauseSleep::get,
                        () -> 0L,
                        () -> 60_000L,
                        false,
                        (key, cause) -> {},
                        key -> {});

        Thread loop = new Thread(task, "consume-loop-pause-sleep");
        loop.start();
        try {
            Thread.sleep(300L);
            int fastPhase = pauseChecks.get();
            assertThat(fastPhase).as("10ms 暂停间隔下循环应快速迭代").isGreaterThan(5);

            pauseSleep.set(1_000L);
            int baseline = pauseChecks.get();
            Thread.sleep(500L);
            int slowDelta = pauseChecks.get() - baseline;
            assertThat(slowDelta)
                    .as("运行期把暂停间隔改为 1000ms 后，500ms 窗口内迭代数应 ≈0（快照实现约 50 次）")
                    .isLessThanOrEqualTo(3);
        } finally {
            running.set(false);
            loop.join(5_000);
            assertThat(loop.isAlive()).isFalse();
        }
    }

    @Test
    @DisplayName("R1-6：brokerErrorBackoffMillis 每轮读取——运行期改大后退避立即变慢")
    void brokerBackoff_isReadEveryIteration() throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger pulls = new AtomicInteger();
        AtomicLong backoff = new AtomicLong(10L);

        StreamMQListener listener = mock(StreamMQListener.class);
        when(listener.pullBlock(anyInt(), any(Duration.class)))
                .thenAnswer(
                        inv -> {
                            pulls.incrementAndGet();
                            throw new StreamMQBrokerException("broker down");
                        });

        ConsumeLoopTask task =
                newTask(
                        listener,
                        running,
                        () -> false,
                        () -> 10L,
                        backoff::get,
                        () -> 60_000L,
                        false,
                        (key, cause) -> {},
                        key -> {});

        Thread loop = new Thread(task, "consume-loop-backoff");
        loop.start();
        try {
            Thread.sleep(300L);
            int fastPhase = pulls.get();
            assertThat(fastPhase).as("10ms 退避下应快速重试").isGreaterThan(5);

            backoff.set(1_000L);
            int baseline = pulls.get();
            Thread.sleep(500L);
            int slowDelta = pulls.get() - baseline;
            assertThat(slowDelta)
                    .as("运行期把退避改为 1000ms 后，500ms 窗口内重试数应 ≈0（快照实现约 25 次）")
                    .isLessThanOrEqualTo(3);
        } finally {
            running.set(false);
            loop.join(5_000);
            assertThat(loop.isAlive()).isFalse();
        }
    }

    // ===================== R1-8：drainPendingOnce 契约 =====================

    @Test
    @DisplayName("R1-8：drainPendingOnce 返回 null（未实现契约）→ WARN 一次、跳过排空、循环继续")
    void nullDrainPendingOnce_warnsOnceAndContinues() throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger pulls = new AtomicInteger();

        StreamMQListener listener = mock(StreamMQListener.class);
        when(listener.drainPendingOnce(anyInt())).thenReturn(null);
        when(listener.pullBlock(anyInt(), any(Duration.class)))
                .thenAnswer(
                        inv -> {
                            pulls.incrementAndGet();
                            return List.of();
                        });

        Logger taskLogger = (Logger) LoggerFactory.getLogger(ConsumeLoopTask.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        taskLogger.addAppender(appender);

        ConsumeLoopTask task =
                newTask(
                        listener,
                        running,
                        () -> false,
                        () -> 10L,
                        () -> 0L,
                        () -> 60_000L,
                        true /* primaryLoop → 触发启动排空 */,
                        (key, cause) -> {},
                        key -> {});

        Thread loop = new Thread(task, "consume-loop-null-drain");
        loop.start();
        try {
            org.awaitility.Awaitility.await()
                    .atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(pulls.get()).isGreaterThan(0));

            long warns =
                    appender.list.stream()
                            .filter(e -> Level.WARN.equals(e.getLevel()))
                            .filter(e -> e.getFormattedMessage().contains("drainPendingOnce"))
                            .count();
            assertThat(warns).as("null = 未实现契约：恰好 WARN 一次").isEqualTo(1);
            assertThat(loop.isAlive()).as("WARN 后循环不得终止").isTrue();
        } finally {
            running.set(false);
            taskLogger.detachAppender(appender);
            appender.stop();
            loop.join(5_000);
        }
    }

    // ===================== 装配辅助 =====================

    @SuppressWarnings("unchecked")
    private ConsumeLoopTask newTask(
            StreamMQListener listener,
            AtomicBoolean running,
            ConsumeLoopTask.LoopContext.LoopFailureReporter reporter,
            ConsumeLoopTask.LoopContext.LoopFailureCleaner cleaner) {
        return newTask(
                listener,
                running,
                () -> false,
                () -> 1L,
                () -> 0L,
                () -> 60_000L,
                false,
                reporter,
                cleaner);
    }

    @SuppressWarnings("unchecked")
    private ConsumeLoopTask newTask(
            StreamMQListener listener,
            AtomicBoolean running,
            BooleanSupplier paused,
            LongSupplier pausedSleepMillis,
            LongSupplier brokerErrorBackoffMillis,
            LongSupplier heartbeatIntervalMillis,
            boolean primaryLoop,
            ConsumeLoopTask.LoopContext.LoopFailureReporter reporter,
            ConsumeLoopTask.LoopContext.LoopFailureCleaner cleaner) {
        ListenerRegistration<Object> reg = mock(ListenerRegistration.class);
        when(reg.key()).thenReturn(TOPIC + ":" + GROUP);
        when(reg.getTopic()).thenReturn(TOPIC);
        when(reg.getGroup()).thenReturn(GROUP);
        when(reg.getType()).thenReturn(ListenerType.AUTO_ACK);
        when(reg.isDlqMode()).thenReturn(false);
        when(reg.getPullBatchSize()).thenReturn(1);
        when(reg.getPullBlockTimeoutMillis()).thenReturn(10L);
        when(reg.getPullIntervalMillis()).thenReturn(0L);
        when(reg.getConsumer()).thenReturn(mock(StreamMessageConsumer.class));

        ConsumeLoopTask.LoopContext ctx =
                new ConsumeLoopTask.LoopContext(
                        reg,
                        false, // retryMode
                        primaryLoop,
                        0, // loopIndex
                        mock(MessageProcessor.class),
                        mock(ConsumeLoopSupervisor.class),
                        mock(ExecutorService.class),
                        running::get,
                        paused,
                        () -> 0, // inflightCapacity=0 → 同步派发，无需真实 executor
                        (r, rm) -> listener,
                        reporter,
                        cleaner,
                        pausedSleepMillis,
                        brokerErrorBackoffMillis,
                        heartbeatIntervalMillis,
                        null, // deferredRetryQueue
                        null); // deferredRetryDispatcher
        return new ConsumeLoopTask(ctx);
    }
}
