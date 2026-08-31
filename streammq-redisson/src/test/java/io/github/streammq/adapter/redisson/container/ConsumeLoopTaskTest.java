/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

/**
 * {@link ConsumeLoopTask} 运行期故障可见性回归测试。
 *
 * <p>覆盖发布前修复 P1-6 的两个闭环：
 *
 * <ul>
 *   <li>持续失败：连续 {@link ConsumeLoopTask#RUNTIME_FAILURE_REPORT_THRESHOLD} 次可恢复异常后，通过 {@code
 *       LoopFailureReporter} 上报健康信号——否则消费线程静默消失、健康检查仍 UP；
 *   <li>恢复清除：任一成功拉取即复位连续失败计数并调用 {@code LoopFailureCleaner}，实现 "持续失败 → DOWN、 恢复 → UP"。
 * </ul>
 */
@DisplayName("消费循环运行期故障上报测试")
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

    // ===================== 装配辅助 =====================

    @SuppressWarnings("unchecked")
    private ConsumeLoopTask newTask(
            StreamMQListener listener,
            AtomicBoolean running,
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

        AtomicBoolean paused = new AtomicBoolean(false);
        ConsumeLoopTask.LoopContext ctx =
                new ConsumeLoopTask.LoopContext(
                        reg,
                        false, // retryMode
                        false, // primaryLoop（跳过 AUTO_ACK drain 分支）
                        0, // loopIndex
                        mock(MessageProcessor.class),
                        mock(ConsumeLoopSupervisor.class),
                        mock(ExecutorService.class),
                        running::get,
                        paused::get,
                        () -> 0, // inflightCapacity=0 → 同步派发，无需真实 executor
                        (r, rm) -> listener,
                        reporter,
                        cleaner);
        return new ConsumeLoopTask(
                ctx, 1L /* pausedSleepMillis */, 0L /* brokerErrorBackoffMillis */);
    }
}
