/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link InflightSink} 泵线程韧性回归测试。
 *
 * <p>覆盖：处理器反复抛异常时泵必须存活（旧实现一条消息的处理异常即杀死泵线程， 该注册的后续消息全部滞留）；容器停止后 dispatch 不再无限阻塞。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("InflightSink 泵线程韧性测试")
class MessageSinkTest {

    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    /** 处理器构造辅助：以单消息行为定义 {@link MessageProcessor}。 */
    private interface MsgHandler {
        void handle(Message<?> message) throws Exception;
    }

    /** 记录 {@link MessageProcessor#handleFailure} 收到的失败消息（用于断言消息未被静默丢弃）。 */
    private final java.util.List<Message<?>> routedFailures =
            java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    private MessageProcessor processorOf(MsgHandler handler) {
        return new MessageProcessor() {
            @Override
            public void processMessage(
                    Message<?> message, ListenerRegistration<?> reg, StreamMQListener listener) {
                try {
                    handler.handle(message);
                } catch (RuntimeException ex) {
                    throw ex;
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }

            @Override
            public void handleFailure(
                    Message<?> message,
                    ListenerRegistration<?> reg,
                    StreamMQListener listener,
                    Throwable cause) {
                routedFailures.add(message);
            }

            @Override
            public void setMetrics(io.github.streammq.core.metrics.StreamMQMetrics metrics) {}

            @Override
            public void setRuntimeStats(
                    io.github.streammq.adapter.redisson.metrics.RuntimeStatsRegistry
                            runtimeStats) {}

            @Override
            public void setTimeoutCancelGraceMillis(long millis) {}

            @Override
            public void setExecutor(java.util.concurrent.ExecutorService executor) {}
        };
    }

    private Message<?> msg(String body) {
        return io.github.streammq.core.message.MessageBuilder.<String>withPayload(body)
                .topic("t")
                .build();
    }

    private InflightSink newSink(int capacity, MessageProcessor processor, AtomicBoolean running) {
        return new InflightSink(
                capacity,
                null,
                org.mockito.Mockito.mock(StreamMQListener.class),
                processor,
                new NoopSupervisor(),
                executor,
                running::get,
                "t:g#" + capacity + "-" + System.nanoTime());
    }

    @Test
    @DisplayName("泵在处理器反复抛 RuntimeException 后仍持续消费后续消息")
    void pumpSurvivesRepeatedProcessorFailures() throws Exception {
        int total = 5;
        CountDownLatch allAttempted = new CountDownLatch(total);
        AtomicInteger invocations = new AtomicInteger();
        AtomicBoolean running = new AtomicBoolean(true);

        InflightSink sink =
                newSink(
                        16,
                        processorOf(
                                message -> {
                                    invocations.incrementAndGet();
                                    allAttempted.countDown();
                                    throw new RuntimeException("boom " + message.getBody());
                                }),
                        running);

        for (int i = 0; i < total; i++) {
            sink.dispatch(msg("m" + i));
        }

        // 每条消息都被泵尝试过：泵吞掉异常退避后继续取下一条，而不是死在第 1 条
        await().atMost(10, TimeUnit.SECONDS).until(() -> allAttempted.getCount() == 0);
        assertThat(invocations.get()).isEqualTo(total);
        await().atMost(5, TimeUnit.SECONDS).until(() -> sink.dispatchQueueEmpty());

        running.set(false);
    }

    @Test
    @DisplayName("P1-8 回归：处理失败的消息必须被交回 handleFailure 路由，不得静默丢弃")
    void failedMessagesAreRoutedBackInsteadOfDropped() throws Exception {
        int total = 4;
        CountDownLatch allRouted = new CountDownLatch(total);
        AtomicBoolean running = new AtomicBoolean(true);

        InflightSink sink =
                newSink(
                        16,
                        new MessageProcessor() {
                            @Override
                            public void processMessage(
                                    Message<?> message,
                                    ListenerRegistration<?> reg,
                                    StreamMQListener listener) {
                                throw new StackOverflowError("simulated Error, not Exception");
                            }

                            @Override
                            public void handleFailure(
                                    Message<?> message,
                                    ListenerRegistration<?> reg,
                                    StreamMQListener listener,
                                    Throwable cause) {
                                routedFailures.add(message);
                                allRouted.countDown();
                            }

                            @Override
                            public void setMetrics(
                                    io.github.streammq.core.metrics.StreamMQMetrics metrics) {}

                            @Override
                            public void setRuntimeStats(
                                    io.github.streammq.adapter.redisson.metrics.RuntimeStatsRegistry
                                            runtimeStats) {}

                            @Override
                            public void setTimeoutCancelGraceMillis(long millis) {}

                            @Override
                            public void setExecutor(
                                    java.util.concurrent.ExecutorService executor) {}
                        },
                        running);

        for (int i = 0; i < total; i++) {
            sink.dispatch(msg("m" + i));
        }

        // 关键断言：每条失败消息都被显式路由（旧实现仅 LOG.error 后丢弃，消息既不在内存队列
        // 也不在重试 ZSet，只能等 PelClaimScheduler 空闲阈值才恢复）
        await().atMost(10, TimeUnit.SECONDS).until(() -> allRouted.getCount() == 0);
        assertThat(routedFailures).hasSize(total);

        // Error 也必须存活泵线程：后续消息继续被处理
        running.set(false);
    }

    @Test
    @DisplayName("dispatch 在队列满且运行标志取消后立即返回而非永久阻塞")
    void dispatchStopsOfferingAfterCancel() throws Exception {
        CountDownLatch never = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger processed = new AtomicInteger();

        InflightSink sink =
                newSink(
                        1,
                        processorOf(
                                message -> {
                                    processed.incrementAndGet();
                                    // 泵被卡住：占住处理位，无法再从队列取下一条
                                    never.await();
                                }),
                        running);

        sink.dispatch(msg("fill"));
        // 等泵把 fill 取走并卡在处理器内（此时队列为空、处理位被占用）
        await().atMost(5, TimeUnit.SECONDS).until(() -> processed.get() == 1);
        assertThat(sink.dispatchQueueEmpty()).isTrue();

        // 队列容量 1：occupy 入队后被卡住的泵无法取走
        sink.dispatch(msg("occupy"));
        await().pollDelay(java.time.Duration.ofMillis(100))
                .atMost(2, TimeUnit.SECONDS)
                .until(() -> !sink.dispatchQueueEmpty());

        Thread dispatcher =
                new Thread(
                        () -> {
                            try {
                                sink.dispatch(msg("blocked"));
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
        dispatcher.start();
        // 确认 dispatch 已进入等待（队列满自旋 park）
        await().pollDelay(java.time.Duration.ofMillis(200))
                .atMost(2, TimeUnit.SECONDS)
                .until(
                        () ->
                                dispatcher.getState() == Thread.State.TIMED_WAITING
                                        || dispatcher.getState() == Thread.State.WAITING);

        // 取消运行标志：dispatch 必须立即退出（旧实现 put 永久阻塞）
        running.set(false);
        long start = System.nanoTime();
        dispatcher.join(TimeUnit.SECONDS.toMillis(5));
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertThat(dispatcher.isAlive()).isFalse();
        assertThat(elapsedMillis).isLessThan(3000L);
    }

    /** 测试用空监督者：登记调用为 no-op。 */
    private static final class NoopSupervisor implements ConsumeLoopSupervisor {

        @Override
        public void submitLoops(ListenerRegistration<?> reg) {}

        @Override
        public void registerInflightPump(String key, java.util.concurrent.Future<?> pumpFuture) {
            // no-op：测试中由外部执行器直接驱动泵
        }

        @Override
        public void cancelForRegistration(String key) {}

        @Override
        public void cancelAll() {}
    }
}
