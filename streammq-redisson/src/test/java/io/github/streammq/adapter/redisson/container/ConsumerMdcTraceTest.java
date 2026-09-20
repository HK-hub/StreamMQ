/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.adapter.redisson.support.MdcKeys;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

/**
 * 消费侧 MDC 跨线程重放回归测试（发布前红队审查 R4-x4）。
 *
 * <p>缺陷背景：{@code inject/clear} 只作用于调用线程。消费超时保护把 {@code onMessage} 提交到另一个线程
 * （虚拟线程/线程池）执行，业务日志在新线程里读不到 topic/group/msgId 等上下文，超时路径的可观测性丢失。 本测试锁定 {@link
 * ConsumerMdcTrace#wrap(Runnable)} / {@link ConsumerMdcTrace#wrap(Callable)} 的 「快照 → 新线程重放 →
 * 执行后清理」语义。
 */
@DisplayName("消费侧 MDC 跨线程重放")
class ConsumerMdcTraceTest {

    private static final String TOPIC = "trade-topic";
    private static final String GROUP = "trade-group";

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    /** 模拟读循环线程上的消费侧注入（inject 的直接后果）。 */
    private static void injectCurrentThread() {
        MDC.put(MdcKeys.TOPIC, TOPIC);
        MDC.put(MdcKeys.CONSUMER_GROUP, GROUP);
        MDC.put(MdcKeys.MSG_ID, "1700000000000-1");
    }

    @Test
    @DisplayName("snapshot/replay：快照携带消费上下文，重放后新线程可见")
    void snapshotAndReplay_areVisibleInAnotherThread() throws Exception {
        injectCurrentThread();

        Map<String, String> snapshot = ConsumerMdcTrace.snapshot();
        assertThat(snapshot)
                .containsEntry(MdcKeys.TOPIC, TOPIC)
                .containsEntry(MdcKeys.CONSUMER_GROUP, GROUP)
                .containsEntry(MdcKeys.MSG_ID, "1700000000000-1");

        AtomicReference<Map<String, String>> seen = new AtomicReference<>();
        Thread other =
                new Thread(
                        () -> {
                            ConsumerMdcTrace.replay(snapshot);
                            seen.set(ConsumerMdcTrace.snapshot());
                            ConsumerMdcTrace.clear();
                        });
        other.start();
        other.join();

        assertThat(seen.get())
                .containsEntry(MdcKeys.TOPIC, TOPIC)
                .containsEntry(MdcKeys.CONSUMER_GROUP, GROUP);
    }

    @Test
    @DisplayName("snapshot：缺失的上下文键不进入快照，空上下文返回空 Map")
    void snapshot_omitsAbsentKeys() {
        assertThat(ConsumerMdcTrace.snapshot()).isEmpty();

        MDC.put(MdcKeys.TOPIC, TOPIC);
        assertThat(ConsumerMdcTrace.snapshot())
                .containsOnlyKeys(MdcKeys.TOPIC)
                .containsEntry(MdcKeys.TOPIC, TOPIC);
    }

    @Test
    @DisplayName("wrap(Runnable)：新线程内可见快照，执行结束后线程内被清理")
    void wrapRunnable_replaysAndClears() throws Exception {
        injectCurrentThread();

        AtomicReference<Map<String, String>> seenInside = new AtomicReference<>();
        AtomicReference<Map<String, String>> seenAfter = new AtomicReference<>();
        Runnable wrapped = ConsumerMdcTrace.wrap(() -> seenInside.set(ConsumerMdcTrace.snapshot()));

        // 模拟"线程池复用"：同一线程先跑包装任务，再跑一个裸任务观察残留
        Thread worker =
                new Thread(
                        () -> {
                            wrapped.run();
                            // 包装任务内部执行完后必须自清理 —— 用同一个 worker 线程观察
                            seenAfter.set(ConsumerMdcTrace.snapshot());
                        });
        worker.start();
        worker.join();

        assertThat(seenInside.get())
                .containsEntry(MdcKeys.TOPIC, TOPIC)
                .containsEntry(MdcKeys.CONSUMER_GROUP, GROUP)
                .containsEntry(MdcKeys.MSG_ID, "1700000000000-1");
        assertThat(seenAfter.get()).isEmpty();
    }

    @Test
    @DisplayName("wrap(Runnable)：任务抛异常时仍然清理（finally 语义）")
    void wrapRunnable_clearsEvenOnFailure() throws Exception {
        injectCurrentThread();

        AtomicReference<Map<String, String>> seenAfter = new AtomicReference<>();
        // 显式声明为 Runnable：throw-only 的块 lambda 同时兼容 Runnable/Callable 两个重载，
        // 直接内联会触发歧义解析，这里用类型化的局部变量锁定意图
        Runnable alwaysThrows =
                () -> {
                    throw new IllegalStateException("boom");
                };
        Runnable wrapped = ConsumerMdcTrace.wrap(alwaysThrows);

        Thread worker =
                new Thread(
                        () -> {
                            try {
                                wrapped.run();
                            } catch (IllegalStateException expected) {
                                // 异常继续向上传播是契约的一部分
                            }
                            seenAfter.set(ConsumerMdcTrace.snapshot());
                        });
        worker.start();
        worker.join();

        assertThat(seenAfter.get()).isEmpty();
    }

    @Test
    @DisplayName("wrap(Callable)：超时包装路径（executor.submit）在新线程可见上下文并返回结果")
    void wrapCallable_replaysInPooledThreadAndClears() throws Exception {
        injectCurrentThread();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Callable<Map<String, String>> task = ConsumerMdcTrace.wrap(ConsumerMdcTrace::snapshot);
            Future<Map<String, String>> future = executor.submit(task);

            assertThat(future.get(5, TimeUnit.SECONDS))
                    .containsEntry(MdcKeys.TOPIC, TOPIC)
                    .containsEntry(MdcKeys.CONSUMER_GROUP, GROUP);

            // 同一池化线程被复用时不得残留上一任务的上下文
            Future<Map<String, String>> next = executor.submit(ConsumerMdcTrace::snapshot);
            assertThat(next.get(5, TimeUnit.SECONDS)).isEmpty();
        } finally {
            executor.shutdownNow();
        }
    }
}
