/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.interceptor.ConsumerInterceptorChain;
import io.github.streammq.core.listener.InFlightAware;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.ListenerType;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.OrderlyShardLockManager;
import io.github.streammq.core.policy.RetryAndDlqHandler;
import io.github.streammq.core.policy.RetryPolicy;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

/**
 * K2 在途消息计数契约（{@link InFlightAware}）单元测试。
 *
 * <p>缺陷背景：契约此前定义在 kubernetes 模块，redisson 容器不可能实现它（依赖方向不允许），优雅关闭的 in-flight 判据是死代码。契约迁到 core 后由
 * {@link DefaultStreamMQListenerContainer} 真实实现，计数点在 {@link
 * DefaultMessageProcessor#processMessage}（所有执行 handler 路径的唯一收口）。
 *
 * <p>失败即红：删除计数或容器委托后，阻塞 handler 期间的断言（计数 = 1 / = 并发数）与 {@code instanceof InFlightAware}
 * 断言立即失败；未阻塞场景无法通过"恒 0"实现伪装。
 *
 * <p>纯 Mockito + 虚拟线程，不依赖 Redis / mock server。
 */
@DisplayName("K2 在途消息计数契约")
@SuppressWarnings({"unchecked", "rawtypes"})
class InFlightCountTrackingTest {

    @Test
    @DisplayName("handler 执行期间计数为 1，handler 返回后归零（finally 自减）")
    void inFlightCount_reflectsBlockedHandlerAndResets() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        StreamMessageConcurrentlyConsumer<String> consumer =
                (msg, ctx) -> {
                    entered.countDown();
                    release.await(5, TimeUnit.SECONDS);
                    return ConsumeAction.SUCCESS;
                };
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            DefaultMessageProcessor processor = newProcessor(executor);
            ListenerRegistration<String> reg = concurrentReg("t-inflight", "g-inflight", consumer);
            Message<String> message = message("t-inflight", "1-1");

            Thread worker = new Thread(() -> processor.processMessage(message, reg, listener()));
            worker.start();
            assertThat(entered.await(5, TimeUnit.SECONDS)).as("handler 应已进入").isTrue();

            assertThat(processor.inFlightCount()).as("handler 阻塞期间在途计数必须为 1").isEqualTo(1);

            release.countDown();
            worker.join(5_000L);
            assertThat(processor.inFlightCount()).as("handler 完成后必须归零").isZero();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("并发提交时计数等于同时执行 handler 的条数，全部完成后归零（无锁线程安全）")
    void inFlightCount_isThreadSafeUnderConcurrentSubmission() throws Exception {
        int concurrency = 6;
        CountDownLatch entered = new CountDownLatch(concurrency);
        CountDownLatch release = new CountDownLatch(1);
        StreamMessageConcurrentlyConsumer<String> consumer =
                (msg, ctx) -> {
                    entered.countDown();
                    release.await(5, TimeUnit.SECONDS);
                    return ConsumeAction.SUCCESS;
                };
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            DefaultMessageProcessor processor = newProcessor(executor);
            ListenerRegistration<String> reg = concurrentReg("t-parallel", "g-parallel", consumer);
            Message<String> message = message("t-parallel", "2-2");

            List<Thread> workers = new ArrayList<>();
            for (int i = 0; i < concurrency; i++) {
                Thread worker =
                        new Thread(() -> processor.processMessage(message, reg, listener()));
                workers.add(worker);
                worker.start();
            }
            assertThat(entered.await(5, TimeUnit.SECONDS)).as("全部 handler 应已并发进入").isTrue();

            assertThat(processor.inFlightCount())
                    .as("在途计数必须等于同时执行 handler 的条数")
                    .isEqualTo(concurrency);

            release.countDown();
            for (Thread worker : workers) {
                worker.join(5_000L);
            }
            assertThat(processor.inFlightCount()).as("全部 handler 完成后必须归零").isZero();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("容器实现 core InFlightAware，getInFlightCount 实时委托到处理管线（非恒 0 桩）")
    void container_delegatesInFlightCountToProcessor() throws Exception {
        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        mock(RedissonClient.class),
                        mock(StreamMQListenerFactory.class),
                        mock(MessageConverter.class),
                        mock(RetryPolicy.class),
                        mock(DlqFailureStrategy.class),
                        DlqConfig.builder().build(),
                        "test-namespace");
        assertThat(container).as("容器必须实现 core 契约（否则优雅关闭判据仍是死代码）").isInstanceOf(InFlightAware.class);
        assertThat(container.getInFlightCount()).as("空闲时计数为 0").isZero();

        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        StreamMessageConcurrentlyConsumer<String> consumer =
                (msg, ctx) -> {
                    entered.countDown();
                    release.await(5, TimeUnit.SECONDS);
                    return ConsumeAction.SUCCESS;
                };
        DefaultMessageProcessor processor = messageProcessorOf(container);
        ListenerRegistration<String> reg = concurrentReg("t-delegate", "g-delegate", consumer);
        Message<String> message = message("t-delegate", "3-3");

        Thread worker = new Thread(() -> processor.processMessage(message, reg, listener()));
        worker.start();
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).as("handler 应已进入").isTrue();
            assertThat(container.getInFlightCount()).as("容器读数必须委托到真实处理管线（恒 0 桩在此失败）").isEqualTo(1);
        } finally {
            release.countDown();
            worker.join(5_000L);
        }
        assertThat(container.getInFlightCount()).as("handler 完成后容器读数必须归零").isZero();
    }

    // ===================== 夹具 =====================

    private static DefaultMessageProcessor newProcessor(ExecutorService executor) {
        ConsumerInterceptorChain chain = mock(ConsumerInterceptorChain.class);
        when(chain.applyBefore(any(), any())).thenReturn(true);
        return new DefaultMessageProcessor(
                chain,
                mock(OrderlyShardLockManager.class),
                new DefaultRegistrationStore(),
                mock(RetryAndDlqHandler.class),
                /* perConsumerEnabled */ false,
                executor);
    }

    private static StreamMQListener listener() {
        return mock(StreamMQListener.class);
    }

    private static ListenerRegistration<String> concurrentReg(
            String topic, String group, StreamMessageConcurrentlyConsumer<String> consumer) {
        return ListenerRegistration.<String>builder()
                .type(ListenerType.AUTO_ACK)
                .consumer(consumer)
                .topic(topic)
                .group(group)
                .consumeTimeoutMillis(0L)
                .maxReconsumeTimes(0)
                .build();
    }

    private static Message<String> message(String topic, String streamEntryId) {
        return MessageBuilder.<String>withTopic(topic)
                .body("b")
                .messageId(MessageId.fromStreamEntry(streamEntryId))
                .build();
    }

    /** 读取容器的私有处理管线（容器无公开测试钩子；容器委托正确性是本契约的关键链路）。 */
    private static DefaultMessageProcessor messageProcessorOf(
            DefaultStreamMQListenerContainer container) throws Exception {
        Field field = DefaultStreamMQListenerContainer.class.getDeclaredField("messageProcessor");
        field.setAccessible(true);
        return (DefaultMessageProcessor) field.get(container);
    }
}
