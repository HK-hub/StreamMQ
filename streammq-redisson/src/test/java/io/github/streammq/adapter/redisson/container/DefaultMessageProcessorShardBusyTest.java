/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.adapter.redisson.support.MdcKeys;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.exception.OrderlyShardBusyException;
import io.github.streammq.core.interceptor.ConsumerInterceptorChain;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.ListenerType;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.policy.OrderlyShardLockManager;
import io.github.streammq.core.policy.RetryAndDlqHandler;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link DefaultMessageProcessor} 顺序消费分片锁竞争语义单元测试（红队审查 R4-B02 / R3-25）。
 *
 * <p>缺陷场景：锁管理器在拿不到锁时返回 {@code RECONSUME_LATER}，处理器把它与业务失败同等对待—— {@code attempt++} 直到耗尽 {@code
 * maxReconsumeTimes}，随后 {@code routeToDlq} 且转投成功后 {@code ack}。于是"分片锁被其它实例持有"这种竞争会耗尽重试预算，把<b>从未执行
 * handler</b> 的消息 ACK 进 DLQ。
 *
 * <p>本测试以 mock 的分片锁管理器持续抛 {@link OrderlyShardBusyException} 模拟竞争，断言：
 *
 * <ul>
 *   <li>预算未消耗：{@code maxReconsumeTimes=2} 时每条消息只发生一次锁获取（旧行为为 1+2=3 次）
 *   <li>不发生 DLQ 转投、不 ACK、不进入 ACK/重试路由
 *   <li>不执行 {@code suspendCurrentQueueTimeMillis} 挂起等待（竞争路径直接返回）
 * </ul>
 *
 * <p>纯 Mockito，不依赖 Redis 与真实锁。
 */
@DisplayName("顺序消费分片锁竞争：不消耗重试预算")
@SuppressWarnings({"unchecked", "rawtypes"})
class DefaultMessageProcessorShardBusyTest {

    @Test
    @DisplayName("maxReconsumeTimes=2 且连续 3 次竞争：无 DLQ 转投、无 ACK、无挂起等待")
    void shardBusy_neverConsumesBudgetNorRoutesToDlq() throws Exception {
        ConsumerInterceptorChain chain = mock(ConsumerInterceptorChain.class);
        when(chain.applyBefore(any(), any())).thenReturn(true);

        OrderlyShardLockManager lockManager = mock(OrderlyShardLockManager.class);
        when(lockManager.consumeWithShardLock(any(), any(), any(), any()))
                .thenThrow(new OrderlyShardBusyException("shard lock busy (unit test)"));

        RetryAndDlqHandler handler = mock(RetryAndDlqHandler.class);
        StreamMQListener listener = mock(StreamMQListener.class);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            DefaultMessageProcessor processor =
                    new DefaultMessageProcessor(
                            chain,
                            lockManager,
                            new DefaultRegistrationStore(),
                            handler,
                            /* perConsumerEnabled */ false,
                            executor);
            StreamMessageOrderlyConsumer<String> orderly = (msg, ctx) -> ConsumeAction.SUCCESS;
            ListenerRegistration<String> reg =
                    ListenerRegistration.<String>builder()
                            .type(ListenerType.ORDERLY)
                            .consumer(orderly)
                            .topic("t-shard-busy")
                            .group("g-shard-busy")
                            .maxReconsumeTimes(2)
                            .suspendCurrentQueueTimeMillis(2_000L)
                            .orderlyConsumeTimeoutMillis(0L)
                            .shardCount(4)
                            .shardLocks(List.of(mock(Lock.class)))
                            .build();
            Message<String> message =
                    MessageBuilder.<String>withTopic("t-shard-busy").body("b").build();

            long startNanos = System.nanoTime();
            for (int i = 0; i < 3; i++) {
                processor.processMessage(message, reg, listener);
            }
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            // 预算未消耗：每条消息仅一次锁获取；旧行为下竞争会被计为业务尝试（1 + 2 = 3 次/条）
            verify(lockManager, times(3)).consumeWithShardLock(any(), any(), any(), any());
            // 不进 DLQ、不 ACK、不触碰 ACK/重试路由（handler 与 listener 零交互）
            verify(handler, never()).routeToDlq(any(), any(), any(), any());
            verify(handler, never()).handleAction(any(), any(), any(), any(), any());
            verify(listener, never()).ack(any());
            // 不执行 suspendCurrentQueueTimeMillis(2s) 挂起等待：3 条消息应瞬时返回
            assertThat(elapsedMillis).as("竞争路径不得进入挂起等待，预算不得被消耗").isLessThan(1_500L);
        } finally {
            executor.shutdownNow();
        }
    }

    // ===================== 红队第六轮 R1-1：延迟重投闭环 =====================

    @Test
    @DisplayName("R1-1：竞争登记延迟重投；到期重投走正常管线并只 ACK 一次（旧实现消息永不重投）")
    void shardBusy_registersDeferredRetry_redeliveryAcksOnce() throws Exception {
        ConsumerInterceptorChain chain = mock(ConsumerInterceptorChain.class);
        when(chain.applyBefore(any(), any())).thenReturn(true);

        OrderlyShardLockManager lockManager = mock(OrderlyShardLockManager.class);
        // 首次竞争（锁被占用）→ 重投时锁已空闲（SUCCESS）
        when(lockManager.consumeWithShardLock(any(), any(), any(), any()))
                .thenThrow(new OrderlyShardBusyException("shard lock busy (unit test)"))
                .thenReturn(ConsumeAction.SUCCESS);

        RetryAndDlqHandler handler = mock(RetryAndDlqHandler.class);
        StreamMQListener listener = mock(StreamMQListener.class);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            DefaultMessageProcessor processor =
                    new DefaultMessageProcessor(
                            chain,
                            lockManager,
                            new DefaultRegistrationStore(),
                            handler,
                            /* perConsumerEnabled */ false,
                            executor);
            OrderlyDeferredRetryQueue queue = new OrderlyDeferredRetryQueue();
            processor.setOrderlyDeferredRetryQueue(queue);

            StreamMessageOrderlyConsumer<String> orderly = (msg, ctx) -> ConsumeAction.SUCCESS;
            ListenerRegistration<String> reg = orderlyReg("t-defer", "g-defer", 0, orderly);
            Message<String> message = orderlyMessage("t-defer", "7-7");

            // 首投：竞争 → 不 ACK、不进 DLQ，但必须登记到延迟重投队列（旧实现只留在 PEL）
            processor.processMessage(message, reg, listener);
            assertThat(queue.size(reg)).as("竞争必须登记本地延迟重投").isEqualTo(1);
            verify(listener, never()).ack(any());
            verify(handler, never()).routeToDlq(any(), any(), any(), any());
            verify(handler, never()).handleAction(any(), any(), any(), any(), any());

            // 驱动注册的 primary 循环的重投（等价于 ConsumeLoopTask.drainDeferredRetries）
            queue.drainDue(
                    reg,
                    System.currentTimeMillis() + 60_000L,
                    listener,
                    (entry, r, l) -> processor.processMessage(entry.message(), r, l));

            assertThat(queue.size(reg)).as("重投成功条目必须移除").isZero();
            verify(lockManager, times(2)).consumeWithShardLock(any(), any(), any(), any());
            // 成功处理走既有 ACK 路径（真实 handler 内部 ack；此处以 SUCCESS 路由恰好一次为证）
            verify(handler, times(1))
                    .handleAction(any(ConsumeAction.class), any(), any(), any(), any());
            verify(handler, never()).routeToDlq(any(), any(), any(), any());
        } finally {
            executor.shutdownNow();
        }
    }

    // ===================== 红队第六轮 R1-2：ORDERLY + defer 语义 =====================

    @Test
    @DisplayName("R1-2：ORDERLY 返回 defer(delay) → 按延迟登记重投，不消耗预算、不进 DLQ、不原地重试")
    void orderlyDefer_scheduledWithoutBudgetOrDlq() throws Exception {
        ConsumerInterceptorChain chain = mock(ConsumerInterceptorChain.class);
        when(chain.applyBefore(any(), any())).thenReturn(true);

        OrderlyShardLockManager lockManager = mock(OrderlyShardLockManager.class);
        when(lockManager.consumeWithShardLock(any(), any(), any(), any()))
                .thenReturn(ConsumeAction.defer(Duration.ofMillis(250L)));

        RetryAndDlqHandler handler = mock(RetryAndDlqHandler.class);
        StreamMQListener listener = mock(StreamMQListener.class);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            DefaultMessageProcessor processor =
                    new DefaultMessageProcessor(
                            chain,
                            lockManager,
                            new DefaultRegistrationStore(),
                            handler,
                            false,
                            executor);
            OrderlyDeferredRetryQueue queue = new OrderlyDeferredRetryQueue();
            processor.setOrderlyDeferredRetryQueue(queue);

            StreamMessageOrderlyConsumer<String> orderly = (msg, ctx) -> ConsumeAction.SUCCESS;
            // maxReconsumeTimes=0：旧语义下 DEFER 被当作失败 → 立即进 DLQ
            ListenerRegistration<String> reg = orderlyReg("t-odefer", "g-odefer", 0, orderly);
            Message<String> message = orderlyMessage("t-odefer", "8-8");

            processor.processMessage(message, reg, listener);

            assertThat(queue.size(reg)).as("DEFER 必须登记延迟重投").isEqualTo(1);
            long now = System.currentTimeMillis();
            assertThat(queue.dueCount(reg, now + 100L)).as("defer 延迟内不得提前重投").isZero();
            assertThat(queue.dueCount(reg, now + 300L)).isEqualTo(1);
            verify(lockManager, times(1))
                    .consumeWithShardLock(any(), any(), any(), any()); // 不原地 sleep 重试
            verify(handler, never()).routeToDlq(any(), any(), any(), any());
            verify(listener, never()).ack(any());
        } finally {
            executor.shutdownNow();
        }
    }

    // ===================== 红队第六轮 R1-10：超时包装下的 MDC =====================

    @Test
    @DisplayName("R1-10：消费超时包装（虚拟线程）下 handler 仍可见 topic/groupId/messageId 的 MDC")
    void timeoutWrapper_propagatesMdcToHandlerThread() throws Exception {
        ConsumerInterceptorChain chain = mock(ConsumerInterceptorChain.class);
        when(chain.applyBefore(any(), any())).thenReturn(true);

        OrderlyShardLockManager lockManager = mock(OrderlyShardLockManager.class);
        RetryAndDlqHandler handler = mock(RetryAndDlqHandler.class);
        StreamMQListener listener = mock(StreamMQListener.class);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            DefaultMessageProcessor processor =
                    new DefaultMessageProcessor(
                            chain,
                            lockManager,
                            new DefaultRegistrationStore(),
                            handler,
                            false,
                            executor);

            java.util.concurrent.atomic.AtomicReference<java.util.Map<String, String>> mdcRef =
                    new java.util.concurrent.atomic.AtomicReference<>();
            io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer<String> consumer =
                    (msg, ctx) -> {
                        mdcRef.set(org.slf4j.MDC.getCopyOfContextMap());
                        return ConsumeAction.SUCCESS;
                    };
            ListenerRegistration<String> reg =
                    ListenerRegistration.<String>builder()
                            .type(ListenerType.AUTO_ACK)
                            .consumer(consumer)
                            .topic("t-mdc")
                            .group("g-mdc")
                            .consumeTimeoutMillis(5_000L)
                            .maxReconsumeTimes(0)
                            .build();
            Message<String> message = orderlyMessage("t-mdc", "9-9");

            processor.processMessage(message, reg, listener);

            java.util.Map<String, String> mdc = mdcRef.get();
            assertThat(mdc).isNotNull();
            assertThat(mdc)
                    .containsEntry(MdcKeys.TOPIC, "t-mdc")
                    .containsEntry(MdcKeys.CONSUMER_GROUP, "g-mdc")
                    .containsEntry(MdcKeys.MSG_ID, "9-9");
        } finally {
            executor.shutdownNow();
        }
    }

    // ===================== 夹具 =====================

    private static Message<String> orderlyMessage(String topic, String messageId) {
        return MessageBuilder.<String>withTopic(topic)
                .body("b")
                .messageId(io.github.streammq.core.message.MessageId.fromStreamEntry(messageId))
                .build();
    }

    private static ListenerRegistration<String> orderlyReg(
            String topic,
            String group,
            int maxReconsumeTimes,
            StreamMessageOrderlyConsumer<String> orderly) {
        return ListenerRegistration.<String>builder()
                .type(ListenerType.ORDERLY)
                .consumer(orderly)
                .topic(topic)
                .group(group)
                .maxReconsumeTimes(maxReconsumeTimes)
                .suspendCurrentQueueTimeMillis(0L)
                .orderlyConsumeTimeoutMillis(0L)
                .shardCount(4)
                .shardLocks(List.of(mock(Lock.class)))
                .build();
    }
}
