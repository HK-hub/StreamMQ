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
}
