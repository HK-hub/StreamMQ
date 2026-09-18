/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.core.consumer.ConsumeOrderlyContext;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.exception.OrderlyShardBusyException;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

/**
 * {@link RedissonOrderlyShardLockManager} 分片锁竞争语义单元测试（红队审查 R4-B02 / R3-25）。
 *
 * <p>核心不变式：<b>拿不到锁 ≠ 业务失败</b>——预算内多轮等待仍失败必须抛 {@link
 * OrderlyShardBusyException}（消息未被处理、不得消耗重试预算），而不是返回 {@code RECONSUME_LATER} 让调用方误计一次业务尝试。
 *
 * <p>纯 Mockito，不依赖 Redis。
 */
@DisplayName("分片锁管理器：竞争信号与预算语义")
@SuppressWarnings({"unchecked", "rawtypes"})
class RedissonOrderlyShardLockManagerTest {

    /** 构造一个 shardCount=1、分片锁为传入 mock 的注册信息。 */
    private static ListenerRegistration<?> registration(RLock lock) {
        ListenerRegistration<?> reg = mock(ListenerRegistration.class);
        when(reg.getShardCount()).thenReturn(1);
        when(reg.getShardLocks()).thenReturn(List.of((Lock) lock));
        when(reg.getTopic()).thenReturn("t-busy");
        when(reg.getGroup()).thenReturn("g-busy");
        return reg;
    }

    /** 真实 Message 值对象（shardingKey 固定，保证路由到 shard=0）。 */
    private static Message<?> message() {
        return MessageBuilder.<String>withTopic("t-busy").shardingKey("k1").body("b").build();
    }

    @Test
    @DisplayName("多轮等待均未获锁：抛 OrderlyShardBusyException,轮数等于配置值,handler 从不执行")
    void allRoundsBusy_throwsBusyWithoutInvokingHandler() throws Exception {
        RedissonClient redisson = mock(RedissonClient.class);
        RLock lock = mock(RLock.class);
        when(lock.tryLock(anyLong(), any(TimeUnit.class))).thenReturn(false);

        RedissonOrderlyShardLockManager manager =
                new RedissonOrderlyShardLockManager(redisson, 2, 5L);
        manager.setAcquireTimeoutMs(50L);

        ListenerRegistration<?> reg = registration(lock);
        StreamMessageOrderlyConsumer orderly = mock(StreamMessageOrderlyConsumer.class);
        ConsumeOrderlyContext ctx = mock(ConsumeOrderlyContext.class);

        assertThatThrownBy(() -> manager.consumeWithShardLock(message(), reg, ctx, orderly))
                .isInstanceOf(OrderlyShardBusyException.class)
                .hasMessageContaining("shard=0");

        // 每轮一次 tryLock；竞争路径绝不调用业务 handler
        verify(lock, times(2)).tryLock(anyLong(), any(TimeUnit.class));
        verify(orderly, never()).onMessage(any(), any());
    }

    @Test
    @DisplayName("第二轮获得锁：正常执行 handler 并返回其结果,不抛竞争异常")
    void lockAcquiredOnSecondRound_invokesHandlerOnce() throws Exception {
        RedissonClient redisson = mock(RedissonClient.class);
        RLock lock = mock(RLock.class);
        when(lock.tryLock(anyLong(), any(TimeUnit.class))).thenReturn(false, true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);

        RedissonOrderlyShardLockManager manager =
                new RedissonOrderlyShardLockManager(redisson, 3, 5L);
        manager.setAcquireTimeoutMs(50L);

        ListenerRegistration<?> reg = registration(lock);
        StreamMessageOrderlyConsumer orderly = mock(StreamMessageOrderlyConsumer.class);
        when(orderly.onMessage(any(), any())).thenReturn(ConsumeAction.SUCCESS);
        ConsumeOrderlyContext ctx = mock(ConsumeOrderlyContext.class);

        assertThat(manager.consumeWithShardLock(message(), reg, ctx, orderly))
                .isEqualTo(ConsumeAction.SUCCESS);
        verify(lock, times(2)).tryLock(anyLong(), any(TimeUnit.class));
        verify(orderly, times(1)).onMessage(any(), any());
        verify(lock, times(1)).unlock();
    }

    @Test
    @DisplayName("等待锁期间被中断(超时取消/停机):抛竞争异常并恢复中断标志,不误计为业务尝试")
    void interruptedDuringWait_throwsBusyAndRestoresInterruptFlag() throws Exception {
        RedissonClient redisson = mock(RedissonClient.class);
        RLock lock = mock(RLock.class);
        try {
            when(lock.tryLock(anyLong(), any(TimeUnit.class)))
                    .thenThrow(new InterruptedException("consume timeout cancel"));
        } catch (InterruptedException ignored) {
            // 打桩阶段不应被中断
        }

        RedissonOrderlyShardLockManager manager =
                new RedissonOrderlyShardLockManager(redisson, 3, 5L);
        ListenerRegistration<?> reg = registration(lock);
        StreamMessageOrderlyConsumer orderly = mock(StreamMessageOrderlyConsumer.class);
        ConsumeOrderlyContext ctx = mock(ConsumeOrderlyContext.class);

        try {
            assertThatThrownBy(() -> manager.consumeWithShardLock(message(), reg, ctx, orderly))
                    .isInstanceOf(OrderlyShardBusyException.class)
                    .hasMessageContaining("interrupted");
            assertThat(Thread.currentThread().isInterrupted()).as("中断标志必须恢复：调用方可感知取消信号").isTrue();
        } finally {
            Thread.interrupted(); // 清除标志，避免污染后续测试（JUnit 工作线程复用）
        }
        verify(orderly, never()).onMessage(any(), any());
    }

    @Test
    @DisplayName("无分片锁(shardCount<=0):直接消费,不触碰锁与竞争异常")
    void noShardLocks_consumesDirectly() throws Exception {
        RedissonClient redisson = mock(RedissonClient.class);
        RedissonOrderlyShardLockManager manager = new RedissonOrderlyShardLockManager(redisson);

        ListenerRegistration<?> reg = mock(ListenerRegistration.class);
        when(reg.getShardCount()).thenReturn(0);
        when(reg.getShardLocks()).thenReturn(null);
        StreamMessageOrderlyConsumer orderly = mock(StreamMessageOrderlyConsumer.class);
        when(orderly.onMessage(any(), any())).thenReturn(ConsumeAction.SUCCESS);
        ConsumeOrderlyContext ctx = mock(ConsumeOrderlyContext.class);

        assertThat(manager.consumeWithShardLock(message(), reg, ctx, orderly))
                .isEqualTo(ConsumeAction.SUCCESS);
        verify(orderly, times(1)).onMessage(any(), any());
    }
}
