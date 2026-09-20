/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.core.consumer.ConsumeOrderlyContext;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

/**
 * {@link RedissonOrderlyShardLockManager} 锁租约单元测试（红队第六轮 R1-9）。
 *
 * <p>缺陷背景：默认看门狗续期下，卡死 handler（持锁且不响应中断）在进程存活期间永不释放—— 该分片只能重启进程才能恢复。新增"逃生舱"配置 {@code
 * orderly-shard-lock-lease-millis > 0} 时， 必须改用带 lease 的 {@code tryLock(wait, lease, unit)}
 * 且不续期（Redisson 不启动 watchdog）， 使租约到期后其它实例可接管（语义降级为"至多一次重叠执行、可能乱序"）。
 */
@DisplayName("顺序消费分片锁租约（R1-9）")
@SuppressWarnings({"unchecked", "rawtypes"})
class RedissonOrderlyShardLockManagerLeaseTest {

    @Test
    @DisplayName("lease>0：调用带 lease 的 tryLock(wait, lease, unit)，不调用看门狗续期版本")
    void leasePositive_usesTryLockWithLease() throws Exception {
        RedissonClient redisson = mock(RedissonClient.class);
        RLock lock = mock(RLock.class);
        when(redisson.getLock(anyString())).thenReturn(lock);
        when(lock.tryLock(anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);

        RedissonOrderlyShardLockManager manager = new RedissonOrderlyShardLockManager(redisson);
        manager.setLeaseMillis(30_000L);
        assertThat(manager.getLeaseMillis()).isEqualTo(30_000L);

        ListenerRegistration<?> reg = regWithLocks(manager, "lease-topic", "lease-group", 1);
        Message<?> message =
                MessageBuilder.<String>withTopic("lease-topic").shardingKey("k1").body("b").build();
        StreamMessageOrderlyConsumer<String> orderly = (msg, ctx) -> ConsumeAction.SUCCESS;

        ConsumeAction action =
                manager.consumeWithShardLock(
                        message, reg, mock(ConsumeOrderlyContext.class), orderly);

        assertThat(action).isEqualTo(ConsumeAction.SUCCESS);
        verify(lock, times(1))
                .tryLock(
                        eq(managerAcquireTimeout(manager)), eq(30_000L), eq(TimeUnit.MILLISECONDS));
        // 不续期：绝不调用 watchdog 版本的 tryLock(wait, unit)
        verify(lock, never()).tryLock(anyLong(), any(TimeUnit.class));
    }

    @Test
    @DisplayName("lease=0（默认）：仍使用看门狗续期版本 tryLock(wait, unit)，严格有序语义不变")
    void leaseZero_keepsWatchdogTryLock() throws Exception {
        RedissonClient redisson = mock(RedissonClient.class);
        RLock lock = mock(RLock.class);
        when(redisson.getLock(anyString())).thenReturn(lock);
        when(lock.tryLock(anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);

        RedissonOrderlyShardLockManager manager = new RedissonOrderlyShardLockManager(redisson);
        assertThat(manager.getLeaseMillis()).isZero();

        ListenerRegistration<?> reg = regWithLocks(manager, "watchdog-topic", "watchdog-group", 1);
        Message<?> message =
                MessageBuilder.<String>withTopic("watchdog-topic")
                        .shardingKey("k1")
                        .body("b")
                        .build();
        StreamMessageOrderlyConsumer<String> orderly = (msg, ctx) -> ConsumeAction.SUCCESS;

        manager.consumeWithShardLock(message, reg, mock(ConsumeOrderlyContext.class), orderly);

        verify(lock, times(1)).tryLock(anyLong(), eq(TimeUnit.MILLISECONDS));
        verify(lock, never()).tryLock(anyLong(), anyLong(), any(TimeUnit.class));
    }

    private static long managerAcquireTimeout(RedissonOrderlyShardLockManager manager) {
        // 默认 acquireTimeout 为常量值；这里用反射无关的方式断言：lease=30s 的 3 参调用已足够，
        // 单独断言 wait 参数保留默认值（5s）
        return RedissonOrderlyShardLockManager.DEFAULT_ACQUIRE_TIMEOUT_MS;
    }

    private static ListenerRegistration<?> regWithLocks(
            RedissonOrderlyShardLockManager manager, String topic, String group, int shardCount) {
        Lock[] locks = manager.createShardLocks("ns", topic, group, null, shardCount);
        ListenerRegistration<?> reg = mock(ListenerRegistration.class);
        when(reg.getTopic()).thenReturn(topic);
        when(reg.getGroup()).thenReturn(group);
        when(reg.getShardCount()).thenReturn(shardCount);
        when(reg.getShardLocks()).thenReturn(java.util.Arrays.asList(locks));
        return reg;
    }
}
