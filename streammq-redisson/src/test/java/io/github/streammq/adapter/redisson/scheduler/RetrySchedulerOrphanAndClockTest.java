/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/**
 * 重试调度器的第六轮红队修复回归测试。
 *
 * <p><b>R2-4（调度时间基准）：</b>扫描侧的到期判定此前用本机时钟，跨主机 NTP 偏差会把重试时长整体平移。本测试注入 可控的「服务器时钟」，断言下发给 Redis 的 {@code
 * ZRANGEBYSCORE ... LIMIT} 上界等于服务器时钟（而非本机时钟）。
 *
 * <p><b>R2-5（孤儿清理 N+1）：</b>孤儿清理此前逐条 {@code EXISTS}（1000 条 = 1000 次往返），现在由单条 Lua 在
 * 服务端完成。本测试断言清理过程「不触碰任何 payload Hash 键」且脚本被调用（即批量下发）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("重试调度：服务器时钟基准与孤儿清理批量往返")
class RetrySchedulerOrphanAndClockTest {

    private static final String NAMESPACE = "retry-clock-ns";
    private static final String TOPIC = "retry-clock-topic";
    private static final String GROUP = "retry-clock-group";
    private static final int BATCH_SIZE = 10;

    /** 可辨识的假服务器时钟（远大于本机 now，避免与真实时钟混淆） */
    private static final long FAKE_SERVER_NOW = 1_800_000_000_000L;

    private RedissonClient redisson;
    private RScoredSortedSet<String> zset;
    private RScript script;
    private RetryScheduler scheduler;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisson = mock(RedissonClient.class);
        zset = mock(RScoredSortedSet.class);
        script = mock(RScript.class);
        doReturn(zset)
                .when(redisson)
                .<String>getScoredSortedSet(
                        StreamMQKeys.retryZSet(NAMESPACE, TOPIC, GROUP), StringCodec.INSTANCE);
        doReturn(script).when(redisson).getScript(StringCodec.INSTANCE);
        scheduler = new RetryScheduler(redisson, NAMESPACE, 1000L, BATCH_SIZE);
    }

    @Test
    @DisplayName("R2-4：到期扫描窗口上界 = Redis 服务器时钟（不是本机时钟）")
    @SuppressWarnings("unchecked")
    void scanRetryEntries_usesRedisServerClock() {
        RetryScheduler spy = spy(scheduler);
        doReturn(FAKE_SERVER_NOW).when(spy).scheduleClockMillis();
        when(zset.valueRange(anyDouble(), eq(true), anyDouble(), eq(true), anyInt(), anyInt()))
                .thenReturn(List.of());

        spy.scanRetryEntries(new RetryScheduler.RetryTarget(NAMESPACE, TOPIC, GROUP, 3));

        // 下发给 Redis 的扫描上界必须等于服务器时钟：本机时钟偏差不再平移重试到期时刻
        verify(zset)
                .valueRange(
                        eq(0.0),
                        eq(true),
                        eq((double) FAKE_SERVER_NOW),
                        eq(true),
                        eq(0),
                        eq(BATCH_SIZE));
    }

    @Test
    @DisplayName("R2-4：退避回写 score 同样基于服务器时钟")
    @SuppressWarnings("unchecked")
    void requeueBackoff_usesRedisServerClock() {
        RetryScheduler spy = spy(scheduler);
        doReturn(FAKE_SERVER_NOW).when(spy).scheduleClockMillis();
        spy.setFailureRequeueBackoffMs(1234L);
        when(zset.valueRange(anyDouble(), eq(true), anyDouble(), eq(true), anyInt(), anyInt()))
                .thenReturn(List.of("m-1"));
        RBucket<String> claim = mock(RBucket.class);
        when(claim.setIfAbsent(anyString(), any(Duration.class))).thenReturn(true);
        doReturn(claim).when(redisson).getBucket(anyString(), eq(StringCodec.INSTANCE));
        RMap<String, String> payload = mock(RMap.class);
        when(payload.readAllMap())
                .thenReturn(new LinkedHashMap<>(Map.of(RetryScheduler.FIELD_TARGET_TOPIC, TOPIC)));
        doReturn(payload).when(redisson).getMap(anyString(), eq(StringCodec.INSTANCE));
        // redisson.createBatch 未 stub（返回 null）→ doTransfer 抛异常 → 走退避回写

        spy.scanRetryEntries(new RetryScheduler.RetryTarget(NAMESPACE, TOPIC, GROUP, 3));

        verify(zset).add(eq((double) (FAKE_SERVER_NOW + 1234L)), eq("m-1"));
    }

    @Test
    @DisplayName("R2-4：服务器时钟不可用时回退本机时钟（不阻塞调度）")
    void scheduleClockMillis_fallsBackToLocalClock() {
        // RScript mock 默认返回 null → RedisServerClock 判定 UNKNOWN
        long before = System.currentTimeMillis();

        long result = scheduler.scheduleClockMillis();

        assertThat(result).isBetween(before, System.currentTimeMillis());
    }

    @Test
    @DisplayName("R2-4：服务器时钟可读时取服务器值")
    void scheduleClockMillis_prefersServerClock() {
        doReturn(FAKE_SERVER_NOW)
                .when(script)
                .eval(
                        any(RScript.Mode.class),
                        anyString(),
                        eq(RScript.ReturnType.INTEGER),
                        anyList());

        assertThat(scheduler.scheduleClockMillis()).isEqualTo(FAKE_SERVER_NOW);
    }

    @Test
    @DisplayName("R2-5：孤儿清理单次 Lua 批量下发，不再逐条 isExists")
    void cleanupOrphanedEntries_singleBatchRoundTrip() {
        scheduler.registerRetryTarget(TOPIC, GROUP, 3);
        doReturn(List.of(3L, 2L))
                .when(script)
                .eval(
                        any(RScript.Mode.class),
                        anyString(),
                        eq(RScript.ReturnType.MULTI),
                        anyList(),
                        any(),
                        any());

        scheduler.cleanupOrphanedEntries();

        // 每个重试目标一次批量脚本（O(1) 往返），且过程中不得逐条读取 payload Hash
        verify(script, times(1))
                .eval(
                        any(RScript.Mode.class),
                        anyString(),
                        eq(RScript.ReturnType.MULTI),
                        anyList(),
                        any(),
                        any());
        verify(redisson, never()).getMap(anyString(), any(org.redisson.client.codec.Codec.class));
    }
}
