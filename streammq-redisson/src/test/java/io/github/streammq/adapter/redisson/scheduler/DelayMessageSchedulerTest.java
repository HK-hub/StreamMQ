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
import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.metrics.StreamMQMetrics;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.BatchOptions;
import org.redisson.api.RBatch;
import org.redisson.api.RBucket;
import org.redisson.api.RKeys;
import org.redisson.api.RMap;
import org.redisson.api.RMapAsync;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScoredSortedSetAsync;
import org.redisson.api.RScript;
import org.redisson.api.RStreamAsync;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/**
 * 延时 / 重试调度扫描窗口与投递指标的回归测试。
 *
 * <p><b>B-18：</b>{@code ZRANGEBYSCORE ... LIMIT 0 count} 的 count 曾写成 {@code batchSize - 1}，
 * 每轮少转投一条（与 {@code MAX_ORPHAN_ZSET_SCAN} 的有界扫描口径也不一致）。本测试锁定实际下发到 Redis 的 count 等于 batchSize。
 *
 * <p><b>B-20：</b>延时投递指标曾在"未真正投递"时也被计入——claim 被其它实例持有、payload 缺失被隔离、
 * 原子批失败回退都算作投递，指标系统性高估。本测试锁定"仅原子批成功才计指标"。
 */
@DisplayName("延时/重试调度扫描窗口与投递指标")
class DelayMessageSchedulerTest {

    private static final String NAMESPACE = "sched-ns";
    private static final int BATCH_SIZE = 10;
    private static final DelayLevel LEVEL = DelayLevel.SECOND_1;

    private RedissonClient redisson;
    private RScoredSortedSet<String> zset;
    private RBucket<String> claim;
    private StreamMQMetrics metrics;
    private DelayMessageScheduler scheduler;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisson = mock(RedissonClient.class);
        zset = mock(RScoredSortedSet.class);
        claim = mock(RBucket.class);
        metrics = mock(StreamMQMetrics.class);

        doReturn(zset)
                .when(redisson)
                .<String>getScoredSortedSet(
                        StreamMQKeys.delayZSet(NAMESPACE, LEVEL.name()), StringCodec.INSTANCE);
        doReturn(claim).when(redisson).<String>getBucket(anyString(), eq(StringCodec.INSTANCE));
        doReturn(mock(RScript.class)).when(redisson).getScript(StringCodec.INSTANCE);

        scheduler = new DelayMessageScheduler(redisson, NAMESPACE, 1000L, BATCH_SIZE);
        scheduler.setMetrics(metrics);
    }

    @Test
    @DisplayName("B-18：扫描窗口 count 等于 batchSize（不再少扫 1 条）")
    void scanExpired_usesFullBatchSize() {
        when(zset.valueRange(anyDouble(), eq(true), anyDouble(), eq(true), anyInt(), anyInt()))
                .thenReturn(List.of());

        scheduler.scanExpired(LEVEL);

        // 锁定下发到 Redis 的 LIMIT count：0.0 起、偏移 0、count=batchSize（旧实现为 batchSize - 1）
        verify(zset).valueRange(eq(0.0), eq(true), anyDouble(), eq(true), eq(0), eq(BATCH_SIZE));
    }

    @Test
    @DisplayName("B-20：claim 未拿到（其它实例转投中）不计投递指标")
    void claimHeldElsewhere_noDeliveryMetric() {
        when(zset.valueRange(anyDouble(), eq(true), anyDouble(), eq(true), anyInt(), anyInt()))
                .thenReturn(List.of("m-claim"));
        when(claim.setIfAbsent(anyString(), any(Duration.class))).thenReturn(false);

        scheduler.scanExpired(LEVEL);

        verify(metrics, never()).recordDelayDelivery(anyString());
        // 未拿到 claim 时也不得触碰 payload（转投归持有 claim 的实例）
        verify(redisson, never()).getMap(anyString(), eq(StringCodec.INSTANCE));
    }

    @Test
    @DisplayName("B-20：payload 缺失走隔离区（未投递）不计投递指标")
    @SuppressWarnings("unchecked")
    void quarantinedPayload_noDeliveryMetric() {
        when(zset.valueRange(anyDouble(), eq(true), anyDouble(), eq(true), anyInt(), anyInt()))
                .thenReturn(List.of("m-quarantine"));
        when(claim.setIfAbsent(anyString(), any(Duration.class))).thenReturn(true);
        RMap<String, String> emptyPayload = mock(RMap.class);
        when(emptyPayload.readAllMap()).thenReturn(Map.of());
        doReturn(emptyPayload)
                .when(redisson)
                .<String, String>getMap(
                        StreamMQKeys.delayPayloadHash(NAMESPACE, "m-quarantine"),
                        StringCodec.INSTANCE);
        @SuppressWarnings("unchecked")
        RScoredSortedSet<String> quarantineZset = mock(RScoredSortedSet.class);
        doReturn(quarantineZset)
                .when(redisson)
                .<String>getScoredSortedSet(
                        StreamMQKeys.quarantineZset(NAMESPACE, "delay"), StringCodec.INSTANCE);

        scheduler.scanExpired(LEVEL);

        verify(metrics, never()).recordDelayDelivery(anyString());
        verify(quarantineZset).add(anyDouble(), eq("m-quarantine|delay"));
    }

    @Test
    @DisplayName("B-20：原子批成功（真正投递）才计投递指标，且恰好一次")
    @SuppressWarnings("unchecked")
    void delivered_recordsMetricExactlyOnce() {
        when(zset.valueRange(anyDouble(), eq(true), anyDouble(), eq(true), anyInt(), anyInt()))
                .thenReturn(List.of("m-ok"));
        when(zset.getName()).thenReturn(StreamMQKeys.delayZSet(NAMESPACE, LEVEL.name()));
        when(claim.setIfAbsent(anyString(), any(Duration.class))).thenReturn(true);
        RMap<String, String> payload = mock(RMap.class);
        // 生产代码会从 payload 中移除目标主题/投递时间元数据字段，因此必须是可变 Map
        Map<String, String> payloadFields =
                new java.util.LinkedHashMap<>(
                        Map.of(DelayMessageScheduler.FIELD_TARGET_TOPIC, "target-topic"));
        when(payload.readAllMap()).thenReturn(payloadFields);
        doReturn(payload)
                .when(redisson)
                .<String, String>getMap(
                        StreamMQKeys.delayPayloadHash(NAMESPACE, "m-ok"), StringCodec.INSTANCE);
        givenSuccessfulAtomicBatch();

        scheduler.scanExpired(LEVEL);

        verify(metrics, times(1)).recordDelayDelivery(LEVEL.name());
    }

    @Test
    @DisplayName("B-18：RetryScheduler 扫描窗口同样扫满 batchSize")
    @SuppressWarnings("unchecked")
    void scanRetryEntries_usesFullBatchSize() {
        RScoredSortedSet<String> retryZset = mock(RScoredSortedSet.class);
        doReturn(retryZset)
                .when(redisson)
                .<String>getScoredSortedSet(
                        StreamMQKeys.retryZSet(NAMESPACE, "topic", "group"), StringCodec.INSTANCE);
        when(retryZset.valueRange(anyDouble(), eq(true), anyDouble(), eq(true), anyInt(), anyInt()))
                .thenReturn(List.of());
        RetryScheduler retryScheduler = new RetryScheduler(redisson, NAMESPACE, 1000L, BATCH_SIZE);

        retryScheduler.scanRetryEntries(
                new RetryScheduler.RetryTarget(NAMESPACE, "topic", "group", 3));

        verify(retryZset)
                .valueRange(eq(0.0), eq(true), anyDouble(), eq(true), eq(0), eq(BATCH_SIZE));
        assertThat(retryScheduler.getTargetCount()).isZero();
    }

    // ===================== 第六轮红队修复（R2-4 / R2-5） =====================

    /** 可辨识的假服务器时钟（远大于本机 now，避免与真实时钟混淆） */
    private static final long FAKE_SERVER_NOW = 1_800_000_000_123L;

    @Test
    @DisplayName("R2-4：到期扫描窗口上界 = Redis 服务器时钟（不是本机时钟）")
    void scanExpired_usesRedisServerClock() {
        DelayMessageScheduler spy = spy(scheduler);
        doReturn(FAKE_SERVER_NOW).when(spy).scheduleClockMillis();
        when(zset.valueRange(anyDouble(), eq(true), anyDouble(), eq(true), anyInt(), anyInt()))
                .thenReturn(List.of());

        spy.scanExpired(LEVEL);

        // 本机时钟与其偏差不再影响到期判定：下发给 Redis 的扫描上界必须等于服务器时钟
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
    @DisplayName("R2-5：延时 ZSet 孤儿清理单次 Lua 批量下发，不再逐条 isExists（N+1）")
    @SuppressWarnings("unchecked")
    void cleanupOrphanedEntries_singleBatchRoundTrip() {
        RScript script = mock(RScript.class);
        doReturn(script).when(redisson).getScript(StringCodec.INSTANCE);
        doReturn(List.of(3L, 2L))
                .when(script)
                .eval(
                        any(RScript.Mode.class),
                        anyString(),
                        eq(RScript.ReturnType.MULTI),
                        anyList(),
                        any(),
                        any());
        RKeys keys = mock(RKeys.class);
        doReturn(keys).when(redisson).getKeys();
        doReturn(List.of()).when(keys).getKeysByPattern(anyString(), anyInt());
        // 反向孤儿清扫（payload → ZSet）会枚举全部延时 ZSet 做引用差集：本用例只关注 ZSet 侧批量清理
        doReturn(zset)
                .when(redisson)
                .<String>getScoredSortedSet(anyString(), eq(StringCodec.INSTANCE));
        when(zset.readAll()).thenReturn(List.of());

        scheduler.cleanupOrphanedEntries();

        // 每个延时 ZSet（各延时等级 + 自定义）一次批量脚本调用；过程中不得逐条读取 payload Hash
        verify(script, times(DelayLevel.values().length + 1))
                .eval(
                        any(RScript.Mode.class),
                        anyString(),
                        eq(RScript.ReturnType.MULTI),
                        anyList(),
                        any(),
                        any());
        verify(redisson, never()).getMap(anyString(), eq(StringCodec.INSTANCE));
    }

    /** 布置"原子批提交成功"的 mock：XADD + DEL payload + ZREM 三步各自返回异步占位对象。 */
    @SuppressWarnings("unchecked")
    private void givenSuccessfulAtomicBatch() {
        RBatch batch = mock(RBatch.class);
        doReturn(batch).when(redisson).createBatch(any(BatchOptions.class));
        RStreamAsync<String, String> streamAsync = mock(RStreamAsync.class);
        RMapAsync<String, String> mapAsync = mock(RMapAsync.class);
        RScoredSortedSetAsync<String> zsetAsync = mock(RScoredSortedSetAsync.class);
        doReturn(streamAsync)
                .when(batch)
                .<String, String>getStream(anyString(), eq(StringCodec.INSTANCE));
        doReturn(mapAsync)
                .when(batch)
                .<String, String>getMap(anyString(), eq(StringCodec.INSTANCE));
        doReturn(zsetAsync)
                .when(batch)
                .<String>getScoredSortedSet(anyString(), eq(StringCodec.INSTANCE));
    }
}
