/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;

/**
 * 调度条目隔离区登记回归测试（payload TTL 过期场景）。
 *
 * <p>历史缺陷：调度 ZSet 条目残留超过 payload 7 天 TTL 后，转投发现 payload 缺失时直接 {@code ZREM + WARN}——消息静默消失。修复后先写入隔离区
 * ZSet（{@code streammq:{ns}:quarantine:{kind}}， score=dueTime，member=msgId|kind）再移除活跃条目。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("ScheduleQuarantine payload 过期隔离登记测试")
class ScheduleQuarantineTest {

    private RedissonClient redisson;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisson = mock(RedissonClient.class);
    }

    @Test
    @DisplayName("Delay：payload 丢失时写入隔离区并移除活跃调度条目")
    @SuppressWarnings("unchecked")
    void delayExpiredPayloadQuarantined() {
        DelayMessageScheduler scheduler = new DelayMessageScheduler(redisson, "ns", 1000L, 10);
        RScoredSortedSet<String> activeZset = mock(RScoredSortedSet.class);
        RScoredSortedSet<String> quarantineZset = mock(RScoredSortedSet.class);
        RMap<String, String> emptyPayload = mock(RMap.class);
        when(emptyPayload.readAllMap()).thenReturn(Map.of());
        doReturn(quarantineZset)
                .when(redisson)
                .<String>getScoredSortedSet(StreamMQKeys.quarantineZset("ns", "delay"));
        doReturn(emptyPayload)
                .when(redisson)
                .<String, String>getMap(StreamMQKeys.delayPayloadHash("ns", "m1"));
        when(activeZset.getScore("m1")).thenReturn(555.0);
        when(activeZset.remove("m1")).thenReturn(true);

        scheduler.doTransferExpired(activeZset, "m1", "SEC_1");

        verify(quarantineZset).add(555L, "m1|delay");
        verify(activeZset).remove("m1");
    }

    @Test
    @DisplayName("Retry：payload 丢失时写入隔离区并移除活跃调度条目")
    @SuppressWarnings("unchecked")
    void retryExpiredPayloadQuarantined() {
        RetryScheduler scheduler = new RetryScheduler(redisson, "ns", 1000L, 10);
        RScoredSortedSet<String> activeZset = mock(RScoredSortedSet.class);
        RScoredSortedSet<String> quarantineZset = mock(RScoredSortedSet.class);
        RMap<String, String> emptyPayload = mock(RMap.class);
        when(emptyPayload.readAllMap()).thenReturn(Map.of());
        doReturn(quarantineZset)
                .when(redisson)
                .<String>getScoredSortedSet(StreamMQKeys.quarantineZset("ns", "retry"));
        doReturn(emptyPayload).when(redisson).<String, String>getMap(anyString());
        when(activeZset.getScore("m2")).thenReturn(777.0);
        when(activeZset.remove("m2")).thenReturn(true);

        RetryScheduler.RetryTarget target = new RetryScheduler.RetryTarget("topic", "group", 16);
        scheduler.doTransfer(
                "m2",
                target,
                StreamMQKeys.retryStream("ns", "topic", "group"),
                StreamMQKeys.dlqStream("ns", "group"),
                activeZset,
                StreamMQKeys.retryPayloadHash("ns", "topic", "group", "m2"));

        verify(quarantineZset).add(777L, "m2|retry");
        verify(activeZset).remove("m2");
    }

    @Test
    @DisplayName("正常 payload 不触发隔离区写入")
    @SuppressWarnings("unchecked")
    void healthyPayloadNotQuarantined() {
        DelayMessageScheduler scheduler = new DelayMessageScheduler(redisson, "ns", 1000L, 10);
        RScoredSortedSet<String> activeZset = mock(RScoredSortedSet.class);
        RScoredSortedSet<String> quarantineZset = mock(RScoredSortedSet.class);
        RMap<String, String> payload = mock(RMap.class);
        when(payload.readAllMap())
                .thenReturn(Map.of(DelayMessageScheduler.FIELD_TARGET_TOPIC, "t"));
        // 原子批走 createBatch；此处只关注隔离区不被触碰
        doReturn(payload).when(redisson).getMap(StreamMQKeys.delayPayloadHash("ns", "m3"));
        doReturn(quarantineZset)
                .when(redisson)
                .<String>getScoredSortedSet(StreamMQKeys.quarantineZset("ns", "delay"));

        try {
            scheduler.doTransferExpired(activeZset, "m3", "SEC_1");
        } catch (RuntimeException ignored) {
            // 批执行在 mock 上可能失败——本测试仅断言隔离区未被误写
        }

        verify(quarantineZset, never()).add(anyLong(), anyString());
        verify(activeZset, never()).remove(Mockito.anyString());
        assertThat(StreamMQKeys.quarantineZset("ns", "delay"))
                .isEqualTo("streammq:ns:quarantine:delay");
    }
}
