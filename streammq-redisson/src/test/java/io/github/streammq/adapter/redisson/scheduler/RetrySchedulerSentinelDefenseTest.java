/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.BatchOptions;
import org.redisson.api.RBatch;
import org.redisson.api.RMap;
import org.redisson.api.RMapAsync;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScoredSortedSetAsync;
import org.redisson.api.RStreamAsync;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/**
 * RetryScheduler 哨兵冲突防御测试（第六轮红队 R3-7）。
 *
 * <p>缺陷背景：业务 topic 名恰为 {@code __dlq__} 时会被重试调度器误判为"DLQ 重试目标"而错误路由。 修复为双重防御：
 *
 * <ol>
 *   <li>注册入口拒绝 {@code __} 保留前缀（core 的命名校验之外的纵深防御）
 *   <li>转投判定要求"哨兵 topic + {@code retryScope=dlq} 独立标记"同时成立
 * </ol>
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("RetryScheduler 哨兵冲突防御")
class RetrySchedulerSentinelDefenseTest {

    private final RedissonClient redisson = mock(RedissonClient.class);

    @Test
    @DisplayName("保留名（__dlq__）不可注册为重试目标")
    void registerRetryTarget_rejectsReservedPrefix() {
        RetryScheduler scheduler = new RetryScheduler(redisson, "ns", 1_000L, 10);

        assertThatThrownBy(
                        () ->
                                scheduler.registerRetryTarget(
                                        "ns",
                                        StreamMQConstants.DLQ_RETRY_TARGET_TOPIC_SENTINEL,
                                        "group",
                                        16))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("__");
        assertThat(scheduler.getTargetCount()).isZero();
    }

    @Test
    @DisplayName("普通 topic 可正常注册（回归对照）")
    void registerRetryTarget_acceptsBusinessTopic() {
        RetryScheduler scheduler = new RetryScheduler(redisson, "ns", 1_000L, 10);
        scheduler.registerRetryTarget("ns", "biz-topic", "group", 16);
        assertThat(scheduler.getTargetCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("targetTopic=哨兵但缺 scope 标记：按业务 topic 转投 retry Stream，不写 DLQ（失败即红）")
    void sentinelWithoutScopeMarker_isTreatedAsBusinessTopic() {
        RetryScheduler scheduler = new RetryScheduler(redisson, "ns", 1_000L, 10);
        RBatch batch = stubBatch(Map.of(RetryScheduler.FIELD_TARGET_TOPIC, "__dlq__"));

        RetryScheduler.RetryTarget target = new RetryScheduler.RetryTarget("ns", "t", "g", 16);
        String retryStreamKey = StreamMQKeys.retryStream("ns", "t", "g");
        String dlqStreamKey = StreamMQKeys.dlqStream("ns", "g");
        scheduler.doTransfer(
                "m1",
                target,
                retryStreamKey,
                dlqStreamKey,
                mockZset("streammq:ns:retry:t:g"),
                StreamMQKeys.retryPayloadHash("ns", "t", "g", "m1"));

        verify(batch).getStream(eq(retryStreamKey), any(StringCodec.class));
        verify(batch, never()).getStream(eq(dlqStreamKey), any(StringCodec.class));
    }

    @Test
    @DisplayName("targetTopic=哨兵且 scope=dlq：转投 DLQ Stream（真实 DLQ 重试路径不受影响）")
    void sentinelWithScopeMarker_routesToDlq() {
        RetryScheduler scheduler = new RetryScheduler(redisson, "ns", 1_000L, 10);
        Map<String, String> payload = new HashMap<>();
        payload.put(RetryScheduler.FIELD_TARGET_TOPIC, "__dlq__");
        payload.put(RetryScheduler.FIELD_RETRY_SCOPE, RetryScheduler.RETRY_SCOPE_DLQ);
        payload.put(RetryScheduler.FIELD_RETRY_COUNT, "2");
        RBatch batch = stubBatch(payload);

        RetryScheduler.RetryTarget target = new RetryScheduler.RetryTarget("ns", "g", "g", 0);
        String retryStreamKey = StreamMQKeys.retryStream("ns", "g", "g");
        String dlqStreamKey = StreamMQKeys.dlqStream("ns", "g");
        scheduler.doTransfer(
                "m2",
                target,
                retryStreamKey,
                dlqStreamKey,
                mockZset("streammq:ns:retry:g:g"),
                StreamMQKeys.retryPayloadHash("ns", "g", "g", "m2"));

        verify(batch).getStream(eq(dlqStreamKey), any(StringCodec.class));
        verify(batch, never()).getStream(eq(retryStreamKey), any(StringCodec.class));
    }

    // ===================== 工具 =====================

    private RBatch stubBatch(Map<String, String> payload) {
        RBatch batch = mock(RBatch.class);
        RMap<String, String> payloadMap = mockReadMap(payload);
        RMapAsync<String, String> payloadMapAsync = mockMapAsync();
        RStreamAsync<String, String> streamAsync = mockStreamAsync();
        RScoredSortedSetAsync<String> zsetAsync = mockZsetAsync();

        doReturn(payloadMap).when(redisson).getMap(anyString(), any(StringCodec.class));
        doReturn(batch).when(redisson).createBatch(any(BatchOptions.class));
        doReturn(streamAsync).when(batch).getStream(anyString(), any(StringCodec.class));
        doReturn(payloadMapAsync).when(batch).getMap(anyString(), any(StringCodec.class));
        doReturn(zsetAsync).when(batch).getScoredSortedSet(anyString(), any(StringCodec.class));
        return batch;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static RMap<String, String> mockReadMap(Map<String, String> payload) {
        RMap<String, String> map = mock(RMap.class);
        when(map.readAllMap()).thenReturn(new HashMap<>(payload));
        return map;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static RScoredSortedSet<String> mockZset(String name) {
        RScoredSortedSet<String> zset = mock(RScoredSortedSet.class);
        when(zset.getName()).thenReturn(name);
        return zset;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static RMapAsync<String, String> mockMapAsync() {
        return mock(RMapAsync.class);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static RScoredSortedSetAsync<String> mockZsetAsync() {
        return mock(RScoredSortedSetAsync.class);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static RStreamAsync<String, String> mockStreamAsync() {
        return mock(RStreamAsync.class);
    }
}
