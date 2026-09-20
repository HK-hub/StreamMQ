/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.metrics.RuntimeStatsRegistry;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.spring.boot.StreamMQSpringConstants;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.PendingResult;
import org.redisson.api.RMap;
import org.redisson.api.RSet;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.client.codec.StringCodec;

/**
 * {@link StreamMQAdminEndpoint} 行为回归测试（mock Redisson，无需真实 Redis）。
 *
 * <p>覆盖发布前修复：
 *
 * <ul>
 *   <li>P1-1：创建 Topic 只登记注册表 Set、不再向业务 Stream 写占位消息；
 *   <li>P1-3：{@code getStats} 返回进程内真实统计（而非永为空 map 的死端点）；
 *   <li>P1-4：组配置更新逐 key 执行真实运行期变更，不支持/非法 key 显式拒绝；
 *   <li>P2：删除 Topic 必须显式 confirm，拒绝不可逆操作被误触发。
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("管理端点行为测试")
class StreamMQAdminEndpointTest {

    private static final String NS = "test-ns";

    @Mock private RedissonClient redisson;
    @Mock private DefaultStreamMQListenerContainer container;
    @Mock private RSet<String> registrySet;
    @Mock private RStream<String, String> stream;
    @Mock private RMap<String, String> statsMap;

    private StreamMQAdminEndpoint newEndpoint() {
        // failureRetryCooldownMillis = 0 → 禁用失败限流，测试不受冷却期影响
        return new StreamMQAdminEndpoint(redisson, container, NS, 0L, null);
    }

    // ===================== P1-1: Topic 注册表 =====================

    @Test
    @DisplayName("createTopic 登记到注册表 Set，不触碰业务 Stream")
    void createTopic_registersInSet_notTouchingStream() {
        when(redisson.<String>getSet(eq(StreamMQKeys.topicRegistry(NS)), any(StringCodec.class)))
                .thenReturn(registrySet);
        when(registrySet.add("order-topic")).thenReturn(true);

        Map<String, Object> result = newEndpoint().createTopic("order-topic");

        assertThat(result.get("success")).isEqualTo(true);
        assertThat(result.get("created")).isEqualTo(true);
        assertThat(result.get("topic")).isEqualTo("order-topic");
        // 关键断言：不再向业务 Stream 写占位消息（旧实现 XADD __placeholder）
        verify(redisson, never()).getStream(anyString(), any());
    }

    @Test
    @DisplayName("createTopic 重复创建返回 created=false（幂等）")
    void createTopic_duplicate_createdFalse() {
        when(redisson.<String>getSet(eq(StreamMQKeys.topicRegistry(NS)), any(StringCodec.class)))
                .thenReturn(registrySet);
        when(registrySet.add("order-topic")).thenReturn(false);

        Map<String, Object> result = newEndpoint().createTopic("order-topic");

        assertThat(result.get("success")).isEqualTo(true);
        assertThat(result.get("created")).isEqualTo(false);
    }

    @Test
    @DisplayName("listTopics 合并注册表 Set 与容器已注册消费者")
    void listTopics_mergesRegistryAndConsumers() {
        when(redisson.<String>getSet(eq(StreamMQKeys.topicRegistry(NS)), any(StringCodec.class)))
                .thenReturn(registrySet);
        when(registrySet.readAll()).thenReturn(java.util.Set.of("b-registered", "a-registered"));
        when(container.getConsumers())
                .thenReturn(
                        java.util.List.of(
                                consumerMeta("c-consumed"), consumerMeta("b-registered")));

        java.util.List<String> topics = newEndpoint().listTopics();

        // 去重 + 排序
        assertThat(topics).containsExactly("a-registered", "b-registered", "c-consumed");
    }

    // ===================== P2: deleteTopic 显式确认 =====================

    @Test
    @DisplayName("deleteTopic confirm 不匹配时拒绝且不触碰 Redis")
    void deleteTopic_wrongConfirm_rejected() {
        Map<String, Object> result = newEndpoint().deleteTopic("order-topic", "other-topic");

        assertThat(result.get("success")).isEqualTo(false);
        assertThat(result.get("error").toString()).contains("confirm=");
        verify(redisson, never()).getStream(anyString(), any());
    }

    @Test
    @DisplayName("deleteTopic confirm 匹配时删除 Stream 并移除注册表项")
    void deleteTopic_matchingConfirm_deletesStream() {
        when(redisson.<String, String>getStream(
                        eq(StreamMQKeys.topicStream(NS, "order-topic")), any(StringCodec.class)))
                .thenReturn(stream);
        when(stream.delete()).thenReturn(true);
        when(redisson.<String>getSet(eq(StreamMQKeys.topicRegistry(NS)), any(StringCodec.class)))
                .thenReturn(registrySet);

        Map<String, Object> result = newEndpoint().deleteTopic("order-topic", "order-topic");

        assertThat(result.get("success")).isEqualTo(true);
        assertThat(result.get("deleted")).isEqualTo(true);
        verify(stream).delete();
        verify(registrySet).remove("order-topic");
    }

    // ===================== P1-3: 运行时统计 =====================

    @Test
    @DisplayName("getStats 返回进程内真实消费统计")
    void getStats_returnsRuntimeRegistryData() {
        RuntimeStatsRegistry registry = new RuntimeStatsRegistry();
        registry.recordConsume("g1", "t1", true, 20_000_000L); // 20ms
        registry.recordConsume("g1", "t1", false, 40_000_000L); // 40ms
        registry.recordRetry("g1", "t1");
        registry.recordDlq("g1", "t1");
        when(container.runtimeStats()).thenReturn(registry);
        when(redisson.<String, String>getMap(
                        eq(StreamMQKeys.metaStats(NS, "g1", "t1")), any(StringCodec.class)))
                .thenReturn(statsMap);
        when(statsMap.readAllMap()).thenReturn(Map.of());
        when(redisson.<String, String>getStream(
                        eq(StreamMQKeys.topicStream(NS, "t1")), any(StringCodec.class)))
                .thenReturn(stream);
        // pendingCount 采用 XPENDING 总数形式（getPendingInfo.getTotal，O(1) 且不被拉取上限截断）
        when(stream.getPendingInfo(anyString()))
                .thenReturn(new PendingResult(0L, null, null, java.util.Map.of()));

        Map<String, Object> stats = newEndpoint().getStats("g1", "t1");

        assertThat(stats.get("consumeSuccess")).isEqualTo(1L);
        assertThat(stats.get("consumeFailure")).isEqualTo(1L);
        assertThat(stats.get("consumeTotal")).isEqualTo(2L);
        assertThat(stats.get("retried")).isEqualTo(1L);
        assertThat(stats.get("dlq")).isEqualTo(1L);
        // 平均耗时 = (20 + 40) / 2 = 30ms
        assertThat((double) stats.get("avgConsumeMillis")).isEqualTo(30.0);
        assertThat(stats.get("pendingCount")).isEqualTo(0L);
    }

    // ===================== P1-4: 组配置运行时应用 =====================

    @Test
    @DisplayName("updateGroupConfig paused=true 按组暂停（不再误伤其它组），并诚实回显生效状态")
    void updateGroupConfig_paused_pausesGroupOnly() {
        when(container.isGroupPaused("g1")).thenReturn(true);

        Map<String, Object> result =
                newEndpoint().updateGroupConfig("g1", Map.of("paused", "true"));

        // R6-S9：按组分发，绝不调用容器级 pause()（那会连带暂停其它消费者组）
        verify(container).pauseGroup("g1");
        verify(container, never()).pause();
        assertThat(result.get("success")).isEqualTo(true);
        assertThat(((Map<?, ?>) result.get("applied")).get("paused")).isEqualTo(Boolean.TRUE);
        assertThat(result.get("groupPaused")).isEqualTo(true);
        assertThat(castEffects(result)).containsEntry("paused", "immediate");
    }

    @Test
    @DisplayName("updateGroupConfig paused=false 按组恢复")
    void updateGroupConfig_paused_falseResumesGroup() {
        Map<String, Object> result =
                newEndpoint().updateGroupConfig("g1", Map.of("paused", "false"));

        verify(container).resumeGroup("g1");
        verify(container, never()).resume();
        assertThat(((Map<?, ?>) result.get("applied")).get("paused")).isEqualTo(Boolean.FALSE);
    }

    @Test
    @DisplayName("updateGroupConfig paused 对非 Default 容器实现显式拒绝（不做脆弱强转、不假装生效）")
    void updateGroupConfig_paused_genericContainerRejected() {
        io.github.streammq.core.listener.StreamMQListenerContainer generic =
                org.mockito.Mockito.mock(
                        io.github.streammq.core.listener.StreamMQListenerContainer.class);
        StreamMQAdminEndpoint endpoint = new StreamMQAdminEndpoint(redisson, generic, NS, 0L, null);

        Map<String, Object> result = endpoint.updateGroupConfig("g1", Map.of("paused", "true"));

        assertThat(result.get("success")).isEqualTo(false);
        Map<String, String> rejected = castRejected(result);
        assertThat(rejected.get("paused")).contains("per-group pause requires");
        verify(generic, never()).pause();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> castRejected(Map<String, Object> result) {
        return (Map<String, String>) result.get("rejected");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> castEffects(Map<String, Object> result) {
        return (Map<String, String>) result.get("effects");
    }

    @Test
    @DisplayName("updateGroupConfig inflightCapacity 按运行中的循环如实标注即时/下次循环生效")
    void updateGroupConfig_inflightCapacity_reportsHonestEffect() {
        when(container.isInflightCapacityAppliedForGroup("g1")).thenReturn(false);
        Map<String, Object> deferred =
                newEndpoint().updateGroupConfig("g1", Map.of("inflightCapacity", "32"));
        assertThat(castEffects(deferred))
                .containsEntry(
                        "inflightCapacity", StreamMQAdminEndpoint.EFFECT_NEXT_CONSUME_LOOP_START);
        verify(container).setInflightCapacity(32);

        when(container.isInflightCapacityAppliedForGroup("g2")).thenReturn(true);
        Map<String, Object> immediate =
                newEndpoint().updateGroupConfig("g2", Map.of("inflightCapacity", "32"));
        assertThat(castEffects(immediate))
                .containsEntry("inflightCapacity", StreamMQAdminEndpoint.EFFECT_IMMEDIATE);
    }

    @Test
    @DisplayName("updateGroupConfig 数值 key 解析并调用对应 setter")
    void updateGroupConfig_numericKey_appliesRuntime() {
        Map<String, Object> result =
                newEndpoint()
                        .updateGroupConfig(
                                "g1",
                                Map.of(
                                        "inflightCapacity", "32",
                                        "pausedSleepMillis", "150"));

        verify(container).setInflightCapacity(32);
        verify(container).setPausedSleepMillis(150L);
        assertThat(result.get("success")).isEqualTo(true);
        assertThat(result.get("applied"))
                .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
                .containsEntry("inflightCapacity", 32L)
                .containsEntry("pausedSleepMillis", 150L);
        // 每轮读取的 key 可宣称即时生效
        assertThat(castEffects(result))
                .containsEntry("pausedSleepMillis", StreamMQAdminEndpoint.EFFECT_IMMEDIATE);
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("updateGroupConfig 非法值与不支持 key 显式拒绝")
    void updateGroupConfig_invalidAndUnsupported_rejected() {
        Map<String, Object> result =
                newEndpoint()
                        .updateGroupConfig(
                                "g1",
                                Map.of(
                                        "paused", "maybe",
                                        "maxReconsumeTimes", "16"));

        assertThat(result.get("success")).isEqualTo(false);
        Map<String, String> rejected = (Map<String, String>) result.get("rejected");
        assertThat(rejected).containsKey("paused").containsKey("maxReconsumeTimes");
        assertThat(result.get("applied"))
                .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
                .isEmpty();
        verify(container, never()).pauseGroup(anyString());
        verify(container, never()).resumeGroup(anyString());
    }

    // ===================== R6-S5: 列表条数上界夹取 =====================

    @Test
    @DisplayName("listDlq 的 count 被夹取到 maxPendingQuerySize（此前直传无上界）")
    void listDlq_clampsCountToMaxPendingQuerySize() {
        when(redisson.<String, String>getStream(
                        eq(StreamMQKeys.dlqStream(NS, "g1")), any(StringCodec.class)))
                .thenReturn(stream);

        newEndpoint().listDlq("g1", Integer.MAX_VALUE);

        verify(stream).range(1000, StreamMessageId.MIN, StreamMessageId.MAX);
    }

    @Test
    @DisplayName("listPending 的 count 同样夹取（且小于 1 时按 1 处理）")
    void listPending_clampsCount() {
        when(redisson.<String, String>getStream(
                        eq(StreamMQKeys.topicStream(NS, "t1")), any(StringCodec.class)))
                .thenReturn(stream);

        newEndpoint().listPending("g1", "t1", 5_000_000);
        newEndpoint().listPending("g1", "t1", 0);

        verify(stream).listPending("g1", StreamMessageId.MIN, StreamMessageId.MAX, 1000);
        verify(stream).listPending("g1", StreamMessageId.MIN, StreamMessageId.MAX, 1);
    }

    @Test
    @DisplayName("setMaxPendingQuerySize 超过硬上限时按上限生效（10000）")
    void setMaxPendingQuerySize_clampsToHardLimit() {
        when(redisson.<String, String>getStream(
                        eq(StreamMQKeys.dlqStream(NS, "g1")), any(StringCodec.class)))
                .thenReturn(stream);

        StreamMQAdminEndpoint endpoint = newEndpoint();
        endpoint.setMaxPendingQuerySize(Integer.MAX_VALUE);
        endpoint.listDlq("g1", Integer.MAX_VALUE);

        verify(stream)
                .range(
                        StreamMQSpringConstants.MAX_ADMIN_LIST_LIMIT,
                        StreamMessageId.MIN,
                        StreamMessageId.MAX);
    }

    private static io.github.streammq.core.listener.StreamMQListenerContainer.ConsumerMetadata
            consumerMeta(String topic) {
        return new io.github.streammq.core.listener.StreamMQListenerContainer.ConsumerMetadata(
                topic, "default-group", String.class, Object.class);
    }
}
