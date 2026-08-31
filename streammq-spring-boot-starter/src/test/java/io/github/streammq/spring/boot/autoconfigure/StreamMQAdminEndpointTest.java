/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.metrics.RuntimeStatsRegistry;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RMap;
import org.redisson.api.RSet;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
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
        verify(redisson, never()).getStream(anyString());
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
        verify(redisson, never()).getStream(anyString());
    }

    @Test
    @DisplayName("deleteTopic confirm 匹配时删除 Stream 并移除注册表项")
    void deleteTopic_matchingConfirm_deletesStream() {
        when(redisson.<String, String>getStream(eq(StreamMQKeys.topicStream(NS, "order-topic"))))
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
        when(redisson.<String, String>getMap(eq(StreamMQKeys.metaStats(NS, "g1", "t1"))))
                .thenReturn(statsMap);
        when(statsMap.readAllMap()).thenReturn(Map.of());
        when(redisson.<String, String>getStream(eq(StreamMQKeys.topicStream(NS, "t1"))))
                .thenReturn(stream);
        when(stream.listPending(anyString(), any(), any(), anyInt()))
                .thenReturn(java.util.Collections.emptyList());

        Map<String, Object> stats = newEndpoint().getStats("g1", "t1");

        assertThat(stats.get("consumeSuccess")).isEqualTo(1L);
        assertThat(stats.get("consumeFailure")).isEqualTo(1L);
        assertThat(stats.get("consumeTotal")).isEqualTo(2L);
        assertThat(stats.get("retried")).isEqualTo(1L);
        assertThat(stats.get("dlq")).isEqualTo(1L);
        // 平均耗时 = (20 + 40) / 2 = 30ms
        assertThat((double) stats.get("avgConsumeMillis")).isEqualTo(30.0);
        assertThat(stats.get("pendingCount")).isEqualTo(0);
    }

    // ===================== P1-4: 组配置运行时应用 =====================

    @Test
    @DisplayName("updateGroupConfig paused=true 真实暂停容器")
    void updateGroupConfig_paused_truePausesContainer() {
        Map<String, Object> result =
                newEndpoint().updateGroupConfig("g1", Map.of("paused", "true"));

        verify(container).pause();
        assertThat(result.get("success")).isEqualTo(true);
        assertThat(((Map<?, ?>) result.get("applied")).get("paused")).isEqualTo(Boolean.TRUE);
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
        verify(container, never()).pause();
        verify(container, never()).resume();
    }

    private static io.github.streammq.core.listener.StreamMQListenerContainer.ConsumerMetadata
            consumerMeta(String topic) {
        return new io.github.streammq.core.listener.StreamMQListenerContainer.ConsumerMetadata(
                topic, "default-group", String.class, Object.class);
    }
}
