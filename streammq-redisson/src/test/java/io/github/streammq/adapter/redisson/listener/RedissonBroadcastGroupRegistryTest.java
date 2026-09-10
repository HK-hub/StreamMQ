/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.adapter.redisson.support.BroadcastGroupNaming;
import io.github.streammq.core.broadcast.BroadcastInstanceLease;
import io.github.streammq.core.broadcast.BroadcastInstanceRegistry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;

/**
 * {@link RedissonBroadcastGroupRegistry#sweepStaleBroadcastGroups()} 的<b>按 topic 维度保护</b>单元测试（无
 * Redis 依赖）。
 *
 * <p>这是"完整修复需按 topic 维度的 release 语义"的核心收口逻辑：清扫任务只在<b>槽位仍持有该 topic</b> 且处于回收宽限期内时， 才保留其消费者组；否则允许销毁——
 * 既避免误伤仍在消费的同组其它主题，也确保被释放的主题组能被回收（修复僵尸组泄漏）。
 *
 * <p>本测试用 Mockito 桩住 Redisson 数据结构，覆盖以下关键分支：
 *
 * <ul>
 *   <li>主题仍被持有 → 组保留（不销毁）；
 *   <li>主题已释放（不在槽位）→ 组销毁；
 *   <li>无实例注册中心（null）→ 槽位不可见，组销毁；
 *   <li>心跳超过回收宽限期 → 即便主题仍持有也销毁；
 *   <li>多主题槽位部分释放 → 仅被释放主题的组被销毁，其余保留；
 *   <li>格式非法成员（无 {@code |} 分隔）→ 直接清理。
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RedissonBroadcastGroupRegistry 清扫按 topic 维度保护")
class RedissonBroadcastGroupRegistryTest {

    private static final String NS = "ns";
    private static final String GROUP = "g";
    private static final String INSTANCE_ID = "id";
    private static final String EFFECTIVE_GROUP =
            BroadcastGroupNaming.effectiveGroup(GROUP, INSTANCE_ID); // 形如 g:g-id

    private static final long STALE_TTL = 10L * 60 * 1000;
    private static final int MAX_SWEEP = 100;
    private static final long LEASE_TIMEOUT = 20_000L;
    private static final long GRACE = 7L * 24 * 60 * 60 * 1000;

    @Mock private RedissonClient redisson;
    @Mock private RScoredSortedSet<String> registry;
    @Mock private RStream<String, String> stream;
    @Mock private BroadcastInstanceRegistry instanceRegistry;

    @BeforeEach
    void setUp() {
        // getScoredSortedSet 每次清扫必调用（严格校验）
        when(redisson.<String>getScoredSortedSet(anyString())).thenReturn(registry);
        // 以下为条件性依赖：是否触发取决于分支（是否销毁组 / 是否遍历实例槽位 / 是否记录残留量），
        // 用 lenient 避免 STRICT_STUBS 对未走分支的桩报 UnnecessaryStubbing。
        lenient().when(redisson.<String, String>getStream(anyString())).thenReturn(stream);
        // sweepInstanceSlots 遍历注册表去重 group；空迭代器避免误触实例槽位清扫
        lenient().when(registry.iterator()).thenReturn(Collections.emptyIterator());
        lenient().when(registry.size()).thenReturn(0);
    }

    private RedissonBroadcastGroupRegistry underTest() {
        return new RedissonBroadcastGroupRegistry(
                redisson, NS, STALE_TTL, MAX_SWEEP, instanceRegistry, LEASE_TIMEOUT, GRACE);
    }

    private RedissonBroadcastGroupRegistry underTestWithoutInstanceRegistry() {
        return new RedissonBroadcastGroupRegistry(
                redisson, NS, STALE_TTL, MAX_SWEEP, null, LEASE_TIMEOUT, GRACE);
    }

    private void stubStaleMembers(String... members) {
        when(registry.valueRange(
                        anyDouble(), anyBoolean(), anyDouble(), anyBoolean(), anyInt(), anyInt()))
                .thenReturn(Arrays.asList(members));
    }

    private void stubLease(long lastHeartbeatMillis, String... topics) {
        BroadcastInstanceLease lease =
                new BroadcastInstanceLease(
                        INSTANCE_ID,
                        "host",
                        List.of(topics),
                        GROUP,
                        -1,
                        1_000L,
                        lastHeartbeatMillis,
                        false);
        when(instanceRegistry.listInstances(NS, GROUP)).thenReturn(List.of(lease));
    }

    @Test
    @DisplayName("主题仍被槽位持有且在宽限期内 → 组保留（不调用 removeGroup）")
    void heldTopicWithinGrace_isRetained() {
        stubLease(System.currentTimeMillis(), "tb");
        stubStaleMembers("tb|" + EFFECTIVE_GROUP);

        int removed = underTest().sweepStaleBroadcastGroups();

        assertThat(removed).isEqualTo(0);
        verify(stream, never()).removeGroup(anyString());
        verify(registry, never()).remove(anyString());
    }

    @Test
    @DisplayName("主题已被释放（不在槽位）→ 组被销毁")
    void releasedTopic_isDestroyed() {
        // 槽位只持有 tb，但待清扫成员是 ta → ta 不在槽位 → 允许销毁
        stubLease(System.currentTimeMillis(), "tb");
        stubStaleMembers("ta|" + EFFECTIVE_GROUP);

        int removed = underTest().sweepStaleBroadcastGroups();

        assertThat(removed).isEqualTo(1);
        verify(stream).removeGroup(EFFECTIVE_GROUP);
        verify(registry).remove("ta|" + EFFECTIVE_GROUP);
    }

    @Test
    @DisplayName("无实例注册中心（null）→ 槽位不可见 → 组被销毁")
    void noInstanceRegistry_isDestroyed() {
        stubStaleMembers("ta|" + EFFECTIVE_GROUP);

        int removed = underTestWithoutInstanceRegistry().sweepStaleBroadcastGroups();

        assertThat(removed).isEqualTo(1);
        verify(stream).removeGroup(EFFECTIVE_GROUP);
        verify(registry).remove("ta|" + EFFECTIVE_GROUP);
    }

    @Test
    @DisplayName("心跳超过回收宽限期 → 即便主题仍持有也销毁")
    void heartbeatBeyondGrace_isDestroyedEvenIfHeld() {
        stubLease(System.currentTimeMillis() - (GRACE + 1000L), "tb");
        stubStaleMembers("tb|" + EFFECTIVE_GROUP);

        int removed = underTest().sweepStaleBroadcastGroups();

        assertThat(removed).isEqualTo(1);
        verify(stream).removeGroup(EFFECTIVE_GROUP);
        verify(registry).remove("tb|" + EFFECTIVE_GROUP);
    }

    @Test
    @DisplayName("多主题槽位部分释放 → 仅被释放主题的组被销毁，其余保留")
    void partialRelease_onlyReleasedTopicDestroyed() {
        // 槽位持有 {tb}，待清扫成员为 ta 与 tb（共享同一生效组名 g:g-id）
        stubLease(System.currentTimeMillis(), "tb");
        stubStaleMembers("ta|" + EFFECTIVE_GROUP, "tb|" + EFFECTIVE_GROUP);

        int removed = underTest().sweepStaleBroadcastGroups();

        // ta 已被释放 → 销毁；tb 仍持有 → 保留；故只销毁 1 个组（两者同组名，removeGroup 仅调用一次）
        assertThat(removed).isEqualTo(1);
        verify(stream, times(1)).removeGroup(EFFECTIVE_GROUP);
        verify(registry).remove("ta|" + EFFECTIVE_GROUP);
        verify(registry, never()).remove("tb|" + EFFECTIVE_GROUP);
    }

    @Test
    @DisplayName("格式非法成员（无 | 分隔）→ 直接清理，不触碰消费者组")
    void malformedMember_isCleanedWithoutGroupDestroy() {
        stubStaleMembers("malformed-no-pipe");

        int removed = underTest().sweepStaleBroadcastGroups();

        // 损坏条目会被从注册表清理（registry.remove），但不在返回的"清扫计数"中累加：
        // 该计数只统计有效的僵尸广播组，损坏条目属数据修复，沿用改动前的语义。
        assertThat(removed).isEqualTo(0);
        verify(registry).remove("malformed-no-pipe");
        verify(stream, never()).removeGroup(anyString());
    }
}
