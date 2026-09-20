/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.adapter.redisson.scheduler.PelClaimScheduler;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.ListenerType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 调度目标绑定器回归测试（发布前红队审查 R5）。
 *
 * <p>锁定三条不变量：
 *
 * <ol>
 *   <li><b>运行期单注册项绑定</b>必须与启动期批量绑定产生<b>同一目标集合</b>——历史上动态注册只建读循环、 漏绑调度目标，这些消费者的失败消息写入重试 ZSet
 *       后永不重投（payload 过期即静默丢失）；
 *   <li><b>注销必须解除绑定</b>——否则调度目标只增不减，调度器持续扫描已注销目标；
 *   <li><b>广播（R4-x1）必须登记共享 retry-stream 认领目标</b>——retry Stream 在两种消费模式下都是共享的 （retryMode
 *       固定复用基组名），漏登记会让广播实例崩溃后遗留的唯一副本永久滞留；但广播的 topic 流认领目标仍不得登记（基组名在该流上不存在，只会刷 NOGROUP 告警且无法恢复 PEL）。
 * </ol>
 */
@DisplayName("调度目标绑定器（单注册项绑定/解绑）")
class DefaultSchedulerTargetBinderTest {

    private static final String NS = "ns";
    private static final String TOPIC = "trade-topic";
    private static final String GROUP = "trade-group";
    private static final int MAX_RECONSUME = 16;

    private RetryScheduler retryScheduler;
    private PelClaimScheduler pelClaimScheduler;
    private RegistrationStore store;
    private DefaultSchedulerTargetBinder binder;

    @BeforeEach
    void setUp() {
        retryScheduler = mock(RetryScheduler.class);
        pelClaimScheduler = mock(PelClaimScheduler.class);
        store = mock(RegistrationStore.class);
        binder = new DefaultSchedulerTargetBinder(store);
    }

    @Test
    @DisplayName("批量绑定（启动期）：广播注册同样登记 retry-stream 认领目标，不登记 topic 目标")
    void batchBind_broadcast_registersRetryStreamTargetOnly() {
        ListenerRegistration<?> reg =
                reg(ListenerType.AUTO_ACK, false, ConsumeMode.BROADCASTING, 0);
        when(store.registrations()).thenReturn(java.util.List.of(reg));
        when(store.registrationCount()).thenReturn(1);

        binder.bindPelClaimTargets(pelClaimScheduler);

        verify(pelClaimScheduler)
                .registerRetryStreamTarget(eq(NS), eq(TOPIC), eq(GROUP), eq(MAX_RECONSUME));
        verify(pelClaimScheduler, never())
                .registerTarget(any(), any(), any(), anyInt(), anyBoolean(), anyInt(), any());
        verify(pelClaimScheduler, never()).registerTarget(any(), any(), any(), anyInt());
    }

    @Test
    @DisplayName("ORDERLY：补绑 topic 重试目标 + 顺序分片认领目标")
    void orderlyRegistration_bindsRetryAndOrderlyPelClaimTargets() {
        ListenerRegistration<?> reg = reg(ListenerType.ORDERLY, false, ConsumeMode.CLUSTERING, 8);

        binder.bindTargets(retryScheduler, pelClaimScheduler, reg);

        verify(retryScheduler).registerRetryTarget(eq(NS), eq(TOPIC), eq(GROUP), eq(MAX_RECONSUME));
        verify(pelClaimScheduler)
                .registerTarget(
                        eq(NS), eq(TOPIC), eq(GROUP), eq(MAX_RECONSUME), eq(true), eq(8), isNull());
    }

    @Test
    @DisplayName("AUTO_ACK 集群：补绑 topic + retry-stream 认领目标与重试目标")
    void concurrentRegistration_bindsTopicAndRetryStreamTargets() {
        ListenerRegistration<?> reg = reg(ListenerType.AUTO_ACK, false, ConsumeMode.CLUSTERING, 0);

        binder.bindTargets(retryScheduler, pelClaimScheduler, reg);

        verify(retryScheduler).registerRetryTarget(eq(NS), eq(TOPIC), eq(GROUP), eq(MAX_RECONSUME));
        verify(pelClaimScheduler).registerTarget(eq(NS), eq(TOPIC), eq(GROUP), eq(MAX_RECONSUME));
        verify(pelClaimScheduler)
                .registerRetryStreamTarget(eq(NS), eq(TOPIC), eq(GROUP), eq(MAX_RECONSUME));
    }

    @Test
    @DisplayName("DLQ 注册：认领目标走 DLQ 种类，重试目标走 (group, group) sentinel 维度")
    void dlqRegistration_bindsDlqTargetsAndSentinelRetryTarget() {
        ListenerRegistration<?> reg = reg(ListenerType.AUTO_ACK, true, ConsumeMode.CLUSTERING, 0);

        binder.bindTargets(retryScheduler, pelClaimScheduler, reg);

        verify(pelClaimScheduler).registerDlqTarget(eq(NS), eq(TOPIC), eq(GROUP));
        verify(retryScheduler).registerRetryTarget(eq(NS), eq(GROUP), eq(GROUP), eq(0));
    }

    @Test
    @DisplayName("广播（非 DLQ）：注册共享 retry-stream 认领目标，不注册 topic 流认领目标")
    void broadcastRegistration_registersRetryStreamTargetOnly() {
        ListenerRegistration<?> reg =
                reg(ListenerType.AUTO_ACK, false, ConsumeMode.BROADCASTING, 0);

        binder.bindTargets(retryScheduler, pelClaimScheduler, reg);

        // R4-x1：retry Stream 是共享的（retryMode 固定复用基组名），广播实例崩溃后
        // 其 PEL 唯一副本只能靠该目标恢复；若不注册则永久滞留
        verify(pelClaimScheduler)
                .registerRetryStreamTarget(eq(NS), eq(TOPIC), eq(GROUP), eq(MAX_RECONSUME));
        // topic 流是"每实例一个生效组"（{group}:{group}-{id}），基组名在该流上不存在，
        // 注册 TOPIC 目标只会让认领循环对不存在的组刷 NOGROUP 告警
        verify(pelClaimScheduler, never())
                .registerTarget(any(), any(), any(), anyInt(), anyBoolean(), anyInt(), any());
        verify(pelClaimScheduler, never()).registerTarget(any(), any(), any(), anyInt());
        verify(pelClaimScheduler, never()).registerDlqTarget(any(), any(), any());
    }

    @Test
    @DisplayName("广播重试目标与集群一致：仍注册重试 ZSet 目标（失败消息可重投）")
    void broadcastRegistration_registersRetryZSetTarget() {
        ListenerRegistration<?> reg =
                reg(ListenerType.AUTO_ACK, false, ConsumeMode.BROADCASTING, 0);

        binder.bindTargets(retryScheduler, pelClaimScheduler, reg);

        verify(retryScheduler).registerRetryTarget(eq(NS), eq(TOPIC), eq(GROUP), eq(MAX_RECONSUME));
    }

    @Test
    @DisplayName("注销：解除全部调度目标（DLQ 的重试目标按 sentinel 维度解除）")
    void unbind_removesTargets() {
        ListenerRegistration<?> reg = reg(ListenerType.AUTO_ACK, false, ConsumeMode.CLUSTERING, 0);

        binder.unbindTargets(retryScheduler, pelClaimScheduler, reg);

        verify(retryScheduler).unregisterRetryTarget(eq(NS), eq(TOPIC), eq(GROUP));
        verify(pelClaimScheduler).unregisterTargets(eq(NS), eq(TOPIC), eq(GROUP));
    }

    @Test
    @DisplayName("注销 DLQ 注册：重试目标按 (group, group) 维度解除")
    void unbindDlq_removesSentinelRetryTarget() {
        ListenerRegistration<?> reg = reg(ListenerType.AUTO_ACK, true, ConsumeMode.CLUSTERING, 0);

        binder.unbindTargets(retryScheduler, pelClaimScheduler, reg);

        verify(retryScheduler).unregisterRetryTarget(eq(NS), eq(GROUP), eq(GROUP));
        verify(pelClaimScheduler).unregisterTargets(eq(NS), eq(TOPIC), eq(GROUP));
    }

    @Test
    @DisplayName("调度器未装配（null）时跳过对应绑定，不抛异常")
    void nullSchedulers_areSkipped() {
        ListenerRegistration<?> reg = reg(ListenerType.AUTO_ACK, false, ConsumeMode.CLUSTERING, 0);

        binder.bindTargets(null, null, reg);
        binder.unbindTargets(null, null, reg);

        verify(retryScheduler, never()).registerRetryTarget(any(), any(), any(), anyInt());
        verify(pelClaimScheduler, never()).registerTarget(any(), any(), any(), anyInt());
    }

    private static ListenerRegistration<?> reg(
            ListenerType type, boolean dlqMode, ConsumeMode mode, int shardCount) {
        ListenerRegistration<?> reg = mock(ListenerRegistration.class);
        when(reg.getType()).thenReturn(type);
        when(reg.isDlqMode()).thenReturn(dlqMode);
        when(reg.getConsumeMode()).thenReturn(mode);
        when(reg.getNamespace()).thenReturn(NS);
        when(reg.getTopic()).thenReturn(TOPIC);
        when(reg.getGroup()).thenReturn(GROUP);
        when(reg.getMaxReconsumeTimes()).thenReturn(MAX_RECONSUME);
        when(reg.getShardCount()).thenReturn(shardCount);
        return reg;
    }
}
