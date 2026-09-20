/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

/**
 * {@link DefaultStreamMQListenerContainer} 生命周期与所有权单元测试。
 *
 * <p>重点覆盖两处曾经存在缺陷的行为：
 *
 * <ol>
 *   <li><b>消费循环启动失败必须可见</b>：此前监听器创建失败只打一条 ERROR 日志就退出，消费者在管理端点 仍可见、健康检查仍为 UP。现在失败会登记到 {@link
 *       DefaultStreamMQListenerContainer#getConsumeLoopFailures()} 并使 {@link
 *       DefaultStreamMQListenerContainer#isConsumeLoopsHealthy()} 返回 false。
 *   <li><b>注入执行器时不得泄漏内部执行器</b>：构造器字段初始化会创建一个虚拟线程执行器，{@code setConsumeExecutor}
 *       替换它时必须先关闭，否则每次注入泄漏一个。
 * </ol>
 *
 * @author StreamMQ Contributors
 * @since 0.1.1
 */
@DisplayName("DefaultStreamMQListenerContainer 生命周期与所有权")
class DefaultStreamMQListenerContainerTest {

    private ExecutorService injectedExecutor;

    @AfterEach
    void tearDown() {
        if (injectedExecutor != null) {
            injectedExecutor.shutdownNow();
            injectedExecutor = null;
        }
    }

    private DefaultStreamMQListenerContainer newContainer() {
        return new DefaultStreamMQListenerContainer(
                mock(RedissonClient.class),
                mock(StreamMQListenerFactory.class),
                mock(MessageConverter.class),
                mock(RetryPolicy.class),
                mock(DlqFailureStrategy.class),
                DlqConfig.builder().build(),
                "test-namespace");
    }

    @Test
    @DisplayName("初始状态下消费循环健康且无失败登记")
    void initiallyHealthy() {
        DefaultStreamMQListenerContainer container = newContainer();
        assertThat(container.isConsumeLoopsHealthy()).isTrue();
        assertThat(container.getConsumeLoopFailures()).isEmpty();
    }

    @Test
    @DisplayName("同 JVM 内多个容器自动推导的 instanceToken 互不相同（广播组名防碰撞）")
    void autoResolvedInstanceTokens_areUniquePerContainer() {
        // 主机名是进程级值：两个容器若都解析到同一主机名，广播组名（group:consumerName）
        // 将碰撞、消息只投递给其一——广播语义退化为集群（回归：P1-10）。
        DefaultStreamMQListenerContainer first = newContainer();
        DefaultStreamMQListenerContainer second = newContainer();
        assertThat(first.getInstanceToken())
                .as("同 JVM 容器级标识必须唯一")
                .isNotEqualTo(second.getInstanceToken());

        // 显式配置的 token 不受序号机制影响
        DefaultStreamMQListenerContainer explicit = newContainer();
        explicit.setInstanceToken("my-instance");
        assertThat(explicit.getInstanceToken()).isEqualTo("my-instance");
    }

    @Test
    @DisplayName("重复注入执行器时，容器不会关闭任何外部注入的执行器（所有权归提供方）")
    void repeatedInjectionNeverShutsDownExternalExecutors() {
        DefaultStreamMQListenerContainer container = newContainer();
        injectedExecutor = Executors.newSingleThreadExecutor();
        container.setConsumeExecutor(injectedExecutor);

        ExecutorService second = Executors.newSingleThreadExecutor();
        try {
            // INIT 状态下允许重复定制。关键在于：第一次注入后 ownsExecutor 已置为 false，
            // 因此第二次注入**不得**关闭第一次注入的执行器——否则容器会关掉不属于自己的资源。
            container.setConsumeExecutor(second);
            assertThat(injectedExecutor.isShutdown()).as("外部注入的执行器不应被容器的后续注入动作关闭").isFalse();
        } finally {
            second.shutdownNow();
        }
    }

    @Test
    @DisplayName("容器离开 INIT 之后不允许再定制执行器（INIT-only 约束）")
    void injectionRejectedAfterLeavingInit() {
        DefaultStreamMQListenerContainer container = newContainer();
        injectedExecutor = Executors.newSingleThreadExecutor();

        // 空注册表的容器可以无副作用地 start（无 group manager、无消费循环），
        // 足以让生命周期离开 INIT。
        container.start();
        try {
            assertThat(container.isRunning()).isTrue();
            assertThatThrownBy(() -> container.setConsumeExecutor(injectedExecutor))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("consumeExecutor");
        } finally {
            container.stop();
        }
    }

    @Test
    @DisplayName("注入 null 执行器抛出 NullPointerException")
    void injectionRejectsNull() {
        DefaultStreamMQListenerContainer container = newContainer();
        assertThatThrownBy(() -> container.setConsumeExecutor(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("executor");
    }

    @Test
    @DisplayName("注入的执行器在容器 stop 时不被关闭（所有权归提供方）")
    void stopDoesNotShutdownInjectedExecutor() throws Exception {
        DefaultStreamMQListenerContainer container = newContainer();
        injectedExecutor = Executors.newSingleThreadExecutor();
        container.setConsumeExecutor(injectedExecutor);

        // 必须先 start 使容器离开 INIT、真正进入 stop 主逻辑。此前该测试直接在 INIT 调 stop，
        // tryBeginStop() 对 INIT 返回 false 提前返回，等于什么都没测到（假阳性）。
        container.start();
        assertThat(container.isRunning()).as("start 后容器应处于运行态").isTrue();
        container.stop();

        assertThat(injectedExecutor.isShutdown()).as("外部注入的执行器生命周期归提供方，容器 stop 时不得关闭").isFalse();
        assertThat(container.isRunning()).isFalse();
    }

    @Test
    @DisplayName("内部默认执行器在容器 stop 时被关闭（所有权归容器，与注入场景对照）")
    void stopShutsDownInternalExecutor() {
        DefaultStreamMQListenerContainer container = newContainer();
        container.start();
        assertThat(container.isRunning()).isTrue();
        container.stop();
        assertThat(container.isRunning()).isFalse();
    }

    @Test
    @DisplayName("stop 后清空消费循环失败登记，避免历史失败影响下一次 start")
    void stopClearsLoopFailures() {
        DefaultStreamMQListenerContainer container = newContainer();
        container.stop();
        assertThat(container.isConsumeLoopsHealthy()).isTrue();
        assertThat(container.getConsumeLoopFailures()).isEmpty();
    }

    @Test
    @DisplayName("getConsumeLoopFailures 返回不可修改快照，调用方无法污染容器内部状态")
    void failuresSnapshotIsImmutable() {
        DefaultStreamMQListenerContainer container = newContainer();
        assertThatThrownBy(
                        () ->
                                container
                                        .getConsumeLoopFailures()
                                        .put("k", "v")) // Map.copyOf 的结果不可修改
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // ===================== 红队第六轮：R1-6 运行期配置生效性 =====================

    @Test
    @DisplayName("R1-6②：按 group 暂停只影响目标注册，容器级暂停仍可全停")
    void pauseGroup_onlyAffectsTargetRegistration() {
        DefaultStreamMQListenerContainer container = newContainer();
        ListenerRegistration<?> target = mock(ListenerRegistration.class);
        when(target.getGroup()).thenReturn("group-a");
        when(target.getTopic()).thenReturn("topic-a");
        ListenerRegistration<?> other = mock(ListenerRegistration.class);
        when(other.getGroup()).thenReturn("group-b");
        when(other.getTopic()).thenReturn("topic-b");

        java.util.function.BooleanSupplier targetPaused = container.pausedSupplierFor(target);
        java.util.function.BooleanSupplier otherPaused = container.pausedSupplierFor(other);

        // 初始：都未暂停
        assertThat(targetPaused.getAsBoolean()).isFalse();
        assertThat(otherPaused.getAsBoolean()).isFalse();

        // 只暂停 group-a：group-b 不受影响（旧实现为容器级 paused，会把两者都停掉）
        container.pauseGroup("group-a");
        assertThat(targetPaused.getAsBoolean()).as("目标 group 必须暂停").isTrue();
        assertThat(otherPaused.getAsBoolean()).as("其它 group 不得被连带暂停").isFalse();
        assertThat(container.isGroupPaused("group-a")).isTrue();
        assertThat(container.isGroupPaused("group-b")).isFalse();

        // 按 group 恢复
        container.resumeGroup("group-a");
        assertThat(targetPaused.getAsBoolean()).isFalse();

        // 容器级暂停仍可全停
        container.pause();
        assertThat(targetPaused.getAsBoolean()).isTrue();
        assertThat(otherPaused.getAsBoolean()).isTrue();
        assertThat(container.isPaused()).isTrue();
    }

    @Test
    @DisplayName("R1-6③：inflightCapacity 未被运行中循环采用时必须回 false（不得宣称已生效）")
    void inflightCapacityApplied_reportsHonestly() {
        DefaultStreamMQListenerContainer container = newContainer();
        // 无运行中循环：不存在"运行中的旧值"，视为已应用
        assertThat(container.isInflightCapacityApplied("topic-x", "group-x")).isTrue();

        // 模拟该注册的循环已用容量 0 启动（记录启动快照）——快照与当前值一致 → true
        container.recordAppliedInflightCapacityForTest("topic-x", "group-x", 0);
        assertThat(container.isInflightCapacityApplied("topic-x", "group-x")).isTrue();

        // 运行期改为 128 但循环未重启：队列仍是旧容量，必须如实回 false
        container.setInflightCapacity(128);
        assertThat(container.isInflightCapacityApplied("topic-x", "group-x"))
                .as("运行期改容量未重启循环：必须如实回 false")
                .isFalse();

        // 循环重启（新的启动快照）后回 true
        container.recordAppliedInflightCapacityForTest("topic-x", "group-x", 128);
        assertThat(container.isInflightCapacityApplied("topic-x", "group-x")).isTrue();
    }
}
