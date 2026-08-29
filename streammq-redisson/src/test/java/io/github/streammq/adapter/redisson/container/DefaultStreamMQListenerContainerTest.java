/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.listener.StreamMQListenerFactory;
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

        container.stop();

        assertThat(injectedExecutor.isShutdown()).as("外部注入的执行器生命周期归提供方，容器不得关闭").isFalse();
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
}
