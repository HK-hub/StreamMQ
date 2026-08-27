/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.streammq.core.scheduler.StreamMQScheduler;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.ObjectProvider;

/**
 * {@link StreamMQSchedulerLifecycle} 部分启动状态跟踪测试。
 *
 * <p>验证单个调度器启动失败时状态表可见（RUNNING / FAILED:&lt;reason&gt;）， 以及停止后状态被清理。
 */
@DisplayName("调度器生命周期状态跟踪测试")
class StreamMQSchedulerLifecycleTest {

    /** 可控失败的假调度器。 */
    static class FakeScheduler implements StreamMQScheduler {
        private final String name;
        private final boolean failOnStart;
        private volatile boolean running = false;

        FakeScheduler(String name, boolean failOnStart) {
            this.name = name;
            this.failOnStart = failOnStart;
        }

        @Override
        public void start() {
            if (failOnStart) {
                throw new IllegalStateException("boom-" + name);
            }
            running = true;
        }

        @Override
        public void stop() {
            running = false;
        }

        @Override
        public boolean isRunning() {
            return running;
        }
    }

    /** 状态表以 {@code getClass().getSimpleName()} 为键——不同调度器实例需以不同子类区分， 否则同名键会相互覆盖（这正是部分失败可见性要避免的）。 */
    static class SchedulerA extends FakeScheduler {
        SchedulerA() {
            super("A", false);
        }
    }

    static class SchedulerB extends FakeScheduler {
        SchedulerB() {
            super("B", false);
        }
    }

    static class OkScheduler extends FakeScheduler {
        OkScheduler(String tag) {
            super(tag, false);
        }
    }

    static class BadScheduler extends FakeScheduler {
        BadScheduler(String tag) {
            super(tag, true);
        }
    }

    @Test
    @DisplayName("全部成功时各调度器状态为 RUNNING")
    void allStarted_statusesRunning() {
        StreamMQSchedulerLifecycle lifecycle =
                new StreamMQSchedulerLifecycle(List.of(new SchedulerA(), new SchedulerB()));
        lifecycle.start();

        Map<String, String> statuses = lifecycle.getSchedulerStatuses();
        assertThat(statuses)
                .containsEntry("SchedulerA", "RUNNING")
                .containsEntry("SchedulerB", "RUNNING");

        lifecycle.stop();
        assertThat(lifecycle.getSchedulerStatuses()).isEmpty();
    }

    @Test
    @DisplayName("部分失败时失败项记录 FAILED:<reason> 且整体仍标记 running")
    void partialFailure_failedStatusVisible() {
        StreamMQScheduler ok = new OkScheduler("Ok");
        StreamMQScheduler bad = new BadScheduler("Bad");
        StreamMQSchedulerLifecycle lifecycle = new StreamMQSchedulerLifecycle(List.of(ok, bad));
        lifecycle.start();

        assertThat(lifecycle.isRunning()).isTrue();
        Map<String, String> statuses = lifecycle.getSchedulerStatuses();
        assertThat(statuses).containsEntry("OkScheduler", "RUNNING");
        assertThat(statuses.get("BadScheduler")).startsWith("FAILED:").contains("boom-Bad");
    }

    @Test
    @DisplayName("全部失败时回滚且不进入 running 状态")
    void allFailure_rolledBack() {
        StreamMQScheduler bad1 = new BadScheduler("Bad1");
        StreamMQScheduler bad2 = new BadScheduler("Bad2") {};
        StreamMQSchedulerLifecycle lifecycle = new StreamMQSchedulerLifecycle(List.of(bad1, bad2));
        lifecycle.start();

        assertThat(lifecycle.isRunning()).isFalse();
        assertThat(lifecycle.getSchedulerStatuses().get("BadScheduler")).startsWith("FAILED:");
    }

    @Test
    @DisplayName("健康检查：存在 FAILED 调度器时报告 DOWN，正常时保持 UP")
    @SuppressWarnings({"rawtypes", "unchecked"})
    void healthIndicator_reflectsSchedulerStatuses() {
        RedissonClient redisson = mock(RedissonClient.class);
        RAtomicLong atomicLong = mock(RAtomicLong.class);
        when(redisson.getAtomicLong(anyString())).thenReturn(atomicLong);
        when(atomicLong.get()).thenReturn(1L);

        // 构造带 FAILED 状态的 lifecycle（通过启动失败路径写入状态表）
        StreamMQSchedulerLifecycle failedLifecycle =
                new StreamMQSchedulerLifecycle(
                        List.of(new OkScheduler("Ok"), new BadScheduler("Broken")));
        failedLifecycle.start();

        ObjectProvider<StreamMQSchedulerLifecycle> provider =
                (ObjectProvider) org.mockito.Mockito.mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(failedLifecycle);

        StreamMQHealthAutoConfiguration.StreamMQHealthIndicator indicator =
                new StreamMQHealthAutoConfiguration.StreamMQHealthIndicator(
                        redisson, null, provider);
        var health = indicator.health();
        assertThat(health.getStatus().getCode()).isEqualTo("DOWN");
        assertThat(health.getDetails()).containsKey("scheduler.statuses");

        // 正常分支：无 FAILED 状态 → UP
        StreamMQSchedulerLifecycle healthyLifecycle =
                new StreamMQSchedulerLifecycle(List.of(new SchedulerA()));
        healthyLifecycle.start();
        when(provider.getIfAvailable()).thenReturn(healthyLifecycle);
        var upHealth = indicator.health();
        assertThat(upHealth.getStatus().getCode()).isEqualTo("UP");
        assertThat((Map<String, String>) upHealth.getDetails().get("scheduler.statuses"))
                .containsEntry("SchedulerA", "RUNNING");

        // 未装配调度器生命周期时不影响健康状态
        when(provider.getIfAvailable()).thenReturn(null);
        var noLifecycleHealth = indicator.health();
        assertThat(noLifecycleHealth.getStatus().getCode()).isEqualTo("UP");
    }
}
