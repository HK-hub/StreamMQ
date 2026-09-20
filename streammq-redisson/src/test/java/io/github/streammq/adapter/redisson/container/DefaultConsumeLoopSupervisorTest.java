/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.ListenerType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * {@link DefaultConsumeLoopSupervisor} 循环登记表回归测试。
 *
 * <p>回归 B-09：登记表用 {@code putIfAbsent} 维护幂等，但 {@code putIfAbsent} 不会覆盖"已结束循环" 遗留的陈旧 Future。旧实现先判
 * {@code hasActiveLoops}（只看 {@code !isDone()}）再提交，于是：
 *
 * <ul>
 *   <li>陈旧条目让 {@code hasActiveLoops} 返回 false → 提交新循环；
 *   <li>提交时 {@code putIfAbsent} 命中陈旧条目 → 新循环被 {@code cancel(true)} 且无任何日志；
 *   <li>结果：该注册永久静默不消费（重试/DLQ/并发扩展循环同样受影响）。
 * </ul>
 *
 * <p>本测试用已完成的 {@link CompletableFuture} 精确复现该状态，并断言"陈旧条目被清理 + 新循环真正生效"。
 */
@DisplayName("DefaultConsumeLoopSupervisor 陈旧 Future 清理")
class DefaultConsumeLoopSupervisorTest {

    private FakeLoopFactory loopFactory;
    private DefaultConsumeLoopSupervisor supervisor;
    private ListenerRegistration<?> reg;
    private Logger supervisorLogger;
    private ListAppender<ILoggingEvent> logAppender;

    @BeforeEach
    void setUp() {
        loopFactory = new FakeLoopFactory();
        supervisor = new DefaultConsumeLoopSupervisor(loopFactory);
        reg = mock(ListenerRegistration.class);
        when(reg.getType()).thenReturn(ListenerType.ORDERLY);
        when(reg.key()).thenReturn("order-topic:order-group");

        supervisorLogger = (Logger) LoggerFactory.getLogger(DefaultConsumeLoopSupervisor.class);
        logAppender = new ListAppender<>();
        logAppender.start();
        supervisorLogger.addAppender(logAppender);
    }

    @AfterEach
    void tearDown() {
        if (supervisorLogger != null && logAppender != null) {
            supervisorLogger.detachAppender(logAppender);
            logAppender.stop();
        }
    }

    @Test
    @DisplayName("已结束循环的陈旧登记项被清理：再次提交真正生效（不再静默不消费）")
    void finishedLoop_isEvictedAndResubmitted() {
        // 首次提交的循环立即结束（模拟循环因异常终止/被取消后残留登记项）
        loopFactory.completeImmediately = true;
        supervisor.submitLoops(reg);
        assertThat(loopFactory.launchedFutures).hasSize(1);
        assertThat(loopFactory.launchedFutures.get(0).isDone()).isTrue();

        // 再次提交：旧实现下新循环会被 putIfAbsent 命中的陈旧 Future 顶掉并 cancel（永不消费）
        loopFactory.completeImmediately = false;
        supervisor.submitLoops(reg);

        assertThat(loopFactory.launchedCount()).as("陈旧登记项必须被清理后重新提交，否则该注册永久不消费").isEqualTo(2);
        Future<?> resubmitted = loopFactory.launchedFutures.get(1);
        assertThat(resubmitted.isCancelled()).as("新提交的循环不得被取消").isFalse();
        assertThat(resubmitted.isDone()).isFalse();
        // 未发生冲突：不应出现取消告警
        assertThat(warnEvents()).isEmpty();
    }

    @Test
    @DisplayName("活跃循环存在时重复提交被幂等拦截（不重复拉取）")
    void activeLoop_isNotResubmitted() {
        supervisor.submitLoops(reg);
        supervisor.submitLoops(reg);

        assertThat(loopFactory.launchedCount()).isEqualTo(1);
        assertThat(loopFactory.launchedFutures.get(0).isCancelled()).isFalse();
    }

    @Test
    @DisplayName("并发提交竞争：重复提交的循环被取消并 WARN 留痕（含 key 与原因）")
    void duplicateLaunch_isCancelledWithWarn() {
        // 用"提交过程中再次提交同一注册"确定性地复现并发竞争：内层提交先登记成功，
        // 外层提交命中冲突分支（旧实现此处静默 cancel，故障无从排查）
        loopFactory.reenter = () -> supervisor.submitLoops(reg);

        supervisor.submitLoops(reg);

        assertThat(loopFactory.launchedCount()).isEqualTo(2);
        assertThat(loopFactory.launchedFutures.get(1).isCancelled()).as("冲突分支必须取消重复提交的循环").isTrue();
        assertThat(warnEvents())
                .as("取消重复循环必须 WARN 留痕（含 key 与原因）")
                .anySatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .contains("Duplicate consume loop cancelled")
                                        .contains("order-topic:order-group"));
    }

    @Test
    @DisplayName("R1-7：replaceLoops 先取消旧循环（含 inflight 泵）再提交新循环——重注册不再静默无效")
    void replaceLoops_cancelsOldAndLaunchesNew() {
        supervisor.submitLoops(reg);
        assertThat(loopFactory.launchedCount()).isEqualTo(1);
        Future<?> oldLoop = loopFactory.launchedFutures.get(0);
        // 登记一个 inflight 泵（键约定 {loopKey}#{idx}:inflight-processor）
        CompletableFuture<Void> pump = new CompletableFuture<>();
        supervisor.registerInflightPump(reg.key() + "#0", pump);

        supervisor.replaceLoops(reg);

        assertThat(oldLoop.isCancelled()).as("旧循环必须被取消（否则旧 consumer 永远继续消费）").isTrue();
        assertThat(pump.isCancelled()).as("旧 inflight 泵必须一并取消（否则孤儿线程泄漏）").isTrue();
        assertThat(loopFactory.launchedCount()).as("新注册的循环必须真正提交").isEqualTo(2);
        Future<?> newLoop = loopFactory.launchedFutures.get(1);
        assertThat(newLoop.isCancelled()).isFalse();
        assertThat(newLoop.isDone()).isFalse();
        assertThat(logEvents())
                .as("替换必须留痕（含 topic/group）")
                .anySatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .contains("Replacing consume loops"));
    }

    private List<ILoggingEvent> warnEvents() {
        return logAppender.list.stream()
                .filter(event -> Level.WARN.equals(event.getLevel()))
                .toList();
    }

    private List<ILoggingEvent> logEvents() {
        return List.copyOf(logAppender.list);
    }

    /** 记录每次 launch 的 Future，并支持"立即完成 / 重入提交"两种故障注入。 */
    private static final class FakeLoopFactory implements ConsumeLoopSupervisor.LoopFactory {

        private final List<Future<?>> launchedFutures = new ArrayList<>();
        private final AtomicBoolean reentered = new AtomicBoolean(false);

        private volatile boolean completeImmediately;
        private volatile Runnable reenter;

        @Override
        public Future<?> launch(
                ListenerRegistration<?> reg,
                boolean retryMode,
                boolean primaryLoop,
                int loopIndex) {
            Runnable reentrant = reenter;
            if (reentrant != null && reentered.compareAndSet(false, true)) {
                reentrant.run();
            }
            CompletableFuture<Void> future = new CompletableFuture<>();
            if (completeImmediately) {
                future.complete(null);
            }
            launchedFutures.add(future);
            return future;
        }

        private int launchedCount() {
            return launchedFutures.size();
        }
    }
}
