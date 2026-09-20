/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.listener.InFlightAware;
import io.github.streammq.core.listener.StreamMQListenerContainer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.SmartLifecycle;

/**
 * StreamMQ 优雅关闭处理器。
 *
 * <p>实现 Spring {@link SmartLifecycle}（并保留 {@link DisposableBean} 作为幂等兜底），按 K8s 优雅终止流程逐步释放 StreamMQ
 * 资源，避免消息丢失或重复消费。
 *
 * <p><b>K2：为什么必须是 SmartLifecycle 而不是仅 DisposableBean</b>——容器生命周期由 starter 的 {@code
 * StreamMQListenerContainerLifecycle}（phase = {@code Integer.MAX_VALUE - 200}）管理，它在 SmartLifecycle
 * 的 {@code stop()} 阶段就把容器停掉；而 {@link DisposableBean#destroy()} 直到 {@code destroyBeans()} 阶段才被调用，
 * 届时再 pause 已无意义。本处理器 phase 取 {@value #PHASE}（降序停止时先于容器生命周期执行），保证顺序为： 本处理器 pause → 等待在途收敛 → 容器停止。
 *
 * <p>关闭流程（{@link #stop()}）：
 *
 * <ol>
 *   <li>置 {@code isShuttingDown = true}（{@link AtomicBoolean}，同时作为幂等守卫）
 *   <li>通过 {@link ObjectProvider} 获取 {@link StreamMQListenerContainer}，调用 {@code pause()} 停止拉取新消息
 *   <li>以有界轮询等待在途消息收敛：容器实现 {@link InFlightAware} 时按其 in-flight 计数提前退出；无判据时跳过空转， 由容器 {@code stop()}
 *       内部排空（见 {@link #waitForInFlightMessages}）
 *   <li>调用 {@code container.stop()} 释放线程与连接
 * </ol>
 *
 * <p>其他组件可通过 {@link #isShuttingDown()} 查询当前是否处于关闭过程中， 据此跳过非必要任务（如心跳、rebalance）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Slf4j
public class GracefulShutdownHandler implements SmartLifecycle, DisposableBean {

    /**
     * 生命周期相位：{@code Integer.MAX_VALUE - 150}。
     *
     * <p>降序停止时先于 starter 的 {@code StreamMQListenerContainerLifecycle}（{@code MAX_VALUE - 200}） 执行，
     * 保证「先暂停拉取并等待在途消息，再停止容器」的顺序。
     */
    public static final int PHASE = Integer.MAX_VALUE - 150;

    /** 在途收敛轮询间隔（毫秒） */
    private static final long IN_FLIGHT_POLL_INTERVAL_MS =
            StreamMQConstants.DEFAULT_PAUSED_SLEEP_MS;

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final ObjectProvider<StreamMQListenerContainer> containerProvider;

    private final CloudK8sProperties properties;

    /**
     * 构造优雅关闭处理器。
     *
     * @param containerProvider 监听器容器的可选注入提供者
     * @param properties K8s 云原生增强配置
     */
    public GracefulShutdownHandler(
            ObjectProvider<StreamMQListenerContainer> containerProvider,
            CloudK8sProperties properties) {
        this.containerProvider = Objects.requireNonNull(containerProvider, "containerProvider");
        this.properties = Objects.requireNonNull(properties, "properties");
    }

    /** {@inheritDoc}：容器启动阶段只需置位运行标志（无资源申请，幂等）。 */
    @Override
    public void start() {
        running.set(true);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Spring 在关闭时按 phase 降序调用各 SmartLifecycle 的 {@code stop()}，本处理器先于容器生命周期执行。
     */
    @Override
    public void stop() {
        running.set(false);
        shutdown();
    }

    /**
     * {@inheritDoc}
     *
     * <p>实现 {@link DisposableBean} 仅为兜底（非 SmartLifecycle 装配路径或 {@code stop()} 未被调用时）： {@link
     * #shutdown()} 幂等，重复触发直接返回。
     */
    @Override
    public void destroy() {
        shutdown();
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    /** 关闭流程（幂等）：pause → 有界等待在途收敛 → stop 容器。 */
    private void shutdown() {
        if (!shuttingDown.compareAndSet(false, true)) {
            log.info("StreamMQ graceful shutdown already in progress, skip duplicate trigger");
            return;
        }
        log.info("StreamMQ graceful shutdown initiated");
        StreamMQListenerContainer container = containerProvider.getIfAvailable();
        if (Objects.isNull(container)) {
            log.info("No StreamMQListenerContainer available, graceful shutdown completed");
            return;
        }
        pauseContainer(container);
        waitForInFlightMessages(container);
        stopContainer(container);
        log.info("StreamMQ graceful shutdown completed");
    }

    /**
     * 返回当前是否处于关闭过程中。
     *
     * <p>其他组件可在关闭过程中查询此标志，据此跳过非必要任务（如心跳、rebalance）， 避免在关闭阶段产生无效副作用。
     *
     * @return true 表示已进入优雅关闭流程
     */
    public boolean isShuttingDown() {
        return shuttingDown.get();
    }

    private void pauseContainer(StreamMQListenerContainer container) {
        try {
            log.info("Pausing StreamMQ listener container to stop pulling new messages");
            container.pause();
        } catch (Exception e) {
            log.warn("Failed to pause StreamMQ listener container: {}", e.getMessage());
        }
    }

    /**
     * 以有界轮询等待在途消息收敛。
     *
     * <p><b>K2：为什么不再无条件睡满 {@code graceful-shutdown-timeout-ms}</b>——pause() 仅停止拉取新消息，不改变 running
     * 状态；旧实现只能无条件睡满配置的 grace 周期（默认 30s），叠加容器停止时间后必然超过 K8s 默认 {@code
     * terminationGracePeriodSeconds=30s} 而被 SIGKILL。现在：
     *
     * <ul>
     *   <li>容器实现 {@link InFlightAware}（core 公共契约，所有容器实现均可依赖）时，以其 in-flight 计数为真实判据逐轮轮询， 计数归零立即退出；
     *   <li>容器未实现该契约（{@code StreamMQListenerContainer} 公共 API 未暴露 in-flight 计数）时，不做无意义 空转，直接进入
     *       {@code stop()}——redisson 容器的 {@code stop()} 内部会 awaitTermination 排空在途消费线程；
     *   <li>无论哪种路径都受 grace 周期硬上界约束，且每轮轮询间隔为 {@value #IN_FLIGHT_POLL_INTERVAL_MS} 毫秒。
     * </ul>
     *
     * @param container 监听器容器
     */
    private void waitForInFlightMessages(StreamMQListenerContainer container) {
        long gracePeriod = Math.max(0L, properties.getGracefulShutdownTimeoutMs());
        if (gracePeriod == 0L) {
            return;
        }
        if (!(container instanceof InFlightAware inFlightAware)) {
            log.info(
                    "Container does not implement {}, skipping grace wait and delegating in-flight"
                            + " draining to container.stop()",
                    InFlightAware.class.getSimpleName());
            return;
        }
        long deadline = System.currentTimeMillis() + gracePeriod;
        while (true) {
            int inFlight = inFlightCount(inFlightAware);
            if (inFlight <= 0) {
                log.info("In-flight messages drained, proceeding to stop listener container");
                return;
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                log.warn(
                        "Graceful shutdown timeout ({}ms) elapsed with {} in-flight message(s);"
                                + " stopping container",
                        gracePeriod,
                        inFlight);
                return;
            }
            try {
                Thread.sleep(Math.min(IN_FLIGHT_POLL_INTERVAL_MS, remaining));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for in-flight messages to complete");
                return;
            }
        }
    }

    /** 读取 in-flight 计数：契约要求实现不抛异常，这里仍做防御，异常时按「无法判断」处理并提前退出。 */
    private int inFlightCount(InFlightAware inFlightAware) {
        try {
            return inFlightAware.getInFlightCount();
        } catch (Exception e) {
            log.warn("Failed to read in-flight count: {}", e.getMessage());
            return 0;
        }
    }

    private void stopContainer(StreamMQListenerContainer container) {
        try {
            log.info("Stopping StreamMQ listener container");
            container.stop();
        } catch (Exception e) {
            log.warn("Failed to stop StreamMQ listener container: {}", e.getMessage());
        }
    }
}
