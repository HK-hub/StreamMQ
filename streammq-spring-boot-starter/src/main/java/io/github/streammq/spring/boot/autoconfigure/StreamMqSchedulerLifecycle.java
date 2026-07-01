package io.github.streammq.spring.boot.autoconfigure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.SmartLifecycle;

import java.util.List;
import java.util.Objects;

/**
 * 统一管理 StreamMQ 调度器（{@code RetryScheduler} / {@code DelayMessageScheduler} / {@code TransactionScanner}）
 * 的启停，作为 Spring {@link SmartLifecycle} 注册。
 *
 * <p>启动相位 {@link #PHASE} 高于 Listener 容器，确保调度器先启动；
 * 停止时反向停止：先停止 Listener 容器（由其自己的 SmartLifecycle 控制），再停止调度器。
 *
 * <p>调度器实例本身不实现 SmartLifecycle，由本类统一管理，避免分散的 lifecycle bean。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMqSchedulerLifecycle implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMqSchedulerLifecycle.class);

    /** 启动相位：高于 Listener 容器（{@code Integer.MAX_VALUE - 200}） */
    public static final int PHASE = Integer.MAX_VALUE - 100;

    private final List<Object> schedulers;
    private volatile boolean running = false;

    /**
     * 构造 Lifecycle。
     *
     * @param schedulers 调度器列表（按启动顺序：先注册者先启动）
     */
    public StreamMqSchedulerLifecycle(List<Object> schedulers) {
        this.schedulers = Objects.requireNonNull(schedulers, "schedulers");
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        LOG.info("Starting StreamMQ schedulers (phase={}, count={})", PHASE, schedulers.size());
        int failedCount = 0;
        for (Object scheduler : schedulers) {
            try {
                invokeStart(scheduler);
            } catch (RuntimeException ex) {
                failedCount++;
                LOG.error("Failed to start scheduler {}: {}", scheduler.getClass().getSimpleName(), ex.getMessage(), ex);
            }
        }
        if (failedCount > 0) {
            LOG.warn("StreamMQ schedulers started with {} failure(s) out of {}, some features may not work",
                failedCount, schedulers.size());
        }
        // 即使部分调度器启动失败也设为 running，但记录警告
        running = true;
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        LOG.info("Stopping StreamMQ schedulers");
        // 反向停止
        for (int i = schedulers.size() - 1; i >= 0; i--) {
            Object scheduler = schedulers.get(i);
            try {
                invokeStop(scheduler);
            } catch (RuntimeException ex) {
                LOG.error("Failed to stop scheduler {}: {}",
                    scheduler.getClass().getSimpleName(), ex.getMessage(), ex);
            }
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    private static void invokeStart(Object scheduler) {
        try {
            var method = scheduler.getClass().getMethod("start");
            method.setAccessible(true);
            method.invoke(scheduler);
        } catch (ReflectiveOperationException ex) {
            throw new IllegalStateException(
                "Scheduler " + scheduler.getClass().getName() + " has no start() method", ex);
        }
    }

    private static void invokeStop(Object scheduler) {
        try {
            var method = scheduler.getClass().getMethod("stop");
            method.setAccessible(true);
            method.invoke(scheduler);
        } catch (ReflectiveOperationException ex) {
            throw new IllegalStateException(
                "Scheduler " + scheduler.getClass().getName() + " has no stop() method", ex);
        }
    }
}
