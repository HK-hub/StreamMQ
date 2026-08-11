package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.core.scheduler.StreamMQScheduler;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * 统一管理 StreamMQ 调度器（{@code RetryScheduler} / {@code DelayMessageScheduler} / {@code
 * TransactionScanner}） 的启停，作为 Spring {@link SmartLifecycle} 注册。
 *
 * <p>启动相位 {@link #PHASE} 高于 Listener 容器，确保调度器先启动； 停止时反向停止：先停止 Listener 容器（由其自己的 SmartLifecycle
 * 控制），再停止调度器。
 *
 * <p>调度器实例本身不实现 SmartLifecycle，由本类统一管理，避免分散的 lifecycle bean。
 *
 * <p>本类依赖 {@link StreamMQScheduler} 接口而非具体实现，遵循"依赖接口而非实现"原则。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMQSchedulerLifecycle implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMQSchedulerLifecycle.class);

    /** 启动相位：高于 Listener 容器（{@code Integer.MAX_VALUE - 200}） */
    public static final int PHASE = Integer.MAX_VALUE - 100;

    private final List<StreamMQScheduler> schedulers;
    private volatile boolean running = false;

    /**
     * 构造 Lifecycle。
     *
     * @param schedulers 调度器列表（按启动顺序：先注册者先启动）
     */
    public StreamMQSchedulerLifecycle(List<StreamMQScheduler> schedulers) {
        this.schedulers = Objects.requireNonNull(schedulers, "schedulers");
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        LOG.info("Starting StreamMQ schedulers (phase={}, count={})", PHASE, schedulers.size());
        int totalCount = schedulers.size();
        int failedCount = 0;
        for (StreamMQScheduler scheduler : schedulers) {
            try {
                scheduler.start();
            } catch (RuntimeException ex) {
                failedCount++;
                LOG.error(
                        "Failed to start scheduler {}: {}",
                        scheduler.getClass().getSimpleName(),
                        ex.getMessage(),
                        ex);
            }
        }
        if (totalCount == 0) {
            LOG.info("No StreamMQ schedulers to start, setting running=true (no-op)");
            running = true;
        } else if (failedCount >= totalCount) {
            LOG.error(
                    "All {} StreamMQ scheduler(s) failed to start, not setting running=true",
                    totalCount);
            // 全部失败时不设 running，后续 stop 不会执行（状态保持一致）
        } else {
            if (failedCount > 0) {
                LOG.warn(
                        "StreamMQ schedulers started with {} failure(s) out of {}, some features"
                                + " may not work",
                        failedCount,
                        totalCount);
            }
            running = true;
        }
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        LOG.info("Stopping StreamMQ schedulers");
        // 反向停止
        for (int i = schedulers.size() - 1; i >= 0; i--) {
            StreamMQScheduler scheduler = schedulers.get(i);
            try {
                scheduler.stop();
            } catch (RuntimeException ex) {
                LOG.error(
                        "Failed to stop scheduler {}: {}",
                        scheduler.getClass().getSimpleName(),
                        ex.getMessage(),
                        ex);
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
}
