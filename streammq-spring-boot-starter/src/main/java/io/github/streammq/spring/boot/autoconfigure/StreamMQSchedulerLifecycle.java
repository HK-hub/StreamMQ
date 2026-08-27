/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.core.scheduler.StreamMQScheduler;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
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

    /**
     * 启动相位：低于 Listener 容器（{@code Integer.MAX_VALUE - 200}）。
     *
     * <p>Spring 按 phase 升序启动、降序停止：调度器先于容器启动（消费前 Retry/Delay/Tx 扫描就绪）， 容器先于调度器停止（停止期间扫描能力保持完整）。
     */
    public static final int PHASE = Integer.MAX_VALUE - 300;

    /** 调度器状态值：运行中 */
    public static final String STATUS_RUNNING = "RUNNING";

    /** 调度器状态前缀：启动失败（后接异常信息） */
    public static final String STATUS_FAILED_PREFIX = "FAILED:";

    private final List<StreamMQScheduler> schedulers;
    private volatile boolean running = false;

    /**
     * 调度器名称 → 运行状态（"RUNNING" / "FAILED:&lt;reason&gt;"）。
     *
     * <p>供健康检查等组件读取，使「部分调度器启动失败」对运维可见而非仅停留在日志中。
     */
    private final Map<String, String> schedulerStatuses = new ConcurrentHashMap<>();

    /**
     * 构造 Lifecycle。
     *
     * @param schedulers 调度器列表（按启动顺序：先注册者先启动）
     */
    public StreamMQSchedulerLifecycle(List<StreamMQScheduler> schedulers) {
        this.schedulers = Objects.requireNonNull(schedulers, "schedulers");
    }

    /**
     * 返回各调度器的运行状态快照（只读视图）。
     *
     * @return 不可变 Map，key 为调度器类简称，value 为 {@code "RUNNING"} 或 {@code "FAILED:<reason>"}
     */
    public Map<String, String> getSchedulerStatuses() {
        return Collections.unmodifiableMap(schedulerStatuses);
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
            String name = scheduler.getClass().getSimpleName();
            try {
                scheduler.start();
                schedulerStatuses.put(name, STATUS_RUNNING);
            } catch (RuntimeException ex) {
                failedCount++;
                schedulerStatuses.put(name, STATUS_FAILED_PREFIX + ex.getMessage());
                LOG.error("Failed to start scheduler {}: {}", name, ex.getMessage(), ex);
            }
        }
        if (totalCount == 0) {
            LOG.info("No StreamMQ schedulers to start, setting running=true (no-op)");
            running = true;
        } else if (failedCount >= totalCount) {
            LOG.error(
                    "All {} StreamMQ scheduler(s) failed to start, rolling back partial state",
                    totalCount);
            // 全部失败：回滚已部分初始化的调度器（其内部资源如 executor 需要释放），保持状态一致
            for (StreamMQScheduler scheduler : schedulers) {
                try {
                    scheduler.stop();
                } catch (RuntimeException stopEx) {
                    LOG.debug(
                            "Rollback stop failed for {}: {}",
                            scheduler.getClass().getSimpleName(),
                            stopEx.getMessage());
                }
            }
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
                schedulerStatuses.remove(scheduler.getClass().getSimpleName());
            } catch (RuntimeException ex) {
                LOG.error(
                        "Failed to stop scheduler {}: {}",
                        scheduler.getClass().getSimpleName(),
                        ex.getMessage(),
                        ex);
            }
        }
        schedulerStatuses.clear();
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
