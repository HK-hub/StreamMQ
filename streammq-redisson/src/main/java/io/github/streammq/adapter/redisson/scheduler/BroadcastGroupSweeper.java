/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.listener.RedissonBroadcastGroupRegistry;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.listener.BroadcastGroupRegistry;
import io.github.streammq.core.scheduler.StreamMQScheduler;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 广播消费者组僵尸回收调度器（独立生命周期，解耦于 {@link PelClaimScheduler}）。
 *
 * <p>广播模式下每个容器实例使用一个独立的 Redis 消费者组，组名随容器实例标识（跨重启不保证相同）生成。 实例崩溃 / 重启后，其消费者组与 PEL 会残留于 Redis
 * 内存并持续增长。本调度器周期扫描注册表，将心跳超时的僵尸组 通过 {@code XGROUP DESTROY} 释放，避免 Redis 内存随重启次数单调膨胀。
 *
 * <p><b>解耦说明（P1-B 修复）：</b>此前僵尸回收搭车于 {@link PelClaimScheduler}（顺序消费专属调度器）的低频扫描， 一旦 {@code
 * PelClaimScheduler} 被条件注解禁用或 standalone 使用，广播组将永久泄漏。本调度器作为独立 Bean， 只要 StreamMQ 启用即运行，与 {@link
 * PelClaimScheduler} 是否启用无关，彻底消除共享路径外的泄漏风险。
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型；{@link #stop()} 幂等。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
public class BroadcastGroupSweeper implements StreamMQScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(BroadcastGroupSweeper.class);

    /** 默认扫描间隔（毫秒）：与广播组心跳过期阈值（10min）相比留足余量，避免僵尸组长期驻留 */
    private static final long DEFAULT_SCAN_INTERVAL_MS =
            StreamMQConstants.DEFAULT_BROADCAST_SWEEP_INTERVAL_MS;

    private static final long AWAIT_TERMINATION_SECONDS =
            StreamMQConstants.DEFAULT_AWAIT_TERMINATION_SECONDS;

    private final RedissonClient redisson;
    private final String namespace;
    private final long scanIntervalMs;
    private final BroadcastGroupRegistry registry;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile ScheduledExecutorService scanExecutor;
    private volatile ScheduledFuture<?> scanFuture;

    /**
     * 构造调度器。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param scanIntervalMs 扫描间隔（毫秒），{@code <= 0} 时回落默认值
     * @param registry 广播组注册表（可为 null，回落默认 Redisson 实现）
     */
    public BroadcastGroupSweeper(
            RedissonClient redisson,
            String namespace,
            long scanIntervalMs,
            BroadcastGroupRegistry registry) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = Objects.isNull(namespace) ? "" : namespace;
        this.scanIntervalMs = scanIntervalMs > 0 ? scanIntervalMs : DEFAULT_SCAN_INTERVAL_MS;
        this.registry =
                Objects.nonNull(registry)
                        ? registry
                        : new RedissonBroadcastGroupRegistry(redisson, this.namespace);
        this.scanExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t =
                                    new Thread(
                                            r, StreamMQConstants.THREAD_BROADCAST_SWEEP_SCHEDULER);
                            t.setDaemon(true);
                            return t;
                        });
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.warn("BroadcastGroupSweeper already started");
            return;
        }
        ensureExecutorAlive();
        scanFuture =
                scanExecutor.scheduleAtFixedRate(
                        this::sweep, 0, scanIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info("BroadcastGroupSweeper started, scanIntervalMs={}", scanIntervalMs);
    }

    /** restart 支持：stop 后 executor 已关闭，start 前按需重建。 */
    private synchronized void ensureExecutorAlive() {
        if (Objects.nonNull(scanExecutor) && !scanExecutor.isShutdown()) {
            return;
        }
        scanExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t =
                                    new Thread(
                                            r, StreamMQConstants.THREAD_BROADCAST_SWEEP_SCHEDULER);
                            t.setDaemon(true);
                            return t;
                        });
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        ScheduledFuture<?> future = this.scanFuture;
        if (Objects.nonNull(future)) {
            future.cancel(false);
            this.scanFuture = null;
        }
        scanExecutor.shutdown();
        try {
            if (!scanExecutor.awaitTermination(AWAIT_TERMINATION_SECONDS, TimeUnit.SECONDS)) {
                scanExecutor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            scanExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        LOG.info("BroadcastGroupSweeper stopped");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    private void sweep() {
        try {
            int swept = registry.sweepStaleBroadcastGroups();
            if (swept > 0) {
                LOG.info("Swept {} stale broadcast group(s)", swept);
            }
        } catch (RuntimeException ex) {
            // 回收失败不得阻塞调度：下轮自动重试
            LOG.debug("Sweep stale broadcast groups failed: {}", ex.getMessage());
        }
    }
}
