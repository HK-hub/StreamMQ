/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.support.RedisClusterCompatibility;
import io.github.streammq.adapter.redisson.support.RedisServerClock;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.metrics.StreamMQMetrics;
import io.github.streammq.core.scheduler.StreamMQScheduler;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Setter;
import org.redisson.api.BatchOptions;
import org.redisson.api.RBatch;
import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 延时消息调度器，周期扫描各 {@link DelayLevel} 的 ZSet，将到期消息转投到目标 Stream。
 *
 * <p>存储模型（对齐 04-detailed-design.md §6）：
 *
 * <ul>
 *   <li>ZSet Key: {@code streammq:{ns}:delay:{level}}，score=deliverAt(ms)，member=msgId
 *   <li>payload Hash Key: {@code streammq:{ns}:delay:payload:{msgId}}，存储消息完整字段 + targetTopic +
 *       deliverAt
 * </ul>
 *
 * <p>转投流程（执行权 claim + 原子批，无丢失窗口）：
 *
 * <ol>
 *   <li>{@code ZRANGEBYSCORE 0 now LIMIT 0 batchSize} 获取到期 msgId
 *   <li>对每个 msgId：SETNX 执行权 claim（TTL 兜底，持有者崩溃后可接管）
 *   <li>原子批（REDIS_WRITE_ATOMIC）：XADD 目标 Stream + DEL payload Hash + ZREM 调度条目 ——要么全部生效、要么全部不生效；批失败时
 *       entry 留在 ZSet 等待下轮重试
 * </ol>
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型。
 *
 * <p>清理机制：
 *
 * <ul>
 *   <li>正常流程：原子批成功后 ZREM 移除 ZSet entry，DEL 删除 payload Hash
 *   <li>安全兜底：{@link #cleanupOrphanedEntries()} 可清理无对应 payload 的孤立 entry（防止异常堆积）
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DelayMessageScheduler implements StreamMQScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(DelayMessageScheduler.class);

    /** payload Hash 中的目标 Topic 字段名 */
    public static final String FIELD_TARGET_TOPIC = StreamMQConstants.FIELD_TARGET_TOPIC;

    /** payload Hash 中的投递时间字段名 */
    public static final String FIELD_DELIVER_AT = StreamMQConstants.FIELD_DELIVER_AT;

    /** 自定义延时等级标识（用于指标与清理逻辑） */
    public static final String DELAY_CUSTOM_LEVEL = "custom";

    /** 默认扫描间隔（毫秒） */
    private static final long DEFAULT_SCAN_INTERVAL_MS = StreamMQConstants.DEFAULT_SCAN_INTERVAL_MS;

    /** 默认单次扫描批量 */
    private static final int DEFAULT_BATCH_SIZE = StreamMQConstants.DEFAULT_BATCH_SIZE;

    /** 关闭调度线程池时的等待超时（秒） */
    private static final long AWAIT_TERMINATION_SECONDS =
            StreamMQConstants.DEFAULT_AWAIT_TERMINATION_SECONDS;

    /** 默认转移失败后的回写退避（毫秒）：避免 Redis 故障时以 scan 间隔高频热循环重试 */
    private static final long DEFAULT_FAILURE_REQUEUE_BACKOFF_MS =
            StreamMQConstants.DEFAULT_FAILURE_REQUEUE_BACKOFF_MS;

    /** 转移失败后的回写退避（毫秒），可通过 {@link #setFailureRequeueBackoffMs(long)} 覆盖 */
    private volatile long failureRequeueBackoffMs = DEFAULT_FAILURE_REQUEUE_BACKOFF_MS;

    /** Redis 服务器时钟不可用（回退本机时钟）时的告警限频间隔（毫秒） */
    private static final long CLOCK_FALLBACK_WARN_INTERVAL_MS = 60_000L;

    /** Redis 服务器时钟回退告警限频时间戳 */
    private final AtomicLong lastClockFallbackWarnMs = new AtomicLong();

    /**
     * 设置转移失败后的回写退避间隔（毫秒）。
     *
     * @param millis 退避间隔，必须 &gt; 0
     */
    public void setFailureRequeueBackoffMs(long millis) {
        if (millis > 0) {
            this.failureRequeueBackoffMs = millis;
        }
    }

    private final RedissonClient redisson;
    private final String namespace;
    private final long scanIntervalMs;
    private final int batchSize;

    /** Lua：仅当转移执行权 claim 仍归本实例持有时删除（原子 compare-and-delete）。 */
    private static final String LUA_RELEASE_CLAIM =
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]);"
                    + " else return 0; end;";

    /** 转移执行权 claim 默认 TTL（毫秒） */
    static final long DEFAULT_TRANSFER_CLAIM_TTL_MS =
            StreamMQConstants.DEFAULT_TRANSFER_CLAIM_TTL_MS;

    /** 本实例的 claim 持有者标识（进程级唯一） */
    private final String instanceId = UUID.randomUUID().toString();

    /** claim TTL（毫秒），可通过 {@link #setTransferClaimTtlMs(long)} 覆盖 */
    private volatile long transferClaimTtlMs = DEFAULT_TRANSFER_CLAIM_TTL_MS;

    /**
     * 设置转移执行权 claim TTL（毫秒）。
     *
     * @param millis TTL，必须 &gt; 0
     */
    public void setTransferClaimTtlMs(long millis) {
        if (millis > 0) {
            this.transferClaimTtlMs = millis;
        }
    }

    /** 释放转移执行权 claim：原子 compare-and-delete，避免误删接管者的 claim。 */
    private void releaseClaim(String claimKey) {
        try {
            redisson.getScript(StringCodec.INSTANCE)
                    .eval(
                            RScript.Mode.READ_WRITE,
                            LUA_RELEASE_CLAIM,
                            RScript.ReturnType.INTEGER,
                            Collections.singletonList(claimKey),
                            instanceId);
        } catch (RuntimeException ex) {
            LOG.debug("Release transfer claim failed (TTL will expire): {}", ex.getMessage());
        }
    }

    private volatile ScheduledExecutorService scanExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** 当前的扫描调度任务，stop 时取消以支持后续 restart */
    private volatile ScheduledFuture<?> scanFuture;

    /** 指标收集器（可选注入，用于记录延时投递指标，null 时为 no-op） */
    @Setter private volatile StreamMQMetrics metrics;

    /**
     * 构造调度器。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param scanIntervalMs 扫描间隔（毫秒）
     * @param batchSize 单次扫描批量大小
     */
    public DelayMessageScheduler(
            RedissonClient redisson, String namespace, long scanIntervalMs, int batchSize) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = Objects.isNull(namespace) ? "" : namespace;
        this.scanIntervalMs = scanIntervalMs > 0 ? scanIntervalMs : DEFAULT_SCAN_INTERVAL_MS;
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.scanExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, StreamMQConstants.THREAD_DELAY_SCHEDULER);
                            t.setDaemon(true);
                            return t;
                        });
    }

    /** 启动调度器。 */
    @Override
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.warn("DelayMessageScheduler already started");
            return;
        }
        ensureScanExecutorAlive();
        scanFuture =
                scanExecutor.scheduleAtFixedRate(
                        () -> {
                            try {
                                scanAllLevels();
                            } catch (Throwable t) {
                                LOG.error("DelayMessageScheduler.scanAllLevels failed fatally", t);
                            }
                        },
                        0,
                        scanIntervalMs,
                        TimeUnit.MILLISECONDS);
        LOG.info(
                "DelayMessageScheduler started, scanIntervalMs={}, batchSize={}",
                scanIntervalMs,
                batchSize);
    }

    /**
     * restart 支持：stop 后 executor 已关闭，start 前按需重建。
     *
     * <p>本方法持有锁而 {@link #stop()} 不持锁，因此 {@code scanExecutor} 字段必须是 volatile， 否则 stop
     * 可能读到过期引用、关闭掉已被重建的执行器（或反之）。
     */
    private synchronized void ensureScanExecutorAlive() {
        if (Objects.nonNull(scanExecutor) && !scanExecutor.isShutdown()) {
            return;
        }
        scanExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, StreamMQConstants.THREAD_DELAY_SCHEDULER);
                            t.setDaemon(true);
                            return t;
                        });
    }

    /** 停止调度器（取消扫描任务并关闭线程池，线程为 daemon，不阻塞 JVM 退出）。 */
    @Override
    public synchronized void stop() {
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
        LOG.info("DelayMessageScheduler stopped");
    }

    /** 扫描所有延时级别。 */
    private void scanAllLevels() {
        for (DelayLevel level : DelayLevel.values()) {
            try {
                scanExpired(level);
            } catch (RuntimeException ex) {
                LOG.warn("scanExpired failed for level {}: {}", level, ex.getMessage(), ex);
            }
        }
        // V1.0+: 扫描自定义延时 ZSet（任意延时）
        try {
            scanExpiredCustom();
        } catch (RuntimeException ex) {
            LOG.warn("scanExpiredCustom failed: {}", ex.getMessage(), ex);
        }
    }

    /**
     * 扫描指定级别的到期消息并转投。
     *
     * <p>到期 msgId 通过 per-msgId 执行权 claim（SETNX+TTL）互斥；XADD 目标流、DEL payload、ZREM
     * 调度条目在同一原子批内提交，消除旧实现「ZREM 成功后进程崩溃导致消息永久丢失」的窗口。
     *
     * @param level 延时级别
     */
    void scanExpired(DelayLevel level) {
        String zsetKey = StreamMQKeys.delayZSet(namespace, level.name());
        RScoredSortedSet<String> zset = redisson.getScoredSortedSet(zsetKey, StringCodec.INSTANCE);
        // R2-4：到期判定使用 Redis 服务器时钟（与生产端 deliverAt score 同一时间基准），
        // 规避跨主机 NTP 偏差平移延时时长；服务器时钟不可用时回退本机时钟并限频 WARN。
        long now = scheduleClockMillis();

        // LIMIT count 必须等于 batchSize：此前写成 batchSize - 1，每轮少转投一条（B-18）
        Collection<String> expired = zset.valueRange(0, true, now, true, 0, batchSize);
        for (String msgId : expired) {
            if (transferExpired(
                    zset,
                    msgId,
                    level.name(),
                    StreamMQKeys.transferClaim(namespace, "delay", level.name(), msgId))) {
                recordDelayMetrics(level.name());
            } else {
                debugSkippedDelayDelivery(level.name(), msgId);
            }
        }
    }

    /** 扫描自定义延时 ZSet 的到期消息并转投（任意延时支持）。 */
    void scanExpiredCustom() {
        String zsetKey = StreamMQKeys.delayCustomZSet(namespace);
        RScoredSortedSet<String> zset = redisson.getScoredSortedSet(zsetKey, StringCodec.INSTANCE);
        // R2-4：同 scanExpired，到期判定使用 Redis 服务器时钟
        long now = scheduleClockMillis();

        // LIMIT count 必须等于 batchSize（B-18，同 scanExpired）
        Collection<String> expired = zset.valueRange(0, true, now, true, 0, batchSize);
        for (String msgId : expired) {
            if (transferExpired(
                    zset,
                    msgId,
                    DELAY_CUSTOM_LEVEL,
                    StreamMQKeys.transferClaim(namespace, "delay", DELAY_CUSTOM_LEVEL, msgId))) {
                recordDelayMetrics(DELAY_CUSTOM_LEVEL);
            } else {
                debugSkippedDelayDelivery(DELAY_CUSTOM_LEVEL, msgId);
            }
        }
    }

    /**
     * 未投递路径的降噪观测：claim 未拿到（其它实例正在转投）、payload 缺失被隔离、原子批失败回退， 都属于"本轮未投递"，不得计入投递指标（B-20）；DEBUG
     * 记录以便排查，不 WARN 避免每轮刷屏。
     */
    private void debugSkippedDelayDelivery(String label, String msgId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Delay[{}] message not delivered this round (claim held elsewhere, payload"
                            + " quarantined, or atomic batch failed): msgId={}",
                    label,
                    msgId);
        }
    }

    /**
     * 单条到期延时消息的互斥转投：claim 保护下执行「XADD + DEL payload + ZREM」原子批。
     *
     * <p>批失败时整体不生效，entry 仍在 ZSet；写入退避 score 防止热循环。 批成功则消息已投递且调度状态一致清理——任何时刻崩溃都不丢消息。
     *
     * @return true 仅当消息真正投递（原子批成功）；claim 未拿到、payload 被隔离、批失败均返回 false （调用方据此决定是否计入投递指标，见 B-20）
     */
    private boolean transferExpired(
            RScoredSortedSet<String> zset, String msgId, String label, String claimKey) {
        try {
            RBucket<String> claim = redisson.getBucket(claimKey, StringCodec.INSTANCE);
            if (!Boolean.TRUE.equals(
                    claim.setIfAbsent(instanceId, Duration.ofMillis(transferClaimTtlMs)))) {
                return false;
            }
            try {
                return doTransferExpired(zset, msgId, label);
            } finally {
                releaseClaim(claimKey);
            }
        } catch (RuntimeException ex) {
            LOG.error(
                    "Failed to transfer delay[{}] message msgId={}: {}",
                    label,
                    msgId,
                    ex.getMessage(),
                    ex);
            requeueWithBackoff(zset, msgId, label);
            return false;
        }
    }

    /**
     * 读取 payload 并原子转投（XADD + DEL payload + ZREM）。
     *
     * @return true 仅当原子批提交成功（消息已投递）
     */
    boolean doTransferExpired(RScoredSortedSet<String> zset, String msgId, String label) {
        // XADD + DEL payload + ZREM 跨三个 key 家族：Cluster 下原子批退化为按节点拆分（或 CROSSSLOT），
        // 两者都不可接受——半写会造成消息丢失/重复，前置拒绝比静默降级安全
        RedisClusterCompatibility.requireCrossKeyAtomicity(
                redisson,
                "Delayed message transfer (target stream + payload hash + schedule ZSet)");
        String payloadKey = StreamMQKeys.delayPayloadHash(namespace, msgId);
        RMap<String, String> payloadMap = redisson.getMap(payloadKey, StringCodec.INSTANCE);
        Map<String, String> fields = payloadMap.readAllMap();
        if (CollectionUtils.isEmpty(fields)) {
            // payload 已被 TTL 兜底回收：先登记隔离区（可观测）再移除活跃调度条目，
            // 不再静默删除——运维可通过隔离区 ZSet 排查/重放
            ScheduleQuarantine.quarantineAndRemove(
                    redisson, namespace, "delay", zset, msgId, label);
            return false;
        }

        String targetTopic = fields.get(FIELD_TARGET_TOPIC);
        if (StringUtils.isEmpty(targetTopic)) {
            LOG.warn("Delay[{}] message has no targetTopic, quarantining: msgId={}", label, msgId);
            ScheduleQuarantine.quarantineAndRemove(
                    redisson, namespace, "delay-no-target", zset, msgId, label);
            return false;
        }

        // 移除调度元数据字段，只保留 Stream Entry 字段
        fields.remove(FIELD_TARGET_TOPIC);
        fields.remove(FIELD_DELIVER_AT);

        // 原子批：XADD 目标流 + DEL payload + ZREM 同生同死
        String targetStreamKey = StreamMQKeys.topicStream(namespace, targetTopic);
        RBatch batch =
                redisson.createBatch(
                        BatchOptions.defaults()
                                .executionMode(BatchOptions.ExecutionMode.REDIS_WRITE_ATOMIC));
        batch.<String, String>getStream(targetStreamKey, StringCodec.INSTANCE)
                .addAsync(StreamAddArgs.entries(fields));
        batch.<String, String>getMap(payloadKey, StringCodec.INSTANCE).deleteAsync();
        batch.<String>getScoredSortedSet(zset.getName(), StringCodec.INSTANCE).removeAsync(msgId);
        batch.execute();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Delay[{}] message transferred: msgId={}, targetTopic={}",
                    label,
                    msgId,
                    targetTopic);
        }
        // 只有走到这里才代表 XADD 真正生效（原子批整体提交），调用方据此计投递指标
        return true;
    }

    /** 转移失败后的退避回写：仅调整 score 推迟下一轮处理（entry 本身仍在 ZSet 中）。 */
    private void requeueWithBackoff(RScoredSortedSet<String> zset, String msgId, String label) {
        try {
            // R2-4：退避 score 与扫描侧/生产端统一使用 Redis 服务器时钟
            zset.add(scheduleClockMillis() + failureRequeueBackoffMs, msgId);
            LOG.warn(
                    "Re-added delay[{}] msgId={} (backoff {}ms)",
                    label,
                    msgId,
                    failureRequeueBackoffMs);
        } catch (RuntimeException reAddEx) {
            LOG.error(
                    "CRITICAL: Failed to back off delay[{}] msgId={} in ZSet: {}",
                    label,
                    msgId,
                    reAddEx.getMessage(),
                    reAddEx);
        }
    }

    /**
     * 调度时间基准（R2-4）：Redis 服务器时钟（与生产端写入的 deliverAt score 同一时间源，规避跨主机 NTP 偏差
     * 平移延时时长，例如拨快的实例让延时消息提前投递）；读取失败时回退本机时钟并限频 WARN。
     *
     * @return 调度用当前毫秒时间戳（服务器时钟优先）
     */
    long scheduleClockMillis() {
        long serverNow = RedisServerClock.nowMillis(redisson);
        if (serverNow == RedisServerClock.UNKNOWN) {
            warnClockFallback();
            return System.currentTimeMillis();
        }
        return serverNow;
    }

    /** 服务器时钟不可用的限频 WARN（默认 60s 一次）。 */
    private void warnClockFallback() {
        long now = System.currentTimeMillis();
        long last = lastClockFallbackWarnMs.get();
        if (now - last >= CLOCK_FALLBACK_WARN_INTERVAL_MS
                && lastClockFallbackWarnMs.compareAndSet(last, now)) {
            LOG.warn(
                    "Redis TIME unavailable, falling back to LOCAL clock for delay scheduling;"
                            + " cross-host NTP skew may shift delay delivery times");
        }
    }

    /**
     * 记录延时投递指标（null 安全，指标异常不影响业务主流程）。
     *
     * @param level 延时等级
     */
    private void recordDelayMetrics(String level) {
        if (Objects.nonNull(metrics)) {
            try {
                metrics.recordDelayDelivery(level);
            } catch (Exception ignored) {
                // 指标收集失败不得影响业务主流程
                LOG.debug("Metrics collection failed", ignored);
            }
        }
    }

    /**
     * 返回调度器是否正在运行。
     *
     * @return true 如果运行中
     */
    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * 清理所有延时 ZSet 中的孤立 entry（无对应 payload Hash 的条目）， 以及反向孤儿：无任何 ZSet 引用的 payload Hash（带 TTL
     * 兜底，此处主动清理）。
     *
     * <p>正常使用中，ZSet entry 在转投成功后会被 ZREM 移除。但在以下异常场景下可能残留：
     *
     * <ul>
     *   <li>ZREM 成功但 payload Hash 读取失败后未 re-add ZSet entry
     *   <li>Redis 崩溃导致部分操作未完成
     *   <li>代码 Bug 导致 payload Hash 被提前删除
     * </ul>
     *
     * <p>此方法扫描所有延时级别和自定义延时 ZSet，移除没有对应 payload Hash 的 entry； 同时反向扫描无调度引用的 payload Hash（孤儿方向），
     * 建议在系统空闲期定期调用（如每天凌晨）。payload 自身另有 TTL 兜底（见生产端写入）。
     */
    public void cleanupOrphanedEntries() {
        int totalCleaned = 0;
        for (DelayLevel level : DelayLevel.values()) {
            totalCleaned +=
                    cleanupOrphanedInZSet(
                            StreamMQKeys.delayZSet(namespace, level.name()), level.name());
        }
        totalCleaned +=
                cleanupOrphanedInZSet(StreamMQKeys.delayCustomZSet(namespace), DELAY_CUSTOM_LEVEL);
        if (totalCleaned > 0) {
            LOG.info("Cleaned up {} orphaned delay ZSet entries", totalCleaned);
        }
        totalCleaned += cleanupOrphanedPayloads();
        if (totalCleaned > 0) {
            LOG.info("Cleaned up {} orphaned delay entries in total", totalCleaned);
        }
    }

    /**
     * 反向孤儿清理：删除不再被任何延时 ZSet 引用的 payload Hash。
     *
     * <p>扫描 {@code streammq:{ns}:delay:payload:*}（上限 {@value #MAX_ORPHAN_PAYLOAD_SCAN} 个）， 对 msgId
     * 不在任何 ZSet 中的 payload 执行 DEL。正常转投流程已 DEL payload； 此处仅兜底「ZREM 后崩溃」等窗口产生的孤儿。生产端 TTL 提供最终兜底。
     *
     * @return 清理的 payload 数量
     */
    private static final int MAX_ORPHAN_PAYLOAD_SCAN = 1000;

    /**
     * 引用集容量上限：超过则跳过本轮孤儿清扫。
     *
     * <p>孤儿清扫需要把全部延时 ZSet 的 msgId 读进内存做差集；延时消息堆积时该集合可能极大（内存尖峰 ∝ 堆积量）。 这里先做 O(1) 的 {@code size()}
     * 容量探测，超限即<b>跳过</b>本轮兜底清扫——这是 fail-safe 方向： 宁可漏删孤儿（生产端 payload TTL 是最终兜底），也绝不误删仍被引用的 payload。
     */
    private static final long MAX_REFERENCED_IDS_FOR_ORPHAN_SCAN = 100_000L;

    /**
     * 单个延时 ZSet 孤儿清扫的单次扫描上限（{@code ZRANGEBYSCORE ... LIMIT 0 N}）。
     *
     * <p>与 {@link #MAX_REFERENCED_IDS_FOR_ORPHAN_SCAN} 同口径：绝不一次性把整个 ZSet materialize
     * 进内存（历史积压可达百万级）；超额时 WARN 提示分多次调用。
     */
    private static final int MAX_ORPHAN_ZSET_SCAN = 1000;

    private int cleanupOrphanedPayloads() {
        String pattern = StreamMQKeys.delayPayloadHash(namespace, "*");
        java.util.List<org.redisson.api.RScoredSortedSet<String>> delayZsets =
                new java.util.ArrayList<>();
        for (DelayLevel level : DelayLevel.values()) {
            delayZsets.add(
                    redisson.getScoredSortedSet(
                            StreamMQKeys.delayZSet(namespace, level.name()), StringCodec.INSTANCE));
        }
        delayZsets.add(
                redisson.getScoredSortedSet(
                        StreamMQKeys.delayCustomZSet(namespace), StringCodec.INSTANCE));

        // 容量探测（O(1)/ZSet）：引用集过大时跳过本轮兜底清扫，避免把堆积量级的数据一次性物化进内存
        long referencedCount = 0;
        for (org.redisson.api.RScoredSortedSet<String> zset : delayZsets) {
            referencedCount += zset.size();
            if (referencedCount > MAX_REFERENCED_IDS_FOR_ORPHAN_SCAN) {
                LOG.warn(
                        "Skip orphan delay-payload cleanup: referenced id set too large (> {}), "
                                + "avoiding a memory spike. Producer-side payload TTL remains the "
                                + "final safety net.",
                        MAX_REFERENCED_IDS_FOR_ORPHAN_SCAN);
                return 0;
            }
        }

        java.util.Set<String> referencedMsgIds = new java.util.HashSet<>();
        for (org.redisson.api.RScoredSortedSet<String> zset : delayZsets) {
            referencedMsgIds.addAll(zset.readAll());
        }

        int cleaned = 0;
        Iterable<String> keys =
                redisson.getKeys().getKeysByPattern(pattern, MAX_ORPHAN_PAYLOAD_SCAN);
        for (String key : keys) {
            // 从 key 中提取 msgId（最后一段）
            int idx = key.lastIndexOf(StreamMQKeys.SEP);
            if (idx < 0) {
                continue;
            }
            String msgId = key.substring(idx + StreamMQKeys.SEP.length());
            if (referencedMsgIds.contains(msgId)) {
                continue;
            }
            try {
                if (redisson.getMap(key, StringCodec.INSTANCE).delete()) {
                    cleaned++;
                }
            } catch (RuntimeException ex) {
                LOG.debug("Failed to delete orphan delay payload {}: {}", key, ex.getMessage());
            }
            if (cleaned >= MAX_ORPHAN_PAYLOAD_SCAN) {
                break;
            }
        }
        if (cleaned > 0) {
            LOG.warn("Cleaned {} orphan delay payload hash(es) (no scheduling reference)", cleaned);
        }
        return cleaned;
    }

    /**
     * 清理指定 ZSet 中的孤立 entry。
     *
     * <p><b>R2-5：</b>孤儿判定从「逐条 {@code isExists}」改为服务端单条 Lua 批量完成（一次往返），语义不变—— 仅删除「无对应 payload
     * Hash」的条目；有 payload 的条目一律保留（防误删）。
     *
     * @param zsetKey ZSet 的 Redis key
     * @param label 日志标签（level 名称或 "custom"）
     * @return 清理的 entry 数量
     */
    private int cleanupOrphanedInZSet(String zsetKey, String label) {
        // payload key 前缀 = 完整 key 去掉末段 msgId（msgId 由 key 构造器强制非空，故用探针 key 截断）
        String probeKey = StreamMQKeys.delayPayloadHash(namespace, "0");
        String payloadPrefix = probeKey.substring(0, probeKey.length() - 1);
        // R-34 同口径的有界扫描：ZRANGE ... LIMIT 0 N，绝不一次性把整个延时 backlog
        // materialize 进内存（历史积压可达百万级）；超额时 WARN 提示分多次调用。
        List<Object> purgeResult =
                redisson.getScript(StringCodec.INSTANCE)
                        .eval(
                                RScript.Mode.READ_WRITE,
                                LUA_PURGE_ORPHAN_ZSET,
                                RScript.ReturnType.MULTI,
                                Collections.singletonList(zsetKey),
                                payloadPrefix,
                                MAX_ORPHAN_ZSET_SCAN);
        long scanned =
                purgeResult != null && purgeResult.size() > 0 ? asLong(purgeResult.get(0)) : 0L;
        long removed =
                purgeResult != null && purgeResult.size() > 1 ? asLong(purgeResult.get(1)) : 0L;
        if (scanned <= 0) {
            return 0;
        }
        if (scanned >= MAX_ORPHAN_ZSET_SCAN) {
            LOG.warn(
                    "Delay ZSet orphan cleanup truncated at {} entries (more may remain); call"
                            + " again to continue: zsetKey={}, label={}",
                    MAX_ORPHAN_ZSET_SCAN,
                    zsetKey,
                    label);
        }
        if (removed > 0) {
            LOG.warn("Cleaned {} orphaned entries from delay ZSet [label={}]", removed, label);
        }
        return (int) removed;
    }

    /**
     * 将脚本返回值稳健转为 long（协议漂移时返回 0，绝不抛 ClassCastException 中断清理）。
     *
     * @param value 脚本返回元素
     * @return long 值；不可解析时为 0
     */
    private static long asLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String str) {
            try {
                return Long.parseLong(str);
            } catch (NumberFormatException ignored) {
                return 0L;
            }
        }
        return 0L;
    }

    /**
     * Lua：批量清理孤儿调度条目（R2-5）。
     *
     * <p>此前逐个 msgId 发 {@code EXISTS}（N+1 往返）；现在服务端一次遍历窗口，仅对 payload 缺失的条目 {@code
     * ZREM}。语义与逐条检查一致（只删无对应 payload 的条目，绝不误删仍有 payload 的条目）。
     *
     * <p>KEYS[1] = 延时 ZSet key；ARGV[1] = payload Hash key 前缀；ARGV[2] = 单次扫描窗口上限。 返回 {@code
     * {scanned, removed}}。
     */
    static final String LUA_PURGE_ORPHAN_ZSET =
            "local ids = redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[2]) - 1);"
                    + "local removed = 0;"
                    + "for i = 1, #ids do"
                    + "  if redis.call('EXISTS', ARGV[1] .. ids[i]) == 0 then"
                    + "    removed = removed + redis.call('ZREM', KEYS[1], ids[i]);"
                    + "  end;"
                    + "end;"
                    + "return { #ids, removed };";
}
