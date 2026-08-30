/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

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
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    public void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.warn("DelayMessageScheduler already started");
            return;
        }
        ensureScanExecutorAlive();
        scanFuture =
                scanExecutor.scheduleAtFixedRate(
                        this::scanAllLevels, 0, scanIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info(
                "DelayMessageScheduler started, scanIntervalMs={}, batchSize={}",
                scanIntervalMs,
                batchSize);
    }

    /** restart 支持：stop 后 executor 已关闭，start 前按需重建。 */
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
        RScoredSortedSet<String> zset = redisson.getScoredSortedSet(zsetKey);
        long now = System.currentTimeMillis();

        Collection<String> expired = zset.valueRange(0, true, now, true, 0, batchSize - 1);
        for (String msgId : expired) {
            transferExpired(
                    zset,
                    msgId,
                    level.name(),
                    StreamMQKeys.transferClaim(namespace, "delay", level.name(), msgId));
            recordDelayMetrics(level.name());
        }
    }

    /** 扫描自定义延时 ZSet 的到期消息并转投（任意延时支持）。 */
    void scanExpiredCustom() {
        String zsetKey = StreamMQKeys.delayCustomZSet(namespace);
        RScoredSortedSet<String> zset = redisson.getScoredSortedSet(zsetKey);
        long now = System.currentTimeMillis();

        Collection<String> expired = zset.valueRange(0, true, now, true, 0, batchSize - 1);
        for (String msgId : expired) {
            transferExpired(
                    zset,
                    msgId,
                    DELAY_CUSTOM_LEVEL,
                    StreamMQKeys.transferClaim(namespace, "delay", DELAY_CUSTOM_LEVEL, msgId));
            recordDelayMetrics(DELAY_CUSTOM_LEVEL);
        }
    }

    /**
     * 单条到期延时消息的互斥转投：claim 保护下执行「XADD + DEL payload + ZREM」原子批。
     *
     * <p>批失败时整体不生效，entry 仍在 ZSet；写入退避 score 防止热循环。 批成功则消息已投递且调度状态一致清理——任何时刻崩溃都不丢消息。
     */
    private void transferExpired(
            RScoredSortedSet<String> zset, String msgId, String label, String claimKey) {
        try {
            RBucket<String> claim = redisson.getBucket(claimKey, StringCodec.INSTANCE);
            if (!Boolean.TRUE.equals(
                    claim.setIfAbsent(instanceId, Duration.ofMillis(transferClaimTtlMs)))) {
                return;
            }
            try {
                doTransferExpired(zset, msgId, label);
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
        }
    }

    void doTransferExpired(RScoredSortedSet<String> zset, String msgId, String label) {
        String payloadKey = StreamMQKeys.delayPayloadHash(namespace, msgId);
        RMap<String, String> payloadMap = redisson.getMap(payloadKey);
        Map<String, String> fields = payloadMap.readAllMap();
        if (CollectionUtils.isEmpty(fields)) {
            // payload 已被 TTL 兜底回收：先登记隔离区（可观测）再移除活跃调度条目，
            // 不再静默删除——运维可通过隔离区 ZSet 排查/重放
            ScheduleQuarantine.quarantineAndRemove(
                    redisson, namespace, "delay", zset, msgId, label);
            return;
        }

        String targetTopic = fields.get(FIELD_TARGET_TOPIC);
        if (StringUtils.isEmpty(targetTopic)) {
            LOG.warn("Delay[{}] message has no targetTopic, skip: msgId={}", label, msgId);
            zset.remove(msgId);
            return;
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
        batch.<String, String>getStream(targetStreamKey).addAsync(StreamAddArgs.entries(fields));
        batch.<String, String>getMap(payloadKey).deleteAsync();
        batch.<String>getScoredSortedSet(zset.getName()).removeAsync(msgId);
        batch.execute();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Delay[{}] message transferred: msgId={}, targetTopic={}",
                    label,
                    msgId,
                    targetTopic);
        }
    }

    /** 转移失败后的退避回写：仅调整 score 推迟下一轮处理（entry 本身仍在 ZSet 中）。 */
    private void requeueWithBackoff(RScoredSortedSet<String> zset, String msgId, String label) {
        try {
            zset.add(System.currentTimeMillis() + failureRequeueBackoffMs, msgId);
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

    private int cleanupOrphanedPayloads() {
        String pattern = StreamMQKeys.delayPayloadHash(namespace, "*");
        java.util.Set<String> referencedMsgIds = new java.util.HashSet<>();
        for (DelayLevel level : DelayLevel.values()) {
            referencedMsgIds.addAll(
                    redisson.<String>getScoredSortedSet(
                                    StreamMQKeys.delayZSet(namespace, level.name()))
                            .readAll());
        }
        referencedMsgIds.addAll(
                redisson.<String>getScoredSortedSet(StreamMQKeys.delayCustomZSet(namespace))
                        .readAll());

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
                if (redisson.getMap(key).delete()) {
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
     * @param zsetKey ZSet 的 Redis key
     * @param label 日志标签（level 名称或 "custom"）
     * @return 清理的 entry 数量
     */
    private int cleanupOrphanedInZSet(String zsetKey, String label) {
        RScoredSortedSet<String> zset = redisson.getScoredSortedSet(zsetKey);
        // 获取所有 entry（不限时间范围，用于清理）
        Collection<String> allMembers = zset.readAll();
        if (allMembers.isEmpty()) {
            return 0;
        }
        int cleaned = 0;
        for (String msgId : allMembers) {
            String payloadKey = StreamMQKeys.delayPayloadHash(namespace, msgId);
            RMap<String, String> payloadMap = redisson.getMap(payloadKey);
            if (!payloadMap.isExists()) {
                // payload Hash 不存在，ZSet entry 为孤立条目，安全移除
                boolean removed = zset.remove(msgId);
                if (removed) {
                    cleaned++;
                    LOG.debug(
                            "Removed orphaned delay ZSet entry: zsetKey={}, msgId={}",
                            zsetKey,
                            msgId);
                }
            }
        }
        if (cleaned > 0) {
            LOG.warn("Cleaned {} orphaned entries from delay ZSet [label={}]", cleaned, label);
        }
        return cleaned;
    }
}
