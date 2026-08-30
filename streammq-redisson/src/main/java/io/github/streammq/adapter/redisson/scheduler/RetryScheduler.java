/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.enums.DlqReason;
import io.github.streammq.core.scheduler.StreamMQScheduler;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * 重试消息调度器，周期扫描重试 ZSet，将到期消息转投到目标 Stream 或 DLQ Stream。
 *
 * <p>存储模型（对齐 04-detailed-design.md §6）：
 *
 * <ul>
 *   <li>ZSet Key: {@code streammq:{ns}:retry:{topic}:{group}}，score=nextRetryAt(ms)，member=msgId
 *   <li>payload Hash Key: {@code streammq:{ns}:retry:payload:{msgId}}， 存储消息完整字段 + {@code
 *       retryCount} + {@code targetTopic}
 * </ul>
 *
 * <p>转投决策（对齐决策 D5）：
 *
 * <ul>
 *   <li>{@code retryCount < maxReconsumeTimes}：转投到目标 Stream（{@code streammq:{ns}:msg:{topic}}）， 递增
 *       {@code retryTimes} 字段
 *   <li>{@code retryCount >= maxReconsumeTimes}：转投到 DLQ Stream（{@code streammq:{ns}:dlq:{group}}）
 * </ul>
 *
 * <p>转投流程（执行权 claim + 原子批，无丢失窗口）：
 *
 * <ol>
 *   <li>{@code ZRANGEBYSCORE 0 now LIMIT 0 batchSize} 获取到期 msgId
 *   <li>对每个 msgId：SETNX 执行权 claim（TTL 兜底，持有者崩溃后可接管）
 *   <li>从 payload Hash 读取字段与 retryCount
 *   <li>原子批（REDIS_WRITE_ATOMIC）：XADD 目标/DLQ Stream + DEL payload Hash + ZREM 调度条目
 *       ——要么全部生效、要么全部不生效；批失败时 entry 留在 ZSet 等待下轮重试
 * </ol>
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class RetryScheduler implements StreamMQScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(RetryScheduler.class);

    /** payload Hash 中的重试次数字段名 */
    public static final String FIELD_RETRY_COUNT = "retryCount";

    /** payload Hash 中的目标 Topic 字段名 */
    public static final String FIELD_TARGET_TOPIC = StreamMQConstants.FIELD_TARGET_TOPIC;

    /** DLQ Stream Entry 字段：进入 DLQ 的原因 */
    public static final String FIELD_DLQ_REASON = StreamMQConstants.FIELD_DLQ_REASON;

    /** DLQ Stream Entry 字段：原始重试次数 */
    public static final String FIELD_ORIGINAL_RETRY_COUNT = "originalRetryCount";

    /** DLQ 原因：达到最大重试次数 */
    public static final String DLQ_REASON_MAX_RETRY = DlqReason.MAX_RETRY.getCode();

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

    /** Lua：仅当转移执行权 claim 仍归本实例持有时删除（原子 compare-and-delete）。 */
    private static final String LUA_RELEASE_CLAIM =
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]);"
                    + " else return 0; end;";

    /** 转移执行权 claim 默认 TTL（毫秒）：持有者崩溃后其它实例可在 TTL 过期后接管 */
    static final long DEFAULT_TRANSFER_CLAIM_TTL_MS =
            StreamMQConstants.DEFAULT_TRANSFER_CLAIM_TTL_MS;

    /** 本实例的 claim 持有者标识（进程级唯一） */
    private final String instanceId = UUID.randomUUID().toString();

    /** claim TTL（毫秒），可通过 {@link #setTransferClaimTtlMs(long)} 覆盖 */
    private volatile long claimTtlMs = DEFAULT_TRANSFER_CLAIM_TTL_MS;

    /**
     * 设置转移执行权 claim TTL（毫秒）。
     *
     * @param millis TTL，必须 &gt; 0
     */
    public void setTransferClaimTtlMs(long millis) {
        if (millis > 0) {
            this.claimTtlMs = millis;
        }
    }

    private final RedissonClient redisson;
    private final String namespace;
    private final long scanIntervalMs;
    private final int batchSize;
    private final int streamMaxLen;
    private volatile ScheduledExecutorService scanExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentMap<String, RetryTarget> targets = new ConcurrentHashMap<>();

    /** 当前的扫描调度任务，stop 时取消以支持后续 restart */
    private volatile ScheduledFuture<?> scanFuture;

    /**
     * 构造调度器。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param scanIntervalMs 扫描间隔（毫秒）
     * @param batchSize 单次扫描批量大小
     */
    public RetryScheduler(
            RedissonClient redisson, String namespace, long scanIntervalMs, int batchSize) {
        this(redisson, namespace, scanIntervalMs, batchSize, 0);
    }

    /**
     * 构造调度器（可指定 retry Stream 最大长度）。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param scanIntervalMs 扫描间隔（毫秒）
     * @param batchSize 单次扫描批量大小
     * @param streamMaxLen retry Stream 最大长度（0=不限制）
     */
    public RetryScheduler(
            RedissonClient redisson,
            String namespace,
            long scanIntervalMs,
            int batchSize,
            int streamMaxLen) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = Objects.isNull(namespace) ? "" : namespace;
        this.scanIntervalMs = scanIntervalMs > 0 ? scanIntervalMs : DEFAULT_SCAN_INTERVAL_MS;
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.streamMaxLen = Math.max(0, streamMaxLen);
        this.scanExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, StreamMQConstants.THREAD_RETRY_SCHEDULER);
                            t.setDaemon(true);
                            return t;
                        });
    }

    /**
     * 注册一个重试目标（topic + group）。
     *
     * @param topic 主题
     * @param group 消费者组名
     * @param maxReconsumeTimes 最大重试次数
     */
    public void registerRetryTarget(String topic, String group, int maxReconsumeTimes) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(group, "group");
        String key = topic + ":" + group;
        targets.put(key, new RetryTarget(topic, group, maxReconsumeTimes));
        LOG.info(
                "Registered retry target: topic={}, group={}, maxReconsumeTimes={}",
                topic,
                group,
                maxReconsumeTimes);
    }

    /** 启动调度器。 */
    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.warn("RetryScheduler already started");
            return;
        }
        ensureScanExecutorAlive();
        scanFuture =
                scanExecutor.scheduleAtFixedRate(
                        this::scanAllTargets, 0, scanIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info(
                "RetryScheduler started, scanIntervalMs={}, batchSize={}, targets={}",
                scanIntervalMs,
                batchSize,
                targets.size());
    }

    /** restart 支持：stop 后 executor 已关闭，start 前按需重建。 */
    private synchronized void ensureScanExecutorAlive() {
        if (Objects.nonNull(scanExecutor) && !scanExecutor.isShutdown()) {
            return;
        }
        scanExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, StreamMQConstants.THREAD_RETRY_SCHEDULER);
                            t.setDaemon(true);
                            return t;
                        });
    }

    /** 停止调度器（取消扫描任务并关闭线程池；restart 时由 ensureScanExecutorAlive 重建）。 */
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
        // 与其它调度器保持一致：stop 必须关闭线程池，否则残留非 daemon 活跃线程
        scanExecutor.shutdown();
        try {
            if (!scanExecutor.awaitTermination(AWAIT_TERMINATION_SECONDS, TimeUnit.SECONDS)) {
                scanExecutor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            scanExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        LOG.info("RetryScheduler stopped");
    }

    /** 扫描所有已注册的重试目标。 */
    private void scanAllTargets() {
        for (RetryTarget target : targets.values()) {
            try {
                scanRetryEntries(target);
            } catch (RuntimeException ex) {
                LOG.warn(
                        "scanRetryEntries failed for topic={}, group={}: {}",
                        target.topic,
                        target.group,
                        ex.getMessage(),
                        ex);
            }
        }
    }

    /**
     * 扫描指定目标的到期重试消息并转投。
     *
     * <p>到期 msgId 通过 per-msgId 执行权 claim（SETNX+TTL）互斥，ZREM 移入原子批内执行—— 「读取 payload → XADD → DEL
     * payload → ZREM」要么整体生效、要么整体不生效， 消除旧实现「ZREM 成功后进程崩溃导致消息永久丢失」的窗口。
     *
     * @param target 重试目标
     */
    void scanRetryEntries(RetryTarget target) {
        String retryKey = StreamMQKeys.retryZSet(namespace, target.topic, target.group);
        RScoredSortedSet<String> zset = redisson.getScoredSortedSet(retryKey);
        long now = System.currentTimeMillis();

        Collection<String> expired = zset.valueRange(0, true, now, true, 0, batchSize - 1);
        if (expired.isEmpty()) {
            return;
        }

        String targetStreamKey = StreamMQKeys.retryStream(namespace, target.topic, target.group);
        String dlqStreamKey = StreamMQKeys.dlqStream(namespace, target.group);

        for (String msgId : expired) {
            transferOne(msgId, target, targetStreamKey, dlqStreamKey, zset);
        }
    }

    private void transferOne(
            String msgId,
            RetryTarget target,
            String targetStreamKey,
            String dlqStreamKey,
            RScoredSortedSet<String> zset) {
        String payloadKey =
                StreamMQKeys.retryPayloadHash(namespace, target.topic, target.group, msgId);
        try {
            RBucket<String> claim =
                    redisson.getBucket(
                            transferClaimKey(target.topic, target.group, msgId),
                            StringCodec.INSTANCE);
            if (!Boolean.TRUE.equals(
                    claim.setIfAbsent(instanceId, Duration.ofMillis(claimTtlMs)))) {
                return;
            }
            try {
                doTransfer(msgId, target, targetStreamKey, dlqStreamKey, zset, payloadKey);
            } finally {
                releaseClaim(target.topic, target.group, msgId);
            }
        } catch (RuntimeException ex) {
            LOG.error("Failed to transfer retry message msgId={}: {}", msgId, ex.getMessage(), ex);
            // 原子批未生效（MULTI/EXEC 整体回滚），ZSet entry 仍在，消息不会丢失；
            // 写入退避 score 避免下一轮扫描立即重试形成热循环
            requeueWithBackoff(zset, msgId);
        }
    }

    /**
     * 在执行权 claim 保护下读取 payload 并原子转投：XADD 目标流 + DEL payload + ZREM 调度条目 在同一 REDIS_WRITE_ATOMIC
     * 批内提交，要么全部生效、要么全部不生效。
     */
    void doTransfer(
            String msgId,
            RetryTarget target,
            String targetStreamKey,
            String dlqStreamKey,
            RScoredSortedSet<String> zset,
            String payloadKey) {
        RMap<String, String> payloadMap = redisson.getMap(payloadKey);
        Map<String, String> fields = payloadMap.readAllMap();
        if (CollectionUtils.isEmpty(fields)) {
            // payload 已被 TTL 兜底回收：先登记隔离区（可观测）再移除活跃调度条目，
            // 不再静默删除——运维可通过隔离区 ZSet 排查/重放
            ScheduleQuarantine.quarantineAndRemove(
                    redisson, namespace, "retry", zset, msgId, target.topic + ":" + target.group);
            return;
        }

        // 检查是否为 DLQ 重试哨兵
        String targetTopic = fields.get(FIELD_TARGET_TOPIC);
        boolean isDlqRetry = StreamMQConstants.DLQ_RETRY_TARGET_TOPIC_SENTINEL.equals(targetTopic);
        boolean deferred = Boolean.parseBoolean(fields.get(StreamMQConstants.FIELD_DEFERRED));

        int retryCount = 0;
        String retryCountStr = fields.get(FIELD_RETRY_COUNT);
        if (StringUtils.isNotEmpty(retryCountStr)) {
            try {
                retryCount = Integer.parseInt(retryCountStr);
            } catch (NumberFormatException ignored) {
                LOG.debug("Failed to parse retry count: {}", retryCountStr);
            }
        }

        // 移除调度元数据字段，只保留 Stream Entry 字段（XADD 后不得残留）
        fields.remove(FIELD_RETRY_COUNT);
        fields.remove(FIELD_TARGET_TOPIC);
        fields.remove(StreamMQConstants.FIELD_DEFERRED);

        String destStreamKey;
        if (isDlqRetry) {
            // DLQ 重试 → 回 DLQ Stream，顶层保留 dlqRetryCount（props JSON 中另有往返副本）
            fields.remove(StreamMQConstants.FIELD_DLQ_RETRY_COUNT);
            fields.put(StreamMQConstants.FIELD_DLQ_RETRY_COUNT, Integer.toString(retryCount));
            destStreamKey = dlqStreamKey;
            LOG.info(
                    "DLQ retry transferring: msgId={}, group={}, dlqRetryCount={}",
                    msgId,
                    target.group,
                    retryCount);
        } else if (deferred) {
            // 业务 DEFER：不递增 retryTimes、不做 MAX_RETRY 判定——DEFER 不消耗重试预算，
            // 由业务自行控制延迟节奏；retryTimes 字段保持原值随消息往返
            destStreamKey = targetStreamKey;
        } else {
            // 消费失败重试：递增 retryTimes 字段（用于消费端 reconsumeTimes）
            int newRetryTimes = retryCount + 1;
            fields.put(DefaultMessageConverter.FIELD_RETRY_TIMES, Integer.toString(newRetryTimes));
            if (retryCount >= target.maxReconsumeTimes) {
                fields.put(FIELD_DLQ_REASON, DLQ_REASON_MAX_RETRY);
                fields.put(FIELD_ORIGINAL_RETRY_COUNT, Integer.toString(retryCount));
                destStreamKey = dlqStreamKey;
                LOG.info(
                        "Message entering DLQ: msgId={}, topic={}, group={}, retryCount={}",
                        msgId,
                        target.topic,
                        target.group,
                        retryCount);
            } else {
                destStreamKey = targetStreamKey;
            }
        }

        StreamAddArgs<String, String> args = StreamAddArgs.entries(fields);
        if (destStreamKey.equals(targetStreamKey) && streamMaxLen > 0) {
            args = args.trimNonStrict().maxLen(streamMaxLen).noLimit();
        }

        // 原子批：XADD + DEL payload + ZREM 同生同死。批失败则整体不生效，
        // entry 留在 ZSet 等待下轮扫描；批成功则消息已投递且调度状态一致清理。
        RBatch batch =
                redisson.createBatch(
                        BatchOptions.defaults()
                                .executionMode(BatchOptions.ExecutionMode.REDIS_WRITE_ATOMIC));
        batch.<String, String>getStream(destStreamKey).addAsync(args);
        batch.<String, String>getMap(payloadKey).deleteAsync();
        batch.<String>getScoredSortedSet(zset.getName()).removeAsync(msgId);
        batch.execute();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Retry message transferred: msgId={}, topic={}, group={}, dest={}",
                    msgId,
                    target.topic,
                    target.group,
                    destStreamKey);
        }
    }

    /** 转移失败后的退避回写：仅调整 score 推迟下一轮处理（entry 本身仍在 ZSet 中）。 */
    private void requeueWithBackoff(RScoredSortedSet<String> zset, String msgId) {
        try {
            zset.add(System.currentTimeMillis() + failureRequeueBackoffMs, msgId);
            LOG.warn("Requeued msgId={} with backoff {}ms", msgId, failureRequeueBackoffMs);
        } catch (RuntimeException reAddEx) {
            LOG.error(
                    "CRITICAL: Failed to back off msgId={} in retry ZSet: {}",
                    msgId,
                    reAddEx.getMessage(),
                    reAddEx);
        }
    }

    /** 转移执行权 claim Key。scope 多段以 ':' 连接（topic/group 禁止冒号，无碰撞风险）。 */
    private String transferClaimKey(String topic, String group, String msgId) {
        return StreamMQKeys.transferClaim(namespace, "retry", topic + ":" + group, msgId);
    }

    /**
     * 释放转移执行权 claim：原子 compare-and-delete，仅当仍归本实例持有时删除。
     *
     * <p>若处理耗时超过 TTL 导致 claim 已被其它实例接管，不得误删他人的 claim。
     */
    private void releaseClaim(String topic, String group, String msgId) {
        try {
            redisson.getScript(StringCodec.INSTANCE)
                    .eval(
                            RScript.Mode.READ_WRITE,
                            LUA_RELEASE_CLAIM,
                            RScript.ReturnType.INTEGER,
                            Collections.singletonList(transferClaimKey(topic, group, msgId)),
                            instanceId);
        } catch (RuntimeException ex) {
            LOG.debug("Release transfer claim failed (TTL will expire): {}", ex.getMessage());
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
     * 返回已注册的重试目标数量。
     *
     * @return 目标数量
     */
    public int getTargetCount() {
        return targets.size();
    }

    /**
     * 清理所有重试 ZSet 中的孤立 entry（无对应 payload Hash 的条目）。
     *
     * <p>与 {@link DelayMessageScheduler#cleanupOrphanedEntries()} 类似， 清理因异常导致残留的重试 ZSet entry。
     */
    public void cleanupOrphanedEntries() {
        int totalCleaned = 0;
        for (RetryTarget target : targets.values()) {
            String retryKey = StreamMQKeys.retryZSet(namespace, target.topic, target.group);
            RScoredSortedSet<String> zset = redisson.getScoredSortedSet(retryKey);
            Collection<String> allMembers = zset.readAll();
            if (allMembers.isEmpty()) {
                continue;
            }
            for (String msgId : allMembers) {
                String payloadKey =
                        StreamMQKeys.retryPayloadHash(namespace, target.topic, target.group, msgId);
                RMap<String, String> payloadMap = redisson.getMap(payloadKey);
                if (!payloadMap.isExists()) {
                    boolean removed = zset.remove(msgId);
                    if (removed) {
                        totalCleaned++;
                        LOG.debug(
                                "Removed orphaned retry ZSet entry: retryKey={}, msgId={}",
                                retryKey,
                                msgId);
                    }
                }
            }
        }
        if (totalCleaned > 0) {
            LOG.warn("Cleaned {} orphaned entries from retry ZSets", totalCleaned);
        }
    }

    // ===================== 内部类 =====================

    /** 重试目标信息 */
    static final class RetryTarget {
        final String topic;
        final String group;
        final int maxReconsumeTimes;

        RetryTarget(String topic, String group, int maxReconsumeTimes) {
            this.topic = topic;
            this.group = group;
            this.maxReconsumeTimes = maxReconsumeTimes;
        }
    }
}
