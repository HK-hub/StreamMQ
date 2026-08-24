package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.enums.DlqReason;
import io.github.streammq.core.scheduler.StreamMQScheduler;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamAddArgs;
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
 * <p>转投流程（Java 端原子操作，ZREM 保证 only-once）：
 *
 * <ol>
 *   <li>{@code ZRANGEBYSCORE 0 now LIMIT 0 batchSize} 获取到期 msgId
 *   <li>对每个 msgId：{@code ZREM}（返回 true 表示成功获取）
 *   <li>从 payload Hash 读取字段与 retryCount
 *   <li>按 retryCount 决策：XADD 到目标 Stream 或 DLQ Stream
 *   <li>{@code DEL} payload Hash
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

    private final RedissonClient redisson;
    private final String namespace;
    private final long scanIntervalMs;
    private final int batchSize;
    private final int streamMaxLen;
    private final ScheduledExecutorService scanExecutor;
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
                new ScheduledThreadPoolExecutor(
                        1,
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
        scanFuture =
                scanExecutor.scheduleAtFixedRate(
                        this::scanAllTargets, 0, scanIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info(
                "RetryScheduler started, scanIntervalMs={}, batchSize={}, targets={}",
                scanIntervalMs,
                batchSize,
                targets.size());
    }

    /** 停止调度器（取消扫描任务但保留线程池，支持后续 restart）。 */
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
            boolean acquired = zset.remove(msgId);
            if (!acquired) {
                continue;
            }
            transferOne(msgId, target, targetStreamKey, dlqStreamKey, zset);
        }
    }

    private void transferOne(
            String msgId,
            RetryTarget target,
            String targetStreamKey,
            String dlqStreamKey,
            RScoredSortedSet<String> zset) {
        String payloadKey = StreamMQKeys.retryPayloadHash(namespace, msgId);
        try {
            RMap<String, String> payloadMap = redisson.getMap(payloadKey);
            Map<String, String> fields = payloadMap.readAllMap();
            if (CollectionUtils.isEmpty(fields)) {
                LOG.warn("Retry payload not found for msgId={}, may have been processed", msgId);
                return;
            }

            // 检查是否为 DLQ 重试哨兵
            String targetTopic = fields.get(FIELD_TARGET_TOPIC);
            boolean isDlqRetry =
                    StreamMQConstants.DLQ_RETRY_TARGET_TOPIC_SENTINEL.equals(targetTopic);

            int retryCount = 0;
            String retryCountStr = fields.get(FIELD_RETRY_COUNT);
            if (StringUtils.isNotEmpty(retryCountStr)) {
                try {
                    retryCount = Integer.parseInt(retryCountStr);
                } catch (NumberFormatException ignored) {
                    LOG.debug("Failed to parse retry count: {}", retryCountStr);
                }
            }

            // 移除调度元数据字段，只保留 Stream Entry 字段
            fields.remove(FIELD_RETRY_COUNT);
            fields.remove(FIELD_TARGET_TOPIC);

            if (isDlqRetry) {
                // DLQ 重试 → XADD 回 DLQ Stream，保留 dlqRetryCount
                fields.remove(StreamMQConstants.FIELD_DLQ_RETRY_COUNT);
                fields.put(
                        StreamMQConstants.FIELD_DLQ_RETRY_COUNT,
                        Integer.toString(retryCount));
                RStream<String, String> dlqStream = redisson.getStream(dlqStreamKey);
                dlqStream.add(StreamAddArgs.entries(fields));
                LOG.info(
                        "DLQ retry transferred: msgId={}, group={}, dlqRetryCount={}",
                        msgId,
                        target.group,
                        retryCount);
            } else {
                // 递增 retryTimes 字段（用于消费端 reconsumeTimes）
                int newRetryTimes = retryCount + 1;
                fields.put(
                        DefaultMessageConverter.FIELD_RETRY_TIMES, Integer.toString(newRetryTimes));

                if (retryCount >= target.maxReconsumeTimes) {
                    // 进入 DLQ
                    fields.put(FIELD_DLQ_REASON, DLQ_REASON_MAX_RETRY);
                    fields.put(FIELD_ORIGINAL_RETRY_COUNT, Integer.toString(retryCount));
                    RStream<String, String> dlqStream = redisson.getStream(dlqStreamKey);
                    dlqStream.add(StreamAddArgs.entries(fields));
                    LOG.info(
                            "Message entered DLQ: msgId={}, topic={}, group={}, retryCount={}",
                            msgId,
                            target.topic,
                            target.group,
                            retryCount);
                } else {
                    // 转投到 retry Stream（非原 Stream，对齐 RocketMQ %RETRY%{group}%）
                    RStream<String, String> targetStream = redisson.getStream(targetStreamKey);
                    StreamAddArgs<String, String> args = StreamAddArgs.entries(fields);
                    if (streamMaxLen > 0) {
                        args = args.trimNonStrict().maxLen(streamMaxLen).noLimit();
                    }
                    targetStream.add(args);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Retry message transferred to retry stream: msgId={}, topic={},"
                                        + " group={}, retryCount={}",
                                msgId,
                                target.topic,
                                target.group,
                                retryCount);
                    }
                }
            }

            // 删除 payload Hash
            payloadMap.delete();
        } catch (RuntimeException ex) {
            LOG.error("Failed to transfer retry message msgId={}: {}", msgId, ex.getMessage(), ex);
            // 处理失败时将 msgId 写回 ZSet（带退避延迟），
            // 避免消息因 ZREM 后处理失败而永久丢失，同时防止 Redis 故障时高频热循环
            try {
                zset.add(System.currentTimeMillis() + failureRequeueBackoffMs, msgId);
                LOG.warn(
                        "Re-added msgId={} to retry ZSet (backoff {}ms)",
                        msgId,
                        failureRequeueBackoffMs);
            } catch (RuntimeException reAddEx) {
                LOG.error(
                        "CRITICAL: Failed to re-add msgId={} to retry ZSet, message may be lost:"
                                + " {}",
                        msgId,
                        reAddEx.getMessage(),
                        reAddEx);
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
                String payloadKey = StreamMQKeys.retryPayloadHash(namespace, msgId);
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
    private static final class RetryTarget {
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
