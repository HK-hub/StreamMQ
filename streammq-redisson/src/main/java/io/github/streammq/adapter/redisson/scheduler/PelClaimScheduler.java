package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.scheduler.StreamMQScheduler;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redisson.api.PendingEntry;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PEL 认领调度器，用于顺序消费 SUSPEND 后的消息恢复（对齐 RocketMQ 顺序消费的 queue 重投）。
 *
 * <p>顺序消费失败时消息留在 PEL 中，本调度器周期扫描各 orderly 消费组的 PEL，将空闲超过阈值的消息重新投递， 保证顺序消息不会因消费者崩溃而永久卡死。
 *
 * <p>实现说明：容器消费者通过 {@code XREADGROUP >}（neverDelivered）读取，XAUTOCLAIM 投递到固定消费者名 的消息永远不会被再次读取（永久卡在
 * PEL）；因此这里采用「XADD 新 entry（递增 {@code retryTimes}，保留 {@code originalMessageId} 原 ID 字段）+ ACK 旧
 * entry」的方式，使消息作为新消息被消费者重新拉取。
 *
 * <p>当消息的 {@code retryTimes} 字段超过 {@code maxReconsumeTimes} 时，从 PEL 中 ACK 移除并 XADD 到 DLQ Stream。
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class PelClaimScheduler implements StreamMQScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(PelClaimScheduler.class);

    /** PEL 空闲阈值（毫秒）：消息在 PEL 中超过此时间未被 ACK 则触发 XAUTOCLAIM */
    private static final long DEFAULT_MIN_IDLE_MS = 30_000L;

    /** 默认扫描间隔（毫秒） */
    private static final long DEFAULT_SCAN_INTERVAL_MS = 5_000L;

    /** 默认单次扫描批量 */
    private static final int DEFAULT_BATCH_SIZE = 100;

    /** DLQ 原因：顺序消费超限 */
    private static final String DLQ_REASON_ORDERLY_MAX_RETRY = "maxRetryOrderly";

    /** 重投消息中保留的原始 Stream Entry ID 字段名（供业务幂等/追踪使用） */
    private static final String FIELD_ORIGINAL_MESSAGE_ID = "originalMessageId";

    private final RedissonClient redisson;
    private final String namespace;
    private final long scanIntervalMs;
    private final int batchSize;
    private final long minIdleMs;
    private final ScheduledExecutorService scanExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentMap<String, PelClaimTarget> targets = new ConcurrentHashMap<>();

    /** 当前的扫描调度任务，stop 时取消以支持后续 restart */
    private volatile ScheduledFuture<?> scanFuture;

    /**
     * 构造调度器。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param scanIntervalMs 扫描间隔（毫秒）
     * @param batchSize 单次扫描批量
     */
    public PelClaimScheduler(
            RedissonClient redisson, String namespace, long scanIntervalMs, int batchSize) {
        this(redisson, namespace, scanIntervalMs, batchSize, DEFAULT_MIN_IDLE_MS);
    }

    /**
     * 构造调度器（可指定 PEL 空闲阈值）。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param scanIntervalMs 扫描间隔（毫秒）
     * @param batchSize 单次扫描批量
     * @param minIdleMs PEL 空闲阈值（毫秒）
     */
    public PelClaimScheduler(
            RedissonClient redisson,
            String namespace,
            long scanIntervalMs,
            int batchSize,
            long minIdleMs) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = Objects.isNull(namespace) ? "" : namespace;
        this.scanIntervalMs = scanIntervalMs > 0 ? scanIntervalMs : DEFAULT_SCAN_INTERVAL_MS;
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.minIdleMs = minIdleMs > 0 ? minIdleMs : DEFAULT_MIN_IDLE_MS;
        this.scanExecutor =
                new ScheduledThreadPoolExecutor(
                        1,
                        r -> {
                            Thread t = new Thread(r, "streammq-pelclaim-scheduler");
                            t.setDaemon(true);
                            return t;
                        });
    }

    /**
     * 注册一个 PEL 认领目标（topic + group）。
     *
     * @param topic 主题
     * @param group 消费者组名
     * @param maxReconsumeTimes 最大重试次数
     */
    public void registerTarget(String topic, String group, int maxReconsumeTimes) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(group, "group");
        String key = topic + ":" + group;
        targets.put(key, new PelClaimTarget(topic, group, maxReconsumeTimes));
        LOG.info(
                "Registered PelClaim target: topic={}, group={}, maxReconsumeTimes={}",
                topic,
                group,
                maxReconsumeTimes);
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.warn("PelClaimScheduler already started");
            return;
        }
        scanFuture =
                scanExecutor.scheduleAtFixedRate(
                        this::scanAllTargets, 0, scanIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info(
                "PelClaimScheduler started, scanIntervalMs={}, minIdleMs={}, targets={}",
                scanIntervalMs,
                minIdleMs,
                targets.size());
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
        LOG.info("PelClaimScheduler stopped");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    private void scanAllTargets() {
        for (PelClaimTarget target : targets.values()) {
            try {
                scanPel(target);
            } catch (RuntimeException ex) {
                LOG.warn(
                        "scanPel failed for topic={}, group={}: {}",
                        target.topic,
                        target.group,
                        ex.getMessage(),
                        ex);
            }
        }
    }

    /** 扫描指定目标的 PEL，对空闲超阈值的消息执行 XAUTOCLAIM 或 DLQ 路由。 */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void scanPel(PelClaimTarget target) {
        String streamKey = StreamMQKeys.topicStream(namespace, target.topic);
        RStream<String, String> stream = redisson.getStream(streamKey);
        String dlqStreamKey = StreamMQKeys.dlqStream(namespace, target.group);

        // 读取 PEL 中的 pending 消息
        List<StreamMessageId> pendingIds;
        try {
            // listPending 返回 PendingEntry 列表，包含 messageId 和 idleTime
            var pending =
                    stream.listPending(
                            target.group, StreamMessageId.MIN, StreamMessageId.MAX, batchSize);
            if (CollectionUtils.isEmpty(pending)) {
                return;
            }
            for (PendingEntry entry : pending) {
                try {
                    StreamMessageId id = entry.getId();
                    long idleTime = entry.getIdleTime();
                    if (idleTime < minIdleMs) {
                        continue;
                    }
                    // 读取消息内容判断 retryTimes
                    var readResult = stream.range(id, id);
                    if (CollectionUtils.isEmpty(readResult)) {
                        continue;
                    }
                    Map<String, String> fields =
                            (Map<String, String>) readResult.values().iterator().next();
                    int retryTimes = parseRetryTimes(fields);
                    if (retryTimes >= target.maxReconsumeTimes) {
                        // 超限 → ACK 移除 + XADD 到 DLQ
                        stream.ack(target.group, id);
                        fields.put(RetryScheduler.FIELD_DLQ_REASON, DLQ_REASON_ORDERLY_MAX_RETRY);
                        fields.put(
                                RetryScheduler.FIELD_ORIGINAL_RETRY_COUNT,
                                Integer.toString(retryTimes));
                        RStream<String, String> dlqStream = redisson.getStream(dlqStreamKey);
                        dlqStream.add(StreamAddArgs.entries(fields));
                        LOG.info(
                                "Orderly message entered DLQ: topic={}, group={}, id={},"
                                        + " retryTimes={}",
                                target.topic,
                                target.group,
                                id,
                                retryTimes);
                    } else {
                        // 重新投递：容器消费者使用 XREADGROUP >（neverDelivered）读取，
                        // XAUTOCLAIM 到固定消费者名（pelclaim-consumer）的消息永远不会被读取（永久卡在 PEL）。
                        // 因此改为：XADD 新 entry（递增 retryTimes，保留 originalMessageId）+ ACK 旧 entry，
                        // 使其作为新消息被消费者重新拉取；重投次数超限后由上方分支进入 DLQ。
                        try {
                            fields.put(
                                    DefaultMessageConverter.FIELD_RETRY_TIMES,
                                    Integer.toString(retryTimes + 1));
                            fields.put(FIELD_ORIGINAL_MESSAGE_ID, id.toString());
                            stream.add(StreamAddArgs.entries(fields));
                            stream.ack(target.group, id);
                            LOG.info(
                                    "Orderly pending redelivered: topic={}, group={}, id={},"
                                            + " retryTimes={}",
                                    target.topic,
                                    target.group,
                                    id,
                                    retryTimes + 1);
                        } catch (RuntimeException ex) {
                            LOG.warn(
                                    "Failed to redeliver orderly pending id={}: {}",
                                    id,
                                    ex.getMessage());
                        }
                    }
                } catch (Exception ex) {
                    LOG.warn("Failed to process pending entry: {}", ex.getMessage());
                }
            }
        } catch (RuntimeException ex) {
            LOG.warn(
                    "listPending failed for topic={}, group={}: {}",
                    target.topic,
                    target.group,
                    ex.getMessage());
        }
    }

    private int parseRetryTimes(Map<String, String> fields) {
        String retryTimesStr = fields.get(DefaultMessageConverter.FIELD_RETRY_TIMES);
        if (StringUtils.isNotEmpty(retryTimesStr)) {
            try {
                return Integer.parseInt(retryTimesStr);
            } catch (NumberFormatException ignored) {
                LOG.debug("Failed to parse retry times: {}", retryTimesStr);
            }
        }
        return 0;
    }

    public int getTargetCount() {
        return targets.size();
    }

    private static final class PelClaimTarget {
        final String topic;
        final String group;
        final int maxReconsumeTimes;

        PelClaimTarget(String topic, String group, int maxReconsumeTimes) {
            this.topic = topic;
            this.group = group;
            this.maxReconsumeTimes = maxReconsumeTimes;
        }
    }
}
