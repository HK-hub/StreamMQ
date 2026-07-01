package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.support.StreamMqKeys;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamAddArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 重试消息调度器，周期扫描重试 ZSet，将到期消息转投到目标 Stream 或 DLQ Stream。
 *
 * <p>存储模型（对齐 04-detailed-design.md §6）：
 * <ul>
 *   <li>ZSet Key: {@code streammq:{ns}:retry:{topic}:{group}}，score=nextRetryAt(ms)，member=msgId</li>
 *   <li>payload Hash Key: {@code streammq:{ns}:retry:payload:{msgId}}，
 *       存储消息完整字段 + {@code retryCount} + {@code targetTopic}</li>
 * </ul>
 *
 * <p>转投决策（对齐决策 D5）：
 * <ul>
 *   <li>{@code retryCount < maxReconsumeTimes}：转投到目标 Stream（{@code streammq:{ns}:msg:{topic}}），
 *       递增 {@code retryTimes} 字段</li>
 *   <li>{@code retryCount >= maxReconsumeTimes}：转投到 DLQ Stream（{@code streammq:{ns}:dlq:{topic}:{group}}）</li>
 * </ul>
 *
 * <p>转投流程（Java 端原子操作，ZREM 保证 only-once）：
 * <ol>
 *   <li>{@code ZRANGEBYSCORE 0 now LIMIT 0 batchSize} 获取到期 msgId</li>
 *   <li>对每个 msgId：{@code ZREM}（返回 true 表示成功获取）</li>
 *   <li>从 payload Hash 读取字段与 retryCount</li>
 *   <li>按 retryCount 决策：XADD 到目标 Stream 或 DLQ Stream</li>
 *   <li>{@code DEL} payload Hash</li>
 * </ol>
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class RetryScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(RetryScheduler.class);

    /** payload Hash 中的重试次数字段名 */
    public static final String FIELD_RETRY_COUNT = "retryCount";
    /** payload Hash 中的目标 Topic 字段名 */
    public static final String FIELD_TARGET_TOPIC = "targetTopic";

    private final RedissonClient redisson;
    private final String namespace;
    private final long scanIntervalMs;
    private final int batchSize;
    private final ScheduledExecutorService scanExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentMap<String, RetryTarget> targets = new ConcurrentHashMap<>();

    /**
     * 构造调度器。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param scanIntervalMs 扫描间隔（毫秒）
     * @param batchSize 单次扫描批量大小
     */
    public RetryScheduler(RedissonClient redisson, String namespace,
                          long scanIntervalMs, int batchSize) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = namespace == null ? "" : namespace;
        this.scanIntervalMs = scanIntervalMs > 0 ? scanIntervalMs : 1000L;
        this.batchSize = batchSize > 0 ? batchSize : 100;
        this.scanExecutor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "streammq-retry-scheduler");
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
        LOG.info("Registered retry target: topic={}, group={}, maxReconsumeTimes={}",
            topic, group, maxReconsumeTimes);
    }

    /**
     * 启动调度器。
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.warn("RetryScheduler already started");
            return;
        }
        scanExecutor.scheduleAtFixedRate(this::scanAllTargets, 0, scanIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info("RetryScheduler started, scanIntervalMs={}, batchSize={}, targets={}",
            scanIntervalMs, batchSize, targets.size());
    }

    /**
     * 停止调度器。
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        scanExecutor.shutdown();
        try {
            if (!scanExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                scanExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scanExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        LOG.info("RetryScheduler stopped");
    }

    /**
     * 扫描所有已注册的重试目标。
     */
    private void scanAllTargets() {
        for (RetryTarget target : targets.values()) {
            try {
                scanRetryEntries(target);
            } catch (RuntimeException ex) {
                LOG.warn("scanRetryEntries failed for topic={}, group={}: {}",
                    target.topic, target.group, ex.getMessage(), ex);
            }
        }
    }

    /**
     * 扫描指定目标的到期重试消息并转投。
     *
     * @param target 重试目标
     */
    void scanRetryEntries(RetryTarget target) {
        String retryKey = StreamMqKeys.retryZSet(namespace, target.topic, target.group);
        RScoredSortedSet<String> zset = redisson.getScoredSortedSet(retryKey);
        long now = System.currentTimeMillis();

        Collection<String> expired = zset.valueRange(0, true, now, true, 0, batchSize - 1);
        if (expired.isEmpty()) {
            return;
        }

        String targetStreamKey = StreamMqKeys.topicStream(namespace, target.topic);
        String dlqStreamKey = StreamMqKeys.dlqStream(namespace, target.topic, target.group);

        for (String msgId : expired) {
            boolean acquired = zset.remove(msgId);
            if (!acquired) {
                continue;
            }
            transferOne(msgId, target, targetStreamKey, dlqStreamKey, zset);
        }
    }

    private void transferOne(String msgId, RetryTarget target, String targetStreamKey, String dlqStreamKey,
                              RScoredSortedSet<String> zset) {
        String payloadKey = StreamMqKeys.delayPayloadHash(namespace, msgId);
        try {
            RMap<String, String> payloadMap = redisson.getMap(payloadKey);
            Map<String, String> fields = payloadMap.readAllMap();
            if (fields == null || fields.isEmpty()) {
                LOG.warn("Retry payload not found for msgId={}, may have been processed", msgId);
                return;
            }

            int retryCount = 0;
            String retryCountStr = fields.get(FIELD_RETRY_COUNT);
            if (retryCountStr != null && !retryCountStr.isEmpty()) {
                try {
                    retryCount = Integer.parseInt(retryCountStr);
                } catch (NumberFormatException ignored) {
                }
            }

            // 移除调度元数据字段，只保留 Stream Entry 字段
            fields.remove(FIELD_RETRY_COUNT);
            fields.remove(FIELD_TARGET_TOPIC);

            // 递增 retryTimes 字段（用于消费端 reconsumeTimes）
            int newRetryTimes = retryCount + 1;
            fields.put(DefaultMessageConverter.FIELD_RETRY_TIMES, Integer.toString(newRetryTimes));

            if (retryCount >= target.maxReconsumeTimes) {
                // 进入 DLQ
                fields.put("dlqReason", "maxRetry");
                fields.put("originalRetryCount", Integer.toString(retryCount));
                RStream<String, String> dlqStream = redisson.getStream(dlqStreamKey);
                dlqStream.add(StreamAddArgs.entries(fields));
                LOG.info("Message entered DLQ: msgId={}, topic={}, group={}, retryCount={}",
                    msgId, target.topic, target.group, retryCount);
            } else {
                // 转投到目标 Stream
                RStream<String, String> targetStream = redisson.getStream(targetStreamKey);
                targetStream.add(StreamAddArgs.entries(fields));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Retry message transferred: msgId={}, topic={}, group={}, retryCount={}",
                        msgId, target.topic, target.group, retryCount);
                }
            }

            // 删除 payload Hash
            payloadMap.delete();
        } catch (RuntimeException ex) {
            LOG.error("Failed to transfer retry message msgId={}: {}", msgId, ex.getMessage(), ex);
            // 处理失败时将 msgId 重新写回 ZSet（score=当前时间，立即重试），
            // 避免消息因 ZREM 后处理失败而永久丢失
            try {
                zset.add(System.currentTimeMillis(), msgId);
                LOG.warn("Re-added msgId={} to retry ZSet for retry", msgId);
            } catch (RuntimeException reAddEx) {
                LOG.error("CRITICAL: Failed to re-add msgId={} to retry ZSet, message may be lost: {}",
                    msgId, reAddEx.getMessage(), reAddEx);
            }
        }
    }

    /**
     * 返回调度器是否正在运行。
     *
     * @return true 如果运行中
     */
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
