package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMqConstants;
import io.github.streammq.core.enums.DelayLevel;
import org.redisson.api.RBatch;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamAddArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 延时消息调度器，周期扫描各 {@link DelayLevel} 的 ZSet，将到期消息转投到目标 Stream。
 *
 * <p>存储模型（对齐 04-detailed-design.md §6）：
 * <ul>
 *   <li>ZSet Key: {@code streammq:{ns}:delay:{level}}，score=deliverAt(ms)，member=msgId</li>
 *   <li>payload Hash Key: {@code streammq:{ns}:delay:payload:{msgId}}，存储消息完整字段 + targetTopic + deliverAt</li>
 * </ul>
 *
 * <p>转投流程（Java 端原子操作，非 Lua）：
 * <ol>
 *   <li>{@code ZRANGEBYSCORE 0 now LIMIT 0 batchSize} 获取到期 msgId</li>
 *   <li>对每个 msgId：{@code ZREM}（返回 1 表示成功获取，避免多实例重复处理）</li>
 *   <li>从 payload Hash 读取字段，{@code XADD} 到目标 Stream</li>
 *   <li>{@code DEL} payload Hash</li>
 * </ol>
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DelayMessageScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(DelayMessageScheduler.class);

    /** payload Hash 中的目标 Topic 字段名 */
    public static final String FIELD_TARGET_TOPIC = "targetTopic";
    /** payload Hash 中的投递时间字段名 */
    public static final String FIELD_DELIVER_AT = "deliverAt";
    /** 默认扫描间隔（毫秒） */
    private static final long DEFAULT_SCAN_INTERVAL_MS = StreamMqConstants.DEFAULT_SCAN_INTERVAL_MS;
    /** 默认单次扫描批量 */
    private static final int DEFAULT_BATCH_SIZE = StreamMqConstants.DEFAULT_BATCH_SIZE;
    /** 关闭调度线程池时的等待超时（秒） */
    private static final long AWAIT_TERMINATION_SECONDS = StreamMqConstants.DEFAULT_AWAIT_TERMINATION_SECONDS;

    private final RedissonClient redisson;
    private final String namespace;
    private final long scanIntervalMs;
    private final int batchSize;
    private final ScheduledExecutorService scanExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * 构造调度器。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param scanIntervalMs 扫描间隔（毫秒）
     * @param batchSize 单次扫描批量大小
     */
    public DelayMessageScheduler(RedissonClient redisson, String namespace,
                                 long scanIntervalMs, int batchSize) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = namespace == null ? "" : namespace;
        this.scanIntervalMs = scanIntervalMs > 0 ? scanIntervalMs : DEFAULT_SCAN_INTERVAL_MS;
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.scanExecutor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "streammq-delay-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 启动调度器。
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.warn("DelayMessageScheduler already started");
            return;
        }
        scanExecutor.scheduleAtFixedRate(this::scanAllLevels, 0, scanIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info("DelayMessageScheduler started, scanIntervalMs={}, batchSize={}", scanIntervalMs, batchSize);
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
            if (!scanExecutor.awaitTermination(AWAIT_TERMINATION_SECONDS, TimeUnit.SECONDS)) {
                scanExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scanExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        LOG.info("DelayMessageScheduler stopped");
    }

    /**
     * 扫描所有延时级别。
     */
    private void scanAllLevels() {
        for (DelayLevel level : DelayLevel.values()) {
            try {
                scanExpired(level);
            } catch (RuntimeException ex) {
                LOG.warn("scanExpired failed for level {}: {}", level, ex.getMessage(), ex);
            }
        }
    }

    /**
     * 扫描指定级别的到期消息并转投。
     *
     * @param level 延时级别
     */
    void scanExpired(DelayLevel level) {
        String zsetKey = StreamMQKeys.delayZSet(namespace, level.name());
        RScoredSortedSet<String> zset = redisson.getScoredSortedSet(zsetKey);
        long now = System.currentTimeMillis();

        // 获取到期 entry（score <= now），限制 batchSize
        Collection<String> expired = zset.valueRange(0, true, now, true, 0, batchSize - 1);
        if (expired.isEmpty()) {
            return;
        }

        int transferred = 0;
        RBatch batch = null;
        for (String msgId : expired) {
            // ZREM 原子移除，返回 true 表示当前实例成功获取
            boolean acquired = zset.remove(msgId);
            if (!acquired) {
                continue;
            }
            try {
                String payloadKey = StreamMQKeys.delayPayloadHash(namespace, msgId);
                RMap<String, String> payloadMap = redisson.getMap(payloadKey);
                Map<String, String> fields = payloadMap.readAllMap();
                if (fields == null || fields.isEmpty()) {
                    LOG.warn("Delay payload not found for msgId={}, may have been processed", msgId);
                    continue;
                }

                String targetTopic = fields.get(FIELD_TARGET_TOPIC);
                if (targetTopic == null || targetTopic.isEmpty()) {
                    LOG.warn("Delay message has no targetTopic, skip: msgId={}", msgId);
                    continue;
                }

                // 移除调度元数据字段，只保留 Stream Entry 字段
                fields.remove(FIELD_TARGET_TOPIC);
                fields.remove(FIELD_DELIVER_AT);

                if (batch == null) {
                    batch = redisson.createBatch();
                }
                String targetStreamKey = StreamMQKeys.topicStream(namespace, targetTopic);
                StreamAddArgs<String, String> args = StreamAddArgs.entries(fields);
                batch.<String, String>getStream(targetStreamKey).addAsync(args);
                batch.<String, String>getMap(payloadKey).deleteAsync();
                transferred++;

                // 批量达到阈值时执行
                if (transferred >= batchSize) {
                    executeBatch(batch);
                    batch = null;
                    transferred = 0;
                }
            } catch (RuntimeException ex) {
                LOG.error("Failed to transfer delay message msgId={}: {}", msgId, ex.getMessage(), ex);
                // 处理失败时将 msgId 重新写回 ZSet（score=当前时间，立即重试），
                // 避免消息因 ZREM 后处理失败而永久丢失
                try {
                    zset.add(System.currentTimeMillis(), msgId);
                    LOG.warn("Re-added msgId={} to delay ZSet for retry", msgId);
                } catch (RuntimeException reAddEx) {
                    LOG.error("CRITICAL: Failed to re-add msgId={} to delay ZSet, message may be lost: {}",
                        msgId, reAddEx.getMessage(), reAddEx);
                }
            }
        }
        if (batch != null) {
            executeBatch(batch);
        }
    }

    private void executeBatch(RBatch batch) {
        try {
            batch.execute();
        } catch (RuntimeException ex) {
            LOG.error("Delay batch execute failed: {}", ex.getMessage(), ex);
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
}
