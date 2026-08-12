package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
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
 * 閲嶈瘯娑堟伅璋冨害鍣紝鍛ㄦ湡鎵弿閲嶈瘯 ZSet锛屽皢鍒版湡娑堟伅杞姇鍒扮洰鏍?Stream 鎴?DLQ Stream銆?
 *
 * <p>瀛樺偍妯″瀷锛堝榻?04-detailed-design.md 搂6锛夛細
 *
 * <ul>
 *   <li>ZSet Key: {@code streammq:{ns}:retry:{topic}:{group}}锛宻core=nextRetryAt(ms)锛宮ember=msgId
 *   <li>payload Hash Key: {@code streammq:{ns}:retry:payload:{msgId}}锛?瀛樺偍娑堟伅瀹屾暣瀛楁 + {@code
 *       retryCount} + {@code targetTopic}
 * </ul>
 *
 * <p>杞姇鍐崇瓥锛堝榻愬喅绛?D5锛夛細
 *
 * <ul>
 *   <li>{@code retryCount < maxReconsumeTimes}锛氳浆鎶曞埌鐩爣 Stream锛坽@code streammq:{ns}:msg:{topic}}锛夛紝
 *       閫掑 {@code retryTimes} 瀛楁
 *   <li>{@code retryCount >= maxReconsumeTimes}锛氳浆鎶曞埌 DLQ Stream锛坽@code
 *       streammq:{ns}:dlq:{group}}锛?
 * </ul>
 *
 * <p>杞姇娴佺▼锛圝ava 绔師瀛愭搷浣滐紝ZREM 淇濊瘉 only-once锛夛細
 *
 * <ol>
 *   <li>{@code ZRANGEBYSCORE 0 now LIMIT 0 batchSize} 鑾峰彇鍒版湡 msgId
 *   <li>瀵规瘡涓?msgId锛歿@code ZREM}锛堣繑鍥?true 琛ㄧず鎴愬姛鑾峰彇锛?
 *   <li>浠?payload Hash 璇诲彇瀛楁涓?retryCount
 *   <li>鎸?retryCount 鍐崇瓥锛歑ADD 鍒扮洰鏍?Stream 鎴?DLQ Stream
 *   <li>{@code DEL} payload Hash
 * </ol>
 *
 * <p>绾跨▼瀹夊叏锛氭墍鏈夊瓧娈靛潎涓?final 鎴栫嚎绋嬪畨鍏ㄧ被鍨嬨€?
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class RetryScheduler implements StreamMQScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(RetryScheduler.class);

    /** payload Hash 涓殑閲嶈瘯娆℃暟瀛楁鍚? */
    public static final String FIELD_RETRY_COUNT = "retryCount";

    /** payload Hash 涓殑鐩爣 Topic 瀛楁鍚? */
    public static final String FIELD_TARGET_TOPIC = "targetTopic";

    /** DLQ Stream Entry 瀛楁锛氳繘鍏?DLQ 鐨勫師鍥? */
    public static final String FIELD_DLQ_REASON = "dlqReason";

    /** DLQ Stream Entry 瀛楁锛氬師濮嬮噸璇曟鏁? */
    public static final String FIELD_ORIGINAL_RETRY_COUNT = "originalRetryCount";

    /** DLQ 鍘熷洜锛氳揪鍒版渶澶ч噸璇曟鏁? */
    public static final String DLQ_REASON_MAX_RETRY = "maxRetry";

    /** 榛樿鎵弿闂撮殧锛堟绉掞級 */
    private static final long DEFAULT_SCAN_INTERVAL_MS = StreamMQConstants.DEFAULT_SCAN_INTERVAL_MS;

    /** 榛樿鍗曟鎵弿鎵归噺 */
    private static final int DEFAULT_BATCH_SIZE = StreamMQConstants.DEFAULT_BATCH_SIZE;

    /** 鍏抽棴璋冨害绾跨▼姹犳椂鐨勭瓑寰呰秴鏃讹紙绉掞級 */
    private static final long AWAIT_TERMINATION_SECONDS =
            StreamMQConstants.DEFAULT_AWAIT_TERMINATION_SECONDS;

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
                            Thread t = new Thread(r, "streammq-retry-scheduler");
                            t.setDaemon(true);
                            return t;
                        });
    }

    /**
     * 娉ㄥ唽涓€涓噸璇曠洰鏍囷紙topic + group锛夈€?
     *
     * @param topic 涓婚
     * @param group 娑堣垂鑰呯粍鍚?
     * @param maxReconsumeTimes 鏈€澶ч噸璇曟鏁?
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

    /** 鍚姩璋冨害鍣ㄣ€? */
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

    /** 鍋滄璋冨害鍣紙鍙栨秷鎵弿浠诲姟骞跺叧闂嚎绋嬫睜锛岀嚎绋嬩负 daemon锛屼笉闃诲 JVM 閫€鍑猴級銆? */
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
        LOG.info("RetryScheduler stopped");
    }

    /** 鎵弿鎵€鏈夊凡娉ㄥ唽鐨勯噸璇曠洰鏍囥€? */
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
     * 鎵弿鎸囧畾鐩爣鐨勫埌鏈熼噸璇曟秷鎭苟杞姇銆?
     *
     * @param target 閲嶈瘯鐩爣
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

            // 妫€鏌ユ槸鍚︿负 DLQ 閲嶈瘯鍝ㄥ叺
            String targetTopic = fields.get(FIELD_TARGET_TOPIC);
            boolean isDlqRetry =
                    io.github.streammq.core.StreamMQConstants.DLQ_RETRY_TARGET_TOPIC_SENTINEL
                            .equals(targetTopic);

            int retryCount = 0;
            String retryCountStr = fields.get(FIELD_RETRY_COUNT);
            if (StringUtils.isNotEmpty(retryCountStr)) {
                try {
                    retryCount = Integer.parseInt(retryCountStr);
                } catch (NumberFormatException ignored) {
                    LOG.debug("Failed to parse retry count: {}", retryCountStr);
                }
            }

            // 绉婚櫎璋冨害鍏冩暟鎹瓧娈碉紝鍙繚鐣?Stream Entry 瀛楁
            fields.remove(FIELD_RETRY_COUNT);
            fields.remove(FIELD_TARGET_TOPIC);

            if (isDlqRetry) {
                // DLQ 閲嶈瘯 鈫?XADD 鍥?DLQ Stream锛屼繚鐣?dlqRetryCount
                fields.remove(io.github.streammq.core.StreamMQConstants.FIELD_DLQ_RETRY_COUNT);
                fields.put(
                        io.github.streammq.core.StreamMQConstants.FIELD_DLQ_RETRY_COUNT,
                        Integer.toString(retryCount));
                RStream<String, String> dlqStream = redisson.getStream(dlqStreamKey);
                dlqStream.add(StreamAddArgs.entries(fields));
                LOG.info(
                        "DLQ retry transferred: msgId={}, group={}, dlqRetryCount={}",
                        msgId,
                        target.group,
                        retryCount);
            } else {
                // 閫掑 retryTimes 瀛楁锛堢敤浜庢秷璐圭 reconsumeTimes锛?
                int newRetryTimes = retryCount + 1;
                fields.put(
                        DefaultMessageConverter.FIELD_RETRY_TIMES, Integer.toString(newRetryTimes));

                if (retryCount >= target.maxReconsumeTimes) {
                    // 杩涘叆 DLQ
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

            // 鍒犻櫎 payload Hash
            payloadMap.delete();
        } catch (RuntimeException ex) {
            LOG.error("Failed to transfer retry message msgId={}: {}", msgId, ex.getMessage(), ex);
            // 澶勭悊澶辫触鏃跺皢 msgId 閲嶆柊鍐欏洖 ZSet锛坰core=褰撳墠鏃堕棿锛岀珛鍗抽噸璇曪級锛?
            // 閬垮厤娑堟伅鍥?ZREM 鍚庡鐞嗗け璐ヨ€屾案涔呬涪澶?
            try {
                zset.add(System.currentTimeMillis(), msgId);
                LOG.warn("Re-added msgId={} to retry ZSet for retry", msgId);
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
     * 杩斿洖璋冨害鍣ㄦ槸鍚︽鍦ㄨ繍琛屻€?
     *
     * @return true 濡傛灉杩愯涓?
     */
    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * 杩斿洖宸叉敞鍐岀殑閲嶈瘯鐩爣鏁伴噺銆?
     *
     * @return 鐩爣鏁伴噺
     */
    public int getTargetCount() {
        return targets.size();
    }

    /**
     * 娓呯悊鎵€鏈夐噸璇?ZSet 涓殑瀛ょ珛 entry锛堟棤瀵瑰簲 payload Hash 鐨勬潯鐩級銆?
     *
     * <p>涓?{@link DelayMessageScheduler#cleanupOrphanedEntries()} 绫讳技锛?娓呯悊鍥犲紓甯稿鑷存畫鐣欑殑閲嶈瘯 ZSet
     * entry銆?
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

    // ===================== 鍐呴儴绫?=====================

    /** 閲嶈瘯鐩爣淇℃伅 */
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
