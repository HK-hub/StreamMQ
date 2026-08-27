/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.LocalTransactionState;
import io.github.streammq.core.enums.TransactionScanState;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.scheduler.StreamMQScheduler;
import io.github.streammq.core.transaction.TransactionChecker;
import io.github.streammq.core.transaction.TransactionContext;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Setter;
import org.redisson.api.BatchOptions;
import org.redisson.api.RBatch;
import org.redisson.api.RMap;
import org.redisson.api.RMapAsync;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScript;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 事务回查调度器（编排层）：周期扫描事务回查 ZSet，对超时的半消息触发 {@link TransactionChecker#check}， 按返回状态决定
 * COMMIT / ROLLBACK / UNKNOWN（继续等待或超限强制 ROLLBACK）。
 *
 * <h2>职责拆分</h2>
 *
 * <p>本类为编排层，仅保留生命周期、注册表、扫描循环与状态机； 下列职责已委托给独立协作类：
 *
 * <ul>
 *   <li>{@link TransactionLockManager} - 事务执行权锁（崩溃安全 SETNX+TTL）
 *   <li>{@link TransactionCommitExecutor} - 半消息→目标 Stream 原子转投
 *   <li>{@link TransactionRetentionSweeper} - 终态保留期 + 孤儿 half 清理
 *   <li>{@link TransactionMetricsRecorder} - 事务指标（commit / rollback / check）
 * </ul>
 *
 * <h2>存储布局</h2>
 *
 * <ul>
 *   <li>半消息暂存 Stream：{@code streammq:{ns}:half:{txGroup}}
 *   <li>事务状态 Hash：{@code streammq:{ns}:txstate:{txGroup}}
 *       <ul>
 *         <li>field={@code {txId}}，value=PREPARE / COMMIT / ROLLBACK / UNKNOWN
 *         <li>field={@code {txId}.target}，value=目标 Topic（COMMIT 时 XADD 目标 Stream）
 *         <li>field={@code {txId}.halfId}，value=半消息 Stream Entry ID（XREAD / XDEL 用）
 *       </ul>
 *   <li>事务回查 ZSet：{@code streammq:{ns}:txcheck:{txGroup}}，score=checkTimeMillis，member=txId
 *   <li>回查计数 Hash：{@code streammq:{ns}:txcheck:{txGroup}:counter}，field=txId，value=已回查次数
 * </ul>
 *
 * <h2>典型使用流程</h2>
 *
 * <ol>
 *   <li>starter 调用 {@link #registerChecker} 注册每个 txGroup 的 {@link TransactionChecker}
 *   <li>template 发送事务消息时调用 {@link #registerHalfMessage} 写入半消息 + 状态 + 调度
 *   <li>template 执行本地事务后调用 {@link #markCommit} / {@link #markRollback} 直接终结
 *   <li>若 UNKNOW 或超时未终结，{@link #start} 启动的周期任务扫描并触发回查
 * </ol>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class TransactionScanner implements StreamMQScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionScanner.class);

    /** Class.forName 缓存，避免重复类加载查找 */
    private static final ConcurrentMap<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();

    /** 事务状态字段值（线上协议编码，委托给 {@link TransactionScanState} 枚举） */
    public static final String STATE_PREPARE = TransactionScanState.PREPARE.getCode();

    public static final String STATE_COMMIT = TransactionScanState.COMMIT.getCode();
    public static final String STATE_ROLLBACK = TransactionScanState.ROLLBACK.getCode();
    public static final String STATE_UNKNOWN = TransactionScanState.UNKNOWN.getCode();

    /** 中间状态：提交中（实例已原子抢占事务，正在执行转投，其它实例见到此状态应等待或重新执行） */
    public static final String STATE_COMMITTING = TransactionScanState.COMMITTING.getCode();

    /** 中间状态：回滚中（实例已原子抢占事务，正在执行删除，其它实例见到此状态应等待或重新执行） */
    public static final String STATE_ROLLBACKING = TransactionScanState.ROLLBACKING.getCode();

    /**
     * Lua 脚本：原子检查状态并设置目标状态，返回旧状态。
     *
     * <p>KEYS[1] = txstate Hash key, ARGV[1] = txId, ARGV[2] = targetState
     *
     * <p>返回值：
     *
     * <ul>
     *   <li>"COMMIT" / "ROLLBACK" — 已是终态，无需操作
     *   <li>"COMMITTING" / "ROLLBACKING" — 其它实例正在处理，重新执行
     *   <li>其他 — 旧状态（PREPARE/UNKNOWN），已原子切换到 targetState
     * </ul>
     */
    private static final String LUA_CAS_STATE =
            "local current = redis.call('HGET', KEYS[1], ARGV[1]);"
                    + "if current == 'COMMIT' or current == 'ROLLBACK' then return current; end;"
                    + "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]);"
                    + "if current == false then return 'nil'; else return current; end;";

    /**
     * Lua 脚本：仅当当前状态非终态时置为 UNKNOWN，绝不覆盖 COMMIT/ROLLBACK。
     *
     * <p>KEYS[1] = txstate Hash key, ARGV[1] = txId。返回 'OK' 表示已写入 UNKNOWN；
     * 返回终态值表示状态已被其它实例终态化，本次不覆盖。
     */
    private static final String LUA_CAS_TO_UNKNOWN =
            "local current = redis.call('HGET', KEYS[1], ARGV[1]);"
                    + "if current == 'COMMIT' or current == 'ROLLBACK' then return current; end;"
                    + "redis.call('HSET', KEYS[1], ARGV[1], 'UNKNOWN');"
                    + "return 'OK';";

    /** 终态字段默认保留期（毫秒）：超过后由维护任务从 txstate Hash 清除，防止 Hash 无限增长 */
    public static final long DEFAULT_TX_STATE_RETENTION_MS = 7L * 24 * 60 * 60 * 1000;

    /** 孤儿半消息默认保留期（毫秒）：half Stream 中无状态引用且超龄的条目由维护任务清除 */
    public static final long DEFAULT_ORPHAN_HALF_RETENTION_MS = 24L * 60 * 60 * 1000;

    /** 维护扫描运行间隔（每 N 轮回查扫描执行一次终态/孤儿清理） */
    private static final int MAINTENANCE_EVERY_N_SCANS = 10;

    /** 默认扫描间隔 60s */
    public static final long DEFAULT_CHECK_INTERVAL_MS =
            StreamMQConstants.DEFAULT_CHECK_INTERVAL_MS;

    /** 默认最大回查次数 15 次 */
    public static final int DEFAULT_MAX_CHECK_TIMES = StreamMQConstants.DEFAULT_MAX_CHECK_TIMES;

    /** 默认单次扫描批量 */
    public static final int DEFAULT_BATCH_SIZE = StreamMQConstants.DEFAULT_BATCH_SIZE;

    /** 回查器执行默认超时（毫秒）：慢回查不得阻塞整个扫描线程 */
    public static final long DEFAULT_CHECKER_TIMEOUT_MILLIS = 30_000L;

    /** txstate Hash 中目标 Topic 字段后缀 */
    private static final String FIELD_TARGET_SUFFIX = StreamMQConstants.TX_FIELD_TARGET_SUFFIX;

    /** txstate Hash 中半消息 Stream Entry ID 字段后缀 */
    private static final String FIELD_HALF_ID_SUFFIX = StreamMQConstants.TX_FIELD_HALF_ID_SUFFIX;

    /** 关闭调度线程池时的等待超时（秒） */
    private static final long AWAIT_TERMINATION_SECONDS =
            StreamMQConstants.DEFAULT_AWAIT_TERMINATION_SECONDS;

    /** Lua：原子递增回查计数（HINCRBY） */
    private static final String LUA_INCR_COUNT =
            "local val = redis.call('HINCRBY', KEYS[1], ARGV[1], 1);" + "return val;";

    private final RedissonClient redisson;
    private final String namespace;
    private final MessageConverter messageConverter;
    private final long checkIntervalMs;
    private final int maxCheckTimes;
    private final int batchSize;
    private volatile ScheduledExecutorService scanExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentMap<String, TransactionChecker<?>> checkerRegistry =
            new ConcurrentHashMap<>();

    /** 协作类：事务执行权锁、原子转投、保留期清理、指标记录 */
    private final TransactionLockManager lockManager;
    private final TransactionCommitExecutor commitExecutor;
    private final TransactionRetentionSweeper retentionSweeper;
    /** 指标 recorder 不为 final：通过 setter 重建以反映最新 metrics 引用（避免引入额外 setter 接口） */
    private volatile TransactionMetricsRecorder metricsRecorder;

    /** 当前的扫描调度任务，stop 时取消以支持后续 restart */
    private volatile ScheduledFuture<?> scanFuture;

    /** 维护扫描轮次计数器 */
    private final AtomicLong maintenanceCounter = new AtomicLong();

    /**
     * per-group 回查互斥锁登记表：同一 txGroup 的回查器串行执行。
     *
     * <p>此前回查在扫描线程上内联执行——一个慢 checker 拖住全部组的扫描，且多实例/多轮扫描可能对 同组并发触发 check（业务侧非幂等时产生重复副作用）。锁对象为普通
     * {@code Object}（无争用时零开销）。
     */
    private final ConcurrentMap<String, Object> groupCheckLocks = new ConcurrentHashMap<>();

    /** 指标收集器（可选注入，用于记录事务指标，null 时为 no-op） */
    private volatile io.github.streammq.core.metrics.StreamMQMetrics metrics;

    /** 事务执行权锁 TTL（毫秒），可通过 {@link #setTxLockTtlMs} 覆盖 */
    private volatile long txLockTtlMs = TransactionLockManager.DEFAULT_TX_LOCK_TTL_MS;

    /** 终态字段保留期（毫秒），超过后由维护任务清理 */
    @Setter private volatile long txStateRetentionMs = DEFAULT_TX_STATE_RETENTION_MS;

    /** 孤儿半消息保留期（毫秒），超过且无状态引用的 half 条目由维护任务清理 */
    @Setter private volatile long orphanHalfRetentionMs = DEFAULT_ORPHAN_HALF_RETENTION_MS;

    /** 回查器执行超时（毫秒），可通过 setter 覆盖 */
    @Setter private volatile long checkerTimeoutMillis = DEFAULT_CHECKER_TIMEOUT_MILLIS;

    /**
     * 构造调度器，使用默认参数。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param messageConverter 消息转换器（用于 COMMIT 时将半消息字段写入目标 Stream）
     */
    public TransactionScanner(
            RedissonClient redisson, String namespace, MessageConverter messageConverter) {
        this(
                redisson,
                namespace,
                messageConverter,
                DEFAULT_CHECK_INTERVAL_MS,
                DEFAULT_MAX_CHECK_TIMES,
                DEFAULT_BATCH_SIZE);
    }

    /**
     * 全参构造。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param messageConverter 消息转换器
     * @param checkIntervalMs 回查间隔（毫秒）
     * @param maxCheckTimes 最大回查次数（连续 UNKNOWN 后强制 ROLLBACK）
     * @param batchSize 单次扫描批量
     */
    public TransactionScanner(
            RedissonClient redisson,
            String namespace,
            MessageConverter messageConverter,
            long checkIntervalMs,
            int maxCheckTimes,
            int batchSize) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = Objects.isNull(namespace) ? "" : namespace;
        this.messageConverter = Objects.requireNonNull(messageConverter, "messageConverter");
        this.checkIntervalMs = checkIntervalMs > 0 ? checkIntervalMs : DEFAULT_CHECK_INTERVAL_MS;
        this.maxCheckTimes = maxCheckTimes > 0 ? maxCheckTimes : DEFAULT_MAX_CHECK_TIMES;
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.scanExecutor = Executors.newSingleThreadScheduledExecutor();
        this.lockManager = new TransactionLockManager(redisson, namespace);
        this.commitExecutor = new TransactionCommitExecutor(redisson, namespace, lockManager);
        this.retentionSweeper = new TransactionRetentionSweeper(redisson, namespace);
        this.metricsRecorder = new TransactionMetricsRecorder(null);
    }

    /**
     * 注入指标收集器（同时更新协作 {@link TransactionMetricsRecorder}）。
     *
     * @param newMetrics 指标收集器，可为 null
     */
    public void setMetrics(io.github.streammq.core.metrics.StreamMQMetrics newMetrics) {
        this.metrics = newMetrics;
        // 重建 recorder 以反映最新指标引用（无 setter 接口的设计选择：明确可见性）
        this.metricsRecorder = new TransactionMetricsRecorder(newMetrics);
    }

    public void setTxLockTtlMs(long millis) {
        if (millis > 0) {
            this.txLockTtlMs = millis;
            this.lockManager.setTxLockTtlMs(millis);
        }
    }

    // ===================== 注册方法 =====================

    /**
     * 注册事务回查器。
     *
     * @param txGroup 事务组名
     * @param checker 回查器
     */
    public void registerChecker(String txGroup, TransactionChecker<?> checker) {
        Objects.requireNonNull(txGroup, "txGroup");
        Objects.requireNonNull(checker, "checker");
        checkerRegistry.put(txGroup, checker);
        LOG.info(
                "Registered TransactionChecker: txGroup={}, checker={}",
                txGroup,
                checker.getClass().getSimpleName());
    }

    /**
     * 注册一条半消息，写入 half Stream + 状态 Hash + 回查 ZSet。
     *
     * <p>由 {@code DefaultStreamMQTemplate.executeInTransaction} 在执行本地事务前调用。
     *
     * @param txId 事务 ID
     * @param txGroup 事务组名
     * @param targetTopic 目标 Topic（COMMIT 时 XADD 到此 Topic 对应的 Stream）
     * @param fields 半消息 Stream Entry 字段（由 {@link MessageConverter#toStreamFields} 生成）
     * @return 半消息 Stream Entry ID
     */
    public StreamMessageId registerHalfMessage(
            String txId, String txGroup, String targetTopic, Map<String, String> fields) {
        Objects.requireNonNull(txId, "txId");
        Objects.requireNonNull(txGroup, "txGroup");
        Objects.requireNonNull(targetTopic, "targetTopic");
        Objects.requireNonNull(fields, "fields");
        StringUtils.requireValidTopic(targetTopic);

        // 写入顺序（崩溃安全性分析，顺序不可调整）：
        //  1. 先原子写入 txstate(PREPARE) + 回查 ZSet —— 若在此步后崩溃，回查发现半消息缺失，
        //     走"force rollback"安全终止（本地事务尚未执行，不存在业务消息已发布的风险）；
        //  2. 再 XADD 半消息并补写 halfId —— 若在此步失败/崩溃，最坏情况是残留一条无状态引用的
        //     半消息条目（可被运维清理），绝不会出现"半消息已投递但事务无状态"导致的重复发布。
        long firstCheckAt = System.currentTimeMillis() + checkIntervalMs;
        RBatch batch =
                redisson.createBatch(
                        BatchOptions.defaults()
                                .executionMode(BatchOptions.ExecutionMode.REDIS_WRITE_ATOMIC));
        String stateHashKey = StreamMQKeys.transactionStateHash(namespace, txGroup);
        RMapAsync<String, String> stateMapAsync = batch.getMap(stateHashKey);
        stateMapAsync.putAsync(txId, STATE_PREPARE);
        stateMapAsync.putAsync(txId + FIELD_TARGET_SUFFIX, targetTopic);
        batch.getScoredSortedSet(StreamMQKeys.transactionCheckZSet(namespace, txGroup))
                .addAsync(firstCheckAt, txId);
        batch.execute();

        // XADD 到 half Stream
        String halfStreamKey = StreamMQKeys.halfStream(namespace, txGroup);
        RStream<String, String> halfStream = redisson.getStream(halfStreamKey);
        StreamMessageId halfId;
        try {
            halfId = halfStream.add(StreamAddArgs.entries(fields));
        } catch (RuntimeException ex) {
            // 补偿：清理步骤 1 写入的状态与调度条目，避免留下永远无法推进的 PREPARE 幽灵事务
            LOG.error(
                    "XADD half message failed, compensating txstate/check entry:"
                            + " txId={}, txGroup={}",
                    txId,
                    txGroup,
                    ex);
            try {
                RMap<String, String> stateMap = redisson.getMap(stateHashKey);
                stateMap.remove(txId);
                stateMap.remove(txId + FIELD_TARGET_SUFFIX);
                stateMap.remove(txId + FIELD_HALF_ID_SUFFIX);
                redisson.getScoredSortedSet(StreamMQKeys.transactionCheckZSet(namespace, txGroup))
                        .remove(txId);
            } catch (RuntimeException cleanupEx) {
                LOG.error(
                        "Compensation failed, orphan PREPARE entry remains (scanner will"
                                + " force-rollback it safely): txId={}",
                        txId,
                        cleanupEx);
            }
            throw ex;
        }

        // 补写 halfId 引用
        redisson.getMap(stateHashKey).put(txId + FIELD_HALF_ID_SUFFIX, halfId.toString());

        LOG.debug(
                "Half message registered: txId={}, txGroup={}, targetTopic={}, halfId={}",
                txId,
                txGroup,
                targetTopic,
                halfId);
        return halfId;
    }

    // ===================== 生命周期方法 =====================

    /** 启动调度器，开始周期扫描回查 ZSet。 */
    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.warn("TransactionScanner already started");
            return;
        }
        ensureScanExecutorAlive();
        scanFuture =
                scanExecutor.scheduleAtFixedRate(
                        this::scanAllGroups, 0, checkIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info(
                "TransactionScanner started, checkIntervalMs={}, maxCheckTimes={}, batchSize={},"
                        + " groups={}",
                checkIntervalMs,
                maxCheckTimes,
                batchSize,
                checkerRegistry.size());
    }

    /** restart 支持：stop 后 executor 已关闭，start 前按需重建。 */
    private synchronized void ensureScanExecutorAlive() {
        if (Objects.nonNull(scanExecutor) && !scanExecutor.isShutdown()) {
            return;
        }
        scanExecutor = Executors.newSingleThreadScheduledExecutor();
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
        // 释放 per-group 回查锁登记表：stop 后 restart 由 computeIfAbsent 按需重建
        groupCheckLocks.clear();
        LOG.info("TransactionScanner stopped");
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

    // ===================== 显式状态变更方法 =====================

    /**
     * 显式标记事务为 COMMIT：将半消息转投到目标 Stream 并清理调度元数据。
     *
     * <p>通过 Lua 原子抢占事务执行权（PREPARE/UNKNOWN → COMMITTING）， 多实例并发时只有最先执行 Lua 的实例获得执行权，其余实例见到
     * COMMITTING 后重新执行。 无需分布式锁。
     *
     * @param txId 事务 ID
     * @param txGroup 事务组名
     */
    public void markCommit(String txId, String txGroup) {
        Objects.requireNonNull(txId, "txId");
        Objects.requireNonNull(txGroup, "txGroup");
        doMarkCommit(txId, txGroup);
    }

    private void doMarkCommit(String txId, String txGroup) {
        String stateHashKey = StreamMQKeys.transactionStateHash(namespace, txGroup);
        RMap<String, String> stateMap = redisson.getMap(stateHashKey);

        // 原子抢占：PREPARE/UNKNOWN → COMMITTING
        String oldState = casState(stateHashKey, txId, STATE_COMMITTING);
        if (STATE_COMMIT.equals(oldState) || STATE_ROLLBACK.equals(oldState)) {
            LOG.debug(
                    "markCommit ignored, transaction already terminal: txId={}, state={}",
                    txId,
                    oldState);
            return;
        }
        // COMMITTING/ROLLBACKING 表示其它实例正在处理；发布临界区由事务执行权锁保护（见
        // TransactionCommitExecutor），非持有者不会重复转投，等待其完成或锁 TTL 过期后接管。

        String targetTopic = stateMap.get(txId + FIELD_TARGET_SUFFIX);
        String halfIdStr = stateMap.get(txId + FIELD_HALF_ID_SUFFIX);
        if (Objects.isNull(targetTopic) || Objects.isNull(halfIdStr)) {
            // 元数据丢失：不能静默卡死在 COMMITTING。转为 UNKNOWN 并重新调度回查，
            // 由 maxCheckTimes 兜底强制回滚；ERROR 日志供运维排查。
            LOG.error(
                    "markCommit missing target/halfId in txstate, degrading to UNKNOWN for"
                            + " bounded recheck: txId={}, txGroup={}",
                    txId,
                    txGroup);
            degradeToUnknown(stateHashKey, stateMap, txId, txGroup);
            return;
        }

        // 转投半消息到目标 Stream + 原子标记 COMMIT（执行权锁内串行化）
        TransactionCommitExecutor.Outcome outcome =
                commitExecutor.publishHalfAndMarkCommit(txGroup, halfIdStr, targetTopic, txId);
        switch (outcome) {
            case PUBLISHED -> {
                /* 终态已由原子批写入 */
            }
            case HALF_MISSING -> {
                // 半消息不存在：不能武断记为 COMMIT（可能什么都没投递，静默丢失），
                // 也不能永久卡死。降级为 UNKNOWN 走有界回查：若为暂时性读取异常则下轮恢复；
                // 连续失败超过 maxCheckTimes 后由 force-rollback 安全终结（此时必然未发布——
                // 发布与状态置位在同一原子 MULTI/EXEC 中，状态非 COMMIT 即未投递）。
                LOG.error(
                        "Half message missing at commit time (never published by this"
                                + " instance; publish+COMMIT are atomic), degrading to bounded"
                                + " recheck: txId={}, txGroup={}, halfId={}",
                        txId,
                        txGroup,
                        halfIdStr);
                degradeToUnknown(stateHashKey, stateMap, txId, txGroup);
                commitExecutor.releaseLock(txGroup, txId);
                return;
            }
            case LOCK_BUSY -> {
                // 其它实例持有执行权：保持 COMMITTING 与回查条目，等待其完成或 TTL 过期后接管
                LOG.debug("Publish skipped, another instance holds the tx lock: txId={}", txId);
                return;
            }
        }

        // 终态收尾
        commitExecutor.releaseLock(txGroup, txId);
        removeCheckEntry(txId, txGroup);
        markTerminalDone(stateMap, txId);
        cleanupTerminalState(stateMap, txId);

        metricsRecorder.recordCommit(txGroup);

        LOG.info(
                "Transaction committed: txId={}, txGroup={}, targetTopic={}",
                txId,
                txGroup,
                targetTopic);
    }

    /**
     * 显式标记事务为 ROLLBACK：从 half Stream 删除半消息并清理调度元数据。
     *
     * <p>通过 Lua 原子抢占事务执行权（PREPARE/UNKNOWN → ROLLBACKING）， 无需分布式锁。
     *
     * @param txId 事务 ID
     * @param txGroup 事务组名
     */
    public void markRollback(String txId, String txGroup) {
        Objects.requireNonNull(txId, "txId");
        Objects.requireNonNull(txGroup, "txGroup");
        doMarkRollback(txId, txGroup);
    }

    private void doMarkRollback(String txId, String txGroup) {
        String stateHashKey = StreamMQKeys.transactionStateHash(namespace, txGroup);
        RMap<String, String> stateMap = redisson.getMap(stateHashKey);

        // 原子抢占：PREPARE/UNKNOWN → ROLLBACKING
        String oldState = casState(stateHashKey, txId, STATE_ROLLBACKING);
        if (STATE_COMMIT.equals(oldState) || STATE_ROLLBACK.equals(oldState)) {
            LOG.debug(
                    "markRollback ignored, transaction already terminal: txId={}, state={}",
                    txId,
                    oldState);
            return;
        }

        // 执行权锁与 COMMIT 路径对称：防止回滚的 XDEL 与另一实例的转投 XADD 竞态
        // （否则可能出现"用户要求回滚却已被发布"或半消息在读取后被删导致的幽灵终态）。
        // LOCK_BUSY 时保持 ROLLBACKING 并重新调度，等待持有者完成或锁 TTL 过期后接管。
        if (!lockManager.tryAcquire(txGroup, txId)) {
            LOG.debug("Rollback skipped, another instance holds the tx lock: txId={}", txId);
            rescheduleCheck(txId, txGroup);
            return;
        }

        String halfIdStr = stateMap.get(txId + FIELD_HALF_ID_SUFFIX);
        boolean halfRemoved = true;
        if (Objects.nonNull(halfIdStr)) {
            // XDEL 半消息；失败时不得终结事务（否则半消息永久残留），保持 ROLLBACKING 并重新调度重试
            String halfStreamKey = StreamMQKeys.halfStream(namespace, txGroup);
            RStream<String, String> halfStream = redisson.getStream(halfStreamKey);
            try {
                halfStream.remove(parseStreamId(halfIdStr));
            } catch (RuntimeException ex) {
                halfRemoved = false;
                LOG.warn(
                        "XDEL half message failed, will retry on next scan: txId={}, halfId={}:"
                                + " {}",
                        txId,
                        halfIdStr,
                        ex.getMessage(),
                        ex);
            }
        }

        if (!halfRemoved) {
            // 有界重试：超过 maxCheckTimes 后强制终结（此时可能残留半消息条目，ERROR 提示人工清理）
            int checkCount = getCheckCount(txId, txGroup);
            if (checkCount >= maxCheckTimes) {
                LOG.error(
                        "Force-finalize ROLLBACK after {} failed XDEL attempts; orphan half entry"
                                + " may remain: txId={}, halfId={}",
                        checkCount,
                        txId,
                        halfIdStr);
            } else {
                incrementCheckCount(txId, txGroup);
                lockManager.release(txGroup, txId);
                rescheduleCheck(txId, txGroup);
                return;
            }
        }

        stateMap.put(txId, STATE_ROLLBACK);
        lockManager.release(txGroup, txId);
        removeCheckEntry(txId, txGroup);
        markTerminalDone(stateMap, txId);
        cleanupTerminalState(stateMap, txId);

        metricsRecorder.recordRollback(txGroup);

        LOG.info("Transaction rolled back: txId={}, txGroup={}", txId, txGroup);
    }

    /**
     * 原子检查并设置事务状态（Lua CAS）。
     *
     * @param stateHashKey txstate Hash 的 Redis key
     * @param txId 事务 ID
     * @param targetState 目标状态（COMMITTING / ROLLBACKING）
     * @return 旧状态值（COMMIT/ROLLBACK 表示已是终态，其余表示已抢占）
     */
    private String casState(String stateHashKey, String txId, String targetState) {
        RScript script = redisson.getScript(StringCodec.INSTANCE);
        return script.eval(
                RScript.Mode.READ_WRITE,
                LUA_CAS_STATE,
                RScript.ReturnType.STATUS,
                Collections.singletonList(stateHashKey),
                txId,
                targetState);
    }

    // ===================== 内部扫描逻辑 =====================

    /** 扫描所有已注册 checker 的 txGroup。 */
    private void scanAllGroups() {
        for (String txGroup : checkerRegistry.keySet()) {
            try {
                scanTimeoutHalf(txGroup);
            } catch (RuntimeException ex) {
                LOG.warn("scanTimeoutHalf failed for txGroup={}: {}", txGroup, ex.getMessage(), ex);
            }
        }
        // 周期性维护：清理超龄终态字段与孤儿半消息，防止 txstate Hash / half Stream 无限增长
        if (maintenanceCounter.incrementAndGet() % MAINTENANCE_EVERY_N_SCANS == 0) {
            for (String txGroup : checkerRegistry.keySet()) {
                try {
                    retentionSweeper.sweepExpiredTerminalStates(txGroup);
                } catch (RuntimeException ex) {
                    LOG.debug(
                            "sweepExpiredTerminalStates failed: txGroup={}: {}",
                            txGroup,
                            ex.getMessage(),
                            ex);
                }
                try {
                    retentionSweeper.sweepOrphanHalves(txGroup);
                } catch (RuntimeException ex) {
                    LOG.debug(
                            "sweepOrphanHalves failed: txGroup={}: {}",
                            txGroup,
                            ex.getMessage(),
                            ex);
                }
            }
        }
    }

    /**
     * 扫描指定 txGroup 的回查 ZSet，对超时 txId 触发回查。
     *
     * @param txGroup 事务组名
     */
    void scanTimeoutHalf(String txGroup) {
        String checkZSetKey = StreamMQKeys.transactionCheckZSet(namespace, txGroup);
        RScoredSortedSet<String> zset = redisson.getScoredSortedSet(checkZSetKey);
        long now = System.currentTimeMillis();
        Collection<String> timeoutTxIds = zset.valueRange(0, true, now, true, 0, batchSize - 1);
        if (timeoutTxIds.isEmpty()) {
            return;
        }
        for (String txId : timeoutTxIds) {
            try {
                triggerCheck(txId, txGroup);
            } catch (RuntimeException ex) {
                LOG.warn(
                        "triggerCheck failed: txId={}, txGroup={}: {}",
                        txId,
                        txGroup,
                        ex.getMessage(),
                        ex);
            }
        }
    }

    /**
     * 对单个 txId 触发回查。
     *
     * @param txId 事务 ID
     * @param txGroup 事务组名
     */
    void triggerCheck(String txId, String txGroup) {
        TransactionChecker<?> checker = checkerRegistry.get(txGroup);
        String stateHashKey = StreamMQKeys.transactionStateHash(namespace, txGroup);
        RMap<String, String> stateMap = redisson.getMap(stateHashKey);

        String currentState = stateMap.get(txId);
        // 已终态：直接清理
        if (STATE_COMMIT.equals(currentState) || STATE_ROLLBACK.equals(currentState)) {
            removeCheckEntry(txId, txGroup);
            return;
        }
        // 中间状态（其它实例正在提交/回滚）：重新执行，幂等安全
        if (STATE_COMMITTING.equals(currentState)) {
            LOG.debug("Transaction in COMMITTING state, re-executing commit: txId={}", txId);
            doMarkCommit(txId, txGroup);
            return;
        }
        if (STATE_ROLLBACKING.equals(currentState)) {
            LOG.debug("Transaction in ROLLBACKING state, re-executing rollback: txId={}", txId);
            doMarkRollback(txId, txGroup);
            return;
        }
        // 非 PREPARE / UNKNOWN 状态视为异常，强制 ROLLBACK
        if (!STATE_PREPARE.equals(currentState) && !STATE_UNKNOWN.equals(currentState)) {
            LOG.warn("Unexpected tx state, force rollback: txId={}, state={}", txId, currentState);
            doMarkRollback(txId, txGroup);
            return;
        }
        // 无 checker 视为回查失败 → ROLLBACK
        if (Objects.isNull(checker)) {
            LOG.warn(
                    "No TransactionChecker registered for txGroup={}, force rollback: txId={}",
                    txGroup,
                    txId);
            doMarkRollback(txId, txGroup);
            return;
        }

        // 读取半消息用于回查上下文
        String halfIdStr = stateMap.get(txId + FIELD_HALF_ID_SUFFIX);
        String targetTopic = stateMap.get(txId + FIELD_TARGET_SUFFIX);
        Message<?> halfMessage = readHalfMessage(txGroup, halfIdStr, targetTopic);
        if (Objects.isNull(halfMessage)) {
            LOG.warn(
                    "Half message not found in half stream, force rollback: txId={}, halfId={}",
                    txId,
                    halfIdStr);
            doMarkRollback(txId, txGroup);
            return;
        }

        // 调用 checker：per-group 串行 + 专用虚拟线程 + 超时 join。
        // 慢/挂死的 checker 不再阻塞扫描线程（超时按 UNKNOWN 有界重查），同组重复触发被串行化
        TransactionContext ctx =
                new TransactionContext(
                        txId, txGroup, null, System.currentTimeMillis(), new HashMap<>());
        LocalTransactionState state =
                invokeCheckerWithTimeout(checker, halfMessage, ctx, txId, txGroup);

        metricsRecorder.recordCheck(txGroup, state.name());

        // 读取回查次数
        int checkCount = getCheckCount(txId, txGroup);

        switch (state) {
            case COMMIT_MESSAGE -> doMarkCommit(txId, txGroup);
            case ROLLBACK_MESSAGE -> doMarkRollback(txId, txGroup);
            case UNKNOW -> {
                if (checkCount >= maxCheckTimes) {
                    LOG.warn(
                            "Transaction exceeded maxCheckTimes ({}), force rollback: txId={}",
                            maxCheckTimes,
                            txId);
                    doMarkRollback(txId, txGroup);
                } else {
                    // 更新状态（Lua CAS：绝不覆盖其它实例已写入的终态）+ 重新调度 + 递增计数
                    RScript script = redisson.getScript(StringCodec.INSTANCE);
                    script.eval(
                            RScript.Mode.READ_WRITE,
                            LUA_CAS_TO_UNKNOWN,
                            RScript.ReturnType.STATUS,
                            Collections.singletonList(stateHashKey),
                            txId);
                    incrementCheckCount(txId, txGroup);
                    rescheduleCheck(txId, txGroup);
                    LOG.debug(
                            "Transaction check UNKNOWN, rescheduled: txId={}, checkCount={},"
                                    + " nextCheckAt={}",
                            txId,
                            checkCount + 1,
                            System.currentTimeMillis() + checkIntervalMs);
                }
            }
            default -> LOG.warn("Unknown LocalTransactionState: txId={}, state={}", txId, state);
        }
    }

    // ===================== 辅助方法 =====================

    /**
     * 执行回查器：同组串行 + 超时控制。
     *
     * <p>checker 在专用虚拟线程上运行，扫描线程 {@code join(timeout)} 等待：
     *
     * <ul>
     *   <li>正常返回 → 使用返回值（null 视为 UNKNOWN）
     *   <li>抛异常 → 记 WARN，按 UNKNOWN 有界重查（既有语义）
     *   <li>超时/中断 → 放弃等待，按 UNKNOWN 处理并记 WARN；孤儿 checker 线程随其自然结束 （虚拟线程无法强制终止），其返回值被丢弃
     * </ul>
     *
     * <p>同组互斥：{@code synchronized(groupLock)} 串行化同一 txGroup 的回查执行， 防止慢回查期间下一轮扫描对同组并发触发。
     *
     * @return 回查结果状态（绝不返回 null）
     */
    private LocalTransactionState invokeCheckerWithTimeout(
            TransactionChecker<?> checker,
            Message<?> halfMessage,
            TransactionContext ctx,
            String txId,
            String txGroup) {
        Object groupLock = groupCheckLocks.computeIfAbsent(txGroup, k -> new Object());
        long timeoutMillis = checkerTimeoutMillis;
        final java.util.concurrent.atomic.AtomicReference<LocalTransactionState> result =
                new java.util.concurrent.atomic.AtomicReference<>();
        synchronized (groupLock) {
            Thread worker =
                    Thread.ofVirtual()
                            .name("tx-checker-" + txGroup + "-" + txId)
                            .start(
                                    () -> {
                                        try {
                                            @SuppressWarnings({"unchecked", "rawtypes"})
                                            LocalTransactionState s =
                                                    ((TransactionChecker) checker)
                                                            .check(halfMessage, ctx);
                                            result.set(s);
                                        } catch (Exception ex) {
                                            LOG.warn(
                                                    "TransactionChecker threw exception, treated"
                                                            + " as UNKNOW: txId={}: {}",
                                                    txId,
                                                    ex.getMessage(),
                                                    ex);
                                        }
                                    });
            try {
                // join(Duration) 为 void 返回：以 isAlive() 判定是否超时
                worker.join(Duration.ofMillis(timeoutMillis));
            } catch (InterruptedException ex) {
                // 扫描线程被停机中断：放弃等待，恢复中断位后走超时路径
                Thread.currentThread().interrupt();
            }
            if (worker.isAlive()) {
                LOG.warn(
                        "TransactionChecker timed out after {}ms, treated as UNKNOW:"
                                + " txId={}, txGroup={}",
                        timeoutMillis,
                        txId,
                        txGroup);
                return LocalTransactionState.UNKNOW;
            }
        }
        LocalTransactionState state = result.get();
        return Objects.nonNull(state) ? state : LocalTransactionState.UNKNOW;
    }

    /** 将事务降级为 UNKNOWN 并重新调度回查（元数据丢失时的有界兜底路径）。 */
    private void degradeToUnknown(
            String stateHashKey, RMap<String, String> stateMap, String txId, String txGroup) {
        RScript script = redisson.getScript(StringCodec.INSTANCE);
        script.eval(
                RScript.Mode.READ_WRITE,
                LUA_CAS_TO_UNKNOWN,
                RScript.ReturnType.STATUS,
                Collections.singletonList(stateHashKey),
                txId);
        int checkCount = getCheckCount(txId, txGroup);
        if (checkCount < maxCheckTimes) {
            incrementCheckCount(txId, txGroup);
            rescheduleCheck(txId, txGroup);
        } else {
            LOG.error(
                    "Degrade-to-UNKNOWN exceeded maxCheckTimes, force rollback: txId={},"
                            + " txGroup={}",
                    txId,
                    txGroup);
            doMarkRollback(txId, txGroup);
        }
    }

    /** 将 txId 重新加入回查 ZSet（score = now + checkInterval），用于失败重试。 */
    private void rescheduleCheck(String txId, String txGroup) {
        long nextCheckAt = System.currentTimeMillis() + checkIntervalMs;
        redisson.getScoredSortedSet(StreamMQKeys.transactionCheckZSet(namespace, txGroup))
                .add(nextCheckAt, txId);
    }

    /** 从 half Stream 读取单条半消息用于回查上下文。 */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Message<?> readHalfMessage(String txGroup, String halfIdStr, String targetTopic) {
        if (Objects.isNull(halfIdStr)) {
            return null;
        }
        String halfStreamKey = StreamMQKeys.halfStream(namespace, txGroup);
        RStream<String, String> halfStream = redisson.getStream(halfStreamKey);
        StreamMessageId halfId = parseStreamId(halfIdStr);
        Map<StreamMessageId, Map<String, String>> entries = halfStream.range(1, halfId, halfId);
        if (CollectionUtils.isEmpty(entries)) {
            return null;
        }
        Map.Entry<StreamMessageId, Map<String, String>> entry =
                entries.entrySet().iterator().next();
        Map<String, String> fields = entry.getValue();

        // 解析 bodyType 反序列化 body
        String bodyTypeName = fields.get(DefaultMessageConverter.FIELD_BODY_TYPE);
        Class<?> bodyType = Object.class;
        if (StringUtils.isNotEmpty(bodyTypeName)) {
            bodyType = CLASS_CACHE.get(bodyTypeName);
            if (Objects.isNull(bodyType)) {
                try {
                    bodyType =
                            Class.forName(
                                    bodyTypeName,
                                    false,
                                    Thread.currentThread().getContextClassLoader());
                    CLASS_CACHE.put(bodyTypeName, bodyType);
                } catch (ClassNotFoundException ex) {
                    LOG.warn(
                            "Body type class not found in transaction scanner, fallback to Object:"
                                    + " {}",
                            bodyTypeName);
                    bodyType = Object.class;
                }
            }
        }
        return messageConverter
                .fromStreamFields(fields, (Class) bodyType, targetTopic)
                .withMessageId(MessageId.fromStreamEntry(entry.getKey().toString()));
    }

    /** 从 txcheck ZSet 移除 txId。 */
    private void removeCheckEntry(String txId, String txGroup) {
        String checkZSetKey = StreamMQKeys.transactionCheckZSet(namespace, txGroup);
        redisson.getScoredSortedSet(checkZSetKey).remove(txId);
        // 同时清理回查计数
        String counterKey = StreamMQKeys.transactionCheckCounter(namespace, txGroup);
        redisson.getMap(counterKey).remove(txId);
    }

    /** 清理 txstate Hash 中 .target / .halfId 等辅助字段（保留主状态字段以便查询）。 */
    private void cleanupTerminalState(RMap<String, String> stateMap, String txId) {
        stateMap.remove(txId + FIELD_TARGET_SUFFIX);
        stateMap.remove(txId + FIELD_HALF_ID_SUFFIX);
    }

    /** 写入终态时间戳（供保留期维护任务判定清理时机）。 */
    private void markTerminalDone(RMap<String, String> stateMap, String txId) {
        try {
            stateMap.put(
                    txId + StreamMQConstants.TX_FIELD_DONE_SUFFIX,
                    Long.toString(System.currentTimeMillis()));
        } catch (RuntimeException ex) {
            LOG.debug(
                    "Mark terminal done failed (retention sweep will miss this entry): txId={}",
                    txId,
                    ex);
        }
    }

    /** 获取 txId 的回查次数。 */
    private int getCheckCount(String txId, String txGroup) {
        String counterKey = StreamMQKeys.transactionCheckCounter(namespace, txGroup);
        String countStr = redisson.<String, String>getMap(counterKey).get(txId);
        if (StringUtils.isEmpty(countStr)) {
            return 0;
        }
        try {
            return Integer.parseInt(countStr);
        } catch (NumberFormatException ex) {
            return 0;
        }
    }

    /** 原子递增 txId 的回查次数（Lua HINCRBY）。 */
    private void incrementCheckCount(String txId, String txGroup) {
        String counterKey = StreamMQKeys.transactionCheckCounter(namespace, txGroup);
        RScript script = redisson.getScript(StringCodec.INSTANCE);
        script.eval(
                RScript.Mode.READ_WRITE,
                LUA_INCR_COUNT,
                RScript.ReturnType.INTEGER,
                Collections.singletonList(counterKey),
                txId);
    }

    /**
     * 解析 StreamMessageId 字符串为 {@link StreamMessageId} 对象。
     *
     * @param halfIdStr 形如 {@code 1234567890-0}
     * @return {@link StreamMessageId}
     */
    private static StreamMessageId parseStreamId(String halfIdStr) {
        int dashIdx = halfIdStr.indexOf('-');
        if (dashIdx < 0) {
            throw new IllegalArgumentException("Invalid stream message id format: " + halfIdStr);
        }
        long timestamp = Long.parseLong(halfIdStr.substring(0, dashIdx));
        long sequence = Long.parseLong(halfIdStr.substring(dashIdx + 1));
        return new StreamMessageId(timestamp, sequence);
    }
}
