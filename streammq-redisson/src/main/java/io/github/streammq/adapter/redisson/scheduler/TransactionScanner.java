/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.support.PayloadTypeSafety;
import io.github.streammq.adapter.redisson.support.RedisClusterCompatibility;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
 * 事务回查调度器（编排层）：周期扫描事务回查 ZSet，对超时的半消息触发 {@link TransactionChecker#check}， 按返回状态决定 COMMIT / ROLLBACK
 * / UNKNOWN（继续等待或超限强制 ROLLBACK）。
 *
 * <h2>职责拆分</h2>
 *
 * <p>本类为编排层，仅保留生命周期、注册表、扫描循环与状态机； 下列职责已委托给独立协作类：
 *
 * <ul>
 *   <li>{@link TransactionCommitExecutor} - 半消息→目标 Stream 原子转投（单 Lua 脚本，天然去重，无需执行权锁）
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
 *   <li>若 UNKNOWN 或超时未终结，{@link #start} 启动的周期任务扫描并触发回查
 * </ol>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class TransactionScanner implements StreamMQScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionScanner.class);

    /** Class.forName 缓存上限：bodyTypeName 来自外部可控的流字段，缓存必须有界，防止无界增长 */
    private static final int CLASS_CACHE_MAX_SIZE = 256;

    /** Class.forName 缓存，避免重复类加载查找（有界 FIFO，超出上限淘汰最早写入项） */
    private static final Map<String, Class<?>> CLASS_CACHE =
            new LinkedHashMap<String, Class<?>>(16, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Class<?>> eldest) {
                    return size() > CLASS_CACHE_MAX_SIZE;
                }
            };

    private static final java.util.concurrent.locks.ReadWriteLock CLASS_CACHE_LOCK =
            new java.util.concurrent.locks.ReentrantReadWriteLock();

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
            "local current = redis.call('HGET', KEYS[1], ARGV[1]);if current == false then return"
                + " 'MISSING'; end;if current == 'COMMIT' or current == 'ROLLBACK' then return"
                + " current; end;if current == 'COMMITTING' or current == 'ROLLBACKING' then return"
                + " current; end;if current ~= 'PREPARE' and current ~= 'UNKNOWN' then return"
                + " current; end;redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]);return current;";

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

    /** txstate Hash 中强制终结原因字段后缀（有界重试耗尽） */
    private static final String FIELD_FAILURE_REASON_SUFFIX =
            StreamMQConstants.TX_FIELD_FAILURE_REASON_SUFFIX;

    /** 关闭调度线程池时的等待超时（秒） */
    private static final long AWAIT_TERMINATION_SECONDS =
            StreamMQConstants.DEFAULT_AWAIT_TERMINATION_SECONDS;

    /** Lua：原子递增回查计数（HINCRBY） */
    private static final String LUA_INCR_COUNT =
            "local val = redis.call('HINCRBY', KEYS[1], ARGV[1], 1);" + "return val;";

    /**
     * Lua：强制终结的<b>状态 CAS</b>——仅当状态仍等于期望的中间态（COMMITTING / ROLLBACKING）时才写终态。
     *
     * <p><b>为什么必须 CAS（R2-1）：</b>强制终结读取状态（是否卡在 COMMITTING）与写终态是两次独立的 Redis 往返。若在两者之间并发实例完成了转投并置位
     * COMMIT，无条件的 {@code HSET ROLLBACK} 会把「消息已投递」改成「回滚」，状态与真实投递永久不一致。这里把校验与写入放进同一脚本，状态不是期望中间态就
     * 原样返回、绝不改写（同时保证 {@code .failureReason} 与终态同生同死）。
     *
     * <p>KEYS[1] = txstate Hash key；ARGV[1]=txId, ARGV[2]=期望中间态, ARGV[3]=原因字段, ARGV[4]=原因值,
     * ARGV[5]=终态值。返回 {@code 'OK'}（已终结）或 {@code 'STATE=<当前值>'} / {@code 'STATE=absent'}（未改写）。
     */
    private static final String LUA_CAS_FINALIZE_STUCK =
            "local current = redis.call('HGET', KEYS[1], ARGV[1]);"
                    + "if current ~= ARGV[2] then"
                    + " return current and ('STATE=' .. current) or 'STATE=absent';"
                    + " end;"
                    + "redis.call('HSET', KEYS[1], ARGV[3], ARGV[4]);"
                    + "redis.call('HSET', KEYS[1], ARGV[1], ARGV[5]);"
                    + "return 'OK';";

    /** 孤儿回查线程日志限频间隔（毫秒）：避免每个扫描周期刷屏 */
    private static final long ORPHAN_CHECKER_WARN_INTERVAL_MS = 60_000L;

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

    /**
     * 超时未结束的回查线程登记表（R2-3）：key=txGroup+txId。
     *
     * <p>此前 {@code worker.join(timeout)} 超时后仅记 WARN，孤儿的虚拟线程仍在运行，而下一轮扫描会为同一 txId
     * 再次启动一个回查线程——并发回查线程数随轮数增长（慢 checker 下无界）。现在超时后登记租约：线程存活期间跳过对 同一事务的重查（本轮按 UNKNOWN
     * 有界重查），线程结束时自行摘除租约，恢复可重查。
     */
    private final ConcurrentMap<CheckerKey, OrphanCheckerLease> orphanCheckers =
            new ConcurrentHashMap<>();

    /** 孤儿回查线程累计数（诊断用：只增计数的累计量，当前存活数见 {@link #getOrphanCheckerCount()}） */
    private final AtomicLong orphanCheckerTotal = new AtomicLong();

    private final AtomicLong lastOrphanCheckerWarnMs = new AtomicLong();

    /** 默认事务组名（来自配置 `streammq.transaction.default-group`），当 checkerRegistry 为空时兜底扫描 */
    private final String defaultGroup;

    /** 协作类：原子转投、保留期清理、指标记录 */
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

    /** 终态字段保留期（毫秒），超过后由维护任务清理 */
    private volatile long txStateRetentionMs = DEFAULT_TX_STATE_RETENTION_MS;

    /** 孤儿半消息保留期（毫秒），超过且无状态引用的 half 条目由维护任务清理 */
    private volatile long orphanHalfRetentionMs = DEFAULT_ORPHAN_HALF_RETENTION_MS;

    /**
     * 设置 txstate 终态字段保留期（毫秒），并转发给保留期清理协作类（否则该配置对实际清理不生效）。
     *
     * @param millis 保留期毫秒数
     */
    public void setTxStateRetentionMs(long millis) {
        this.txStateRetentionMs = millis;
        this.retentionSweeper.setTxStateRetentionMs(millis);
    }

    /** 设置孤儿半消息保留期（毫秒），并转发给保留期清理协作类。 */
    public void setOrphanHalfRetentionMs(long millis) {
        this.orphanHalfRetentionMs = millis;
        this.retentionSweeper.setOrphanHalfRetentionMs(millis);
    }

    /** 返回 txstate 终态字段保留期（毫秒）。 */
    public long getTxStateRetentionMs() {
        return txStateRetentionMs;
    }

    /** 返回孤儿半消息保留期（毫秒）。 */
    public long getOrphanHalfRetentionMs() {
        return orphanHalfRetentionMs;
    }

    /**
     * 设置单轮保留期清理的最大条目数（R2-2②：默认 {@link TransactionRetentionSweeper#DEFAULT_SWEEP_BATCH_SIZE}）。
     *
     * @param size 批量上限，必须 &gt; 0
     */
    public void setRetentionSweepBatchSize(int size) {
        this.retentionSweeper.setSweepBatchSize(size);
    }

    /** 返回单轮保留期清理的最大条目数。 */
    public int getRetentionSweepBatchSize() {
        return retentionSweeper.getSweepBatchSize();
    }

    /** 回查器执行超时（毫秒）；恒 > 0（见 {@link #setCheckerTimeoutMillis(long)}）。 */
    private volatile long checkerTimeoutMillis = DEFAULT_CHECKER_TIMEOUT_MILLIS;

    /**
     * 设置回查器执行超时。
     *
     * <p><b>为什么必须拒绝非正值（发布前红队审查 R5）：</b>等待实现为 {@code worker.join(Duration.ofMillis(t))}， 而 {@code
     * join(0)} 的语义是<b>无限等待</b>——配置为 0 时，一个挂死的 {@code TransactionChecker} 会让扫描 线程永久持有该事务组的 {@code
     * groupLock}，整个事务回查调度停摆（且无超时日志、无自愈路径）。
     *
     * @param millis 超时毫秒数，必须 > 0
     * @throws IllegalArgumentException 取值 <= 0
     */
    public void setCheckerTimeoutMillis(long millis) {
        if (millis <= 0) {
            throw new IllegalArgumentException(
                    "checkerTimeoutMillis must be > 0 (join(0) means wait forever), got: "
                            + millis);
        }
        this.checkerTimeoutMillis = millis;
    }

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
                DEFAULT_BATCH_SIZE,
                null);
    }

    /**
     * 六参构造（向后兼容：无 defaultGroup 兜底扫描）。
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
        this(
                redisson,
                namespace,
                messageConverter,
                checkIntervalMs,
                maxCheckTimes,
                batchSize,
                null);
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
     * @param defaultGroup 默认事务组名（未注册 checker 时兜底扫描），可为 null
     */
    public TransactionScanner(
            RedissonClient redisson,
            String namespace,
            MessageConverter messageConverter,
            long checkIntervalMs,
            int maxCheckTimes,
            int batchSize,
            String defaultGroup) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = Objects.isNull(namespace) ? "" : namespace;
        this.messageConverter = Objects.requireNonNull(messageConverter, "messageConverter");
        this.checkIntervalMs = checkIntervalMs > 0 ? checkIntervalMs : DEFAULT_CHECK_INTERVAL_MS;
        this.maxCheckTimes = maxCheckTimes > 0 ? maxCheckTimes : DEFAULT_MAX_CHECK_TIMES;
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.defaultGroup = defaultGroup;
        this.scanExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, StreamMQConstants.THREAD_TXCHECK_SCHEDULER);
                            t.setDaemon(true);
                            return t;
                        });
        this.commitExecutor = new TransactionCommitExecutor(redisson, namespace);
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
        // 状态 Hash + 回查 ZSet 是跨 key 原子批（Cluster 下按节点拆分提交），在写半消息之前拒绝，
        // 避免留下需要补偿的中间态
        RedisClusterCompatibility.requireCrossKeyAtomicity(
                redisson, "Transaction prepare metadata (state hash + check ZSet)");

        // 写入顺序（崩溃安全性分析，顺序不可调整）：
        //  1. 先 XADD 半消息到 half Stream —— 若在此步失败，事务尚未注册，无任何副作用；
        //  2. 再原子写入 txstate(PREPARE) + target + halfId + 回查 ZSet —— 若在此步后崩溃，
        //     回查发现半消息存在，正常触发 check；若半消息读取失败，走"force rollback"安全终止。
        // 顺序不可颠倒：若先写状态再 XADD，崩溃后可能出现 PREPARE 状态但无 half 消息的幽灵事务。
        long firstCheckAt = System.currentTimeMillis() + checkIntervalMs;
        String stateHashKey = StreamMQKeys.transactionStateHash(namespace, txGroup);

        // XADD 到 half Stream
        String halfStreamKey = StreamMQKeys.halfStream(namespace, txGroup);
        RStream<String, String> halfStream =
                redisson.getStream(halfStreamKey, StringCodec.INSTANCE);
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
                RMap<String, String> stateMap = redisson.getMap(stateHashKey, StringCodec.INSTANCE);
                stateMap.remove(txId);
                stateMap.remove(txId + FIELD_TARGET_SUFFIX);
                stateMap.remove(txId + FIELD_HALF_ID_SUFFIX);
                redisson.getScoredSortedSet(
                                StreamMQKeys.transactionCheckZSet(namespace, txGroup),
                                StringCodec.INSTANCE)
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
        try {
            RBatch metadataBatch =
                    redisson.createBatch(
                            BatchOptions.defaults()
                                    .executionMode(BatchOptions.ExecutionMode.REDIS_WRITE_ATOMIC));
            RMapAsync<String, String> metadataMap =
                    metadataBatch.getMap(stateHashKey, StringCodec.INSTANCE);
            metadataMap.putAsync(txId + FIELD_HALF_ID_SUFFIX, halfId.toString());
            metadataMap.putAsync(txId + FIELD_TARGET_SUFFIX, targetTopic);
            metadataMap.putAsync(txId, STATE_PREPARE);
            metadataBatch
                    .getScoredSortedSet(
                            StreamMQKeys.transactionCheckZSet(namespace, txGroup),
                            StringCodec.INSTANCE)
                    .addAsync(firstCheckAt, txId);
            metadataBatch.execute();
        } catch (RuntimeException ex) {
            LOG.error(
                    "Failed to publish transaction metadata, cleaning up half message: txId={}",
                    txId,
                    ex);
            try {
                halfStream.remove(halfId);
                RMap<String, String> stateMap = redisson.getMap(stateHashKey, StringCodec.INSTANCE);
                stateMap.remove(txId);
                stateMap.remove(txId + FIELD_TARGET_SUFFIX);
                stateMap.remove(txId + FIELD_HALF_ID_SUFFIX);
                redisson.getScoredSortedSet(
                                StreamMQKeys.transactionCheckZSet(namespace, txGroup),
                                StringCodec.INSTANCE)
                        .remove(txId);
            } catch (RuntimeException cleanupEx) {
                LOG.error("Failed to clean up transaction registration: txId={}", txId, cleanupEx);
            }
            throw ex;
        }

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
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.warn("TransactionScanner already started");
            return;
        }
        ensureScanExecutorAlive();
        scanFuture =
                scanExecutor.scheduleAtFixedRate(
                        () -> {
                            try {
                                scanAllGroups();
                            } catch (Throwable t) {
                                LOG.error("TransactionScanner.scanAllGroups failed fatally", t);
                            }
                        },
                        0,
                        checkIntervalMs,
                        TimeUnit.MILLISECONDS);
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
        scanExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, StreamMQConstants.THREAD_TXCHECK_SCHEDULER);
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
        RMap<String, String> stateMap = redisson.getMap(stateHashKey, StringCodec.INSTANCE);

        // 原子抢占：PREPARE/UNKNOWN → COMMITTING
        String oldState = casState(stateHashKey, txId, STATE_COMMITTING);
        if (STATE_COMMIT.equals(oldState)
                || STATE_ROLLBACK.equals(oldState)
                || STATE_ROLLBACKING.equals(oldState)) {
            LOG.debug(
                    "markCommit ignored, transaction already terminal or in flight: txId={},"
                            + " state={}",
                    txId,
                    oldState);
            return;
        }
        if ("MISSING".equals(oldState)) {
            // 状态字段缺失（txId 从未注册 / 注册期 Crash 于 XADD 半消息与元数据写入之间）：
            // 旧实现按"已终态"静默返回，commit 请求被吞掉——半消息既不投递也不清理。
            // 这里显式降级为 UNKNOWN 走有界回查（计数消耗回查预算）：元数据仍在则正常提交；
            // 元数据确实丢失时，回查路径以 ROLLBACK 明确终结并 ERROR 告警（孤儿半消息由保留期
            // 维护任务清理），保证半消息最终"要么投递、要么明确失败"。
            LOG.error(
                    "markCommit on missing txstate entry (never registered or metadata lost),"
                            + " degrading to UNKNOWN for bounded recheck: txId={}, txGroup={}",
                    txId,
                    txGroup);
            degradeToUnknown(stateHashKey, stateMap, txId, txGroup);
            return;
        }
        // COMMITTING/ROLLBACKING 表示其它实例正在处理。转投由单 Lua 脚本原子完成且天然去重
        // （先执行者已 XDEL 半消息，后执行者读到 HALF_MISSING），因此无需分布式锁，也不会重复转投。

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

        // 转投半消息到目标 Stream + 原子标记 COMMIT（单 Lua 脚本，详见 TransactionCommitExecutor）
        TransactionCommitExecutor.Outcome outcome =
                commitExecutor.publishHalfAndMarkCommit(txGroup, halfIdStr, targetTopic, txId);
        switch (outcome) {
            case PUBLISHED, ALREADY_COMMIT -> {
                /* 终态已由脚本（或并发实例）写入 */
            }
            case ABORTED_TERMINAL -> {
                // R2-1：状态已被并发路径改写（强制终结 ROLLBACK / 回滚中 / 回退态），脚本未投递。
                // 此处绝不覆盖终态：终态已置时仅补齐收尾标记；非终态交由有界回查继续推进。
                handleAbortedCommit(txId, txGroup, stateMap);
                return;
            }
            case HALF_MISSING -> {
                // 半消息不存在：可能已被其它实例的转投脚本转投（此时状态已被置为 COMMIT，degrade 不会覆盖），
                // 也可能是注册期 XADD 失败遗留的孤儿元数据。降级为 UNKNOWN 走有界回查：
                // 若为暂时性读取异常则下轮恢复；连续失败超过 maxCheckTimes 后由 force-rollback 安全终结
                // （转投与状态置位在同一原子脚本中，状态非 COMMIT 即未投递）。
                LOG.error(
                        "Half message missing at commit time (either already published by a"
                                + " concurrent instance, or never written), degrading to bounded"
                                + " recheck: txId={}, txGroup={}, halfId={}",
                        txId,
                        txGroup,
                        halfIdStr);
                degradeToUnknown(stateHashKey, stateMap, txId, txGroup);
                return;
            }
        }

        // 终态收尾
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
     * 提交被状态 CAS 拒绝后的收尾（R2-1）：<b>绝不改写状态</b>。
     *
     * <p>终态（COMMIT/ROLLBACK）说明事务已由其它路径终结：仅补齐 {@code .done} 标记与回查条目清理，避免「脚本已置终态
     * 但实例在收尾前崩溃」的字段永不被保留期清理。中间态/回退态（ROLLBACKING/PREPARE/UNKNOWN）不做任何处置，交由 扫描周期按既有状态机推进（强制终结 /
     * 有界重查），避免把正在推进的事务改写成 UNKNOWN。
     *
     * @param txId 事务 ID
     * @param txGroup 事务组名
     * @param stateMap txstate Hash 视图
     */
    private void handleAbortedCommit(String txId, String txGroup, RMap<String, String> stateMap) {
        String state = stateMap.get(txId);
        if (STATE_COMMIT.equals(state) || STATE_ROLLBACK.equals(state)) {
            LOG.warn(
                    "Commit aborted by state CAS: transaction already terminal ({}), half message"
                            + " NOT published by this instance: txId={}, txGroup={}",
                    state,
                    txId,
                    txGroup);
            markTerminalDone(stateMap, txId);
            removeCheckEntry(txId, txGroup);
            return;
        }
        LOG.warn(
                "Commit aborted by state CAS: state is {} (not COMMITTING), leaving it to the"
                        + " bounded recheck path: txId={}, txGroup={}",
                state,
                txId,
                txGroup);
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
        RMap<String, String> stateMap = redisson.getMap(stateHashKey, StringCodec.INSTANCE);

        // 原子抢占：PREPARE/UNKNOWN → ROLLBACKING
        String oldState = casState(stateHashKey, txId, STATE_ROLLBACKING);
        if (STATE_COMMIT.equals(oldState)
                || STATE_ROLLBACK.equals(oldState)
                || STATE_COMMITTING.equals(oldState)
                || "MISSING".equals(oldState)) {
            LOG.debug(
                    "markRollback ignored, transaction already terminal: txId={}, state={}",
                    txId,
                    oldState);
            return;
        }

        // 回滚的 XDEL 与另一实例的转投由状态机串行化：COMMITTING 时回滚直接忽略（见上方 CAS），
        // 且 XDEL 幂等，因此无需分布式锁。XDEL 失败时保持 ROLLBACKING 并重新调度重试。

        String halfIdStr = stateMap.get(txId + FIELD_HALF_ID_SUFFIX);
        boolean halfRemoved = true;
        if (Objects.nonNull(halfIdStr)) {
            // XDEL 半消息；失败时不得终结事务（否则半消息永久残留），保持 ROLLBACKING 并重新调度重试
            String halfStreamKey = StreamMQKeys.halfStream(namespace, txGroup);
            RStream<String, String> halfStream =
                    redisson.getStream(halfStreamKey, StringCodec.INSTANCE);
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
                rescheduleCheck(txId, txGroup);
                return;
            }
        }

        stateMap.put(txId, STATE_ROLLBACK);
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

    /** 扫描所有已注册 checker 的 txGroup，以及兜底扫描 defaultGroup。 */
    private void scanAllGroups() {
        for (String txGroup : checkerRegistry.keySet()) {
            try {
                scanTimeoutHalf(txGroup);
            } catch (RuntimeException ex) {
                LOG.warn("scanTimeoutHalf failed for txGroup={}: {}", txGroup, ex.getMessage(), ex);
            }
        }
        // 兜底扫描 defaultGroup：即使无 checker 注册，也检查默认事务组中是否存在超时半消息。
        // 典型场景：用户仅通过 @StreamMQTransactionConsumer 注册自定义事务组，使用模板
        // executeInTransaction 采用了默认配置组（streammq.transaction.default-group），
        // 此时 checkerRegistry 不包含 defaultGroup → 半消息永不被回查 → 永久悬挂。
        if (StringUtils.isNotEmpty(defaultGroup) && !checkerRegistry.containsKey(defaultGroup)) {
            LOG.warn(
                    "Default txGroup '{}' has no registered checker; scanning it with auto-rollback"
                        + " fallback (configure streammq.transaction.default-group or register a"
                        + " @StreamMQTransactionConsumer bean for this group to provide a custom"
                        + " TransactionChecker)",
                    defaultGroup);
            try {
                scanTimeoutHalf(defaultGroup);
            } catch (RuntimeException ex) {
                LOG.warn(
                        "scanTimeoutHalf failed for default txGroup={}: {}",
                        defaultGroup,
                        ex.getMessage(),
                        ex);
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
        RScoredSortedSet<String> zset =
                redisson.getScoredSortedSet(checkZSetKey, StringCodec.INSTANCE);
        long now = System.currentTimeMillis();
        // 限界语义为 (offset, count)：count 必须是 batchSize，不能是 batchSize - 1。
        // batchSize == 1 时 count=0 会让 LIMIT 退化为 0，本方法永远扫不到任何超时半消息——
        // 事务回查彻底失效、半消息永久悬挂（且无任何错误信号，只在配置为 1 时命中）。
        Collection<String> timeoutTxIds = zset.valueRange(0, true, now, true, 0, batchSize);
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
        RMap<String, String> stateMap = redisson.getMap(stateHashKey, StringCodec.INSTANCE);

        String currentState = stateMap.get(txId);
        // 已终态：直接清理。
        // R2-2①：必须同时补齐 .done 标记——否则「终态脚本已执行、实例在收尾（markTerminalDone）前崩溃」
        // 的字段永远不会被保留期清理（sweep 只认 .done），txstate Hash 出现无法回收的常驻字段。
        if (STATE_COMMIT.equals(currentState) || STATE_ROLLBACK.equals(currentState)) {
            markTerminalDone(stateMap, txId);
            removeCheckEntry(txId, txGroup);
            return;
        }
        // 中间状态（其它实例正在提交/回滚）：重新执行，幂等安全。
        // 有界重试：持续失败（目标 key 类型冲突 / ACL 拒绝 / 长期不可用）不得无限重放——
        // 复用回查计数作为「恢复尝试」预算，耗尽后按「未发布」终结并 ERROR 告警。
        if (STATE_COMMITTING.equals(currentState)) {
            int attempts = getCheckCount(txId, txGroup) + 1;
            if (attempts > maxCheckTimes) {
                forceFinalizeStuckCommit(txId, txGroup, stateMap, attempts);
                return;
            }
            incrementCheckCount(txId, txGroup);
            LOG.debug(
                    "Transaction in COMMITTING state, re-executing commit: txId={}, attempt={}",
                    txId,
                    attempts);
            doMarkCommit(txId, txGroup);
            return;
        }
        if (STATE_ROLLBACKING.equals(currentState)) {
            int attempts = getCheckCount(txId, txGroup) + 1;
            if (attempts > maxCheckTimes) {
                forceFinalizeStuckRollback(txId, txGroup, stateMap, attempts);
                return;
            }
            incrementCheckCount(txId, txGroup);
            LOG.debug(
                    "Transaction in ROLLBACKING state, re-executing rollback: txId={}, attempt={}",
                    txId,
                    attempts);
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
        // null 视为 UNKNOWN：checker 返回 null 与"状态未知"是同一语义，都走有界重查后强制回滚。
        if (state == null) {
            state = LocalTransactionState.UNKNOWN;
        }

        metricsRecorder.recordCheck(txGroup, state.name());

        // 读取回查次数
        int checkCount = getCheckCount(txId, txGroup);

        switch (state) {
            case COMMIT_MESSAGE -> doMarkCommit(txId, txGroup);
            case ROLLBACK_MESSAGE -> doMarkRollback(txId, txGroup);
            case LocalTransactionState.UNKNOWN -> {
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
     * <p><b>孤儿回查的有界性（R2-3）：</b>超时后虚拟线程无法强制终止，其返回值被丢弃。此前实现每轮都会为同一 txId 再起一个回查线程，慢 checker
     * 下并发线程数随扫描轮数无界增长。现在超时未结束的线程登记租约（txGroup+txId → 线程 + 开始时间）：租约存活期间对同一事务不再重查（本轮直接按 UNKNOWN
     * 有界重查），线程结束时自行摘除租约；同时暴露 {@link #getOrphanCheckerCount()} 供诊断，孤儿告警按 60s 限频。
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
        // 兜底夹取：即便 setter 被绕过（反射/反序列化），也不允许 join(0) 的无限等待语义
        long timeoutMillis = Math.max(1L, checkerTimeoutMillis);
        final java.util.concurrent.atomic.AtomicReference<LocalTransactionState> result =
                new java.util.concurrent.atomic.AtomicReference<>();
        CheckerKey checkerKey = new CheckerKey(txGroup, txId);
        if (orphanCheckers.containsKey(checkerKey)) {
            // 上一个回查线程仍存活（超时未结束）：不得再起新线程，否则并发回查数随轮数增长
            warnOrphanCheckerIfDue(txId, txGroup);
            return LocalTransactionState.UNKNOWN;
        }
        synchronized (groupLock) {
            // 先登记租约再启动线程：若线程在登记前就结束，其结束钩子不会遗留失效登记
            OrphanCheckerLease lease = new OrphanCheckerLease(System.currentTimeMillis());
            orphanCheckers.put(checkerKey, lease);
            orphanCheckerTotal.incrementAndGet();
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
                                                            + " as UNKNOWN: txId={}: {}",
                                                    txId,
                                                    ex.getMessage(),
                                                    ex);
                                        } finally {
                                            // 线程结束即摘除租约（CAS 语义：只摘自己的租约）
                                            orphanCheckers.remove(checkerKey, lease);
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
                        "TransactionChecker timed out after {}ms, treated as UNKNOWN:"
                                + " txId={}, txGroup={}",
                        timeoutMillis,
                        txId,
                        txGroup);
                // R2-3：线程仍存活 → 保留租约，后续轮次跳过同一事务的重查（租约在线程结束时自摘）
                warnOrphanCheckerIfDue(txId, txGroup);
                return LocalTransactionState.UNKNOWN;
            }
            // 正常结束：确保租约不残留（线程可能在上面的 finally 之前就被观测为已结束）
            orphanCheckers.remove(checkerKey, lease);
        }
        LocalTransactionState state = result.get();
        return Objects.nonNull(state) ? state : LocalTransactionState.UNKNOWN;
    }

    /**
     * 返回当前存活（超时未结束）的孤儿回查线程数（R2-3 诊断指标）。
     *
     * @return 孤儿回查线程数
     */
    public int getOrphanCheckerCount() {
        return orphanCheckers.size();
    }

    /**
     * 返回累计产生的孤儿回查线程数（只增计数，与 {@link #getOrphanCheckerCount()} 的当前存活数配合诊断）。
     *
     * @return 累计孤儿回查线程数
     */
    public long getOrphanCheckerTotal() {
        return orphanCheckerTotal.get();
    }

    /** 孤儿回查线程存活时的限频 WARN（默认 60s 一次，避免每轮扫描刷屏）。 */
    private void warnOrphanCheckerIfDue(String txId, String txGroup) {
        int alive = orphanCheckers.size();
        if (alive <= 0) {
            return;
        }
        long now = System.currentTimeMillis();
        long last = lastOrphanCheckerWarnMs.get();
        if (now - last >= ORPHAN_CHECKER_WARN_INTERVAL_MS
                && lastOrphanCheckerWarnMs.compareAndSet(last, now)) {
            LOG.warn(
                    "TransactionChecker still running past timeout, recheck skipped for the same"
                            + " transaction until it finishes: aliveOrphans={}, totalOrphans={},"
                            + " txId={}, txGroup={}",
                    alive,
                    orphanCheckerTotal.get(),
                    txId,
                    txGroup);
        }
    }

    /** 孤儿回查线程的租约：仅承载「身份 + 开始时间」——身份用于 {@code remove(key, lease)} 安全摘除（默认引用相等）。 */
    private static final class OrphanCheckerLease {
        private final long startedAtMs;

        private OrphanCheckerLease(long startedAtMs) {
            this.startedAtMs = startedAtMs;
        }

        @Override
        public String toString() {
            return "startedAtMs=" + startedAtMs;
        }
    }

    /** 孤儿回查线程的登记键（txGroup + txId，避免跨事务组同 txId 相互抑制）。 */
    private record CheckerKey(String txGroup, String txId) {}

    /** 将事务降级为 UNKNOWN 并重新调度回查（元数据丢失时的有界兜底路径）。 */
    private void degradeToUnknown(
            String stateHashKey, RMap<String, String> stateMap, String txId, String txGroup) {
        RScript script = redisson.getScript(StringCodec.INSTANCE);
        String current =
                script.eval(
                        RScript.Mode.READ_WRITE,
                        LUA_CAS_TO_UNKNOWN,
                        RScript.ReturnType.STATUS,
                        Collections.singletonList(stateHashKey),
                        txId);
        // 状态已被其它实例终态化（如并发转投脚本已置 COMMIT）：本轮无需计数或重新调度，
        // 直接清理回查条目，避免对已终结事务的无谓重试与计数污染。
        if (STATE_COMMIT.equals(current) || STATE_ROLLBACK.equals(current)) {
            LOG.debug(
                    "Degrade skipped, transaction already terminal: txId={}, state={}",
                    txId,
                    current);
            // R2-2①：终态收尾标记一并补齐（终态脚本执行后、markTerminalDone 前崩溃的字段才能被清理）
            markTerminalDone(stateMap, txId);
            removeCheckEntry(txId, txGroup);
            return;
        }
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
        redisson.getScoredSortedSet(
                        StreamMQKeys.transactionCheckZSet(namespace, txGroup), StringCodec.INSTANCE)
                .add(nextCheckAt, txId);
    }

    /** 从 half Stream 读取单条半消息用于回查上下文。 */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Message<?> readHalfMessage(String txGroup, String halfIdStr, String targetTopic) {
        if (Objects.isNull(halfIdStr)) {
            return null;
        }
        String halfStreamKey = StreamMQKeys.halfStream(namespace, txGroup);
        RStream<String, String> halfStream =
                redisson.getStream(halfStreamKey, StringCodec.INSTANCE);
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
        // 安全护栏：半消息载荷可被写入方控制，载荷驱动的类型解析需拒绝 JDK/框架危险命名空间
        // （与消费回退链共用 PayloadTypeSafety 同一策略，见其类注释）
        if (StringUtils.isNotEmpty(bodyTypeName) && PayloadTypeSafety.isBlocked(bodyTypeName)) {
            LOG.warn(
                    "Payload-driven transaction body type blocked by safety guard (dangerous"
                            + " namespace): {}, fallback to Object",
                    bodyTypeName);
        } else if (StringUtils.isNotEmpty(bodyTypeName)) {
            CLASS_CACHE_LOCK.readLock().lock();
            try {
                bodyType = CLASS_CACHE.get(bodyTypeName);
            } finally {
                CLASS_CACHE_LOCK.readLock().unlock();
            }
            if (Objects.isNull(bodyType)) {
                try {
                    bodyType =
                            Class.forName(
                                    bodyTypeName,
                                    false,
                                    Thread.currentThread().getContextClassLoader());
                    CLASS_CACHE_LOCK.writeLock().lock();
                    try {
                        CLASS_CACHE.put(bodyTypeName, bodyType);
                    } finally {
                        CLASS_CACHE_LOCK.writeLock().unlock();
                    }
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

    /**
     * 强制终结长期卡在 COMMITTING 的事务（有界恢复尝试耗尽）。
     *
     * <p><b>为什么可以终结：</b>转投半消息与写入终态在同一 Lua 脚本中原子完成，状态不是 COMMIT 就等同于「未发布」。
     *
     * <p><b>为什么要 CAS（R2-1）：</b>「判定卡死」与「写终态」之间隔着多次 Redis 往返，期间并发实例可能已完成转投并置位 COMMIT。无条件的 {@code HSET
     * ROLLBACK} 会把「消息已投递」覆盖成「回滚」，出现状态与真实投递永久不一致。因此终态 写入必须是 CAS：仅当状态仍为 COMMITTING 时才改写成
     * ROLLBACK；状态已变（COMMIT/ROLLBACK/…）则放弃终结， 绝不覆盖。
     *
     * <p><b>为什么要告警：</b>调用方（本地事务）通常已提交，消息未投递需要业务方按 {@code .failureReason} 字段
     * 与事务指标对账补偿；同时尽力删除半消息，避免半消息流残留。
     *
     * @param txId 事务 ID
     * @param txGroup 事务组名
     * @param stateMap txstate Hash 视图
     * @param attempts 已累计的恢复尝试次数（仅用于日志）
     */
    private void forceFinalizeStuckCommit(
            String txId, String txGroup, RMap<String, String> stateMap, int attempts) {
        LOG.error(
                "Transaction stuck in COMMITTING after {} attempts, force-finalizing as ROLLBACK"
                        + " (message NOT published; the local transaction may already be"
                        + " committed — reconcile manually): txId={}, txGroup={}",
                attempts,
                txId,
                txGroup);
        if (!casFinalizeStuck(
                txId,
                txGroup,
                STATE_COMMITTING,
                STATE_ROLLBACK,
                FIELD_FAILURE_REASON_SUFFIX,
                "COMMIT_FAILED_FORCE_ROLLBACK")) {
            // 状态在判定与写入之间被并发路径改写（最典型：并发实例转投成功并置位 COMMIT）。
            // 绝不覆盖：仅补齐终态收尾标记（若已是终态），交由真实状态呈现。
            handleAbortedCommit(txId, txGroup, stateMap);
            return;
        }
        removeHalfMessageQuietly(txId, txGroup, stateMap);
        removeCheckEntry(txId, txGroup);
        markTerminalDone(stateMap, txId);
        cleanupTerminalState(stateMap, txId);
        metricsRecorder.recordRollback(txGroup);
    }

    /**
     * 强制终结长期卡在 ROLLBACKING 的事务（有界恢复尝试耗尽）。
     *
     * <p>此时 XDEL 半消息可能未成功：终态仍取 ROLLBACK（语义正确），但半消息可能残留，由 ERROR 日志 提示人工清理（保留期维护任务也会兜底清理孤儿半消息）。
     *
     * <p>与 COMMITTING 强制终结同样使用状态 CAS：状态若已被并发路径改写则放弃写入，绝不覆盖。
     *
     * @param txId 事务 ID
     * @param txGroup 事务组名
     * @param stateMap txstate Hash 视图
     * @param attempts 已累计的恢复尝试次数（仅用于日志）
     */
    private void forceFinalizeStuckRollback(
            String txId, String txGroup, RMap<String, String> stateMap, int attempts) {
        LOG.error(
                "Transaction stuck in ROLLBACKING after {} attempts, force-finalizing as ROLLBACK;"
                        + " a half message may still exist and needs manual cleanup: txId={},"
                        + " txGroup={}",
                attempts,
                txId,
                txGroup);
        if (!casFinalizeStuck(
                txId,
                txGroup,
                STATE_ROLLBACKING,
                STATE_ROLLBACK,
                FIELD_FAILURE_REASON_SUFFIX,
                "ROLLBACK_FAILED_FORCE_FINALIZE")) {
            LOG.warn(
                    "Force-finalize skipped, state changed under us (not ROLLBACKING anymore):"
                            + " txId={}, txGroup={}",
                    txId,
                    txGroup);
            return;
        }
        removeCheckEntry(txId, txGroup);
        markTerminalDone(stateMap, txId);
        cleanupTerminalState(stateMap, txId);
        metricsRecorder.recordRollback(txGroup);
    }

    /**
     * 强制终结的状态 CAS（Lua 原子校验 + 写入）。
     *
     * @param txId 事务 ID
     * @param txGroup 事务组名
     * @param expectedState 期望的中间态（COMMITTING / ROLLBACKING）
     * @param terminalState 终态值（ROLLBACK）
     * @param reasonSuffix 原因字段后缀
     * @param reasonValue 原因值
     * @return true 表示状态仍为期望中间态、已原子改写成终态（并写入失败原因）
     */
    private boolean casFinalizeStuck(
            String txId,
            String txGroup,
            String expectedState,
            String terminalState,
            String reasonSuffix,
            String reasonValue) {
        String stateHashKey = StreamMQKeys.transactionStateHash(namespace, txGroup);
        String result =
                redisson.getScript(StringCodec.INSTANCE)
                        .eval(
                                RScript.Mode.READ_WRITE,
                                LUA_CAS_FINALIZE_STUCK,
                                RScript.ReturnType.STATUS,
                                Collections.singletonList(stateHashKey),
                                txId,
                                expectedState,
                                txId + reasonSuffix,
                                reasonValue,
                                terminalState);
        return "OK".equals(result);
    }

    /** 尽力删除半消息（失败仅告警：孤儿半消息由保留期维护任务兜底清理）。 */
    private void removeHalfMessageQuietly(
            String txId, String txGroup, RMap<String, String> stateMap) {
        String halfIdStr = stateMap.get(txId + FIELD_HALF_ID_SUFFIX);
        if (StringUtils.isEmpty(halfIdStr)) {
            return;
        }
        try {
            redisson.getStream(StreamMQKeys.halfStream(namespace, txGroup), StringCodec.INSTANCE)
                    .remove(parseStreamId(halfIdStr));
        } catch (RuntimeException ex) {
            LOG.warn(
                    "XDEL half message failed during force-finalize (orphan half may remain):"
                            + " txId={}, halfId={}: {}",
                    txId,
                    halfIdStr,
                    ex.getMessage());
        }
    }

    /** 从 txcheck ZSet 移除 txId。 */
    private void removeCheckEntry(String txId, String txGroup) {
        String checkZSetKey = StreamMQKeys.transactionCheckZSet(namespace, txGroup);
        redisson.getScoredSortedSet(checkZSetKey, StringCodec.INSTANCE).remove(txId);
        // 同时清理回查计数
        String counterKey = StreamMQKeys.transactionCheckCounter(namespace, txGroup);
        redisson.getMap(counterKey, StringCodec.INSTANCE).remove(txId);
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
        String countStr =
                redisson.<String, String>getMap(counterKey, StringCodec.INSTANCE).get(txId);
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
