package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.enums.LocalTransactionState;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.metrics.StreamMQMetrics;
import io.github.streammq.core.scheduler.StreamMQScheduler;
import io.github.streammq.core.transaction.TransactionChecker;
import io.github.streammq.core.transaction.TransactionContext;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import lombok.Setter;
import org.redisson.api.*;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 事务回查调度器，周期扫描事务回查 ZSet，对超时的半消息触发 {@link TransactionChecker#check}，
 * 按返回状态决定 COMMIT / ROLLBACK / UNKNOWN（继续等待或超限强制 ROLLBACK）。
 *
 * <p>对齐 04-detailed-design.md §3.7 决策 D4：
 * <ul>
 *   <li>半消息暂存 Stream：{@code streammq:{ns}:half:{txGroup}}</li>
 *   <li>事务状态 Hash：{@code streammq:{ns}:txstate:{txGroup}}
 *       <ul>
 *         <li>field={@code {txId}}，value=PREPARE / COMMIT / ROLLBACK / UNKNOWN</li>
 *         <li>field={@code {txId}.target}，value=目标 Topic（COMMIT 时 XADD 目标 Stream）</li>
 *         <li>field={@code {txId}.halfId}，value=半消息 Stream Entry ID（XREAD / XDEL 用）</li>
 *       </ul>
 *   </li>
 *   <li>事务回查 ZSet：{@code streammq:{ns}:txcheck:{txGroup}}，score=checkTimeMillis，member=txId</li>
 *   <li>回查计数 Hash：{@code streammq:{ns}:txcheck:{txGroup}:counter}，field=txId，value=已回查次数</li>
 * </ul>
 *
 * <p>典型使用流程：
 * <ol>
 *   <li>starter 调用 {@link #registerChecker} 注册每个 txGroup 的 {@link TransactionChecker}</li>
 *   <li>template 发送事务消息时调用 {@link #registerHalfMessage} 写入半消息 + 状态 + 调度</li>
 *   <li>template 执行本地事务后调用 {@link #markCommit} / {@link #markRollback} 直接终结</li>
 *   <li>若 UNKNOW 或超时未终结，{@link #start} 启动的周期任务扫描并触发回查</li>
 * </ol>
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class TransactionScanner implements StreamMQScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionScanner.class);

    /** Class.forName 缓存，避免重复类加载查找 */
    private static final ConcurrentMap<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();

    /** 事务状态字段值 */
    public static final String STATE_PREPARE = "PREPARE";
    public static final String STATE_COMMIT = "COMMIT";
    public static final String STATE_ROLLBACK = "ROLLBACK";
    public static final String STATE_UNKNOWN = "UNKNOWN";
    /** 中间状态：提交中（实例已原子抢占事务，正在执行转投，其它实例见到此状态应等待或重新执行） */
    public static final String STATE_COMMITTING = "COMMITTING";
    /** 中间状态：回滚中（实例已原子抢占事务，正在执行删除，其它实例见到此状态应等待或重新执行） */
    public static final String STATE_ROLLBACKING = "ROLLBACKING";

    /**
     * Lua 脚本：原子检查状态并设置目标状态，返回旧状态。
     *
     * <p>KEYS[1] = txstate Hash key, ARGV[1] = txId, ARGV[2] = targetState
     *
     * <p>返回值：
     * <ul>
     *   <li>"COMMIT" / "ROLLBACK" — 已是终态，无需操作</li>
     *   <li>"COMMITTING" / "ROLLBACKING" — 其它实例正在处理，重新执行</li>
     *   <li>其他 — 旧状态（PREPARE/UNKNOWN），已原子切换到 targetState</li>
     * </ul>
     */
    private static final String LUA_CAS_STATE =
        "local current = redis.call('HGET', KEYS[1], ARGV[1]);"
        + "if current == 'COMMIT' or current == 'ROLLBACK' then return current; end;"
        + "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]);"
        + "if current == false then return 'nil'; else return current; end;";

    /** 默认扫描间隔 60s */
    public static final long DEFAULT_CHECK_INTERVAL_MS = StreamMQConstants.DEFAULT_CHECK_INTERVAL_MS;
    /** 默认最大回查次数 15 次 */
    public static final int DEFAULT_MAX_CHECK_TIMES = StreamMQConstants.DEFAULT_MAX_CHECK_TIMES;
    /** 默认单次扫描批量 */
    public static final int DEFAULT_BATCH_SIZE = StreamMQConstants.DEFAULT_BATCH_SIZE;

    /** txstate Hash 中目标 Topic 字段后缀 */
    private static final String FIELD_TARGET_SUFFIX = StreamMQConstants.TX_FIELD_TARGET_SUFFIX;
    /** txstate Hash 中半消息 Stream Entry ID 字段后缀 */
    private static final String FIELD_HALF_ID_SUFFIX = StreamMQConstants.TX_FIELD_HALF_ID_SUFFIX;
    /** 关闭调度线程池时的等待超时（秒） */
    private static final long AWAIT_TERMINATION_SECONDS = StreamMQConstants.DEFAULT_AWAIT_TERMINATION_SECONDS;

    private final RedissonClient redisson;
    private final String namespace;
    private final MessageConverter messageConverter;
    private final long checkIntervalMs;
    private final int maxCheckTimes;
    private final int batchSize;
    private final ScheduledExecutorService scanExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentMap<String, TransactionChecker<?>> checkerRegistry = new ConcurrentHashMap<>();
    /** 当前的扫描调度任务，stop 时取消以支持后续 restart */
    private volatile ScheduledFuture<?> scanFuture;

    /** 指标收集器（可选注入，用于记录事务指标，null 时为 no-op） */
    @Setter
    private volatile StreamMQMetrics metrics;

    /**
     * 构造调度器，使用默认参数。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param messageConverter 消息转换器（用于 COMMIT 时将半消息字段写入目标 Stream）
     */
    public TransactionScanner(RedissonClient redisson, String namespace, MessageConverter messageConverter) {
        this(redisson, namespace, messageConverter,
            DEFAULT_CHECK_INTERVAL_MS, DEFAULT_MAX_CHECK_TIMES, DEFAULT_BATCH_SIZE);
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
    public TransactionScanner(RedissonClient redisson, String namespace, MessageConverter messageConverter,
                              long checkIntervalMs, int maxCheckTimes, int batchSize) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = Objects.isNull(namespace) ? "" : namespace;
        this.messageConverter = Objects.requireNonNull(messageConverter, "messageConverter");
        this.checkIntervalMs = checkIntervalMs > 0 ? checkIntervalMs : DEFAULT_CHECK_INTERVAL_MS;
        this.maxCheckTimes = maxCheckTimes > 0 ? maxCheckTimes : DEFAULT_MAX_CHECK_TIMES;
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.scanExecutor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "streammq-txcheck-scheduler");
            t.setDaemon(true);
            return t;
        });
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
        LOG.info("Registered TransactionChecker: txGroup={}, checker={}",
            txGroup, checker.getClass().getSimpleName());
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
    public StreamMessageId registerHalfMessage(String txId, String txGroup,
                                                String targetTopic, Map<String, String> fields) {
        Objects.requireNonNull(txId, "txId");
        Objects.requireNonNull(txGroup, "txGroup");
        Objects.requireNonNull(targetTopic, "targetTopic");
        Objects.requireNonNull(fields, "fields");

        // 1. XADD 到 half Stream
        String halfStreamKey = StreamMQKeys.halfStream(namespace, txGroup);
        RStream<String, String> halfStream = redisson.getStream(halfStreamKey);
        StreamMessageId halfId = halfStream.add(StreamAddArgs.entries(fields));

        // 2. 写入 txstate Hash
        String stateHashKey = StreamMQKeys.transactionStateHash(namespace, txGroup);
        RMap<String, String> stateMap = redisson.getMap(stateHashKey);
        Map<String, String> stateFields = new HashMap<>(3);
        stateFields.put(txId, STATE_PREPARE);
        stateFields.put(txId + FIELD_TARGET_SUFFIX, targetTopic);
        stateFields.put(txId + FIELD_HALF_ID_SUFFIX, halfId.toString());
        stateMap.putAll(stateFields);

        // 3. 写入 txcheck ZSet，score = now + checkInterval
        String checkZSetKey = StreamMQKeys.transactionCheckZSet(namespace, txGroup);
        long firstCheckAt = System.currentTimeMillis() + checkIntervalMs;
        redisson.getScoredSortedSet(checkZSetKey).add(firstCheckAt, txId);

        LOG.debug("Half message registered: txId={}, txGroup={}, targetTopic={}, halfId={}",
            txId, txGroup, targetTopic, halfId);
        return halfId;
    }

    // ===================== 生命周期方法 =====================

    /**
     * 启动调度器，开始周期扫描回查 ZSet。
     */
    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.warn("TransactionScanner already started");
            return;
        }
        scanFuture = scanExecutor.scheduleAtFixedRate(this::scanAllGroups, 0, checkIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info("TransactionScanner started, checkIntervalMs={}, maxCheckTimes={}, batchSize={}, groups={}",
            checkIntervalMs, maxCheckTimes, batchSize, checkerRegistry.size());
    }

    /**
     * 停止调度器（取消扫描任务但保留线程池，支持后续 restart）。
     */
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
     * <p>通过 Lua 原子抢占事务执行权（PREPARE/UNKNOWN → COMMITTING），
     * 多实例并发时只有最先执行 Lua 的实例获得执行权，其余实例见到 COMMITTING 后重新执行。
     * 无需分布式锁。
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
            LOG.debug("markCommit ignored, transaction already terminal: txId={}, state={}", txId, oldState);
            return;
        }
        // COMMITTING/ROLLBACKING 表示其它实例正在处理，我们的执行是幂等的

        String targetTopic = stateMap.get(txId + FIELD_TARGET_SUFFIX);
        String halfIdStr = stateMap.get(txId + FIELD_HALF_ID_SUFFIX);
        if (Objects.isNull(targetTopic) || Objects.isNull(halfIdStr)) {
            LOG.warn("markCommit missing target/halfId in txstate: txId={}, txGroup={}", txId, txGroup);
            return;
        }

        // 转投半消息到目标 Stream（幂等：halfStream XDEL 已删除则无操作）
        publishHalfToBusiness(txGroup, halfIdStr, targetTopic);

        // 终态
        stateMap.put(txId, STATE_COMMIT);
        removeCheckEntry(txId, txGroup);
        cleanupTerminalState(stateMap, txId);

        recordTransactionCommitMetrics(txGroup);

        LOG.info("Transaction committed: txId={}, txGroup={}, targetTopic={}", txId, txGroup, targetTopic);
    }

    /**
     * 显式标记事务为 ROLLBACK：从 half Stream 删除半消息并清理调度元数据。
     *
     * <p>通过 Lua 原子抢占事务执行权（PREPARE/UNKNOWN → ROLLBACKING），
     * 无需分布式锁。
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
            LOG.debug("markRollback ignored, transaction already terminal: txId={}, state={}", txId, oldState);
            return;
        }

        String halfIdStr = stateMap.get(txId + FIELD_HALF_ID_SUFFIX);
        if (Objects.nonNull(halfIdStr)) {
            // XDEL 半消息
            String halfStreamKey = StreamMQKeys.halfStream(namespace, txGroup);
            RStream<String, String> halfStream = redisson.getStream(halfStreamKey);
            try {
                halfStream.remove(parseStreamId(halfIdStr));
            } catch (RuntimeException ex) {
                LOG.warn("XDEL half message failed: txId={}, halfId={}: {}",
                    txId, halfIdStr, ex.getMessage(), ex);
            }
        }

        stateMap.put(txId, STATE_ROLLBACK);
        removeCheckEntry(txId, txGroup);
        cleanupTerminalState(stateMap, txId);

        recordTransactionRollbackMetrics(txGroup);

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
        return script.eval(RScript.Mode.READ_WRITE,
            LUA_CAS_STATE, RScript.ReturnType.STATUS,
            Collections.singletonList(stateHashKey), txId, targetState);
    }

    // ===================== 内部扫描逻辑 =====================

    /**
     * 扫描所有已注册 checker 的 txGroup。
     */
    private void scanAllGroups() {
        for (String txGroup : checkerRegistry.keySet()) {
            try {
                scanTimeoutHalf(txGroup);
            } catch (RuntimeException ex) {
                LOG.warn("scanTimeoutHalf failed for txGroup={}: {}", txGroup, ex.getMessage(), ex);
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
                LOG.warn("triggerCheck failed: txId={}, txGroup={}: {}", txId, txGroup, ex.getMessage(), ex);
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
            LOG.warn("No TransactionChecker registered for txGroup={}, force rollback: txId={}", txGroup, txId);
            doMarkRollback(txId, txGroup);
            return;
        }

        // 读取半消息用于回查上下文
        String halfIdStr = stateMap.get(txId + FIELD_HALF_ID_SUFFIX);
        String targetTopic = stateMap.get(txId + FIELD_TARGET_SUFFIX);
        Message<?> halfMessage = readHalfMessage(txGroup, halfIdStr, targetTopic);
        if (Objects.isNull(halfMessage)) {
            LOG.warn("Half message not found in half stream, force rollback: txId={}, halfId={}", txId, halfIdStr);
            doMarkRollback(txId, txGroup);
            return;
        }

        // 调用 checker
        TransactionContext ctx = new TransactionContext(
            txId, txGroup, null, System.currentTimeMillis(), new HashMap<>());
        LocalTransactionState state;
        try {
            @SuppressWarnings({"unchecked", "rawtypes"})
            LocalTransactionState s = ((TransactionChecker) checker).check(halfMessage, ctx);
            state = s;
        } catch (Exception ex) {
            LOG.warn("TransactionChecker threw exception, treated as UNKNOW: txId={}: {}",
                txId, ex.getMessage(), ex);
            state = LocalTransactionState.UNKNOW;
        }

        recordTransactionCheckMetrics(txGroup, state.name());

        // 读取回查次数
        int checkCount = getCheckCount(txId, txGroup);

        switch (state) {
            case COMMIT_MESSAGE -> doMarkCommit(txId, txGroup);
            case ROLLBACK_MESSAGE -> doMarkRollback(txId, txGroup);
            case UNKNOW -> {
                if (checkCount >= maxCheckTimes) {
                    LOG.warn("Transaction exceeded maxCheckTimes ({}), force rollback: txId={}",
                        maxCheckTimes, txId);
                    doMarkRollback(txId, txGroup);
                } else {
                    // 更新状态 + 重新调度 + 递增计数
                    stateMap.put(txId, STATE_UNKNOWN);
                    incrementCheckCount(txId, txGroup);
                    long nextCheckAt = System.currentTimeMillis() + checkIntervalMs;
                    redisson.getScoredSortedSet(
                        StreamMQKeys.transactionCheckZSet(namespace, txGroup)).add(nextCheckAt, txId);
                    LOG.debug("Transaction check UNKNOWN, rescheduled: txId={}, checkCount={}, nextCheckAt={}",
                        txId, checkCount + 1, nextCheckAt);
                }
            }
            default -> LOG.warn("Unknown LocalTransactionState: txId={}, state={}", txId, state);
        }
    }

    // ===================== 辅助方法 =====================

    /**
     * 将半消息从 half Stream 转投到目标 Stream（COMMIT 时调用）。
     */
    private void publishHalfToBusiness(String txGroup, String halfIdStr, String targetTopic) {
        String halfStreamKey = StreamMQKeys.halfStream(namespace, txGroup);
        RStream<String, String> halfStream = redisson.getStream(halfStreamKey);
        StreamMessageId halfId = parseStreamId(halfIdStr);

        // 读取半消息（使用 range 读取指定 ID 的单条 entry）
        Map<StreamMessageId, Map<String, String>> entries = halfStream.range(1, halfId, halfId);
        if (CollectionUtils.isEmpty(entries)) {
            LOG.warn("Half message not found, cannot publish: txGroup={}, halfId={}", txGroup, halfIdStr);
            return;
        }
        Map<String, String> fields = new HashMap<>(entries.values().iterator().next());
        // 移除 originTopic 等调度元数据（如有）
        fields.remove(DefaultMessageConverter.FIELD_ORIGIN_TOPIC);

        // XADD 到目标 Stream
        String targetStreamKey = StreamMQKeys.topicStream(namespace, targetTopic);
        RStream<String, String> targetStream = redisson.getStream(targetStreamKey);
        targetStream.add(StreamAddArgs.entries(fields));

        // XDEL 半消息
        try {
            halfStream.remove(halfId);
        } catch (RuntimeException ex) {
            LOG.warn("XDEL half message after publish failed: halfId={}: {}", halfIdStr, ex.getMessage(), ex);
        }
    }

    /**
     * 从 half Stream 读取单条半消息用于回查上下文。
     */
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
        Map.Entry<StreamMessageId, Map<String, String>> entry = entries.entrySet().iterator().next();
        Map<String, String> fields = entry.getValue();

        // 解析 bodyType 反序列化 body
        String bodyTypeName = fields.get(DefaultMessageConverter.FIELD_BODY_TYPE);
        Class<?> bodyType = Object.class;
        if (StringUtils.isNotEmpty(bodyTypeName)) {
            bodyType = CLASS_CACHE.get(bodyTypeName);
            if (Objects.isNull(bodyType)) {
                try {
                    bodyType = Class.forName(bodyTypeName, false, Thread.currentThread().getContextClassLoader());
                    CLASS_CACHE.put(bodyTypeName, bodyType);
                } catch (ClassNotFoundException ex) {
                    LOG.warn("Body type class not found in transaction scanner, fallback to Object: {}", bodyTypeName);
                    bodyType = Object.class;
                }
            }
        }
        Message<?> message = messageConverter.fromStreamFields(fields, (Class) bodyType);
        DefaultMessageConverter.applyTopic(message, targetTopic);
        DefaultMessageConverter.applyMessageId(message, entry.getKey().toString());
        return message;
    }

    /**
     * 从 txcheck ZSet 移除 txId。
     */
    private void removeCheckEntry(String txId, String txGroup) {
        String checkZSetKey = StreamMQKeys.transactionCheckZSet(namespace, txGroup);
        redisson.getScoredSortedSet(checkZSetKey).remove(txId);
        // 同时清理回查计数
        String counterKey = StreamMQKeys.transactionCheckCounter(namespace, txGroup);
        redisson.getMap(counterKey).remove(txId);
    }

    /**
     * 清理 txstate Hash 中 .target / .halfId 等辅助字段（保留主状态字段以便查询）。
     */
    private void cleanupTerminalState(RMap<String, String> stateMap, String txId) {
        stateMap.remove(txId + FIELD_TARGET_SUFFIX);
        stateMap.remove(txId + FIELD_HALF_ID_SUFFIX);
    }

    /**
     * 获取 txId 的回查次数。
     */
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

    /**
     * 原子递增 txId 的回查次数（Lua HINCRBY）。
     */
    private static final String LUA_INCR_COUNT =
        "local val = redis.call('HINCRBY', KEYS[1], ARGV[1], 1);"
        + "return val;";

    private void incrementCheckCount(String txId, String txGroup) {
        String counterKey = StreamMQKeys.transactionCheckCounter(namespace, txGroup);
        RScript script = redisson.getScript(StringCodec.INSTANCE);
        script.eval(RScript.Mode.READ_WRITE,
            LUA_INCR_COUNT, RScript.ReturnType.INTEGER,
            Collections.singletonList(counterKey), txId);
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

    // ===================== 指标收集 =====================

    /**
     * 记录事务提交指标（null 安全，指标异常不影响业务主流程）。
     *
     * @param txGroup 事务组名
     */
    private void recordTransactionCommitMetrics(String txGroup) {
        if (Objects.nonNull(metrics)) {
            try {
                metrics.recordTransactionCommit(txGroup);
            } catch (Exception ignored) {
                // 指标收集失败不得影响业务主流程
                LOG.debug("Metrics collection failed", ignored);
            }
        }
    }

    /**
     * 记录事务回滚指标（null 安全，指标异常不影响业务主流程）。
     *
     * @param txGroup 事务组名
     */
    private void recordTransactionRollbackMetrics(String txGroup) {
        if (Objects.nonNull(metrics)) {
            try {
                metrics.recordTransactionRollback(txGroup);
            } catch (Exception ignored) {
                // 指标收集失败不得影响业务主流程
                LOG.debug("Metrics collection failed", ignored);
            }
        }
    }

    /**
     * 记录事务回查指标（null 安全，指标异常不影响业务主流程）。
     *
     * @param txGroup 事务组名
     * @param result  回查结果
     */
    private void recordTransactionCheckMetrics(String txGroup, String result) {
        if (Objects.nonNull(metrics)) {
            try {
                metrics.recordTransactionCheck(txGroup, result);
            } catch (Exception ignored) {
                // 指标收集失败不得影响业务主流程
                LOG.debug("Metrics collection failed", ignored);
            }
        }
    }
}
