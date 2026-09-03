/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.listener.RedissonBroadcastGroupRegistry;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.enums.DlqReason;
import io.github.streammq.core.listener.BroadcastGroupRegistry;
import io.github.streammq.core.scheduler.StreamMQScheduler;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redisson.api.PendingEntry;
import org.redisson.api.RLock;
import org.redisson.api.RScript;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PEL 认领调度器，用于消费组 PEL 滞留消息的恢复（对齐 RocketMQ 顺序消费的 queue 重投）。
 *
 * <p>支持三类扫描目标（{@link PelClaimTargetKind}）：业务流（TOPIC，含顺序分片锁保护）、 重试流（RETRY）、死信流（DLQ）。 消费者名含容器随机
 * token，实例崩溃后其 PEL 遗留无人排空； 本调度器周期扫描各类目标，将空闲超过阈值的消息以「XADD 副本 + ACK 旧条目」方式重新投递， 保证消息不因消费者崩溃而永久卡死。
 *
 * <p>当 TOPIC/RETRY 种类消息的 {@code retryTimes} 字段超过 {@code maxReconsumeTimes} 时， 从 PEL 中 ACK 移除并 XADD
 * 到 DLQ Stream；DLQ 种类一律尾部复制重投（终局投递语义）。
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class PelClaimScheduler implements StreamMQScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(PelClaimScheduler.class);

    /**
     * PEL 空闲阈值默认值（毫秒）：消息在 PEL 中超过此时间未被 ACK 则触发认领重投。
     *
     * <p><b>实现说明（勿按注释猜实现）：</b>扫描走 {@code XPENDING}（{@code RStream#listPending}）+ 按 idleTime 过滤 +
     * 「XADD 副本 + ACK 旧条目」，<b>并未</b>使用 {@code XAUTOCLAIM}。 原因是本调度器需要在认领前读取消息体判断
     * retryTimes、并按顺序消费分片锁活性决定是否放行，XAUTOCLAIM 无法满足该语义。
     *
     * <p>由此带来的吞吐特性：每轮扫描只检查 PEL 头部最多 {@code batchSize} 条。PEL 很大时，恢复延迟约为 {@code ceil(PEL 长度 /
     * batchSize) × scanIntervalMs}——不会永久卡死（idle 时间单调增长， 头部条目终将越过阈值被清理后窗口前移），但大积压场景下恢复偏慢，可通过调大
     * batchSize 缓解。
     */
    private static final long DEFAULT_MIN_IDLE_MS = StreamMQConstants.DEFAULT_PEL_CLAIM_MIN_IDLE_MS;

    /** 默认扫描间隔（毫秒） */
    private static final long DEFAULT_SCAN_INTERVAL_MS =
            StreamMQConstants.DEFAULT_PEL_CLAIM_SCAN_INTERVAL_MS;

    /** 默认单次扫描批量 */
    private static final int DEFAULT_BATCH_SIZE = StreamMQConstants.DEFAULT_BATCH_SIZE;

    /** 重投消息中保留的原始 Stream Entry ID 字段名（供业务幂等/追踪使用） */
    private static final String FIELD_ORIGINAL_MESSAGE_ID =
            StreamMQConstants.FIELD_ORIGINAL_MESSAGE_ID;

    private final RedissonClient redisson;
    private final String namespace;
    private final long scanIntervalMs;
    private final int batchSize;

    /** 关闭调度线程池时的等待超时（秒） */
    private static final long AWAIT_TERMINATION_SECONDS =
            StreamMQConstants.DEFAULT_AWAIT_TERMINATION_SECONDS;

    /**
     * PEL 扫描锁 lease（毫秒）。
     *
     * <p>使用 {@code -1} 启用 Redisson 看门狗自动续期：扫描持有期间锁持续有效，实例崩溃时看门狗停止续期
     * → 锁自动释放，避免死锁。
     *
     * <p><b>历史缺陷（已修复）：</b>此前误用 {@code DEFAULT_AWAIT_TERMINATION_SECONDS * 1000}（=5s）作为 lease，
     * 会关闭看门狗续期，导致单轮扫描 >5s（跨 AZ / 大 PEL）时锁中途过期，另一实例并发扫描同一目标 → 重复投递。
     */
    private static final long PEL_CLAIM_LOCK_LEASE_MS = -1L;

    /**
     * 原子「XADD 副本 + XACK 旧条目」脚本。
     *
     * <p>KEYS[1]=源 stream（XACK 目标），KEYS[2]=目标 stream（XADD 目标）。 ARGV[1]=消费组，ARGV[2]=旧 entry
     * id，ARGV[3..]=XADD 的 field/value 对。
     *
     * <p>先 XACK 再 XADD 且整体原子：仅当本次 XACK 真正移除该 pending（返回 1，即本实例成功认领）时才写副本；
     * 若该条目已被其它实例/消费者先行 ACK（返回 0），直接跳过，<b>杜绝重复投递</b>。 整个脚本在 Redis 端单线程原子执行，
     * 消除了此前「XADD 成功但 XACK 失败 → 下一轮重复重投」的窗口（见 P1-A）。
     */
    private static final String LUA_XADD_AND_ACK =
            "local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])\n"
                    + "if acked == 1 then\n"
                    + "  return redis.call('XADD', KEYS[2], '*', unpack(ARGV, 3))\n"
                    + "else\n"
                    + "  return '0'\n"
                    + "end";

    private final long minIdleMs;
    private volatile ScheduledExecutorService scanExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentMap<String, PelClaimTarget> targets = new ConcurrentHashMap<>();

    /** 当前的扫描调度任务，stop 时取消以支持后续 restart */
    private volatile ScheduledFuture<?> scanFuture;

    /**
     * 广播组注册表（僵尸组回收）。
     *
     * <p>非 final：允许通过 {@link #setBroadcastGroupRegistry(BroadcastGroupRegistry)} 在装配后替换 （Spring
     * 场景下注册表 Bean 可能晚于调度器构造）。
     */
    private volatile BroadcastGroupRegistry broadcastGroupRegistry;

    /**
     * 替换广播组注册表（僵尸组回收策略）。
     *
     * @param registry 注册表实现，null 表示回落默认 Redisson 实现
     */
    public void setBroadcastGroupRegistry(BroadcastGroupRegistry registry) {
        this.broadcastGroupRegistry =
                Objects.nonNull(registry)
                        ? registry
                        : new RedissonBroadcastGroupRegistry(redisson, namespace);
    }

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
        this(redisson, namespace, scanIntervalMs, batchSize, DEFAULT_MIN_IDLE_MS, null);
    }

    /**
     * 构造调度器（可指定 PEL 空闲阈值与广播组注册表）。
     *
     * <p><b>依赖倒置：</b>{@code broadcastGroupRegistry} 以接口 {@link BroadcastGroupRegistry} 注入，
     * 用户可替换僵尸组回收策略（如接入外部监控、或提供空实现并自行承担 PEL 泄漏风险）。 传 {@code null} 时回落到默认 Redisson 实现。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param scanIntervalMs 扫描间隔（毫秒）
     * @param batchSize 单次扫描批量
     * @param minIdleMs PEL 空闲阈值（毫秒）
     * @param broadcastGroupRegistry 广播组注册表（可为 null，回落默认实现）
     */
    public PelClaimScheduler(
            RedissonClient redisson,
            String namespace,
            long scanIntervalMs,
            int batchSize,
            long minIdleMs,
            BroadcastGroupRegistry broadcastGroupRegistry) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = Objects.isNull(namespace) ? "" : namespace;
        this.scanIntervalMs = scanIntervalMs > 0 ? scanIntervalMs : DEFAULT_SCAN_INTERVAL_MS;
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.minIdleMs = minIdleMs > 0 ? minIdleMs : DEFAULT_MIN_IDLE_MS;
        this.broadcastGroupRegistry =
                Objects.nonNull(broadcastGroupRegistry)
                        ? broadcastGroupRegistry
                        : new RedissonBroadcastGroupRegistry(redisson, this.namespace);
        this.scanExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, StreamMQConstants.THREAD_PELCLAIM_SCHEDULER);
                            t.setDaemon(true);
                            return t;
                        });
    }

    /**
     * 构造调度器（可指定 PEL 空闲阈值），广播组注册表回落到默认 Redisson 实现。
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
        this(redisson, namespace, scanIntervalMs, batchSize, minIdleMs, null);
    }

    /**
     * 注册一个 PEL 认领目标（topic + group，TOPIC 种类）。
     *
     * @param topic 主题
     * @param group 消费者组名
     * @param maxReconsumeTimes 最大重试次数
     */
    public void registerTarget(String topic, String group, int maxReconsumeTimes) {
        registerTarget(topic, group, maxReconsumeTimes, false, 0);
    }

    /**
     * 注册一个 PEL 认领目标（完整参数，TOPIC 种类）。
     *
     * @param topic 主题
     * @param group 消费者组名
     * @param maxReconsumeTimes 最大重试次数
     * @param orderly 是否为顺序消费目标（顺序目标认领前需检查分片锁活性）
     * @param shardCount 顺序消费分片数（orderly=false 时忽略）
     */
    public void registerTarget(
            String topic, String group, int maxReconsumeTimes, boolean orderly, int shardCount) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(group, "group");
        String key = targetKey(PelClaimTargetKind.TOPIC, topic, group);
        targets.put(
                key,
                new PelClaimTarget(
                        PelClaimTargetKind.TOPIC,
                        topic,
                        group,
                        maxReconsumeTimes,
                        orderly,
                        shardCount));
        LOG.info(
                "Registered PelClaim TOPIC target: topic={}, group={}, maxReconsumeTimes={},"
                        + " orderly={}, shardCount={}",
                topic,
                group,
                maxReconsumeTimes,
                orderly,
                shardCount);
    }

    /**
     * 注册重试流 PEL 认领目标（RETRY 种类）。
     *
     * <p>并发集群消费失败的消息经 RetryScheduler 写入 retry Stream；消费者名含容器随机 token， 实例崩溃后其 PEL 遗留无人排空——本目标以「超限转
     * DLQ / 尾部复制重投」恢复该部分消息。
     *
     * @param topic 原始主题
     * @param group 消费者组名
     * @param maxReconsumeTimes 最大重试次数
     */
    public void registerRetryStreamTarget(String topic, String group, int maxReconsumeTimes) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(group, "group");
        String key = targetKey(PelClaimTargetKind.RETRY, topic, group);
        targets.put(
                key,
                new PelClaimTarget(
                        PelClaimTargetKind.RETRY, topic, group, maxReconsumeTimes, false, 0));
        LOG.info(
                "Registered PelClaim RETRY target: topic={}, group={}, maxReconsumeTimes={}",
                topic,
                group,
                maxReconsumeTimes);
    }

    /**
     * 注册死信流 PEL 认领目标（DLQ 种类）。
     *
     * <p>此前 DLQ 组被绑定器整体跳过，DLQ Stream 中滞留的 pending（实例崩溃后消费者名失效） 永久卡死。本目标将滞留条目原样复制到流尾 + ACK 旧条目，DLQ
     * 消费者重新处理； 消息携带的失败策略计数随行，循环仍受策略约束。
     *
     * @param topic 注册的 DLQ 监听主题（即组名，用于日志与互斥锁键）
     * @param group 死信所属消费者组名
     */
    public void registerDlqTarget(String topic, String group) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(group, "group");
        String key = targetKey(PelClaimTargetKind.DLQ, topic, group);
        targets.put(key, new PelClaimTarget(PelClaimTargetKind.DLQ, topic, group, 0, false, 0));
        LOG.info("Registered PelClaim DLQ target: topic={}, group={}", topic, group);
    }

    /** 目标去重键：种类前缀避免不同类别在同一 topic/group 维度上互相覆盖。 */
    private static String targetKey(PelClaimTargetKind kind, String topic, String group) {
        return kind.name() + ":" + topic + ":" + group;
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.warn("PelClaimScheduler already started");
            return;
        }
        ensureScanExecutorAlive();
        scanFuture =
                scanExecutor.scheduleAtFixedRate(
                        this::scanAllTargets, 0, scanIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info(
                "PelClaimScheduler started, scanIntervalMs={}, minIdleMs={}, targets={}",
                scanIntervalMs,
                minIdleMs,
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
                            Thread t = new Thread(r, StreamMQConstants.THREAD_PELCLAIM_SCHEDULER);
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
        // 低频搭车任务：回收心跳超时的僵尸广播消费者组（策略由注入的 BroadcastGroupRegistry 决定）
        if (Math.floorMod(scanCounter.incrementAndGet(), BROADCAST_SWEEP_EVERY_N_SCANS) == 0) {
            try {
                int swept = broadcastGroupRegistry.sweepStaleBroadcastGroups();
                if (swept > 0) {
                    LOG.info("Swept {} stale broadcast group(s)", swept);
                }
            } catch (RuntimeException ex) {
                LOG.debug("Sweep stale broadcast groups failed: {}", ex.getMessage());
            }
        }
    }

    /** 广播僵尸组回收频率：每 N 轮扫描执行一次 */
    private static final int BROADCAST_SWEEP_EVERY_N_SCANS = 60;

    private final java.util.concurrent.atomic.AtomicLong scanCounter =
            new java.util.concurrent.atomic.AtomicLong();

    /**
     * 扫描指定目标的 PEL，对空闲超阈值的消息执行重投或 DLQ 路由。
     *
     * <p><b>多实例互斥：</b>整个目标扫描持分布式锁（tryLock，不等待）， 同一时刻仅一个实例对同一目标执行「XADD 副本 + ACK」， 消除滚动发布期间 N
     * 实例并发扫描造成的 ×N 重复投递。
     *
     * <p><b>活消费者保护：</b>
     *
     * <ul>
     *   <li>阈值 {@code minIdleMs} 默认 60s，为消费超时（30s）+ 取消宽限期的约 2 倍， 正常慢处理不会被误判；
     *   <li>ORDERLY（TOPIC 种类）额外检查消息所属分片的分布式锁是否仍被持有——看门狗续期中的分片锁 代表该消息正被某个实例合法处理（顺序消费无超时包装），此时绝不认领。
     * </ul>
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void scanPel(PelClaimTarget target) {
        String lockKey = StreamMQKeys.pelClaimLock(namespace, target.topic, target.group);
        RLock scanLock = redisson.getLock(lockKey);
        // 不等待：其它实例正在扫该目标时直接跳过本轮；lease=-1 启用看门狗续期，持有者崩溃后自动释放
        boolean locked;
        try {
            locked = scanLock.tryLock(0, PEL_CLAIM_LOCK_LEASE_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return;
        }
        if (!locked) {
            LOG.debug(
                    "PelClaim scan skipped, another instance holds the scan lock:"
                            + " kind={}, topic={}, group={}",
                    target.kind,
                    target.topic,
                    target.group);
            return;
        }
        try {
            doScanPel(target);
        } finally {
            if (scanLock.isHeldByCurrentThread()) {
                scanLock.unlock();
            }
        }
    }

    private void doScanPel(PelClaimTarget target) {
        switch (target.kind) {
            case RETRY -> doScanRetryStreamPel(target);
            case DLQ -> doScanDlqPel(target);
            case TOPIC -> doScanTopicPel(target);
        }
    }

    /** TOPIC 种类扫描：业务流 PEL 认领（既有语义，保持不变——分片锁保护、超限转 DLQ、 递增 retryTimes 重投）。 */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void doScanTopicPel(PelClaimTarget target) {
        String streamKey = StreamMQKeys.topicStream(namespace, target.topic);
        RStream<String, String> stream = redisson.getStream(streamKey);
        String dlqStreamKey = StreamMQKeys.dlqStream(namespace, target.group);

        // 读取 PEL 中的 pending 消息
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
                        // 条目已被 MAXLEN 裁剪：内容永久不可恢复，无法重投也无法进 DLQ。
                        // ACK 移除 PEL 引用避免积压永生，WARN 提示运维关注裁剪配置。
                        stream.ack(target.group, id);
                        LOG.warn(
                                "Pending entry trimmed from stream (MAXLEN), ACK to unblock"
                                        + " PEL: topic={}, group={}, id={}, consumer={}",
                                target.topic,
                                target.group,
                                id,
                                entry.getConsumerName());
                        continue;
                    }
                    Map<String, String> fields =
                            (Map<String, String>) readResult.values().iterator().next();
                    int retryTimes = parseRetryTimes(fields);
                    if (retryTimes >= target.maxReconsumeTimes) {
                        fields.put(
                                RetryScheduler.FIELD_DLQ_REASON,
                                DlqReason.MAX_RETRY_ORDERLY.getCode());
                        fields.put(
                                RetryScheduler.FIELD_ORIGINAL_RETRY_COUNT,
                                Integer.toString(retryTimes));
                        // 原子认领：先 XACK 旧条目，认领成功才 XADD 到 DLQ（Lua 端原子执行，
                        // 消除「XADD 成功但 XACK 失败」导致的重复死信；旧条目已被他人认领则跳过）
                        if (xaddAndAck(streamKey, dlqStreamKey, target.group, id, fields)
                                == null) {
                            continue;
                        }
                        LOG.info(
                                "Orderly message entered DLQ: topic={}, group={}, id={},"
                                        + " retryTimes={}",
                                target.topic,
                                target.group,
                                id,
                                retryTimes);
                    } else {
                        // ORDERLY 活消费者保护：消息所属分片的看门狗锁仍在续期 ⇒ 该消息正被
                        // 某实例合法消费中（顺序消费无超时取消），跳过认领，避免重复副作用与乱序。
                        if (target.orderly && isShardLockHeld(target, fields)) {
                            LOG.debug(
                                    "Skip claiming orderly pending, shard lock held by live"
                                            + " consumer: topic={}, group={}, id={}",
                                    target.topic,
                                    target.group,
                                    id);
                            continue;
                        }
                        // 重新投递：容器消费者使用 XREADGROUP >（neverDelivered）读取，
                        // XAUTOCLAIM 到固定消费者名（pelclaim-consumer）的消息永远不会被读取（永久卡在 PEL）。
                        // 因此改为：XADD 新 entry（递增 retryTimes，保留 originalMessageId）+ ACK 旧 entry，
                        // 使其作为新消息被消费者重新拉取；重投次数超限后由上方分支进入 DLQ。
                        try {
                            fields.put(
                                    DefaultMessageConverter.FIELD_RETRY_TIMES,
                                    Integer.toString(retryTimes + 1));
                            fields.put(FIELD_ORIGINAL_MESSAGE_ID, id.toString());
                            // 原子认领：先 XACK 旧条目，认领成功才 XADD 副本（杜绝重复投递）
                            if (xaddAndAck(streamKey, streamKey, target.group, id, fields)
                                    == null) {
                                continue;
                            }
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

    /**
     * RETRY 种类扫描：重试流 PEL 认领。
     *
     * <p>重试流消费者名含容器随机 token，实例崩溃后其 pending 永远等不到原消费者 ACK。 恢复方式：
     *
     * <ul>
     *   <li>{@code retryTimes >= maxReconsumeTimes} → 先 XADD 到 DLQ 流（附 DLQ 原因字段）， 成功后 ACK
     *       重试流旧条目（顺序不可颠倒）；
     *   <li>否则将条目<b>原样</b>复制到同一重试流尾部 + ACK 旧条目——消费循环经 {@code >} 读取 新 ID 重新处理，retryTimes
     *       等计数字段保持不变，计数继续正确累计。
     * </ul>
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void doScanRetryStreamPel(PelClaimTarget target) {
        String streamKey = StreamMQKeys.retryStream(namespace, target.topic, target.group);
        RStream<String, String> stream = redisson.getStream(streamKey);
        String dlqStreamKey = StreamMQKeys.dlqStream(namespace, target.group);
        try {
            var pending =
                    stream.listPending(
                            target.group, StreamMessageId.MIN, StreamMessageId.MAX, batchSize);
            if (CollectionUtils.isEmpty(pending)) {
                return;
            }
            for (PendingEntry entry : pending) {
                try {
                    StreamMessageId id = entry.getId();
                    if (entry.getIdleTime() < minIdleMs) {
                        continue;
                    }
                    var readResult = stream.range(id, id);
                    if (CollectionUtils.isEmpty(readResult)) {
                        // 条目已被 MAXLEN 裁剪：ACK 解除 PEL 引用避免积压永生
                        stream.ack(target.group, id);
                        LOG.warn(
                                "Pending retry entry trimmed from stream (MAXLEN), ACK to"
                                        + " unblock PEL: topic={}, group={}, id={}, consumer={}",
                                target.topic,
                                target.group,
                                id,
                                entry.getConsumerName());
                        continue;
                    }
                    Map<String, String> fields =
                            (Map<String, String>) readResult.values().iterator().next();
                    int retryTimes = parseRetryTimes(fields);
                    if (retryTimes >= target.maxReconsumeTimes) {
                        // 超限 → 原子认领后进 DLQ（先 XACK 旧条目，认领成功才 XADD DLQ）
                        fields.put(RetryScheduler.FIELD_DLQ_REASON, DlqReason.MAX_RETRY.getCode());
                        fields.put(
                                RetryScheduler.FIELD_ORIGINAL_RETRY_COUNT,
                                Integer.toString(retryTimes));
                        if (xaddAndAck(streamKey, dlqStreamKey, target.group, id, fields)
                                == null) {
                            continue;
                        }
                        LOG.info(
                                "Retry-stream message entered DLQ: topic={}, group={}, id={},"
                                        + " retryTimes={}",
                                target.topic,
                                target.group,
                                id,
                                retryTimes);
                    } else {
                        // 未超限：原子认领后原样复制到流尾（先 XACK 旧条目，认领成功才 XADD 副本）
                        if (xaddAndAck(streamKey, streamKey, target.group, id, fields) == null) {
                            continue;
                        }
                        LOG.info(
                                "Retry-stream pending redelivered (tail copy): topic={},"
                                        + " group={}, id={}, retryTimes={}",
                                target.topic,
                                target.group,
                                id,
                                retryTimes);
                    }
                } catch (Exception ex) {
                    LOG.warn("Failed to process pending retry entry: {}", ex.getMessage());
                }
            }
        } catch (RuntimeException ex) {
            LOG.warn(
                    "listPending failed for retry stream, topic={}, group={}: {}",
                    target.topic,
                    target.group,
                    ex.getMessage());
        }
    }

    /**
     * DLQ 种类扫描：死信流 PEL 认领。
     *
     * <p>DLQ 条目不设重试上限判定（进入 DLQ 即终局投递），一律原样复制到流尾 + ACK 旧条目； DLQ 消费者重新处理，消息携带的失败策略计数随行，失败策略仍能约束循环。
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void doScanDlqPel(PelClaimTarget target) {
        String streamKey = StreamMQKeys.dlqStream(namespace, target.group);
        RStream<String, String> stream = redisson.getStream(streamKey);
        try {
            var pending =
                    stream.listPending(
                            target.group, StreamMessageId.MIN, StreamMessageId.MAX, batchSize);
            if (CollectionUtils.isEmpty(pending)) {
                return;
            }
            for (PendingEntry entry : pending) {
                try {
                    StreamMessageId id = entry.getId();
                    if (entry.getIdleTime() < minIdleMs) {
                        continue;
                    }
                    var readResult = stream.range(id, id);
                    if (CollectionUtils.isEmpty(readResult)) {
                        stream.ack(target.group, id);
                        LOG.warn(
                                "Pending DLQ entry trimmed from stream (MAXLEN), ACK to unblock"
                                        + " PEL: group={}, id={}, consumer={}",
                                target.group,
                                id,
                                entry.getConsumerName());
                        continue;
                    }
                    Map<String, String> fields =
                            (Map<String, String>) readResult.values().iterator().next();
                    // 原子认领后原样复制到流尾（先 XACK 旧条目，认领成功才 XADD 副本，杜绝重复投递）
                    if (xaddAndAck(streamKey, streamKey, target.group, id, fields) == null) {
                        continue;
                    }
                    LOG.info(
                            "DLQ pending redelivered (tail copy): group={}, id={}",
                            target.group,
                            id);
                } catch (Exception ex) {
                    LOG.warn("Failed to process pending DLQ entry: {}", ex.getMessage());
                }
            }
        } catch (RuntimeException ex) {
            LOG.warn(
                    "listPending failed for DLQ stream, group={}: {}",
                    target.group,
                    ex.getMessage());
        }
    }

    /** 判断消息所属分片的分布式锁是否仍被持有（持有 = 有实例正在处理该分片的消息）。 */
    private boolean isShardLockHeld(PelClaimTarget target, Map<String, String> fields) {
        if (target.shardCount <= 0) {
            return false;
        }
        String shardingKey = fields.get(DefaultMessageConverter.FIELD_SHARDING_KEY);
        if (StringUtils.isEmpty(shardingKey)) {
            shardingKey = "";
        }
        int shardIndex = (shardingKey.hashCode() & 0x7fffffff) % target.shardCount;
        try {
            RLock shardLock =
                    redisson.getLock(
                            StreamMQKeys.shardLock(
                                    namespace, target.topic, target.group, shardIndex));
            return shardLock.isLocked();
        } catch (RuntimeException ex) {
            // 锁状态查询失败时保守处理：视为被持有，宁可延迟认领也不重复投递
            LOG.debug("Shard lock state check failed, assuming held: {}", ex.getMessage());
            return true;
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

    /**
     * 原子地「认领旧条目 + 写副本」：在 Redis 端先 {@code XACK}（认领），仅当认领成功（返回 1）才 {@code XADD}
     * 副本，整个脚本单线程原子执行。
     *
     * @param sourceStreamKey 源 stream（XACK 目标）
     * @param destStreamKey 目标 stream（XADD 目标，重投时与源相同）
     * @param group 消费组名
     * @param id 待认领的旧 entry id
     * @param fields XADD 副本的字段表
     * @return 新写入的 entry id；若旧条目已被其它实例/消费者先行认领（XACK 返回 0）则返回 {@code null}
     */
    private String xaddAndAck(
            String sourceStreamKey,
            String destStreamKey,
            String group,
            StreamMessageId id,
            Map<String, String> fields) {
        String[] argv = new String[fields.size() * 2 + 2];
        argv[0] = group;
        argv[1] = id.toString();
        int i = 2;
        for (Map.Entry<String, String> e : fields.entrySet()) {
            argv[i++] = e.getKey();
            argv[i++] = e.getValue();
        }
        String result =
                redisson
                        .getScript(StringCodec.INSTANCE)
                        .eval(
                                RScript.Mode.READ_WRITE,
                                LUA_XADD_AND_ACK,
                                RScript.ReturnType.STATUS,
                                Arrays.asList(sourceStreamKey, destStreamKey),
                                (Object[]) argv);
        return "0".equals(result) ? null : result;
    }

    public int getTargetCount() {
        return targets.size();
    }

    private static final class PelClaimTarget {
        final PelClaimTargetKind kind;
        final String topic;
        final String group;
        final int maxReconsumeTimes;
        final boolean orderly;
        final int shardCount;

        PelClaimTarget(
                PelClaimTargetKind kind,
                String topic,
                String group,
                int maxReconsumeTimes,
                boolean orderly,
                int shardCount) {
            this.kind = Objects.requireNonNull(kind, "kind");
            this.topic = topic;
            this.group = group;
            this.maxReconsumeTimes = maxReconsumeTimes;
            this.orderly = orderly;
            this.shardCount = shardCount;
        }
    }
}
