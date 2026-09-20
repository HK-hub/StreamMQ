/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.support.BroadcastGroupNaming;
import io.github.streammq.adapter.redisson.support.RedisClusterCompatibility;
import io.github.streammq.adapter.redisson.support.RedisServerClock;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.enums.DlqReason;
import io.github.streammq.core.scheduler.StreamMQScheduler;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import java.util.concurrent.atomic.AtomicReference;
import org.redisson.api.PendingEntry;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RScript;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
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
 * <p><b>P1-B 修复：</b>广播消费者组僵尸回收已从此调度器解耦，改为由独立的 {@link BroadcastGroupSweeper} 负责 （只要 StreamMQ 启用即运行，与
 * PelClaimScheduler 是否启用无关），彻底消除「禁用 PelClaimScheduler 时广播组永久泄漏」的风险。
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
     * <p>使用 {@code -1} 启用 Redisson 看门狗自动续期：扫描持有期间锁持续有效，实例崩溃时看门狗停止续期 → 锁自动释放，避免死锁。
     *
     * <p><b>历史缺陷（已修复）：</b>此前误用 {@code DEFAULT_AWAIT_TERMINATION_SECONDS * 1000}（=5s）作为 lease，
     * 会关闭看门狗续期，导致单轮扫描 >5s（跨 AZ / 大 PEL）时锁中途过期，另一实例并发扫描同一目标 → 重复投递。
     */
    private static final long PEL_CLAIM_LOCK_LEASE_MS = -1L;

    /** 认领补偿时隔离 payload 的保留期（与调度 payload TTL 一致：7 天，给运维留出重放窗口）。 */
    private static final Duration QUARANTINE_PAYLOAD_TTL = Duration.ofDays(7);

    /** 目的键类型异常（WRONGTYPE）告警的最小间隔，避免每轮扫描刷屏。 */
    private static final long DESTINATION_WARN_INTERVAL_MS = 300_000L;

    /**
     * 原子「XACK 旧条目 + XADD 副本」脚本。
     *
     * <p>KEYS[1]=源 stream（XACK 目标），KEYS[2]=目标 stream（XADD 目标）。 ARGV[1]=消费组，ARGV[2]=旧 entry
     * id，ARGV[3..]=XADD 的 field/value 对。
     *
     * <p>先 XACK 再 XADD：仅当本次 XACK 真正移除该 pending（返回 1，即本实例成功认领）时才写副本； 若该条目已被其它实例/消费者先行 ACK（返回
     * 0），直接跳过，<b>杜绝重复投递</b>。
     *
     * <p><b>XADD 失败可恢复（R4-B01）：</b>Redis Lua 无回滚语义——一旦 XADD 抛错，先前的 XACK 已生效， 消息会从 PEL
     * 消失且副本未写入（静默丢失）。因此 XADD 用 {@code pcall} 包裹：失败时返回 {@link #XADD_FAILED_MARKER}，由 Java 侧用内存中仍持有的
     * fields 做补偿（直接重试 → 隔离区持久化），保证「认领了就必须有下落」。
     */
    private static final String LUA_XADD_AND_ACK =
            "local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])\n"
                    + "if acked == 1 then\n"
                    + "  local ok, res = pcall(function() return redis.call('XADD', KEYS[2], '*',"
                    + " unpack(ARGV, 3)) end)\n"
                    + "  if ok then return res end\n"
                    + "  return 'XADD_FAILED'\n"
                    + "else\n"
                    + "  return '0'\n"
                    + "end";

    /** {@link #LUA_XADD_AND_ACK} 的 XADD 失败标记（Java 侧据此触发补偿）。 */
    static final String XADD_FAILED_MARKER = "XADD_FAILED";

    /**
     * 目标键类型自检脚本：返回 {@code TYPE} 的字符串（{@code none} / {@code stream} / 其它）。
     *
     * <p>认领前先自检，可在「DLQ 键被非 stream 占用（WRONGTYPE）」这类必然失败的目标上 <b>提前放弃认领</b>——条目继续留在 PEL
     * 等待人工修复，而不是认领后被迫走隔离区。
     */
    private static final String LUA_TYPE_CHECK = "return redis.call('TYPE', KEYS[1])['ok']";

    private final long minIdleMs;
    private volatile ScheduledExecutorService scanExecutor;
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

    public PelClaimScheduler(
            RedissonClient redisson,
            String namespace,
            long scanIntervalMs,
            int batchSize,
            long minIdleMs) {
        this(redisson, namespace, scanIntervalMs, batchSize, minIdleMs, null);
    }

    /**
     * 构造调度器（含广播组注册表参数，保持与自动装配历史 6 参构造签名兼容）。
     *
     * <p><b>P1-B：</b>{@code broadcastGroupRegistry} 不再被本调度器使用——广播组僵尸回收已解耦为独立的
     * BroadcastGroupSweeper。此处保留该参数仅为兼容既有自动装配调用，值被忽略。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param scanIntervalMs 扫描间隔（毫秒）
     * @param batchSize 单次扫描批量
     * @param minIdleMs PEL 空闲阈值（毫秒）
     * @param broadcastGroupRegistry 广播组注册表（已弃用，忽略）
     */
    public PelClaimScheduler(
            RedissonClient redisson,
            String namespace,
            long scanIntervalMs,
            int batchSize,
            long minIdleMs,
            io.github.streammq.core.listener.BroadcastGroupRegistry broadcastGroupRegistry) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = Objects.isNull(namespace) ? "" : namespace;
        this.scanIntervalMs = scanIntervalMs > 0 ? scanIntervalMs : DEFAULT_SCAN_INTERVAL_MS;
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.minIdleMs = minIdleMs > 0 ? minIdleMs : DEFAULT_MIN_IDLE_MS;
        this.scanExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, StreamMQConstants.THREAD_PELCLAIM_SCHEDULER);
                            t.setDaemon(true);
                            return t;
                        });
    }

    /**
     * 注册一个 PEL 认领目标（topic + group，TOPIC 种类）。
     *
     * @param topic 主题
     * @param group 消费者组名
     * @param maxReconsumeTimes 最大重试次数
     */
    public void registerTarget(String topic, String group, int maxReconsumeTimes) {
        registerTarget(namespace, topic, group, maxReconsumeTimes, false, 0, null);
    }

    /**
     * 注册 TOPIC 目标（命名空间感知，非顺序）。语义见 {@link #registerTarget(String, String, String, int, boolean, int,
     * String)}。
     */
    public void registerTarget(
            String namespace, String topic, String group, int maxReconsumeTimes) {
        registerTarget(namespace, topic, group, maxReconsumeTimes, false, 0, null);
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
        registerTarget(namespace, topic, group, maxReconsumeTimes, orderly, shardCount, null);
    }

    /**
     * 注册 TOPIC 目标（命名空间感知 + 分片键字段感知）。
     *
     * <p>namespace 必须取注册项自身的 namespace（{@code @StreamMQConsumer#namespace} 可覆盖全局值）， 否则认领会扫描错误的
     * Stream/Group，使该消费者的 PEL 恢复静默失效。shardingField 取该注册项 Converter 的分片键字段名，保证按与消费端一致的约定计算分片。
     */
    public void registerTarget(
            String namespace,
            String topic,
            String group,
            int maxReconsumeTimes,
            boolean orderly,
            int shardCount,
            String shardingField) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(group, "group");
        String key = targetKey(PelClaimTargetKind.TOPIC, namespace, topic, group);
        targets.put(
                key,
                new PelClaimTarget(
                        PelClaimTargetKind.TOPIC,
                        namespace,
                        topic,
                        group,
                        maxReconsumeTimes,
                        orderly,
                        shardCount,
                        shardingField));
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
        registerRetryStreamTarget(namespace, topic, group, maxReconsumeTimes);
    }

    /** 注册 RETRY 目标（命名空间感知，语义见 {@link #registerTarget}）。 */
    public void registerRetryStreamTarget(
            String namespace, String topic, String group, int maxReconsumeTimes) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(group, "group");
        String key = targetKey(PelClaimTargetKind.RETRY, namespace, topic, group);
        targets.put(
                key,
                new PelClaimTarget(
                        PelClaimTargetKind.RETRY,
                        namespace,
                        topic,
                        group,
                        maxReconsumeTimes,
                        false,
                        0,
                        null));
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
        registerDlqTarget(namespace, topic, group);
    }

    /** 注册 DLQ 目标（命名空间感知，语义见 {@link #registerTarget}）。 */
    public void registerDlqTarget(String namespace, String topic, String group) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(group, "group");
        String key = targetKey(PelClaimTargetKind.DLQ, namespace, topic, group);
        targets.put(
                key,
                new PelClaimTarget(
                        PelClaimTargetKind.DLQ, namespace, topic, group, 0, false, 0, null));
        LOG.info("Registered PelClaim DLQ target: topic={}, group={}", topic, group);
    }

    /** 目标去重键：种类前缀避免不同类别在同一 topic/group 维度上互相覆盖。 */
    private static String targetKey(
            PelClaimTargetKind kind, String namespace, String topic, String group) {
        return kind.name() + ":" + namespace + ":" + topic + ":" + group;
    }

    /**
     * 注销某 (namespace, topic, group) 维度上的全部 PEL 认领目标（TOPIC / RETRY / DLQ）。
     *
     * <p>此前目标表只增不减：容器 {@code unregister} 不会清理调度目标，反复动态注册/注销后调度器仍持续扫描 已注销目标（每轮多一次 Stream 扫描的 RTT
     * 与内存开销，随注册变更单调增长）。
     *
     * @param namespace 命名空间
     * @param topic 主题
     * @param group 消费者组名
     * @return true 表示至少移除一个目标
     */
    public boolean unregisterTargets(String namespace, String topic, String group) {
        boolean removed = false;
        for (PelClaimTargetKind kind : PelClaimTargetKind.values()) {
            removed |= Objects.nonNull(targets.remove(targetKey(kind, namespace, topic, group)));
        }
        if (removed) {
            LOG.info("Unregistered PelClaim targets: topic={}, group={}", topic, group);
        }
        return removed;
    }

    @Override
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.warn("PelClaimScheduler already started");
            return;
        }
        ensureScanExecutorAlive();
        scanFuture =
                scanExecutor.scheduleAtFixedRate(
                        () -> {
                            try {
                                scanAllTargets();
                            } catch (Throwable t) {
                                LOG.error("PelClaimScheduler.scanAllTargets failed fatally", t);
                            }
                        },
                        0,
                        scanIntervalMs,
                        TimeUnit.MILLISECONDS);
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
        LOG.info("PelClaimScheduler stopped");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /** 最近一次读取到的 Redis 服务器时钟（毫秒）；0 = 未取到，回退本地时钟。 */
    private volatile long lastRedisNowMs = 0L;

    /**
     * 读取 Redis 服务器时钟，与消费者组心跳使用同一时钟源（跨主机本地时钟偏差可达数十秒，会把存活实例判死）。
     *
     * <p>每轮扫描刷新一次，避免逐条 pending 判定都发一次 TIME。
     */
    private void refreshRedisClock() {
        long serverNow = RedisServerClock.nowMillis(redisson);
        if (serverNow == RedisServerClock.UNKNOWN) {
            lastRedisNowMs = 0L;
            LOG.debug("Redis TIME unavailable, falling back to local clock");
            return;
        }
        lastRedisNowMs = serverNow;
    }

    /**
     * 加载本目标的实例心跳快照（每轮扫描、每目标一次，避免逐条 pending 重复读整个 Hash）。
     *
     * <p>心跳 Hash 的 key 与 Redis 消费者名内嵌的实例标识<b>同源</b>（生产端见 {@code
     * DefaultConsumerGroupManagerFactory#resolveInstanceId}）——这是判活成立的前提：两处身份若不同源，
     * 精确匹配永远失败，活跃慢消费者会被复制重投。
     *
     * @return 心跳快照；查询异常返回 {@code null}，调用方按「保守视为存活」处理
     */
    private Map<String, String> loadInstanceHeartbeats(PelClaimTarget target) {
        try {
            Map<String, String> heartbeats =
                    redisson.<String, String>getMap(
                                    StreamMQKeys.consumerGroupInstances(
                                            target.namespace, target.group),
                                    StringCodec.INSTANCE)
                            .readAllMap();
            // 空结果与「键不存在」同义：没有存活实例证据（可认领），不能与查询失败混为一谈
            return heartbeats == null ? Map.of() : heartbeats;
        } catch (RuntimeException ex) {
            LOG.debug(
                    "Consumer liveness lookup failed, assuming alive to avoid duplicate"
                            + " delivery: group={}, cause={}",
                    target.group,
                    ex.getMessage());
            return null;
        }
    }

    /**
     * 判断 PEL 条目所属消费者是否仍存活。
     *
     * <p><b>为什么不能只看 idle：</b>并发消费的内联处理跑在读循环线程上，慢回调期间该消费者无法发出任何 Redis 命令（也无法心跳），因此「消息 idle
     * 超阈值」<b>不等于</b>「消费者已死」。若据此认领，会把仍在正常处理的消息 复制重投（重复副作用），并在 {@code retryTimes}
     * 累积到上限后把<b>已成功处理</b>的消息误投入 DLQ。
     *
     * <p><b>判活依据：</b>消费者组管理器由<b>独立心跳线程</b>周期性写入 instances Hash（{@code instanceId → 心跳毫秒}）， 且该
     * {@code instanceId} 与消费者名内嵌标识同源（见 {@link
     * io.github.streammq.adapter.redisson.container.DefaultConsumerGroupManagerFactory}）。这里按实例标识<b>精确匹配</b>心跳行，
     * 并以心跳新鲜度作为存活证据；心跳时间与判定时间均取自 Redis 服务器时钟，规避跨主机偏差。
     *
     * <p>查询异常（快照为 {@code null}）时保守返回 {@code true}（视为存活、跳过认领）：宁可延后恢复，也不制造重复投递。
     */
    private boolean isOwnerConsumerAlive(
            Map<String, String> heartbeats,
            long nowMs,
            PelClaimTarget target,
            String consumerName) {
        if (heartbeats == null) {
            return true;
        }
        if (StringUtils.isEmpty(consumerName)) {
            return false;
        }
        if (heartbeats.isEmpty()) {
            return false;
        }
        String ownerInstanceId =
                BroadcastGroupNaming.instanceIdFromConsumerName(target.group, consumerName);
        if (ownerInstanceId == null) {
            return false;
        }
        String heartbeat = heartbeats.get(ownerInstanceId);
        if (heartbeat == null) {
            return false;
        }
        try {
            return nowMs - Long.parseLong(heartbeat) < minIdleMs;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    private void scanAllTargets() {
        refreshRedisClock();
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
        // P1-B：广播组僵尸回收已解耦为独立的 BroadcastGroupSweeper，
        // 不再搭车于本调度器，确保广播模式在 PelClaimScheduler 未启用时仍能可靠回收。
    }

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
        String lockKey = StreamMQKeys.pelClaimLock(target.namespace, target.topic, target.group);
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
        String streamKey = StreamMQKeys.topicStream(target.namespace, target.topic);
        RStream<String, String> stream = redisson.getStream(streamKey, StringCodec.INSTANCE);
        String dlqStreamKey = StreamMQKeys.dlqStream(target.namespace, target.group);

        // 读取 PEL 中的 pending 消息（游标分页：头部被存活消费者长期占据时，尾部条目仍会被检查到）
        try {
            var pending =
                    stream.listPending(
                            target.group, target.scanCursor.get(), StreamMessageId.MAX, batchSize);
            if (CollectionUtils.isEmpty(pending)) {
                target.scanCursor.set(StreamMessageId.MIN);
                return;
            }
            Map<String, String> heartbeats = loadInstanceHeartbeats(target);
            long nowMs = lastRedisNowMs > 0 ? lastRedisNowMs : System.currentTimeMillis();
            for (PendingEntry entry : pending) {
                try {
                    StreamMessageId id = entry.getId();
                    long idleTime = entry.getIdleTime();
                    if (idleTime < minIdleMs) {
                        continue;
                    }
                    if (isOwnerConsumerAlive(heartbeats, nowMs, target, entry.getConsumerName())) {
                        LOG.debug(
                                "Skip claiming pending entry owned by a live consumer:"
                                        + " topic={}, group={}, id={}, consumer={}",
                                target.topic,
                                target.group,
                                id,
                                entry.getConsumerName());
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
                        // 目的键自检只作用于**确实要写 DLQ 的这一条分支**：DLQ 键被非 stream 占用时
                        // 跳过本条（消息留在 PEL，绝不丢），但同批次里未超限、只需同流重投的条目
                        // 仍会被正常认领——此前把自检放在扫描入口，会让一条错误键拖停整个目标。
                        if (!warnIfDestinationUnwritable(target, dlqStreamKey)) {
                            continue;
                        }
                        fields.put(
                                RetryScheduler.FIELD_DLQ_REASON,
                                DlqReason.MAX_RETRY_ORDERLY.getCode());
                        fields.put(
                                RetryScheduler.FIELD_ORIGINAL_RETRY_COUNT,
                                Integer.toString(retryTimes));
                        // 原子认领：先 XACK 旧条目，认领成功才 XADD 到 DLQ（Lua 端原子执行，
                        // 消除「XADD 成功但 XACK 失败」导致的重复死信；旧条目已被他人认领则跳过）
                        if (xaddAndAck(streamKey, dlqStreamKey, target.group, id, fields) == null) {
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
            advanceCursor(target, pending);
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
        String streamKey = StreamMQKeys.retryStream(target.namespace, target.topic, target.group);
        RStream<String, String> stream = redisson.getStream(streamKey, StringCodec.INSTANCE);
        String dlqStreamKey = StreamMQKeys.dlqStream(target.namespace, target.group);
        if (!warnIfDestinationUnwritable(target, dlqStreamKey)) {
            return;
        }
        try {
            var pending =
                    stream.listPending(
                            target.group, target.scanCursor.get(), StreamMessageId.MAX, batchSize);
            if (CollectionUtils.isEmpty(pending)) {
                target.scanCursor.set(StreamMessageId.MIN);
                return;
            }
            Map<String, String> heartbeats = loadInstanceHeartbeats(target);
            long nowMs = lastRedisNowMs > 0 ? lastRedisNowMs : System.currentTimeMillis();
            for (PendingEntry entry : pending) {
                try {
                    StreamMessageId id = entry.getId();
                    if (entry.getIdleTime() < minIdleMs) {
                        continue;
                    }
                    if (isOwnerConsumerAlive(heartbeats, nowMs, target, entry.getConsumerName())) {
                        LOG.debug(
                                "Skip claiming pending entry owned by a live consumer:"
                                        + " topic={}, group={}, id={}, consumer={}",
                                target.topic,
                                target.group,
                                id,
                                entry.getConsumerName());
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
                        // 超限 → 原子认领后进 DLQ（先 XACK 旧条目，认领成功才 XADD DLQ）。
                        // 目的键自检同样只作用于这条分支：DLQ 键不可写时跳过本条（留 PEL），
                        // 同批次未超限的重投条目不受影响。
                        if (!warnIfDestinationUnwritable(target, dlqStreamKey)) {
                            continue;
                        }
                        fields.put(RetryScheduler.FIELD_DLQ_REASON, DlqReason.MAX_RETRY.getCode());
                        fields.put(
                                RetryScheduler.FIELD_ORIGINAL_RETRY_COUNT,
                                Integer.toString(retryTimes));
                        if (xaddAndAck(streamKey, dlqStreamKey, target.group, id, fields) == null) {
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
            advanceCursor(target, pending);
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
        String streamKey = StreamMQKeys.dlqStream(target.namespace, target.group);
        RStream<String, String> stream = redisson.getStream(streamKey, StringCodec.INSTANCE);
        try {
            var pending =
                    stream.listPending(
                            target.group, target.scanCursor.get(), StreamMessageId.MAX, batchSize);
            if (CollectionUtils.isEmpty(pending)) {
                target.scanCursor.set(StreamMessageId.MIN);
                return;
            }
            Map<String, String> heartbeats = loadInstanceHeartbeats(target);
            long nowMs = lastRedisNowMs > 0 ? lastRedisNowMs : System.currentTimeMillis();
            for (PendingEntry entry : pending) {
                try {
                    StreamMessageId id = entry.getId();
                    if (entry.getIdleTime() < minIdleMs) {
                        continue;
                    }
                    if (isOwnerConsumerAlive(heartbeats, nowMs, target, entry.getConsumerName())) {
                        LOG.debug(
                                "Skip claiming pending entry owned by a live consumer:"
                                        + " topic={}, group={}, id={}, consumer={}",
                                target.topic,
                                target.group,
                                id,
                                entry.getConsumerName());
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
            advanceCursor(target, pending);
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
        String shardingField =
                StringUtils.isEmpty(target.shardingField)
                        ? DefaultMessageConverter.FIELD_SHARDING_KEY
                        : target.shardingField;
        String shardingKey = fields.get(shardingField);
        if (StringUtils.isEmpty(shardingKey)) {
            shardingKey = "";
        }
        int shardIndex = (shardingKey.hashCode() & 0x7fffffff) % target.shardCount;
        try {
            RLock shardLock =
                    redisson.getLock(
                            StreamMQKeys.shardLock(
                                    target.namespace, target.topic, target.group, shardIndex));
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
     * 原子地「认领旧条目 + 写副本」：在 Redis 端先 {@code XACK}（认领），仅当认领成功（返回 1）才 {@code XADD} 副本，整个脚本单线程原子执行。
     *
     * <p><b>失败补偿（R4-B01）：</b>XADD 在脚本内失败时返回 {@link #XADD_FAILED_MARKER}（XACK 已生效、条目已不在 PEL），由
     * {@link #compensateFailedXadd} 用内存中的 fields 重写或落盘隔离区——认领后消息必须有下落， 不允许静默丢失。
     *
     * @param sourceStreamKey 源 stream（XACK 目标）
     * @param destStreamKey 目标 stream（XADD 目标，重投时与源相同）
     * @param group 消费组名
     * @param id 待认领的旧 entry id
     * @param fields XADD 副本的字段表
     * @return 新写入的 entry id；若旧条目已被其它实例/消费者先行认领（XACK 返回 0）或补偿失败则返回 {@code null}
     */
    private String xaddAndAck(
            String sourceStreamKey,
            String destStreamKey,
            String group,
            StreamMessageId id,
            Map<String, String> fields) {
        // 同流重投（KEYS[1]==KEYS[2]）是单 key 脚本，Cluster 安全；跨流转投（如 DLQ 目标）触达两个
        // key 家族，Cluster 上必然 CROSSSLOT——前置拒绝并给出可操作错误
        if (!sourceStreamKey.equals(destStreamKey)) {
            RedisClusterCompatibility.requireCrossKeyAtomicity(
                    redisson, "PEL claim across streams (source stream + destination stream)");
        }
        String[] argv = new String[fields.size() * 2 + 2];
        argv[0] = group;
        argv[1] = id.toString();
        int i = 2;
        for (Map.Entry<String, String> e : fields.entrySet()) {
            argv[i++] = e.getKey();
            argv[i++] = e.getValue();
        }
        String result =
                redisson.getScript(StringCodec.INSTANCE)
                        .eval(
                                RScript.Mode.READ_WRITE,
                                LUA_XADD_AND_ACK,
                                RScript.ReturnType.STATUS,
                                Arrays.asList(sourceStreamKey, destStreamKey),
                                (Object[]) argv);
        if (Objects.isNull(result) || "0".equals(result)) {
            return null;
        }
        if (XADD_FAILED_MARKER.equals(result)) {
            return compensateFailedXadd(destStreamKey, group, id, fields);
        }
        return result;
    }

    /**
     * 认领后 XADD 失败的补偿：直接重试 → 隔离区落盘（保证消息可被运维重放）。
     *
     * <p>触发条件（脚本内 XADD 失败）：目标键被非 stream 类型占用（WRONGTYPE）、maxmemory OOM、ACL 拒绝 XADD。此时旧条目已被
     * XACK，若不做补偿即静默丢失，因此按以下顺序兜底：
     *
     * <ol>
     *   <li>直接用内存中的 fields 重试一次 {@code XADD}（瞬时故障 / 键刚被修复）；
     *   <li>仍失败 → 把完整 fields 写入隔离区 Hash 并在隔离区 ZSet 登记索引（7 天 TTL），ERROR 日志给出 key 便于运维重放；
     *   <li>连隔离区也写不进去（Redis 整体不可写）→ ERROR 明确记录"消息丢失"，不再静默。
     * </ol>
     *
     * @return 补偿成功（重试写回）时返回新 entry id；落盘隔离区或彻底失败返回 {@code null}
     */
    private String compensateFailedXadd(
            String destStreamKey, String group, StreamMessageId id, Map<String, String> fields) {
        LOG.error(
                "Claimed pending entry but XADD failed inside script (XACK already applied):"
                        + " dest={}, group={}, id={} — compensating",
                destStreamKey,
                group,
                id);
        try {
            RStream<String, String> destStream =
                    redisson.getStream(destStreamKey, StringCodec.INSTANCE);
            StreamMessageId newId = destStream.add(StreamAddArgs.entries(fields));
            LOG.warn(
                    "Claim compensation succeeded on direct retry: dest={}, group={}, oldId={},"
                            + " newId={}",
                    destStreamKey,
                    group,
                    id,
                    newId);
            return newId.toString();
        } catch (RuntimeException retryEx) {
            LOG.error(
                    "Claim compensation retry failed, persisting payload to quarantine:"
                            + " dest={}, group={}, id={}, cause={}",
                    destStreamKey,
                    group,
                    id,
                    retryEx.getMessage());
        }
        String quarantineKind = "pel-claim";
        String payloadKey = StreamMQKeys.quarantinePayloadHash(namespace, group, id.toString());
        try {
            RMap<String, String> quarantinePayload =
                    redisson.getMap(payloadKey, StringCodec.INSTANCE);
            quarantinePayload.putAll(fields);
            quarantinePayload.expire(QUARANTINE_PAYLOAD_TTL);
            redisson.getScoredSortedSet(
                            StreamMQKeys.quarantineZset(namespace, quarantineKind),
                            StringCodec.INSTANCE)
                    .add(System.currentTimeMillis(), id + "|" + quarantineKind);
            LOG.error(
                    "Message payload preserved in quarantine for manual replay: key={}, group={},"
                            + " id={}",
                    payloadKey,
                    group,
                    id);
        } catch (RuntimeException quarantineEx) {
            LOG.error(
                    "Message LOST — claimed entry {} could not be rewritten to {} nor quarantined"
                            + " (group={}): {}",
                    id,
                    destStreamKey,
                    group,
                    quarantineEx.getMessage());
        }
        return null;
    }

    /**
     * 目标键类型自检：键不存在或为 stream 才允许认领写入。
     *
     * <p>某目标若类型冲突（例如 DLQ 键被非 stream 值占用），认领脚本必然 XADD 失败；提前放弃认领可让条目 留在 PEL
     * 等待人工修复，而不是被认领后只能进隔离区。自检失败（脚本/网络异常）时保守返回 true， 由失败补偿兜底。
     */
    private boolean isDestinationWritable(String destStreamKey) {
        try {
            String type =
                    (String)
                            redisson.getScript(StringCodec.INSTANCE)
                                    .eval(
                                            RScript.Mode.READ_ONLY,
                                            LUA_TYPE_CHECK,
                                            RScript.ReturnType.STATUS,
                                            Collections.singletonList(destStreamKey));
            return Objects.isNull(type) || "none".equals(type) || "stream".equals(type);
        } catch (RuntimeException ex) {
            LOG.debug(
                    "Destination type self-check failed for {}: {}",
                    destStreamKey,
                    ex.getMessage());
            return true;
        }
    }

    /**
     * 目的键自检 + 限频告警：不可写时返回 {@code false}（本轮放弃该目标的全部认领）。
     *
     * <p>放弃认领不会丢消息——条目仍在 PEL 中，运维把目的键改回 stream 后自动恢复认领； 告警间隔 {@link
     * #DESTINATION_WARN_INTERVAL_MS}，避免每轮扫描刷屏。
     */
    private boolean warnIfDestinationUnwritable(PelClaimTarget target, String destStreamKey) {
        if (isDestinationWritable(destStreamKey)) {
            return true;
        }
        long now = System.currentTimeMillis();
        long last = target.lastDestinationWarnMs.get();
        if (now - last >= DESTINATION_WARN_INTERVAL_MS
                && target.lastDestinationWarnMs.compareAndSet(last, now)) {
            LOG.warn(
                    "Destination key {} exists but is not a stream (WRONGTYPE?); skipping PEL"
                            + " claims for topic={}, group={} until fixed — pending entries stay"
                            + " in PEL and are not lost",
                    destStreamKey,
                    target.topic,
                    target.group);
        }
        return false;
    }

    /**
     * 推进 PEL 扫描游标：本页取满时从最后一条之后继续，页未满说明已到 PEL 尾部（下一轮回到 MIN）。
     *
     * <p>修复「头部饥饿」：此前每轮都从 MIN 取固定窗口，头部被存活消费者长期占据时，其后的死亡实例 遗留条目永远不进入扫描窗口；游标分页保证全量 PEL 会被逐轮遍历到。
     */
    private void advanceCursor(PelClaimTarget target, List<PendingEntry> pending) {
        if (pending.size() < batchSize) {
            target.scanCursor.set(StreamMessageId.MIN);
            return;
        }
        target.scanCursor.set(nextAfter(pending.get(pending.size() - 1).getId()));
    }

    /** 返回严格大于给定 ID 的下一个 Stream ID（用于 XPENDING 分页起点）。 */
    private static StreamMessageId nextAfter(StreamMessageId id) {
        if (id.getId1() == Long.MAX_VALUE) {
            return new StreamMessageId(id.getId0() + 1, 0);
        }
        return new StreamMessageId(id.getId0(), id.getId1() + 1);
    }

    public int getTargetCount() {
        return targets.size();
    }

    private static final class PelClaimTarget {
        final PelClaimTargetKind kind;
        final String namespace;
        final String topic;
        final String group;
        final int maxReconsumeTimes;
        final boolean orderly;
        final int shardCount;
        final String shardingField;

        /** PEL 扫描游标（分页起点；到达 PEL 尾部后回到 MIN）。 */
        final AtomicReference<StreamMessageId> scanCursor =
                new AtomicReference<>(StreamMessageId.MIN);

        /** 目的键类型异常告警的限频时间戳（毫秒）。 */
        final AtomicLong lastDestinationWarnMs = new AtomicLong(0L);

        PelClaimTarget(
                PelClaimTargetKind kind,
                String namespace,
                String topic,
                String group,
                int maxReconsumeTimes,
                boolean orderly,
                int shardCount,
                String shardingField) {
            this.kind = Objects.requireNonNull(kind, "kind");
            this.namespace = Objects.isNull(namespace) ? "" : namespace;
            this.topic = topic;
            this.group = group;
            this.maxReconsumeTimes = maxReconsumeTimes;
            this.orderly = orderly;
            this.shardCount = shardCount;
            this.shardingField = shardingField;
        }
    }
}
