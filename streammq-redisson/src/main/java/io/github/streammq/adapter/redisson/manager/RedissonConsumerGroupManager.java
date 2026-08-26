/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.manager;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.policy.ConsumerGroupManager;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redisson.api.*;
import org.redisson.client.RedisException;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 Redisson 的 ConsumerGroupManager 实现，管理消费者实例注册、心跳、活跃列表维护与分片分配。
 *
 * <p>对齐 04-detailed-design.md §3.5 决策 D3（Gossip + RSemaphore）：
 *
 * <ul>
 *   <li>实例注册：HSET streammq:{ns}:cg:{group}:instances instanceId=lastHeartbeatTs
 *   <li>心跳：每 heartbeatIntervalMs（默认 5s）HSET 更新心跳时间戳
 *   <li>活跃列表：HGETALL 过滤超时实例（instanceTimeoutMs 默认 20s）
 *   <li>分片分配：RSemaphore 防并发 + RebalanceStrategy 计算分配 + RTopic 通知
 * </ul>
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class RedissonConsumerGroupManager implements ConsumerGroupManager {

    private static final Logger LOG = LoggerFactory.getLogger(RedissonConsumerGroupManager.class);

    /** 默认心跳间隔 5s */
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS =
            StreamMQConstants.DEFAULT_HEARTBEAT_INTERVAL_MS;

    /** 默认实例超时 20s（4 个心跳周期） */
    public static final long DEFAULT_INSTANCE_TIMEOUT_MS =
            StreamMQConstants.DEFAULT_INSTANCE_TIMEOUT_MS;

    private final RedissonClient redisson;
    private final String namespace;
    private final String group;
    private final String instanceId;
    private final long heartbeatIntervalMs;
    private final long instanceTimeoutMs;
    private final RebalanceStrategy rebalanceStrategy;
    private final ScheduledExecutorService heartbeatExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> heartbeatFuture;

    /** 当前持有的 RSemaphore（Rebalance 仲裁用） */
    private final String semaphoreKey;

    /** 当前实例的状态监听器注册 ID（RTopic），用于接收 Rebalance 通知 */
    private volatile int listenerId = -1;

    /** 连续心跳失败计数 */
    private int heartbeatFailCount = 0;

    /** 心跳失败阈值 */
    private static final int HEARTBEAT_FAIL_THRESHOLD = 3;

    /** 重平衡通知 Pub/Sub 载荷 */
    private static final String NOTIFY_REBALANCE = "REBALANCE";

    /** 分片分配 CSV 序列化分隔符 */
    private static final String SHARD_CSV_DELIMITER = ",";

    /**
     * 构造 ConsumerGroupManager。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param group 消费者组名
     * @param instanceId 实例唯一标识
     * @param rebalanceStrategy 重平衡策略
     */
    public RedissonConsumerGroupManager(
            RedissonClient redisson,
            String namespace,
            String group,
            String instanceId,
            RebalanceStrategy rebalanceStrategy) {
        this(
                redisson,
                namespace,
                group,
                instanceId,
                rebalanceStrategy,
                DEFAULT_HEARTBEAT_INTERVAL_MS,
                DEFAULT_INSTANCE_TIMEOUT_MS);
    }

    /**
     * 全参构造。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param group 消费者组名
     * @param instanceId 实例唯一标识
     * @param rebalanceStrategy 重平衡策略
     * @param heartbeatIntervalMs 心跳间隔（毫秒）
     * @param instanceTimeoutMs 实例超时（毫秒）
     */
    public RedissonConsumerGroupManager(
            RedissonClient redisson,
            String namespace,
            String group,
            String instanceId,
            RebalanceStrategy rebalanceStrategy,
            long heartbeatIntervalMs,
            long instanceTimeoutMs) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = Objects.isNull(namespace) ? "" : namespace;
        this.group = Objects.requireNonNull(group, "group");
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId");
        this.rebalanceStrategy = Objects.requireNonNull(rebalanceStrategy, "rebalanceStrategy");
        this.heartbeatIntervalMs =
                heartbeatIntervalMs > 0 ? heartbeatIntervalMs : DEFAULT_HEARTBEAT_INTERVAL_MS;
        this.instanceTimeoutMs =
                instanceTimeoutMs > 0 ? instanceTimeoutMs : DEFAULT_INSTANCE_TIMEOUT_MS;
        this.semaphoreKey = StreamMQKeys.consumerGroupSemaphore(namespace, group);
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * 实例注册表 Hash（显式使用 StringCodec，避免客户端全局 codec 类型不兼容导致 Long/Map 编解码失败）。
     *
     * @return instances Hash
     */
    private RMap<String, String> instances() {
        return redisson.getMap(
                StreamMQKeys.consumerGroupInstances(namespace, group), StringCodec.INSTANCE);
    }

    /**
     * 获取 Redis 服务器当前时间（毫秒）。
     *
     * <p>心跳写入与超时判定必须使用同一时钟源：若使用各客户端本地时钟，跨主机时钟偏差 （NTP 失步可达数十秒）会扭曲 instanceTimeoutMs（默认
     * 20s）的判定——快钟实例被误判 存活，慢钟实例被误踢出组触发无谓 rebalance。Redis TIME 为 O(1) 命令，开销可接受； 调用失败时降级为本地时钟（与旧行为一致）。
     */
    private long redisNowMs() {
        try {
            java.util.List<Long> time =
                    redisson.getScript(StringCodec.INSTANCE)
                            .eval(
                                    org.redisson.api.RScript.Mode.READ_ONLY,
                                    "local t = redis.call('TIME');return tonumber(t[1]) * 1000 +"
                                            + " math.floor(tonumber(t[2]) / 1000);",
                                    org.redisson.api.RScript.ReturnType.MULTI,
                                    java.util.Collections.emptyList());
            return time.get(0);
        } catch (RuntimeException ex) {
            LOG.debug("Redis TIME failed, falling back to local clock: {}", ex.getMessage());
            return System.currentTimeMillis();
        }
    }

    /**
     * 注册当前实例到消费者组。
     *
     * <ol>
     *   <li>写入 instances Hash
     *   <li>申请 RSemaphore
     *   <li>订阅 RTopic 接收 Rebalance 通知
     *   <li>启动心跳
     * </ol>
     */
    @Override
    public void register() {
        if (running.get()) {
            LOG.warn(
                    "ConsumerGroupManager already registered: group={}, instanceId={}",
                    group,
                    instanceId);
            return;
        }
        // 1. 写入 instances Hash（时间戳以字符串存储，配合 StringCodec 保证跨 codec 兼容；
        //    时间取 Redis 服务器时钟，避免跨主机时钟偏差扭曲超时判定）
        instances().put(instanceId, String.valueOf(redisNowMs()));
        LOG.info("Consumer instance registered: group={}, instanceId={}", group, instanceId);

        // 2. 初始化 Rebalance 互斥信号量（1 个许可；仅 rebalance() 临界区内 acquire/release，
        //    注册时不再长期占用许可——此前注册即 acquire 且从不释放，导致其它实例的
        //    rebalance 永远拿不到许可、注销时又释放从未持有的许可，计数被破坏）
        try {
            RSemaphore sem = redisson.getSemaphore(semaphoreKey);
            sem.trySetPermits(1);
        } catch (RuntimeException ex) {
            LOG.warn("Failed to init RSemaphore for group={}: {}", group, ex.getMessage());
        }

        // 3. 订阅 Rebalance 通知
        String notifyKey = StreamMQKeys.consumerGroupNotify(namespace, group);
        RTopic topic = redisson.getTopic(notifyKey);
        listenerId =
                topic.addListener(
                        String.class,
                        (channel, msg) -> {
                            if (NOTIFY_REBALANCE.equals(msg)) {
                                LOG.info(
                                        "Received REBALANCE notification: group={}, instanceId={}",
                                        group,
                                        instanceId);
                            }
                        });

        // 4. 启动心跳
        running.set(true);
        heartbeatFuture =
                heartbeatExecutor.scheduleAtFixedRate(
                        this::heartbeat,
                        heartbeatIntervalMs,
                        heartbeatIntervalMs,
                        TimeUnit.MILLISECONDS);
        LOG.info(
                "Heartbeat started: group={}, instanceId={}, intervalMs={}",
                group,
                instanceId,
                heartbeatIntervalMs);
    }

    /** 注销当前实例。 */
    @Override
    public void unregister() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        // 取消心跳
        ScheduledFuture<?> future = this.heartbeatFuture;
        if (Objects.nonNull(future)) {
            future.cancel(false);
            this.heartbeatFuture = null;
        }
        // 取消订阅
        if (listenerId >= 0) {
            String notifyKey = StreamMQKeys.consumerGroupNotify(namespace, group);
            RTopic topic = redisson.getTopic(notifyKey);
            topic.removeListener(listenerId);
            listenerId = -1;
        }
        // 从 instances Hash 移除
        instances().remove(instanceId);
        // 关闭心跳线程池，避免线程泄漏
        heartbeatExecutor.shutdown();
        LOG.info("Consumer instance unregistered: group={}, instanceId={}", group, instanceId);
    }

    /** 心跳：更新 instances Hash 中的最后活跃时间戳。 */
    @Override
    public void heartbeat() {
        if (!running.get()) {
            return;
        }
        try {
            instances().put(instanceId, String.valueOf(redisNowMs()));
            heartbeatFailCount = 0;
            LOG.debug("Heartbeat OK: group={}, instanceId={}", group, instanceId);
        } catch (RedisException ex) {
            heartbeatFailCount++;
            LOG.warn(
                    "Heartbeat failed ({}/{}): group={}, instanceId={}: {}",
                    heartbeatFailCount,
                    HEARTBEAT_FAIL_THRESHOLD,
                    group,
                    instanceId,
                    ex.getMessage());
            if (heartbeatFailCount >= HEARTBEAT_FAIL_THRESHOLD) {
                LOG.error(
                        "Heartbeat failed {} times, consumer may be considered dead: group={},"
                                + " instanceId={}",
                        heartbeatFailCount,
                        group,
                        instanceId);
            }
        }
    }

    /**
     * 获取当前活跃的消费者实例列表（剔除超时实例）。
     *
     * @return 活跃实例 ID 列表
     */
    @Override
    public List<String> getActiveConsumers() {
        Map<String, String> all = instances().readAllMap();
        if (CollectionUtils.isEmpty(all)) {
            return List.of();
        }
        // 以 Redis 服务器时钟为基准判定超时（与心跳写入同一时钟源，见 redisNowMs）
        long now = redisNowMs();
        // 清理超时实例（惰性清理）
        List<String> toRemove = new ArrayList<>();
        List<String> active = new ArrayList<>();
        for (Map.Entry<String, String> entry : all.entrySet()) {
            long lastHeartbeat = parseTimestamp(entry.getValue());
            if (now - lastHeartbeat > instanceTimeoutMs) {
                toRemove.add(entry.getKey());
                LOG.debug(
                        "Stale instance detected: group={}, instanceId={}, lastHeartbeat={}ms ago",
                        group,
                        entry.getKey(),
                        now - lastHeartbeat);
            } else {
                active.add(entry.getKey());
            }
        }
        // 批量清理超时实例
        if (!toRemove.isEmpty()) {
            instances().fastRemove(toRemove.toArray(new String[0]));
        }
        Collections.sort(active);
        return active;
    }

    /** 解析心跳时间戳字符串（非法值视为 0，立即视为过期）。 */
    private long parseTimestamp(String value) {
        if (value == null || value.isEmpty()) {
            return 0L;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ex) {
            LOG.debug("Invalid heartbeat timestamp '{}' for group={}", value, group);
            return 0L;
        }
    }

    /**
     * 执行 Rebalance 分片分配。
     *
     * @param shardCount 总分片数
     * @return 当前实例分配到的分片 ID 列表
     */
    @Override
    @SuppressWarnings("deprecation")
    public List<Integer> rebalance(int shardCount) {
        if (shardCount <= 0) {
            return List.of();
        }
        List<String> activeInstances = getActiveConsumers();
        if (activeInstances.isEmpty()) {
            LOG.warn("No active consumers for Rebalance: group={}", group);
            return List.of();
        }
        RSemaphore sem = redisson.getSemaphore(semaphoreKey);
        try {
            if (!sem.tryAcquire(5, TimeUnit.SECONDS)) {
                LOG.debug(
                        "Semaphore acquire failed for Rebalance: group={}, another instance is"
                                + " processing",
                        group);
                return getAssignedShards();
            }
            try {
                // 构造 Shard ID 列表
                List<Integer> allShards = new ArrayList<>(shardCount);
                for (int i = 0; i < shardCount; i++) {
                    allShards.add(i);
                }
                // 调用 RebalanceStrategy 计算分配（返回 shardId -> consumerName）
                Map<Integer, String> shardToConsumer =
                        rebalanceStrategy.assign(allShards, activeInstances, group);
                // 反转为 consumerName -> List<shardId>
                Map<String, List<Integer>> assignment = new HashMap<>();
                for (Map.Entry<Integer, String> entry : shardToConsumer.entrySet()) {
                    assignment
                            .computeIfAbsent(entry.getValue(), k -> new ArrayList<>())
                            .add(entry.getKey());
                }
                // 写入 assignment Hash；同时移除已离场实例的残留分配行，
                // 否则 Hash 随实例更替无限增长，且管理端点会展示幽灵实例的分片
                String assignmentKey = StreamMQKeys.consumerGroupAssignment(namespace, group);
                RMap<String, String> assignMap = redisson.getMap(assignmentKey);
                java.util.Set<String> activeSet = new HashSet<>(activeInstances);
                for (String known : assignMap.keySet()) {
                    if (!activeSet.contains(known)) {
                        assignMap.remove(known);
                    }
                }
                for (Map.Entry<String, List<Integer>> entry : assignment.entrySet()) {
                    String shardsCsv =
                            String.join(
                                    SHARD_CSV_DELIMITER,
                                    entry.getValue().stream().map(String::valueOf).toList());
                    assignMap.put(entry.getKey(), shardsCsv);
                }
                // 广播 REBALANCE 通知
                String notifyKey = StreamMQKeys.consumerGroupNotify(namespace, group);
                RTopic topic = redisson.getTopic(notifyKey);
                topic.publish(NOTIFY_REBALANCE);
                LOG.info(
                        "Rebalance completed: group={}, instances={}, assignment={}",
                        group,
                        activeInstances.size(),
                        assignment);
                return assignment.getOrDefault(instanceId, List.of());
            } finally {
                sem.release();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Rebalance interrupted: group={}", group);
            return getAssignedShards();
        }
    }

    /** 获取当前实例已分配的 Shard 列表（从 assignment Hash 读取）。 */
    private List<Integer> getAssignedShards() {
        String assignmentKey = StreamMQKeys.consumerGroupAssignment(namespace, group);
        RMap<String, String> assignMap = redisson.getMap(assignmentKey);
        String csv = assignMap.get(instanceId);
        if (StringUtils.isEmpty(csv)) {
            return List.of();
        }
        List<Integer> shards = new ArrayList<>();
        for (String part : csv.split(SHARD_CSV_DELIMITER)) {
            try {
                shards.add(Integer.parseInt(part.trim()));
            } catch (NumberFormatException ignored) {
                LOG.debug("Failed to parse number: {}", part);
            }
        }
        return shards;
    }

    /** 返回当前实例是否已注册。 */
    @Override
    public boolean isRegistered() {
        return running.get();
    }

    /** 返回实例 ID。 */
    @Override
    public String getInstanceId() {
        return instanceId;
    }

    /**
     * 清理旧实例残留的过期心跳数据。
     *
     * <p>广播消费模式下，每次启动时调用此方法清理已超时的旧实例记录， 防止 instances Hash 无限增长。
     */
    @Override
    public void cleanupStaleGroups() {
        Map<String, String> all = instances().readAllMap();
        if (CollectionUtils.isEmpty(all)) {
            return;
        }
        long now = System.currentTimeMillis();
        List<String> staleIds = new ArrayList<>();
        for (Map.Entry<String, String> entry : all.entrySet()) {
            if (now - parseTimestamp(entry.getValue()) > instanceTimeoutMs) {
                staleIds.add(entry.getKey());
            }
        }
        if (!staleIds.isEmpty()) {
            instances().fastRemove(staleIds.toArray(new String[0]));
            LOG.info(
                    "Cleaned up {} stale instance records for group={}: {}",
                    staleIds.size(),
                    group,
                    staleIds);
        }
    }
}
