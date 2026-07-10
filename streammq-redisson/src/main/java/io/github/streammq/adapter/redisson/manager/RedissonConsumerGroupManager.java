package io.github.streammq.adapter.redisson.manager;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.policy.ConsumerGroupManager;
import io.github.streammq.core.policy.RebalanceStrategy;
import org.redisson.api.*;
import org.redisson.client.RedisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于 Redisson 的 ConsumerGroupManager 实现，管理消费者实例注册、心跳、活跃列表维护与分片分配。
 *
 * <p>对齐 04-detailed-design.md §3.5 决策 D3（Gossip + RSemaphore）：
 * <ul>
 *   <li>实例注册：HSET streammq:{ns}:cg:{group}:instances instanceId=lastHeartbeatTs</li>
 *   <li>心跳：每 heartbeatIntervalMs（默认 5s）HSET 更新心跳时间戳</li>
 *   <li>活跃列表：HGETALL 过滤超时实例（instanceTimeoutMs 默认 20s）</li>
 *   <li>分片分配：RSemaphore 防并发 + RebalanceStrategy 计算分配 + RTopic 通知</li>
 * </ul>
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型。
 *
 * @author StreamMQ Contributors
 * @since 0.2.0
 */
public class RedissonConsumerGroupManager implements ConsumerGroupManager {

    private static final Logger LOG = LoggerFactory.getLogger(RedissonConsumerGroupManager.class);

    /** 默认心跳间隔 5s */
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 5_000L;
    /** 默认实例超时 20s（4 个心跳周期） */
    public static final long DEFAULT_INSTANCE_TIMEOUT_MS = 20_000L;

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

    /**
     * 构造 ConsumerGroupManager。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @param group 消费者组名
     * @param instanceId 实例唯一标识
     * @param rebalanceStrategy 重平衡策略
     */
    public RedissonConsumerGroupManager(RedissonClient redisson, String namespace, String group,
                                         String instanceId, RebalanceStrategy rebalanceStrategy) {
        this(redisson, namespace, group, instanceId, rebalanceStrategy,
            DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_INSTANCE_TIMEOUT_MS);
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
    public RedissonConsumerGroupManager(RedissonClient redisson, String namespace, String group,
                                         String instanceId, RebalanceStrategy rebalanceStrategy,
                                         long heartbeatIntervalMs, long instanceTimeoutMs) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = namespace == null ? "" : namespace;
        this.group = Objects.requireNonNull(group, "group");
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId");
        this.rebalanceStrategy = Objects.requireNonNull(rebalanceStrategy, "rebalanceStrategy");
        this.heartbeatIntervalMs = heartbeatIntervalMs > 0 ? heartbeatIntervalMs : DEFAULT_HEARTBEAT_INTERVAL_MS;
        this.instanceTimeoutMs = instanceTimeoutMs > 0 ? instanceTimeoutMs : DEFAULT_INSTANCE_TIMEOUT_MS;
        this.semaphoreKey = StreamMQKeys.consumerGroupSemaphore(namespace, group);
        this.heartbeatExecutor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "streammq-hb-" + group);
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 注册当前实例到消费者组。
     * <ol>
     *   <li>写入 instances Hash</li>
     *   <li>申请 RSemaphore</li>
     *   <li>订阅 RTopic 接收 Rebalance 通知</li>
     *   <li>启动心跳</li>
     * </ol>
     */
    @Override
    public void register() {
        if (running.get()) {
            LOG.warn("ConsumerGroupManager already registered: group={}, instanceId={}", group, instanceId);
            return;
        }
        // 1. 写入 instances Hash
        String instancesKey = StreamMQKeys.consumerGroupInstances(namespace, group);
        RMap<String, Long> instances = redisson.getMap(instancesKey);
        instances.put(instanceId, System.currentTimeMillis());
        LOG.info("Consumer instance registered: group={}, instanceId={}", group, instanceId);

        // 2. 申请 RSemaphore（防并发 Rebalance）
        try {
            RSemaphore sem = redisson.getSemaphore(semaphoreKey);
            sem.tryAcquire();
        } catch (RuntimeException ex) {
            LOG.warn("Failed to acquire RSemaphore for group={}: {}", group, ex.getMessage());
        }

        // 3. 订阅 Rebalance 通知
        String notifyKey = StreamMQKeys.consumerGroupNotify(namespace, group);
        RTopic topic = redisson.getTopic(notifyKey);
        listenerId = topic.addListener(String.class, (channel, msg) -> {
            if ("REBALANCE".equals(msg)) {
                LOG.info("Received REBALANCE notification: group={}, instanceId={}", group, instanceId);
            }
        });

        // 4. 启动心跳
        running.set(true);
        heartbeatFuture = heartbeatExecutor.scheduleAtFixedRate(
            this::heartbeat, heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info("Heartbeat started: group={}, instanceId={}, intervalMs={}", group, instanceId, heartbeatIntervalMs);
    }

    /**
     * 注销当前实例。
     */
    @Override
    public void unregister() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        // 取消心跳
        ScheduledFuture<?> future = this.heartbeatFuture;
        if (future != null) {
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
        String instancesKey = StreamMQKeys.consumerGroupInstances(namespace, group);
        RMap<String, Long> instances = redisson.getMap(instancesKey);
        instances.remove(instanceId);
        // 释放信号量
        try {
            RSemaphore sem = redisson.getSemaphore(semaphoreKey);
            sem.release();
        } catch (RuntimeException ex) {
            LOG.warn("Failed to release RSemaphore for group={}: {}", group, ex.getMessage());
        }
        LOG.info("Consumer instance unregistered: group={}, instanceId={}", group, instanceId);
    }

    /**
     * 心跳：更新 instances Hash 中的最后活跃时间戳。
     */
    @Override
    public void heartbeat() {
        if (!running.get()) {
            return;
        }
        try {
            String instancesKey = StreamMQKeys.consumerGroupInstances(namespace, group);
            RMap<String, Long> instances = redisson.getMap(instancesKey);
            instances.put(instanceId, System.currentTimeMillis());
            heartbeatFailCount = 0;
            LOG.debug("Heartbeat OK: group={}, instanceId={}", group, instanceId);
        } catch (RedisException ex) {
            heartbeatFailCount++;
            LOG.warn("Heartbeat failed ({}/{}): group={}, instanceId={}: {}",
                heartbeatFailCount, HEARTBEAT_FAIL_THRESHOLD, group, instanceId, ex.getMessage());
            if (heartbeatFailCount >= HEARTBEAT_FAIL_THRESHOLD) {
                LOG.error("Heartbeat failed {} times, consumer may be considered dead: group={}, instanceId={}",
                    heartbeatFailCount, group, instanceId);
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
        String instancesKey = StreamMQKeys.consumerGroupInstances(namespace, group);
        RMap<String, Long> instances = redisson.getMap(instancesKey);
        Map<String, Long> all = instances.readAllMap();
        if (all == null || all.isEmpty()) {
            return List.of();
        }
        long now = System.currentTimeMillis();
        // 清理超时实例（惰性清理）
        List<String> toRemove = new ArrayList<>();
        List<String> active = new ArrayList<>();
        for (Map.Entry<String, Long> entry : all.entrySet()) {
            if (now - entry.getValue() > instanceTimeoutMs) {
                toRemove.add(entry.getKey());
                LOG.debug("Stale instance detected: group={}, instanceId={}, lastHeartbeat={}ms ago",
                    group, entry.getKey(), now - entry.getValue());
            } else {
                active.add(entry.getKey());
            }
        }
        // 批量清理超时实例
        if (!toRemove.isEmpty()) {
            instances.fastRemove(toRemove.toArray(new String[0]));
        }
        Collections.sort(active);
        return active;
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
                LOG.debug("Semaphore acquire failed for Rebalance: group={}, another instance is processing", group);
                return getAssignedShards();
            }
            try {
                // 构造 Shard ID 列表
                List<Integer> allShards = new ArrayList<>(shardCount);
                for (int i = 0; i < shardCount; i++) {
                    allShards.add(i);
                }
                // 调用 RebalanceStrategy 计算分配（返回 shardId -> consumerName）
                Map<Integer, String> shardToConsumer = rebalanceStrategy.assign(
                    allShards, activeInstances, group);
                // 反转为 consumerName -> List<shardId>
                Map<String, List<Integer>> assignment = new HashMap<>();
                for (Map.Entry<Integer, String> entry : shardToConsumer.entrySet()) {
                    assignment.computeIfAbsent(entry.getValue(), k -> new ArrayList<>())
                        .add(entry.getKey());
                }
                // 写入 assignment Hash
                String assignmentKey = StreamMQKeys.consumerGroupAssignment(namespace, group);
                RMap<String, String> assignMap = redisson.getMap(assignmentKey);
                for (Map.Entry<String, List<Integer>> entry : assignment.entrySet()) {
                    String shardsCsv = String.join(",", entry.getValue().stream()
                        .map(String::valueOf).toList());
                    assignMap.put(entry.getKey(), shardsCsv);
                }
                // 广播 REBALANCE 通知
                String notifyKey = StreamMQKeys.consumerGroupNotify(namespace, group);
                RTopic topic = redisson.getTopic(notifyKey);
                topic.publish("REBALANCE");
                LOG.info("Rebalance completed: group={}, instances={}, assignment={}",
                    group, activeInstances.size(), assignment);
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

    /**
     * 获取当前实例已分配的 Shard 列表（从 assignment Hash 读取）。
     */
    private List<Integer> getAssignedShards() {
        String assignmentKey = StreamMQKeys.consumerGroupAssignment(namespace, group);
        RMap<String, String> assignMap = redisson.getMap(assignmentKey);
        String csv = assignMap.get(instanceId);
        if (csv == null || csv.isEmpty()) {
            return List.of();
        }
        List<Integer> shards = new ArrayList<>();
        for (String part : csv.split(",")) {
            try {
                shards.add(Integer.parseInt(part.trim()));
            } catch (NumberFormatException ignored) {
            }
        }
        return shards;
    }

    /**
     * 返回当前实例是否已注册。
     */
    @Override
    public boolean isRegistered() {
        return running.get();
    }

    /**
     * 返回实例 ID。
     */
    @Override
    public String getInstanceId() {
        return instanceId;
    }
}
