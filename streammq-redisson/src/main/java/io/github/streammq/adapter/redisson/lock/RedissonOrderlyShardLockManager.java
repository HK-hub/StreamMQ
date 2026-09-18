/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.lock;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.consumer.ConsumeOrderlyContext;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.exception.OrderlyShardBusyException;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.policy.OrderlyShardLockManager;
import io.github.streammq.core.util.StringUtils;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 顺序消费分片锁管理器默认实现（策略类，基于 Redisson）。
 *
 * <p>负责为顺序消费 Consumer 创建 shard 级分布式锁，并在消费时按 shardingKey 路由到对应 shard 加锁执行， 保证同一 shardingKey
 * 的消息串行消费，不同 shard 之间可并行。
 *
 * <p>设计模式：策略模式，将顺序消费的锁逻辑从容器中分离。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@RequiredArgsConstructor
public class RedissonOrderlyShardLockManager implements OrderlyShardLockManager {

    private static final Logger LOG =
            LoggerFactory.getLogger(RedissonOrderlyShardLockManager.class);

    /** 分片锁单轮获取等待上限默认值（毫秒） */
    public static final long DEFAULT_ACQUIRE_TIMEOUT_MS =
            io.github.streammq.core.StreamMQConstants.DEFAULT_ORDERLY_LOCK_ACQUIRE_TIMEOUT_MS;

    /** 分片锁竞争默认等待轮数 */
    public static final int DEFAULT_LOCK_WAIT_ROUNDS =
            io.github.streammq.core.StreamMQConstants.DEFAULT_ORDERLY_LOCK_WAIT_ROUNDS;

    /** 分片锁竞争默认轮间等待间隔（毫秒） */
    public static final long DEFAULT_LOCK_WAIT_INTERVAL_MS =
            io.github.streammq.core.StreamMQConstants.DEFAULT_ORDERLY_LOCK_WAIT_INTERVAL_MS;

    @NonNull private final RedissonClient redisson;

    /**
     * 分片锁单轮获取等待上限（毫秒）。
     *
     * <p>旧实现使用无限期 {@code lock.lock()}：持有者线程挂死时 watchdog 持续续期， 其它实例在该 shard
     * 上的消费线程将永久阻塞并不断累积，最终耗尽线程资源。 有界等待下，单轮超时进入下一轮等待，全部轮次仍未获得锁则抛 {@link
     * OrderlyShardBusyException}（不消耗业务重试预算、不进 DLQ）， 最坏停摆时间被限制为 {@code 轮数 × acquireTimeout + 轮间隔}。
     */
    private volatile long acquireTimeoutMs = DEFAULT_ACQUIRE_TIMEOUT_MS;

    /** 分片锁竞争的等待轮数（每轮各等待一次 {@link #acquireTimeoutMs}） */
    private volatile int lockWaitRounds = DEFAULT_LOCK_WAIT_ROUNDS;

    /** 分片锁竞争的轮间等待间隔（毫秒） */
    private volatile long lockWaitIntervalMs = DEFAULT_LOCK_WAIT_INTERVAL_MS;

    /**
     * 全参构造：Redisson 客户端 + 分片锁竞争的等待轮数与轮间间隔。
     *
     * <p>未传入轮数 / 间隔时使用 {@link #DEFAULT_LOCK_WAIT_ROUNDS} 与 {@link #DEFAULT_LOCK_WAIT_INTERVAL_MS}（即
     * {@link #RedissonOrderlyShardLockManager(RedissonClient)} 的默认值）， 也可在构造后通过 setter 调整。
     *
     * @param redisson Redisson 客户端
     * @param lockWaitRounds 等待轮数，必须 &gt; 0
     * @param lockWaitIntervalMs 轮间间隔（毫秒），必须 &gt;= 0
     */
    public RedissonOrderlyShardLockManager(
            RedissonClient redisson, int lockWaitRounds, long lockWaitIntervalMs) {
        this(redisson);
        setLockWaitRounds(lockWaitRounds);
        setLockWaitIntervalMs(lockWaitIntervalMs);
    }

    /**
     * 设置分片锁单轮获取等待上限（毫秒）。
     *
     * @param millis 等待上限，必须 &gt; 0
     */
    public void setAcquireTimeoutMs(long millis) {
        if (millis > 0) {
            this.acquireTimeoutMs = millis;
        }
    }

    /**
     * 设置分片锁竞争的等待轮数。
     *
     * @param rounds 等待轮数，必须 &gt; 0（非法值忽略）
     */
    public void setLockWaitRounds(int rounds) {
        if (rounds > 0) {
            this.lockWaitRounds = rounds;
        }
    }

    /**
     * 设置分片锁竞争的轮间等待间隔（毫秒）。
     *
     * @param millis 轮间间隔，必须 &gt;= 0（0 表示不等待；非法值忽略）
     */
    public void setLockWaitIntervalMs(long millis) {
        if (millis >= 0) {
            this.lockWaitIntervalMs = millis;
        }
    }

    /**
     * 为顺序消费 Consumer 创建 shard 级分布式锁数组。
     *
     * @param defaultNs 默认命名空间
     * @param topic 主题
     * @param group 消费组
     * @param ns 注解指定的命名空间（可为空）
     * @param shardCount 分片数
     * @return RLock 数组，shardCount &lt;= 0 时返回 null
     */
    @Override
    public Lock[] createShardLocks(
            String defaultNs, String topic, String group, String ns, int shardCount) {
        if (shardCount <= 0) {
            return null;
        }
        String namespace = StringUtils.isEmpty(ns) ? defaultNs : ns;
        RLock[] locks = new RLock[shardCount];
        for (int i = 0; i < shardCount; i++) {
            String lockKey = StreamMQKeys.shardLock(namespace, topic, group, i);
            locks[i] = redisson.getLock(lockKey);
        }
        return locks;
    }

    /**
     * 按 shardingKey 路由到对应 shard 加锁后执行顺序消费。
     *
     * <p>无分片锁时直接消费（shardCount &lt;= 0 场景）。
     *
     * <p><b>竞争信号与业务失败分离（红队审查 R4-B02 / R3-25）：</b>预算内多轮等待仍拿不到锁时， 抛 {@link OrderlyShardBusyException}
     * 而不是返回 {@code RECONSUME_LATER}——本条消息 <b>未被 handler 处理</b>，由调用方直接稍后重投、不消耗重试预算， 避免"锁竞争耗尽预算 →
     * 未处理消息被 ACK 进 DLQ"的误路由。
     *
     * @param message 待消费消息
     * @param reg Listener 注册信息
     * @param ctx 顺序消费上下文
     * @param orderly 顺序消费 Consumer
     * @return 消费动作
     * @throws OrderlyShardBusyException 预算内多轮等待仍未获得分片锁（或等待期间被中断）， 本条消息未被处理、不得消耗重试预算
     * @throws Exception Listener 抛出的异常
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public ConsumeAction consumeWithShardLock(
            Message<?> message,
            ListenerRegistration reg,
            ConsumeOrderlyContext ctx,
            StreamMessageOrderlyConsumer orderly)
            throws Exception {
        if (Objects.isNull(reg.getShardLocks()) || reg.getShardCount() <= 0) {
            return orderly.onMessage(message, ctx);
        }
        String shardingKey = message.getShardingKey();
        if (Objects.isNull(shardingKey)) {
            shardingKey = "";
        }
        int shardIndex = (shardingKey.hashCode() & 0x7fffffff) % reg.getShardCount();
        RLock lock = (RLock) reg.getShardLocks().get(shardIndex);
        if (!tryLockWithRounds(lock, shardIndex, reg)) {
            // 不得返回成功/失败动作：竞争必须走独立异常通道，调用方据此跳过重试预算。
            throw new OrderlyShardBusyException(
                    "Orderly shard lock busy: held by another instance/thread, message NOT"
                            + " handled, retry later without consuming reconsume budget"
                            + " (topic="
                            + reg.getTopic()
                            + ", group="
                            + reg.getGroup()
                            + ", shard="
                            + shardIndex
                            + ", waitRounds="
                            + lockWaitRounds
                            + ", acquireTimeoutMs="
                            + acquireTimeoutMs
                            + ")");
        }
        try {
            return orderly.onMessage(message, ctx);
        } finally {
            releaseLockQuietly(lock, shardIndex, reg);
        }
    }

    /**
     * 预算内多轮等待分片锁（每轮 {@code tryLock(acquireTimeoutMs)}，轮间休眠 {@code lockWaitIntervalMs}）。
     *
     * <p><b>为什么是多轮而非单轮：</b>多实例 rebalance 窗口与慢 handler 收尾期间，锁通常在数十至数百毫秒内
     * 被释放；单轮超时即放弃会把可自愈的短暂竞争当成长时间不可用，降低顺序消费吞吐。
     *
     * <p><b>等待期间被中断</b>（消费超时取消 / 容器停机 {@code Future.cancel(true)}）：同样归类为
     * "分片繁忙"——锁未获得意味着本条消息一定未被处理，抛 {@link OrderlyShardBusyException}
     * 可以保证"竞争不消耗预算"在超时取消路径下依然成立（中断标志已恢复，不吞取消信号）。
     *
     * @param lock 分片锁
     * @param shardIndex 分片序号（日志用）
     * @param reg 注册信息（日志用）
     * @return true 表示已持有锁；false 表示全部轮次仍未获得
     * @throws OrderlyShardBusyException 等待期间线程被中断
     */
    private boolean tryLockWithRounds(RLock lock, int shardIndex, ListenerRegistration reg) {
        int rounds = Math.max(1, lockWaitRounds);
        for (int round = 1; round <= rounds; round++) {
            try {
                // 有界等待 + 看门狗租约：获得锁后由 watchdog 自动续期保证顺序性。
                if (lock.tryLock(acquireTimeoutMs, TimeUnit.MILLISECONDS)) {
                    return true;
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new OrderlyShardBusyException(
                        "Orderly shard lock wait interrupted before acquiring the lock, message"
                                + " NOT handled (topic="
                                + reg.getTopic()
                                + ", group="
                                + reg.getGroup()
                                + ", shard="
                                + shardIndex
                                + ", round="
                                + round
                                + "/"
                                + rounds
                                + ")",
                        ex);
            }
            LOG.debug(
                    "Shard lock busy (round {}/{}), retrying in {}ms: topic={}, group={}, shard={}",
                    round,
                    rounds,
                    lockWaitIntervalMs,
                    reg.getTopic(),
                    reg.getGroup(),
                    shardIndex);
            if (round < rounds && lockWaitIntervalMs > 0) {
                try {
                    Thread.sleep(lockWaitIntervalMs);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new OrderlyShardBusyException(
                            "Orderly shard lock wait interrupted between rounds, message NOT"
                                    + " handled (topic="
                                    + reg.getTopic()
                                    + ", group="
                                    + reg.getGroup()
                                    + ", shard="
                                    + shardIndex
                                    + ", nextRound="
                                    + (round + 1)
                                    + "/"
                                    + rounds
                                    + ")",
                            ex);
                }
            }
        }
        return false;
    }

    /**
     * 释放分片锁，且<b>屏蔽中断对解锁的干扰</b>。
     *
     * <p><b>为什么必须清除中断标志：</b>顺序消费超时由 {@code Future.cancel(true)} 中断业务线程实现。若 handler
     * 响应中断，其返回时线程已携带中断标志；此时 Redisson 的同步 {@code unlock()}（底层为网络 I/O）会<b>立即
     * 失败</b>，锁被看门狗持续续期——后续重试全部阻塞在 {@code tryLock} 上、直到再次超时取消，表现为 「超时重试形同空转：handler 只被调用 1
     * 次，重试次数空耗后直接进 DLQ」（集成测试实测复现）。
     *
     * <p>因此这里先读取并清除中断标志，保证解锁的网络调用不被打断；解锁完成后恢复中断标志， 不吞掉取消信号（调用方可据此感知中断语义）。
     *
     * @param lock 分片锁
     * @param shardIndex 分片序号（日志用）
     * @param reg 注册信息（日志用）
     */
    private void releaseLockQuietly(RLock lock, int shardIndex, ListenerRegistration reg) {
        boolean interrupted = Thread.currentThread().isInterrupted();
        if (interrupted) {
            Thread.interrupted(); // 读取并清除中断标志
        }
        try {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        } catch (RuntimeException ex) {
            // 解锁失败：锁最终会在 watchdog 租约到期后自动释放，但本分片会停摆到那时。
            // 记录 ERROR 以便运维定位，不向上抛出以免掩盖 handler 的真实结果。
            LOG.error(
                    "Failed to unlock orderly shard lock: topic={}, group={}, shard={};"
                            + " this shard stalls until the watchdog lease expires",
                    reg.getTopic(),
                    reg.getGroup(),
                    shardIndex,
                    ex);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt(); // 恢复中断语义
            }
        }
    }
}
