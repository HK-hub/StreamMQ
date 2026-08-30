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
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.policy.OrderlyShardLockManager;
import io.github.streammq.core.util.StringUtils;
import java.util.Objects;
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

    /** 分片锁获取默认等待上限（毫秒）：超时未获得则转 RECONSUME_LATER */
    public static final long DEFAULT_ACQUIRE_TIMEOUT_MS =
            io.github.streammq.core.StreamMQConstants.DEFAULT_ORDERLY_LOCK_ACQUIRE_TIMEOUT_MS;

    @NonNull private final RedissonClient redisson;

    /**
     * 分片锁获取等待上限（毫秒）。
     *
     * <p>旧实现使用无限期 {@code lock.lock()}：持有者线程挂死时 watchdog 持续续期， 其它实例在该 shard
     * 上的消费线程将永久阻塞并不断累积，最终耗尽线程资源。 有界等待下，超时方转 {@link ConsumeAction#RECONSUME_LATER} 稍后重投， 最坏停摆时间被限制为
     * acquireTimeout + watchdog TTL。
     */
    private volatile long acquireTimeoutMs = DEFAULT_ACQUIRE_TIMEOUT_MS;

    /**
     * 设置分片锁获取等待上限（毫秒）。
     *
     * @param millis 等待上限，必须 &gt; 0
     */
    public void setAcquireTimeoutMs(long millis) {
        if (millis > 0) {
            this.acquireTimeoutMs = millis;
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
     * @param message 待消费消息
     * @param reg Listener 注册信息
     * @param ctx 顺序消费上下文
     * @param orderly 顺序消费 Consumer
     * @return 消费动作
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
        boolean locked;
        try {
            // 有界等待 + 看门狗租约：等待超过上限即放弃本条消息（RECONSUME_LATER 重投），
            // 避免持有者挂死时本实例线程无限阻塞；获得锁后由 watchdog 自动续期保证顺序性。
            locked = lock.tryLock(acquireTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return ConsumeAction.RECONSUME_LATER;
        }
        if (!locked) {
            LOG.warn(
                    "Shard lock acquire timed out ({}ms), deferring message: topic={}, group={},"
                            + " shard={}",
                    acquireTimeoutMs,
                    reg.getTopic(),
                    reg.getGroup(),
                    shardIndex);
            return ConsumeAction.RECONSUME_LATER;
        }
        try {
            return orderly.onMessage(message, ctx);
        } finally {
            releaseLockQuietly(lock, shardIndex, reg);
        }
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
