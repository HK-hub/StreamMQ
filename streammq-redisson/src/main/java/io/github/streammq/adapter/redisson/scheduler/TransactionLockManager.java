/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import org.redisson.api.RBucket;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 事务执行权锁（崩溃安全）。
 *
 * <p>在事务回查器执行 publishHalfAndMarkCommit / markRollback 等临界区时， 通过 {@code SETNX + TTL} 串行化同一事务的并发处理。
 * 持有者在临界区内崩溃时，锁随 TTL 自动过期， 其它实例可在后续回查中接管，事务不会永久卡死。
 *
 * <p>释放使用 Lua compare-and-delete：仅当锁仍归本实例持有时才删除，避免误删接管者的锁。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class TransactionLockManager {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionLockManager.class);

    /** Lua：仅当锁仍归本实例持有时删除（原子 compare-and-delete，避免误删接管者的锁）。 */
    private static final String LUA_RELEASE_LOCK =
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]);"
                    + " else return 0; end;";

    private final RedissonClient redisson;
    private final String namespace;
    private final String lockHolderId = UUID.randomUUID().toString();

    /** 事务锁默认 TTL（毫秒） */
    public static final long DEFAULT_TX_LOCK_TTL_MS = 30_000L;

    private volatile long txLockTtlMs = DEFAULT_TX_LOCK_TTL_MS;

    public TransactionLockManager(RedissonClient redisson, String namespace) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = Objects.isNull(namespace) ? "" : namespace;
    }

    /**
     * 设置事务锁 TTL（毫秒），仅在 Scanner 未运行时变更即时生效。
     *
     * @param millis TTL，必须 &gt; 0
     */
    public void setTxLockTtlMs(long millis) {
        if (millis > 0) {
            this.txLockTtlMs = millis;
        }
    }

    /**
     * 尝试获取指定事务的执行权锁。
     *
     * @param txGroup 事务组
     * @param txId 事务 ID
     * @return true 获取成功；false 其它实例持有中
     */
    public boolean tryAcquire(String txGroup, String txId) {
        RBucket<String> lockBucket =
                redisson.getBucket(
                        StreamMQKeys.transactionLock(namespace, txGroup, txId),
                        StringCodec.INSTANCE);
        return Boolean.TRUE.equals(
                lockBucket.setIfAbsent(lockHolderId, Duration.ofMillis(txLockTtlMs)));
    }

    /**
     * 释放指定事务的执行权锁：原子 compare-and-delete，仅当锁仍归本实例持有时才删除。
     *
     * <p>若本实例处理超时导致锁已被其它实例接管（或已过期被抢），不得删除他人的锁—— 否则会允许第三个实例同时进入发布临界区造成重复投递。
     */
    public void release(String txGroup, String txId) {
        try {
            RScript script = redisson.getScript(StringCodec.INSTANCE);
            script.eval(
                    RScript.Mode.READ_WRITE,
                    LUA_RELEASE_LOCK,
                    RScript.ReturnType.INTEGER,
                    Collections.singletonList(
                            StreamMQKeys.transactionLock(namespace, txGroup, txId)),
                    lockHolderId);
        } catch (RuntimeException ex) {
            LOG.debug("Release transaction lock failed (TTL will expire): txId={}", txId, ex);
        }
    }
}
