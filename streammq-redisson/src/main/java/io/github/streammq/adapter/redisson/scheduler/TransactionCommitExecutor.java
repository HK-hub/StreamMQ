/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.exception.StreamMQBrokerException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.redisson.api.BatchOptions;
import org.redisson.api.RBatch;
import org.redisson.api.RMapAsync;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 事务提交执行器：原子地将 half Stream 中的半消息转投到目标 Stream 并标记 COMMIT 状态。
 *
 * <p><b>并发控制：</b>进入临界区前必须通过 {@link TransactionLockManager}（{@code SETNX+TTL}）串行化——Lua CAS
 * 只保证状态机迁移互斥，但 CAS→批量执行之间存在窗口， 两个实例可先后看到 COMMITTING 并各自转投造成业务消息重复发布；执行权锁保证同一时刻仅一个实例执行 XADD。
 * 持有者崩溃时锁随 TTL 过期，其它实例可在后续回查中接管。
 *
 * <p>批内三个动作：{@code XADD 目标流} + {@code XDEL 半消息} + {@code HSET 状态} 全部提交或全部回滚。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class TransactionCommitExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionCommitExecutor.class);

    private final RedissonClient redisson;
    private final String namespace;
    private final TransactionLockManager lockManager;

    public TransactionCommitExecutor(
            RedissonClient redisson, String namespace, TransactionLockManager lockManager) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = Objects.isNull(namespace) ? "" : namespace;
        this.lockManager = Objects.requireNonNull(lockManager, "lockManager");
    }

    /**
     * 尝试以原子批将半消息转投到目标 Stream 并标记 COMMIT。
     *
     * <p>本方法必须由调用方在 {@code STATE_COMMITTING} 状态写回后调用（即 {@code casState} 已成功）； 如锁获取失败返回 {@link
     * Outcome#LOCK_BUSY}，调用方应保持 {@code COMMITTING} 等待接管。
     *
     * @return 执行结果：PUBLISHED / HALF_MISSING / LOCK_BUSY
     */
    public Outcome publishHalfAndMarkCommit(
            String txGroup, String halfIdStr, String targetTopic, String txId) {
        if (!lockManager.tryAcquire(txGroup, txId)) {
            return Outcome.LOCK_BUSY;
        }
        String halfStreamKey = StreamMQKeys.halfStream(namespace, txGroup);
        RStream<String, String> halfStream = redisson.getStream(halfStreamKey);
        StreamMessageId halfId = parseStreamId(halfIdStr);

        // 读取半消息（使用 range 读取指定 ID 的单条 entry）
        Map<StreamMessageId, Map<String, String>> entries = halfStream.range(1, halfId, halfId);
        if (entries == null || entries.isEmpty()) {
            LOG.warn(
                    "Half message not found, cannot publish: txGroup={}, halfId={}",
                    txGroup,
                    halfIdStr);
            return Outcome.HALF_MISSING;
        }
        Map<String, String> fields = new HashMap<>(entries.values().iterator().next());
        // 移除 originTopic 等调度元数据（如有）
        fields.remove(DefaultMessageConverter.FIELD_ORIGIN_TOPIC);

        // 通过 WRITE_ATOMIC 批处理将「XADD 目标流 + XDEL 半消息 + 状态置 COMMIT」原子执行：
        // 要么全部生效、要么全部不生效，避免崩溃窗口内重复投递（此前 XADD 与状态更新非原子）。
        String targetStreamKey = StreamMQKeys.topicStream(namespace, targetTopic);
        String stateHashKey = StreamMQKeys.transactionStateHash(namespace, txGroup);
        RBatch batch =
                redisson.createBatch(
                        BatchOptions.defaults()
                                .executionMode(BatchOptions.ExecutionMode.REDIS_WRITE_ATOMIC));
        batch.<String, String>getStream(targetStreamKey).addAsync(StreamAddArgs.entries(fields));
        batch.<String, String>getStream(halfStreamKey).removeAsync(halfId);
        RMapAsync<String, String> stateMapAsync = batch.getMap(stateHashKey, StringCodec.INSTANCE);
        stateMapAsync.putAsync(txId, TransactionScanner.STATE_COMMIT);
        try {
            batch.execute();
            return Outcome.PUBLISHED;
        } catch (RuntimeException ex) {
            LOG.error(
                    "Failed to atomically publish transaction message, txId={}: {}",
                    txId,
                    ex.getMessage(),
                    ex);
            throw new StreamMQBrokerException(
                    "Failed to atomically publish transaction message for txId " + txId, null, ex);
        }
    }

    /** 释放临界区（与 {@code tryAcquire} 配对）。 */
    public void releaseLock(String txGroup, String txId) {
        lockManager.release(txGroup, txId);
    }

    /** 解析流 ID，兼容 long 与 Redis Stream 字符串两种格式。 */
    private static StreamMessageId parseStreamId(String idStr) {
        if (idStr == null || idStr.isEmpty()) {
            return StreamMessageId.MIN;
        }
        int dashIdx = idStr.indexOf('-');
        if (dashIdx < 0) {
            try {
                return new StreamMessageId(Long.parseLong(idStr));
            } catch (NumberFormatException ex) {
                return StreamMessageId.MIN;
            }
        }
        long timestamp = Long.parseLong(idStr.substring(0, dashIdx));
        long sequence = Long.parseLong(idStr.substring(dashIdx + 1));
        return new StreamMessageId(timestamp, sequence);
    }

    /** 执行结果枚举。 */
    public enum Outcome {
        /** 半消息已成功转投到目标 Stream 并标记 COMMIT */
        PUBLISHED,
        /** 半消息已被其它路径清理（XADD 后 XDEL 顺序崩溃窗口） */
        HALF_MISSING,
        /** 执行权锁被其它实例持有，本实例应保持中间态等待 */
        LOCK_BUSY
    }
}
