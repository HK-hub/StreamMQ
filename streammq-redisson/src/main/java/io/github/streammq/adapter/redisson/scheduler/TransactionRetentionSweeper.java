/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.redisson.api.RMap;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 事务维护任务：定期清理过期终态字段与孤儿半消息，防止 Redis 资源随事务量线性增长。
 *
 * <p>两个清理任务：
 *
 * <ul>
 *   <li>{@link #sweepExpiredTerminalStates} - 清除超过保留期（默认 7 天）的 txstate Hash 终态字段与 {@code .done} 时间戳
 *   <li>{@link #sweepOrphanHalves} - 清除无状态引用的孤儿 half Stream 条目（超过保留期 1 天）
 * </ul>
 *
 * <p>两类任务均按 txGroup 独立执行；本类不持有线程——由 {@link TransactionScanner} 在每次扫描周期中 （每 N 轮）调用一次。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class TransactionRetentionSweeper {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionRetentionSweeper.class);

    /** 单次清理的最大字段数（避免大 Hash 上的长阻塞） */
    private static final int SWEEP_BATCH_SIZE = 128;

    private final RedissonClient redisson;
    private final String namespace;
    private volatile long txStateRetentionMs = TransactionScanner.DEFAULT_TX_STATE_RETENTION_MS;
    private volatile long orphanHalfRetentionMs =
            TransactionScanner.DEFAULT_ORPHAN_HALF_RETENTION_MS;

    public TransactionRetentionSweeper(RedissonClient redisson, String namespace) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = Objects.isNull(namespace) ? "" : namespace;
    }

    /** 设置 txstate 终态字段保留期（毫秒）。 */
    public void setTxStateRetentionMs(long millis) {
        if (millis > 0) {
            this.txStateRetentionMs = millis;
        }
    }

    /** 设置孤儿 half Stream 条目保留期（毫秒）。 */
    public void setOrphanHalfRetentionMs(long millis) {
        if (millis > 0) {
            this.orphanHalfRetentionMs = millis;
        }
    }

    /**
     * 维护任务：清除超过保留期的终态字段，防止 txstate Hash 随事务量线性增长。
     *
     * <p>HSCAN 游标遍历，单次最多处理 {@link #SWEEP_BATCH_SIZE} 条终态，避免大 Hash 上的长阻塞。
     */
    public int sweepExpiredTerminalStates(String txGroup) {
        String stateHashKey = StreamMQKeys.transactionStateHash(namespace, txGroup);
        RMap<String, String> stateMap = redisson.getMap(stateHashKey, StringCodec.INSTANCE);
        long cutoff = System.currentTimeMillis() - txStateRetentionMs;
        int removed = 0;
        // keySet 会整表读取；txstate 经保留期清理后规模有界，且本任务低频执行，可接受
        Set<String> fields = stateMap.keySet();
        List<String> doneFields = new ArrayList<>();
        for (String field : fields) {
            if (Objects.nonNull(field) && field.endsWith(StreamMQConstants.TX_FIELD_DONE_SUFFIX)) {
                doneFields.add(field);
            }
            if (doneFields.size() >= SWEEP_BATCH_SIZE * 4) {
                break;
            }
        }
        for (String doneField : doneFields) {
            String txId =
                    doneField.substring(
                            0,
                            doneField.length() - StreamMQConstants.TX_FIELD_DONE_SUFFIX.length());
            String doneStr = stateMap.get(doneField);
            if (doneStr == null || doneStr.isEmpty()) {
                continue;
            }
            long doneTime;
            try {
                doneTime = Long.parseLong(doneStr);
            } catch (NumberFormatException ex) {
                continue;
            }
            if (doneTime >= cutoff) {
                continue;
            }
            // 同时清理主状态字段 + 目标/half 辅助字段
            stateMap.remove(txId);
            stateMap.remove(txId + StreamMQConstants.TX_FIELD_TARGET_SUFFIX);
            stateMap.remove(txId + StreamMQConstants.TX_FIELD_HALF_ID_SUFFIX);
            stateMap.remove(doneField);
            removed++;
            if (removed >= SWEEP_BATCH_SIZE) {
                break;
            }
        }
        if (removed > 0) {
            LOG.info("Swept {} expired terminal txstate entries: txGroup={}", removed, txGroup);
        }
        return removed;
    }

    /** 维护任务：清除孤儿半消息（half Stream 中超过保留期且无状态引用的条目）。 */
    public int sweepOrphanHalves(String txGroup) {
        String halfStreamKey = StreamMQKeys.halfStream(namespace, txGroup);
        RStream<String, String> halfStream = redisson.getStream(halfStreamKey);
        long cutoff = System.currentTimeMillis() - orphanHalfRetentionMs;
        int removed = 0;
        try {
            // XRANGE 扫描全部条目；批量上限 SWEEP_BATCH_SIZE
            Map<StreamMessageId, Map<String, String>> pending =
                    halfStream.range(SWEEP_BATCH_SIZE, StreamMessageId.MIN, StreamMessageId.MAX);
            if (pending == null) {
                return 0;
            }
            for (var entry : pending.entrySet()) {
                StreamMessageId entryId = entry.getKey();
                long entryTimeMs = extractEntryTimestampMs(entryId);
                if (entryTimeMs <= 0 || entryTimeMs >= cutoff) {
                    continue;
                }
                // 检查 txstate 是否仍引用：若状态非终态则保留
                Map<String, String> fields = entry.getValue();
                if (fields == null) {
                    continue;
                }
                String txId = fields.get("txId");
                if (txId == null) {
                    halfStream.remove(entryId);
                    removed++;
                    continue;
                }
                String stateHashKey = StreamMQKeys.transactionStateHash(namespace, txGroup);
                String state =
                        redisson.<String, String>getMap(stateHashKey, StringCodec.INSTANCE)
                                .get(txId);
                if (state == null
                        || TransactionScanner.STATE_COMMIT.equals(state)
                        || TransactionScanner.STATE_ROLLBACK.equals(state)) {
                    halfStream.remove(entryId);
                    removed++;
                }
                if (removed >= SWEEP_BATCH_SIZE) {
                    break;
                }
            }
        } catch (RuntimeException ex) {
            LOG.warn("Sweep orphan halves failed for txGroup={}: {}", txGroup, ex.getMessage());
        }
        if (removed > 0) {
            LOG.info("Swept {} orphan half entries: txGroup={}", removed, txGroup);
        }
        return removed;
    }

    private static long extractEntryTimestampMs(StreamMessageId entryId) {
        // StreamMessageId 格式 "{timestampMs}-{sequence}"，本方法稳健地按 "-" 切分
        try {
            String idStr = String.valueOf(entryId);
            int dash = idStr.indexOf('-');
            if (dash <= 0) {
                return 0L;
            }
            return Long.parseLong(idStr.substring(0, dash));
        } catch (NumberFormatException ex) {
            return 0L;
        }
    }
}
