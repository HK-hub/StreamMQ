/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.redisson.api.RScript;
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
 * <p><b>R2-2 清理吞吐与公平性：</b>
 *
 * <ol>
 *   <li><b>分页扫描 + 游标跨调用保留：</b>终态清理改用 {@code HSCAN}（{@code MATCH *<suffix>}）并保留游标；
 *       孤儿半消息清理同样按游标分页推进。此前两者都是「固定取最早 N 条」，头部被长期存活/长事务占用时尾部条目 永远扫不到（饿死）；
 *   <li><b>批量上限可配且默认提高一个数量级：</b>单轮清理上限从固定 128 提升到 {@link #DEFAULT_SWEEP_BATCH_SIZE} （1280），可通过
 *       {@link #setSweepBatchSize(int)} 调整；低事务量部署可调小以降低单轮阻塞；
 *   <li><b>单轮往返 O(1)：</b>过期字段的删除用一条 Lua（{@code HDEL} 多字段）批量下发，不再逐字段往返。
 * </ol>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class TransactionRetentionSweeper {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionRetentionSweeper.class);

    /**
     * 单次清理的最大字段数/条目数（避免大 Hash / 长 Stream 上的长阻塞）。
     *
     * <p>R2-2②：此前固定 128 且每 10 轮才执行一次，在数万事务/天的部署下清理速度远低于写入速度，txstate Hash 无界增长。默认提升一个数量级到
     * 1280（可配），并配合游标分页保证「每轮都在前进」。
     */
    public static final int DEFAULT_SWEEP_BATCH_SIZE = 1280;

    /** HSCAN 起始游标（与 Redis 约定一致：0 表示从头/结束） */
    private static final String SCAN_START_CURSOR = "0";

    /** 单次调用内允许的最大 HSCAN 页数：防止 MATCH 命中率极低时单轮调用长时间占用扫描线程 */
    private static final int MAX_SCAN_PAGES_PER_CALL = 64;

    /** txstate Hash 终态匹配模式：{@code *.done} */
    private static final String TERMINAL_FIELD_PATTERN =
            "*" + StreamMQConstants.TX_FIELD_DONE_SUFFIX;

    /**
     * Lua：单页 HSCAN + 过期终态字段批量删除（R2-2②）。
     *
     * <p>过往实现一次性 {@code HGETALL} 整个 Hash 再删最多 128 条，既物化全表又清不动高事务量部署；现在用 HSCAN 分页（服务端游标返回给 Java
     * 跨调用保留），并在一页内直接 HDEL 过期字段——单次往返，往返次数与 页数成正比而非字段数。
     *
     * <p>KEYS[1] = txstate Hash key；ARGV[1]=cursor, ARGV[2]=MATCH 模式, ARGV[3]=COUNT,
     * ARGV[4]=保留期截止时间戳, ARGV[5]=done 字段后缀, ARGV[6..9]=target/halfId/failureReason/done 辅助字段后缀。返回
     * {@code {cursor, removed}}。
     */
    static final String LUA_SWEEP_EXPIRED_TERMINAL =
            "local res = redis.call('HSCAN', KEYS[1], ARGV[1], 'MATCH', ARGV[2], 'COUNT',"
                    + " ARGV[3]);"
                    + "local cursor = res[1];"
                    + "local fields = res[2];"
                    + "local removed = 0;"
                    + "for i = 1, #fields, 2 do"
                    + "  local field = fields[i];"
                    + "  local doneTime = tonumber(fields[i + 1]);"
                    + "  if doneTime ~= nil and doneTime < tonumber(ARGV[4]) then"
                    + "    local txId = string.sub(field, 1, #field - #ARGV[5]);"
                    + "    local deleted = redis.call('HDEL', KEYS[1], txId, txId .. ARGV[6],"
                    + " txId .. ARGV[7], txId .. ARGV[8], txId .. ARGV[9]);"
                    + "    if deleted > 0 then removed = removed + 1; end;"
                    + "  end;"
                    + "end;"
                    + "return { cursor, removed };";

    private final RedissonClient redisson;
    private final String namespace;
    private volatile long txStateRetentionMs = TransactionScanner.DEFAULT_TX_STATE_RETENTION_MS;
    private volatile long orphanHalfRetentionMs =
            TransactionScanner.DEFAULT_ORPHAN_HALF_RETENTION_MS;
    private volatile int sweepBatchSize = DEFAULT_SWEEP_BATCH_SIZE;

    /** 终态清理的 HSCAN 游标（按 txGroup 保留，跨调用推进——避免固定取头导致尾部过期字段饿死） */
    private final ConcurrentMap<String, String> terminalScanCursors = new ConcurrentHashMap<>();

    /** 孤儿半消息清理的 XRANGE 游标（按 txGroup 保留，跨调用推进） */
    private final ConcurrentMap<String, StreamMessageId> orphanScanCursors =
            new ConcurrentHashMap<>();

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
     * 设置单轮清理的最大条目数（终态字段与孤儿半消息共用）。
     *
     * @param size 批量上限，必须 &gt; 0
     */
    public void setSweepBatchSize(int size) {
        if (size > 0) {
            this.sweepBatchSize = size;
        }
    }

    /** 返回单轮清理的最大条目数。 */
    public int getSweepBatchSize() {
        return sweepBatchSize;
    }

    /**
     * 维护任务：清除超过保留期的终态字段，防止 txstate Hash 随事务量线性增长。
     *
     * <p><b>分页与吞吐（R2-2②）：</b>HSCAN（{@code MATCH *.done}）按 {@code sweepBatchSize} 分页，游标跨调用
     * 保留——若某轮未扫完（页数预算耗尽或被删除条目达到批量上限），下一轮从上次游标继续，不会永远只清理头部； 删除在服务端随页完成（单条 Lua），单轮往返 O(页数)。
     *
     * @param txGroup 事务组名
     * @return 本轮删除的终态事务数
     */
    public int sweepExpiredTerminalStates(String txGroup) {
        String stateHashKey = StreamMQKeys.transactionStateHash(namespace, txGroup);
        long cutoff = System.currentTimeMillis() - txStateRetentionMs;
        String cursor = terminalScanCursors.getOrDefault(txGroup, SCAN_START_CURSOR);
        int removed = 0;
        int pages = 0;
        do {
            List<Object> result = sweepTerminalPage(stateHashKey, cursor, cutoff);
            if (result == null || result.size() < 2) {
                break;
            }
            cursor = String.valueOf(result.get(0));
            removed += (int) parseScriptLong(result.get(1));
            pages++;
        } while (removed < sweepBatchSize
                && pages < MAX_SCAN_PAGES_PER_CALL
                && !SCAN_START_CURSOR.equals(cursor));
        // 游标跨调用保留：本轮未扫完时下轮从这里继续（头部恒定占用时尾部仍可最终被清理）
        terminalScanCursors.put(txGroup, Objects.isNull(cursor) ? SCAN_START_CURSOR : cursor);
        if (removed > 0) {
            LOG.info(
                    "Swept {} expired terminal txstate entries (pages={}): txGroup={}",
                    removed,
                    pages,
                    txGroup);
        }
        return removed;
    }

    /** 执行一页「HSCAN + 删除过期终态字段」的服务端脚本（单次往返）。 */
    private List<Object> sweepTerminalPage(String stateHashKey, String cursor, long cutoff) {
        return redisson.getScript(StringCodec.INSTANCE)
                .eval(
                        RScript.Mode.READ_WRITE,
                        LUA_SWEEP_EXPIRED_TERMINAL,
                        RScript.ReturnType.MULTI,
                        Collections.singletonList(stateHashKey),
                        cursor,
                        TERMINAL_FIELD_PATTERN,
                        sweepBatchSize,
                        cutoff,
                        StreamMQConstants.TX_FIELD_DONE_SUFFIX,
                        StreamMQConstants.TX_FIELD_TARGET_SUFFIX,
                        StreamMQConstants.TX_FIELD_HALF_ID_SUFFIX,
                        StreamMQConstants.TX_FIELD_FAILURE_REASON_SUFFIX,
                        StreamMQConstants.TX_FIELD_DONE_SUFFIX);
    }

    /**
     * 维护任务：清除孤儿半消息（half Stream 中超过保留期且无状态引用的条目）。
     *
     * <p><b>游标分页（R2-2③）：</b>XRANGE 从上次游标推进（{@code count = sweepBatchSize}），不再固定取最早 N
     * 条——头部被长期存活事务占用时，尾部孤儿条目仍会被扫到；扫到流尾后游标复位，下轮从头开始。
     *
     * @param txGroup 事务组名
     * @return 本轮删除的孤儿半消息数
     */
    public int sweepOrphanHalves(String txGroup) {
        String halfStreamKey = StreamMQKeys.halfStream(namespace, txGroup);
        RStream<String, String> halfStream =
                redisson.getStream(halfStreamKey, StringCodec.INSTANCE);
        long cutoff = System.currentTimeMillis() - orphanHalfRetentionMs;
        int removed = 0;
        try {
            StreamMessageId start = orphanScanCursors.getOrDefault(txGroup, StreamMessageId.MIN);
            Map<StreamMessageId, Map<String, String>> pending =
                    halfStream.range(sweepBatchSize, start, StreamMessageId.MAX);
            if (pending == null || pending.isEmpty()) {
                // 已扫到流尾：游标复位，下一轮重新从头扫描（新写入的条目才会被重新纳入窗口）
                orphanScanCursors.put(txGroup, StreamMessageId.MIN);
                return 0;
            }
            StreamMessageId lastScanned = null;
            for (var entry : pending.entrySet()) {
                StreamMessageId entryId = entry.getKey();
                lastScanned = entryId;
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
                if (removed >= sweepBatchSize) {
                    break;
                }
            }
            // 游标推进到窗口末尾之后：本轮已检查过的条目不再重复占用下一轮窗口
            boolean reachedTail = pending.size() < sweepBatchSize;
            orphanScanCursors.put(
                    txGroup,
                    reachedTail || Objects.isNull(lastScanned)
                            ? StreamMessageId.MIN
                            : nextAfter(lastScanned));
        } catch (RuntimeException ex) {
            LOG.warn("Sweep orphan halves failed for txGroup={}: {}", txGroup, ex.getMessage());
        }
        if (removed > 0) {
            LOG.info("Swept {} orphan half entries: txGroup={}", removed, txGroup);
        }
        return removed;
    }

    /** 返回严格递增于给定 id 的下一个 Stream id（作为 XRANGE 的闭区间起点）。 */
    private static StreamMessageId nextAfter(StreamMessageId id) {
        String idStr = String.valueOf(id);
        int dash = idStr.indexOf('-');
        if (dash <= 0) {
            return StreamMessageId.MIN;
        }
        try {
            long time = Long.parseLong(idStr.substring(0, dash));
            long seq = Long.parseLong(idStr.substring(dash + 1));
            if (seq == Long.MAX_VALUE) {
                return StreamMessageId.MIN;
            }
            return new StreamMessageId(time, seq + 1);
        } catch (NumberFormatException ex) {
            return StreamMessageId.MIN;
        }
    }

    /** 将脚本返回值稳健转为 long（协议漂移时返回 0，绝不抛 ClassCastException 中断清理）。 */
    private static long parseScriptLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String str) {
            try {
                return Long.parseLong(str);
            } catch (NumberFormatException ignored) {
                return 0L;
            }
        }
        return 0L;
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
