/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 顺序消费"延迟重投"登记队列（红队审查 R1-1，容器内部实现，非公开 SPI）。
 *
 * <p><b>要解决的问题：</b>顺序消费在分片锁竞争（{@code deferShardBusy}）、DLQ 路由失败等路径上 只返回 {@code RECONSUME_LATER} 且不
 * ACK，注释声称"由 PelClaimScheduler 兜底重投"；但 PEL 认领对心跳新鲜的属主 实例直接跳过，属主存活期间该消息永远不会被重投——静默黑洞。本队列为这些消息提供
 * <b>进程内的有界延迟重投</b>；PEL 仍然是进程死亡后的兜底（本队列不持久化）。
 *
 * <p><b>语义（与主审设计一致）：</b>
 *
 * <ul>
 *   <li><b>单飞</b>：同一 messageId 在队列中最多一个条目；重投尝试期间被再次登记只更新该条目的 到期时间与退避轮次，绝不产生重复投递。
 *   <li><b>有界</b>：每注册容量上限（{@link #DEFAULT_MAX_ENTRIES_PER_REGISTRATION}）。溢出时不登记、 消息保留在 PEL，并打限频
 *       ERROR + 计数（绝不静默）。
 *   <li><b>退避</b>：锁竞争路径 1s 起、每次翻倍、上限 30s；消费端 {@code defer(delay)} 路径按 delay 延迟。
 *   <li><b>同分片 FIFO</b>：按分片维护登记顺序，重投尝试严格按登记顺序（head-of-line），避免同分片 消息被乱序处理。
 *   <li><b>不消耗重试预算</b>：重投走与正常消息相同的 {@code MessageProcessor.processMessage} 路径， 成功由既有 ACK 路径
 *       ACK；锁仍繁忙则再次登记退避。
 * </ul>
 *
 * <p><b>执行位置：</b>由该注册的 primary 读循环在每个迭代节拍调用 {@link #drainDue}，不新增线程、 不阻塞其它注册（每次最多 {@link
 * #MAX_DRAIN_PER_TICK} 条）。容器停止时 {@link #clearAll()} 清空——剩余消息由 PEL 认领兜底。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
final class OrderlyDeferredRetryQueue {

    private static final Logger LOG = LoggerFactory.getLogger(OrderlyDeferredRetryQueue.class);

    /** 首次退避间隔（毫秒） */
    static final long INITIAL_BACKOFF_MILLIS = 1_000L;

    /** 退避上限（毫秒） */
    static final long MAX_BACKOFF_MILLIS = 30_000L;

    /** 每注册延迟重投条目容量上限（超出后保留在 PEL，绝不静默丢弃） */
    static final int DEFAULT_MAX_ENTRIES_PER_REGISTRATION = 512;

    /** 溢出 ERROR 日志的限频间隔（毫秒） */
    static final long OVERFLOW_WARN_INTERVAL_MILLIS = 10_000L;

    /** 单轮最多重投条数：限制单次循环节拍的耗时，保持循环对暂停/停止的响应性 */
    static final int MAX_DRAIN_PER_TICK = 16;

    /** 条目种类：REPROCESS = 走正常消费管线；DLQ_ROUTE = 只重试 DLQ 转投（重试预算已耗尽） */
    enum Kind {
        REPROCESS,
        DLQ_ROUTE
    }

    /** 延迟重投条目（线程安全：字段 volatile，结构性修改持 {@link #lock}）。 */
    static final class Entry {

        private final Message<?> message;
        private final String messageId;
        private final int shardIndex;
        private final long sequence;
        private volatile Kind kind;
        private volatile long nextAttemptAtMillis;
        private volatile long deferEpoch;
        private volatile boolean attempting;

        private Entry(
                Message<?> message,
                String messageId,
                int shardIndex,
                long sequence,
                Kind kind,
                long nextAttemptAtMillis) {
            this.message = message;
            this.messageId = messageId;
            this.shardIndex = shardIndex;
            this.sequence = sequence;
            this.kind = kind;
            this.nextAttemptAtMillis = nextAttemptAtMillis;
        }

        Message<?> message() {
            return message;
        }

        String messageId() {
            return messageId;
        }

        int shardIndex() {
            return shardIndex;
        }

        long sequence() {
            return sequence;
        }

        Kind kind() {
            return kind;
        }

        long nextAttemptAtMillis() {
            return nextAttemptAtMillis;
        }

        /** 已发生的"再次登记"次数（0 = 仅登记过一次；用于退避与"本轮是否被重新登记"判定）。 */
        long deferEpoch() {
            return deferEpoch;
        }
    }

    /** 重投执行器：由读循环提供，负责走正常消费管线或只重试 DLQ 转投。 */
    @FunctionalInterface
    interface DeferredRetryDispatcher {
        void dispatch(Entry entry, ListenerRegistration<?> reg, StreamMQListener listener);
    }

    /** 单个注册的条目表（按登记顺序的 LinkedHashMap，分片内顺序由 {@link Entry#sequence()} 保证）。 */
    private static final class RegistrationEntries {
        private final Map<String, Entry> byMessageId = new LinkedHashMap<>();
        private long lastOverflowWarnMillis;
    }

    /** 注册键 → 条目表。 */
    private final ConcurrentMap<String, RegistrationEntries> registrations =
            new ConcurrentHashMap<>();

    /** 结构性修改锁（登记 / 选取 / 移除）。 */
    private final Object lock = new Object();

    private final AtomicLong sequenceGenerator = new AtomicLong();

    private final AtomicLong registeredCount = new AtomicLong();
    private final AtomicLong redeliveryCount = new AtomicLong();
    private final AtomicLong rejectedCount = new AtomicLong();

    private volatile int maxEntriesPerRegistration = DEFAULT_MAX_ENTRIES_PER_REGISTRATION;

    /** 仅测试/高级调优：调整每注册容量上限（<=0 忽略）。 */
    void setMaxEntriesPerRegistration(int maxEntries) {
        if (maxEntries > 0) {
            this.maxEntriesPerRegistration = maxEntries;
        }
    }

    // ===================== 登记（三条触发路径） =====================

    /**
     * 登记"分片锁竞争"延迟重投（退避 1s 起、翻倍、上限 30s）。
     *
     * @return true 已登记（或已存在条目被更新）；false 容量溢出（消息保留在 PEL，已打限频 ERROR）
     */
    boolean deferShardBusy(ListenerRegistration<?> reg, Message<?> message) {
        return defer(reg, message, Kind.REPROCESS, 0L);
    }

    /**
     * 登记消费端显式 {@code ConsumeAction.defer(delay)} 延迟重投（按 delay 延迟，不消耗重试预算）。
     *
     * @param delayMillis 消费端声明的延迟（毫秒）
     * @return true 已登记（或已存在条目被更新）；false 容量溢出（消息保留在 PEL，已打限频 ERROR）
     */
    boolean deferByAction(ListenerRegistration<?> reg, Message<?> message, long delayMillis) {
        return defer(reg, message, Kind.REPROCESS, Math.max(1L, delayMillis));
    }

    /**
     * 登记"重试预算已耗尽但 DLQ 转投失败"的重试（只重试 DLQ 转投，不重新执行 handler）。
     *
     * @return true 已登记（或已存在条目被更新）；false 容量溢出（消息保留在 PEL，已打限频 ERROR）
     */
    boolean deferDlqRouteFailure(ListenerRegistration<?> reg, Message<?> message) {
        return defer(reg, message, Kind.DLQ_ROUTE, 0L);
    }

    private boolean defer(
            ListenerRegistration<?> reg, Message<?> message, Kind kind, long explicitDelayMillis) {
        String messageId =
                Objects.nonNull(message.getMessageId()) ? message.getMessageId().toString() : null;
        if (Objects.isNull(messageId)) {
            // 无 messageId 无法单飞去重：消息保留在 PEL，由 PEL 认领兜底（调用方已有的日志）
            LOG.debug(
                    "Deferred retry skipped: message has no messageId (stays in PEL): topic={},"
                            + " group={}, kind={}",
                    reg.getTopic(),
                    reg.getGroup(),
                    kind);
            return false;
        }
        int shardIndex = shardIndexOf(reg, message);
        long now = System.currentTimeMillis();
        RegistrationEntries entries =
                registrations.computeIfAbsent(reg.key(), key -> new RegistrationEntries());
        synchronized (lock) {
            Entry existing = entries.byMessageId.get(messageId);
            if (Objects.nonNull(existing)) {
                // 单飞：同一 messageId 只保留一个条目，仅更新到期时间 / 种类（如耗尽后转 DLQ 重试）
                existing.deferEpoch++;
                existing.kind = kind;
                existing.nextAttemptAtMillis =
                        now
                                + (explicitDelayMillis > 0
                                        ? explicitDelayMillis
                                        : backoffMillis(existing.deferEpoch));
                registeredCount.incrementAndGet();
                return true;
            }
            int max = maxEntriesPerRegistration;
            if (entries.byMessageId.size() >= max) {
                rejectedCount.incrementAndGet();
                if (now - entries.lastOverflowWarnMillis >= OVERFLOW_WARN_INTERVAL_MILLIS) {
                    entries.lastOverflowWarnMillis = now;
                    LOG.error(
                            "Deferred retry queue full ({} entries), message kept in PEL without"
                                    + " local redelivery — increase capacity or drain the backlog:"
                                    + " topic={}, group={}, messageId={}, rejectedTotal={}",
                            max,
                            reg.getTopic(),
                            reg.getGroup(),
                            messageId,
                            rejectedCount.get());
                }
                return false;
            }
            Entry entry =
                    new Entry(
                            message,
                            messageId,
                            shardIndex,
                            sequenceGenerator.incrementAndGet(),
                            kind,
                            now
                                    + (explicitDelayMillis > 0
                                            ? explicitDelayMillis
                                            : backoffMillis(0)));
            entries.byMessageId.put(messageId, entry);
            registeredCount.incrementAndGet();
            return true;
        }
    }

    /** 锁竞争退避：1s、2s、4s… 上限 {@link #MAX_BACKOFF_MILLIS}。 */
    static long backoffMillis(long deferEpoch) {
        if (deferEpoch <= 0) {
            return INITIAL_BACKOFF_MILLIS;
        }
        int shift = (int) Math.min(deferEpoch, 5);
        long delay = INITIAL_BACKOFF_MILLIS << shift;
        return Math.min(delay, MAX_BACKOFF_MILLIS);
    }

    /** 分片序号：与 {@code RedissonOrderlyShardLockManager} 的路由公式保持一致（仅用于 FIFO 归组）。 */
    private static int shardIndexOf(ListenerRegistration<?> reg, Message<?> message) {
        int shardCount = reg.getShardCount();
        if (shardCount <= 0) {
            return 0;
        }
        String shardingKey = message.getShardingKey();
        if (Objects.isNull(shardingKey)) {
            shardingKey = "";
        }
        return (shardingKey.hashCode() & 0x7fffffff) % shardCount;
    }

    // ===================== 重投（由 primary 读循环驱动） =====================

    /**
     * 取出到期条目并按"同分片登记顺序（FIFO）"重投。
     *
     * <p>每个条目在重投期间标记为 attempting（单飞）；重投过程中被再次登记（锁仍繁忙 / 再次 defer）则保留条目并 使用新的到期时间，否则本轮结束即移除（成功路径已由既有
     * ACK 路径 ACK）。
     *
     * <p><b>同分片 FIFO 的"卡头"语义：</b>按登记序号整体扫描；同一分片只有前序条目已成功（移除）才继续尝试
     * 后续条目——前序条目未到期或再次被推迟时，该分片的后续条目本轮一律不尝试（head-of-line）， 从而绝不出现"同分片后登记的消息先于前一条被处理"。不同分片互不影响。
     */
    void drainDue(
            ListenerRegistration<?> reg,
            long nowMillis,
            StreamMQListener listener,
            DeferredRetryDispatcher dispatcher) {
        RegistrationEntries entries = registrations.get(reg.key());
        if (Objects.isNull(entries)) {
            return;
        }
        List<Entry> candidates;
        synchronized (lock) {
            candidates = orderedCandidates(entries);
        }
        java.util.Set<Integer> blockedShards = new java.util.HashSet<>();
        int attempted = 0;
        for (Entry entry : candidates) {
            if (attempted >= MAX_DRAIN_PER_TICK) {
                break;
            }
            if (blockedShards.contains(entry.shardIndex)) {
                continue;
            }
            boolean attempting;
            synchronized (lock) {
                // 单飞：正被尝试（含并发 drain）或未到期的条目本轮跳过
                attempting = entry.attempting;
                if (!attempting && entry.nextAttemptAtMillis > nowMillis) {
                    blockedShards.add(entry.shardIndex);
                    continue;
                }
                if (!attempting) {
                    entry.attempting = true;
                }
            }
            if (attempting) {
                continue;
            }
            attempted++;
            long epochBefore;
            synchronized (lock) {
                epochBefore = entry.deferEpoch;
            }
            try {
                redeliveryCount.incrementAndGet();
                dispatcher.dispatch(entry, reg, listener);
            } catch (Throwable t) {
                // 兜底：重投本身抛异常（理论上 processMessage/handleFailure 已内部兜底）。
                // 条目保留并退避，绝不因一次异常把消息从"唯一的进展通道"中静默移除。
                LOG.error(
                        "Deferred retry dispatch failed, entry re-scheduled with backoff:"
                                + " topic={}, group={}, messageId={}",
                        reg.getTopic(),
                        reg.getGroup(),
                        entry.messageId,
                        t);
                synchronized (lock) {
                    entry.deferEpoch++;
                    entry.nextAttemptAtMillis =
                            System.currentTimeMillis() + backoffMillis(entry.deferEpoch);
                }
            } finally {
                boolean kept;
                synchronized (lock) {
                    entry.attempting = false;
                    kept = entry.deferEpoch != epochBefore;
                    if (!kept) {
                        entries.byMessageId.remove(entry.messageId, entry);
                    }
                }
                if (kept) {
                    // 本条被再次推迟：同分片后续条目本轮不得越序尝试
                    blockedShards.add(entry.shardIndex);
                }
            }
        }
    }

    /** 按登记序号（FIFO）返回全部候选条目。 */
    private static List<Entry> orderedCandidates(RegistrationEntries entries) {
        List<Entry> candidates = new ArrayList<>(entries.byMessageId.values());
        candidates.sort(Comparator.comparingLong(Entry::sequence));
        return candidates;
    }

    // ===================== 生命周期与可观测 =====================

    /** 清空指定注册的条目（注册被替换/注销时调用；剩余消息由 PEL 认领兜底）。 */
    void clear(ListenerRegistration<?> reg) {
        registrations.remove(reg.key());
    }

    /** 容器停止时清空全部条目（不持久化；剩余消息由 PEL 认领兜底）。 */
    void clearAll() {
        registrations.clear();
    }

    /** 未处理条目总数（可观测 / 测试）。 */
    int size() {
        synchronized (lock) {
            int total = 0;
            for (RegistrationEntries entries : registrations.values()) {
                total += entries.byMessageId.size();
            }
            return total;
        }
    }

    /** 指定注册的未处理条目数（测试）。 */
    int size(ListenerRegistration<?> reg) {
        RegistrationEntries entries = registrations.get(reg.key());
        synchronized (lock) {
            return Objects.isNull(entries) ? 0 : entries.byMessageId.size();
        }
    }

    /** 已到期条目数（测试）。 */
    int dueCount(ListenerRegistration<?> reg, long nowMillis) {
        RegistrationEntries entries = registrations.get(reg.key());
        synchronized (lock) {
            if (Objects.isNull(entries)) {
                return 0;
            }
            java.util.Set<Integer> blockedShards = new java.util.HashSet<>();
            int count = 0;
            for (Entry entry : orderedCandidates(entries)) {
                if (blockedShards.contains(entry.shardIndex)) {
                    continue;
                }
                if (entry.attempting || entry.nextAttemptAtMillis > nowMillis) {
                    blockedShards.add(entry.shardIndex);
                    continue;
                }
                count++;
            }
            return count;
        }
    }

    /** 累计登记次数（含重复登记）。 */
    long registeredCount() {
        return registeredCount.get();
    }

    /** 累计重投尝试次数。 */
    long redeliveryCount() {
        return redeliveryCount.get();
    }

    /** 累计容量溢出拒绝次数（消息保留在 PEL）。 */
    long rejectedCount() {
        return rejectedCount.get();
    }
}
