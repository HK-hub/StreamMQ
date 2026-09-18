/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.ListenerType;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link ConsumeLoopSupervisor} 默认实现。并发度决策同样收拢于此。 */
public class DefaultConsumeLoopSupervisor implements ConsumeLoopSupervisor {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultConsumeLoopSupervisor.class);

    static final String RETRY_FUTURE_SUFFIX = ":retry";
    static final String CONCURRENCY_FUTURE_SUFFIX = ":cc-";
    static final String INFLIGHT_PROCESSOR_SUFFIX = ":inflight-processor";

    private final ConcurrentMap<String, Future<?>> futures = new ConcurrentHashMap<>();
    private final LoopFactory loopFactory;

    public DefaultConsumeLoopSupervisor(LoopFactory loopFactory) {
        this.loopFactory = Objects.requireNonNull(loopFactory, "loopFactory");
    }

    @Override
    public void submitLoops(ListenerRegistration<?> reg) {
        // 幂等守卫必须覆盖全部循环形态：基础、retry、以及 :cc-N 并发扩展循环——
        // 此前仅检查前两者，consumeThreadMin>1 时部分并发循环仍在运行也会重复提交
        //
        // 清理顺序不可颠倒（B-09 修复）：putIfAbsent 不会覆盖已结束循环的陈旧登记项，
        // 若先判 hasActiveLoops/先提交，陈旧项会让本注册被误判为"已在消费"而跳过提交，
        // 或让刚提交的新循环被 cancel 且无任何日志——该注册永久静默不消费。
        evictDoneFutures(reg.key());
        if (hasActiveLoops(reg.key())) {
            return;
        }
        if (reg.getType() == ListenerType.AUTO_ACK && !reg.isDlqMode()) {
            int concurrency = effectiveConcurrency(reg);
            submitPrimaryWithConcurrency(reg, concurrency);
            submitRetryWithConcurrency(reg, concurrency);
        } else {
            Future<?> future = loopFactory.launch(reg, false, true, 0);
            registerLoopFuture(reg.key(), future);
        }
    }

    /** 该注册的任一读循环（基础 / retry / 并发扩展）是否仍在运行。 */
    private boolean hasActiveLoops(String baseKey) {
        for (Map.Entry<String, Future<?>> entry : futures.entrySet()) {
            if (isLoopKeyOf(entry.getKey(), baseKey) && !entry.getValue().isDone()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 清理该注册已完成（正常结束 / 异常终止 / 已取消）的循环登记项。
     *
     * <p>读循环登记表用 {@code putIfAbsent} 维护幂等，但 putIfAbsent 不会覆盖已结束循环遗留的陈旧条目： 陈旧条目既不会被 {@link
     * #hasActiveLoops} 视为活跃（{@code isDone()} 为 true），又会在 {@code putIfAbsent}
     * 时把新提交的循环顶掉——表现为该注册永久不再消费且无日志。按注册键前缀 （基础 / {@code :retry} / {@code
     * :cc-N}）过滤，仅清理已结束项，绝不触碰仍在运行的循环。
     */
    private void evictDoneFutures(String baseKey) {
        for (Iterator<Map.Entry<String, Future<?>>> it = futures.entrySet().iterator();
                it.hasNext(); ) {
            Map.Entry<String, Future<?>> entry = it.next();
            if (isLoopKeyOf(entry.getKey(), baseKey) && entry.getValue().isDone()) {
                it.remove();
            }
        }
    }

    /** 判断登记键是否属于该注册的读循环（基础 / retry / 并发扩展；inflight 泵另见 {@link #isInflightPumpOf}）。 */
    private static boolean isLoopKeyOf(String futureKey, String baseKey) {
        String retryKey = baseKey + RETRY_FUTURE_SUFFIX;
        return futureKey.equals(baseKey)
                || futureKey.equals(retryKey)
                || futureKey.startsWith(baseKey + CONCURRENCY_FUTURE_SUFFIX)
                || futureKey.startsWith(retryKey + CONCURRENCY_FUTURE_SUFFIX);
    }

    /**
     * 登记循环 Future；同键已有活跃登记时不覆盖，取消新提交的循环并 WARN。
     *
     * <p>调用前必须已执行 {@link #evictDoneFutures(String)}，因此此处的冲突只可能来自与其它线程 并发提交同一注册。取消必须留痕：静默 cancel
     * 会让"提交成功但永不消费"无从排查。
     *
     * @return true 新循环登记成功；false 冲突（新循环已被取消）
     */
    private boolean registerLoopFuture(String key, Future<?> future) {
        if (Objects.nonNull(futures.putIfAbsent(key, future))) {
            LOG.warn(
                    "Duplicate consume loop cancelled: key={} — an active loop with the same"
                            + " registration is already running (finished futures are evicted"
                            + " before submission, so this is a concurrent submit race)",
                    key);
            future.cancel(true);
            return false;
        }
        return true;
    }

    private void submitPrimaryWithConcurrency(ListenerRegistration<?> reg, int concurrency) {
        Future<?> primary = loopFactory.launch(reg, false, true, 0);
        registerLoopFuture(reg.key(), primary);
        for (int i = 1; i < concurrency; i++) {
            final int idx = i;
            Future<?> f = loopFactory.launch(reg, false, false, idx);
            registerLoopFuture(reg.key() + CONCURRENCY_FUTURE_SUFFIX + idx, f);
        }
    }

    private void submitRetryWithConcurrency(ListenerRegistration<?> reg, int concurrency) {
        Future<?> retry = loopFactory.launch(reg, true, true, 0);
        String retryKey = reg.key() + RETRY_FUTURE_SUFFIX;
        if (!registerLoopFuture(retryKey, retry)) {
            return;
        }
        for (int i = 1; i < concurrency; i++) {
            final int idx = i;
            Future<?> f = loopFactory.launch(reg, true, false, idx);
            registerLoopFuture(retryKey + CONCURRENCY_FUTURE_SUFFIX + idx, f);
        }
    }

    /**
     * 登记 inflight 泵 Future（供 unregister/stop 取消）。
     *
     * <p>{@code key} 必须按循环唯一（调用方传入 {@code reg.key()[":retry"]#loopIndex}）， 否则同一注册的多个并发泵互相覆盖登记项，
     * 先前的泵泄漏为无法取消的孤儿线程。
     */
    @Override
    public void registerInflightPump(String key, Future<?> pumpFuture) {
        futures.put(key + INFLIGHT_PROCESSOR_SUFFIX, pumpFuture);
    }

    @Override
    public void cancelForRegistration(String key) {
        List<Future<?>> cancelled = new ArrayList<>();
        for (Iterator<Map.Entry<String, Future<?>>> it = futures.entrySet().iterator();
                it.hasNext(); ) {
            Map.Entry<String, Future<?>> entry = it.next();
            if (belongsTo(entry.getKey(), key)) {
                cancelled.add(entry.getValue());
                it.remove();
            }
        }
        for (Future<?> f : cancelled) {
            f.cancel(true);
        }
    }

    @Override
    public void cancelAll() {
        for (Future<?> f : futures.values()) {
            f.cancel(true);
        }
        futures.clear();
    }

    private boolean belongsTo(String futureKey, String key) {
        return futureKey.equals(key)
                || futureKey.startsWith(key + CONCURRENCY_FUTURE_SUFFIX)
                || futureKey.equals(key + RETRY_FUTURE_SUFFIX)
                || futureKey.startsWith(key + RETRY_FUTURE_SUFFIX + CONCURRENCY_FUTURE_SUFFIX)
                || isInflightPumpOf(futureKey, key)
                || isInflightPumpOf(futureKey, key + RETRY_FUTURE_SUFFIX);
    }

    /**
     * 判断泵 Future 键是否属于给定循环前缀。
     *
     * <p>泵键形如 {@code {loopKey}#{idx}:inflight-processor}（loopKey = 注册键或注册键+{@code :retry}）；{@code
     * '#'} 不可能出现在注册键中，按前缀匹配不会误伤其它注册。
     */
    private static boolean isInflightPumpOf(String futureKey, String prefix) {
        return futureKey.startsWith(prefix + "#") && futureKey.endsWith(INFLIGHT_PROCESSOR_SUFFIX);
    }

    private boolean isActive(String futureKey) {
        Future<?> f = futures.get(futureKey);
        return Objects.nonNull(f) && !f.isDone();
    }

    /**
     * 计算注册的并发消费循环数：仅 CONCURRENT 集群消费生效，取 {@code getConsumeThreads()} 夹取到 {@code [1, 64]}；顺序 / DLQ /
     * 广播固定为 1。
     */
    static int effectiveConcurrency(ListenerRegistration<?> reg) {
        if (reg.getType() != ListenerType.AUTO_ACK
                || reg.isDlqMode()
                || reg.getConsumeMode() == ConsumeMode.BROADCASTING
                || reg.getType() == ListenerType.ORDERLY) {
            return 1;
        }
        return Math.max(
                1, Math.min(StreamMQConstants.DEFAULT_CONSUME_THREAD_MAX, reg.getConsumeThreads()));
    }
}
