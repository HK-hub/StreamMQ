/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

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

/** {@link ConsumeLoopSupervisor} 默认实现。并发度决策同样收拢于此。 */
public class DefaultConsumeLoopSupervisor implements ConsumeLoopSupervisor {

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
        if (hasActiveLoops(reg.key())) {
            return;
        }
        if (reg.getType() == ListenerType.AUTO_ACK && !reg.isDlqMode()) {
            int concurrency = effectiveConcurrency(reg);
            submitPrimaryWithConcurrency(reg, concurrency);
            submitRetryWithConcurrency(reg, concurrency);
        } else {
            Future<?> future = loopFactory.launch(reg, false, true, 0);
            if (Objects.nonNull(futures.putIfAbsent(reg.key(), future))) {
                future.cancel(true);
            }
        }
    }

    /** 该注册的任一读循环（基础 / retry / 并发扩展）是否仍在运行。 */
    private boolean hasActiveLoops(String baseKey) {
        String retryKey = baseKey + RETRY_FUTURE_SUFFIX;
        for (Map.Entry<String, Future<?>> entry : futures.entrySet()) {
            String k = entry.getKey();
            boolean isLoop =
                    k.equals(baseKey)
                            || k.equals(retryKey)
                            || k.startsWith(baseKey + CONCURRENCY_FUTURE_SUFFIX)
                            || k.startsWith(retryKey + CONCURRENCY_FUTURE_SUFFIX);
            if (isLoop && !entry.getValue().isDone()) {
                return true;
            }
        }
        return false;
    }

    private void submitPrimaryWithConcurrency(ListenerRegistration<?> reg, int concurrency) {
        Future<?> primary = loopFactory.launch(reg, false, true, 0);
        if (Objects.nonNull(futures.putIfAbsent(reg.key(), primary))) {
            primary.cancel(true);
        }
        for (int i = 1; i < concurrency; i++) {
            final int idx = i;
            Future<?> f = loopFactory.launch(reg, false, false, idx);
            if (Objects.nonNull(
                    futures.putIfAbsent(reg.key() + CONCURRENCY_FUTURE_SUFFIX + idx, f))) {
                f.cancel(true);
            }
        }
    }

    private void submitRetryWithConcurrency(ListenerRegistration<?> reg, int concurrency) {
        Future<?> retry = loopFactory.launch(reg, true, true, 0);
        String retryKey = reg.key() + RETRY_FUTURE_SUFFIX;
        if (Objects.nonNull(futures.putIfAbsent(retryKey, retry))) {
            retry.cancel(true);
            return;
        }
        for (int i = 1; i < concurrency; i++) {
            final int idx = i;
            Future<?> f = loopFactory.launch(reg, true, false, idx);
            if (Objects.nonNull(
                    futures.putIfAbsent(retryKey + CONCURRENCY_FUTURE_SUFFIX + idx, f))) {
                f.cancel(true);
            }
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
     * 计算注册的并发消费循环数：仅 CONCURRENT 集群消费生效，取 {@code consumeThreadMin} 夹取到 {@code [1,
     * consumeThreadMax]}；顺序 / DLQ / 广播固定为 1。
     */
    static int effectiveConcurrency(ListenerRegistration<?> reg) {
        if (reg.getType() != ListenerType.AUTO_ACK
                || reg.isDlqMode()
                || reg.getConsumeMode() == ConsumeMode.BROADCASTING
                || reg.getType() == ListenerType.ORDERLY) {
            return 1;
        }
        int max = Math.max(1, reg.getConsumeThreadMax());
        return Math.max(1, Math.min(max, reg.getConsumeThreadMin()));
    }
}
