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
        if (isActive(reg.key()) || isActive(reg.key() + RETRY_FUTURE_SUFFIX)) {
            return;
        }
        if (reg.getType() == ListenerType.AUTO_ACK && !reg.isDlqMode()) {
            int concurrency = effectiveConcurrency(reg);
            submitPrimaryWithConcurrency(reg, concurrency);
            submitRetryWithConcurrency(reg, concurrency);
        } else {
            Future<?> future = loopFactory.launch(reg, false, true);
            if (Objects.nonNull(futures.putIfAbsent(reg.key(), future))) {
                future.cancel(true);
            }
        }
    }

    private void submitPrimaryWithConcurrency(ListenerRegistration<?> reg, int concurrency) {
        Future<?> primary = loopFactory.launch(reg, false, true);
        if (Objects.nonNull(futures.putIfAbsent(reg.key(), primary))) {
            primary.cancel(true);
        }
        for (int i = 1; i < concurrency; i++) {
            final int idx = i;
            Future<?> f = loopFactory.launch(reg, false, false);
            if (Objects.nonNull(
                    futures.putIfAbsent(reg.key() + CONCURRENCY_FUTURE_SUFFIX + idx, f))) {
                f.cancel(true);
            }
        }
    }

    private void submitRetryWithConcurrency(ListenerRegistration<?> reg, int concurrency) {
        Future<?> retry = loopFactory.launch(reg, true, true);
        String retryKey = reg.key() + RETRY_FUTURE_SUFFIX;
        if (Objects.nonNull(futures.putIfAbsent(retryKey, retry))) {
            retry.cancel(true);
            return;
        }
        for (int i = 1; i < concurrency; i++) {
            final int idx = i;
            Future<?> f = loopFactory.launch(reg, true, false);
            if (Objects.nonNull(
                    futures.putIfAbsent(retryKey + CONCURRENCY_FUTURE_SUFFIX + idx, f))) {
                f.cancel(true);
            }
        }
    }

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
                || futureKey.equals(key + INFLIGHT_PROCESSOR_SUFFIX);
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
