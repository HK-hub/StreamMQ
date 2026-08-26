/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.filter.ConsumerFilter;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.policy.ConsumerGroupManager;
import io.github.streammq.core.policy.RetryAndDlqHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * {@link RegistrationStore} 默认实现：全部基于 {@link ConcurrentHashMap}。
 *
 * <p>组管理器的注销失败仅记录、不抛出——心跳超时后由回收任务兜底，不阻塞其余清理。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultRegistrationStore implements RegistrationStore {

    private final ConcurrentMap<String, ListenerRegistration<?>> registrations =
            new ConcurrentHashMap<>();

    private final ConcurrentMap<String, RetryAndDlqHandler> handlers = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, List<ConsumerFilter>> filters = new ConcurrentHashMap<>();

    // ===================== 注册表 =====================

    @Override
    public Collection<ListenerRegistration<?>> registrations() {
        return registrations.values();
    }

    @Override
    public int registrationCount() {
        return registrations.size();
    }

    @Override
    public ListenerRegistration<?> registration(String key) {
        return registrations.get(key);
    }

    @Override
    public ListenerRegistration<?> putRegistration(ListenerRegistration<?> reg) {
        return registrations.put(reg.key(), reg);
    }

    @Override
    public ListenerRegistration<?> removeRegistration(String key) {
        return registrations.remove(key);
    }

    @Override
    public List<ListenerRegistration<?>> snapshotRegistrations() {
        return new ArrayList<>(registrations.values());
    }

    // ===================== per-consumer 处理器 =====================

    @Override
    public RetryAndDlqHandler handler(String key) {
        return handlers.get(key);
    }

    @Override
    public void putHandler(String key, RetryAndDlqHandler handler) {
        handlers.put(key, handler);
    }

    @Override
    public Collection<RetryAndDlqHandler> handlers() {
        return handlers.values();
    }

    @Override
    public void removeHandler(String key) {
        handlers.remove(key);
    }

    // ===================== per-consumer 过滤器链缓存 =====================

    @Override
    public List<ConsumerFilter> filters(String key) {
        return filters.get(key);
    }

    @Override
    public void putFilters(String key, List<ConsumerFilter> chain) {
        filters.put(key, chain);
    }

    @Override
    public void removeFilters(String key) {
        filters.remove(key);
    }

    // ===================== 组管理器 =====================

    private final ConcurrentMap<String, ConsumerGroupManager> groupManagers =
            new ConcurrentHashMap<>();

    @Override
    public ConsumerGroupManager groupManager(String key) {
        return groupManagers.get(key);
    }

    @Override
    public void putGroupManager(String key, ConsumerGroupManager manager) {
        groupManagers.put(key, manager);
    }

    @Override
    public Collection<ConsumerGroupManager> groupManagers() {
        return groupManagers.values();
    }

    /** 注销并移除指定组管理器（注销失败仅记录）。 */
    public void removeAndUnregisterGroupManager(String key) {
        ConsumerGroupManager manager = groupManagers.remove(key);
        if (manager != null) {
            try {
                manager.unregister();
            } catch (RuntimeException ignored) {
                // 心跳超时后由回收任务兜底
            }
        }
    }

    /** 注销全部组管理器（容器 stop）。 */
    public void clearGroupManagers() {
        for (ConsumerGroupManager manager : groupManagers.values()) {
            try {
                manager.unregister();
            } catch (RuntimeException ignored) {
                // 同上
            }
        }
        groupManagers.clear();
    }

    public Map<String, ListenerRegistration<?>> registrationMap() {
        return Collections.unmodifiableMap(registrations);
    }
}
