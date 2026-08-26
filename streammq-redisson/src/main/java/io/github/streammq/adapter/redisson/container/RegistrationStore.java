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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 容器注册存储：集中持有注册表与 per-consumer 策略实例缓存。
 *
 * <p>从 {@code DefaultStreamMQListenerContainer} 拆出的状态载体（God class 拆分，红队审查
 * F-02-12）：容器只负责编排生命周期与消费循环，注册/策略缓存的存取统一经由本类， 保证键语义（{@code reg.key()} / DLQ 前缀）只有一处定义。
 *
 * <p>线程安全：全部基于 {@link ConcurrentHashMap}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
final class RegistrationStore {

    private final ConcurrentMap<String, ListenerRegistration<?>> registrations =
            new ConcurrentHashMap<>();

    /** per-consumer ACK/重试/DLQ 路由处理器（按注解实例化的策略组合） */
    private final ConcurrentMap<String, RetryAndDlqHandler> handlers = new ConcurrentHashMap<>();

    /** per-consumer 过滤器链缓存（key: reg.key()，value: 预构建的过滤器列表） */
    private final ConcurrentMap<String, List<ConsumerFilter>> filters = new ConcurrentHashMap<>();

    /** 消费者组管理器（per-group 实例，管理心跳与重平衡） */
    private final ConcurrentMap<String, ConsumerGroupManager> groupManagers =
            new ConcurrentHashMap<>();

    Collection<ListenerRegistration<?>> registrations() {
        return registrations.values();
    }

    int registrationCount() {
        return registrations.size();
    }

    ListenerRegistration<?> registration(String key) {
        return registrations.get(key);
    }

    ListenerRegistration<?> putRegistration(ListenerRegistration<?> reg) {
        return registrations.put(reg.key(), reg);
    }

    ListenerRegistration<?> removeRegistration(String key) {
        return registrations.remove(key);
    }

    RetryAndDlqHandler handler(String key) {
        return handlers.get(key);
    }

    void putHandler(String key, RetryAndDlqHandler handler) {
        handlers.put(key, handler);
    }

    Collection<RetryAndDlqHandler> handlers() {
        return handlers.values();
    }

    void removeHandler(String key) {
        handlers.remove(key);
    }

    List<ConsumerFilter> filters(String key) {
        return filters.get(key);
    }

    void putFilters(String key, List<ConsumerFilter> chain) {
        filters.put(key, chain);
    }

    void removeFilters(String key) {
        filters.remove(key);
    }

    ConsumerGroupManager groupManager(String key) {
        return groupManagers.get(key);
    }

    void putGroupManager(String key, ConsumerGroupManager manager) {
        groupManagers.put(key, manager);
    }

    Collection<ConsumerGroupManager> groupManagers() {
        return groupManagers.values();
    }

    void removeAndUnregisterGroupManager(String key) {
        ConsumerGroupManager manager = groupManagers.remove(key);
        if (manager != null) {
            try {
                manager.unregister();
            } catch (RuntimeException ex) {
                // 注销失败不影响其余清理；心跳超时后由回收任务兜底
            }
        }
    }

    void clearGroupManagers() {
        for (ConsumerGroupManager manager : groupManagers.values()) {
            try {
                manager.unregister();
            } catch (RuntimeException ex) {
                // 同上
            }
        }
        groupManagers.clear();
    }

    /** 运行中的消费组管理器数量（诊断用）。 */
    int groupManagerCount() {
        return groupManagers.size();
    }

    /** 快照当前全部注册（供遍历期间并发注册安全）。 */
    List<ListenerRegistration<?>> snapshotRegistrations() {
        return new ArrayList<>(registrations.values());
    }

    Map<String, ListenerRegistration<?>> registrationMap() {
        return java.util.Collections.unmodifiableMap(registrations);
    }
}
