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
import java.util.Collection;
import java.util.List;

/**
 * 容器注册存储（状态载体）。
 *
 * <p><b>SPI：</b>容器与消费管线仅依赖本接口；默认实现 {@link DefaultRegistrationStore}。 高级用户可在容器 start 前通过 {@code
 * setRegistrationStore} 注入自定义实现 （例如增加注册审计、指标埋点等装饰逻辑）。
 *
 * <p>键语义唯一于此定义：{@code reg.key()} / DLQ 前缀。
 *
 * <p>实现要求：线程安全。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface RegistrationStore {

    // ===================== 注册表 =====================

    Collection<ListenerRegistration<?>> registrations();

    int registrationCount();

    ListenerRegistration<?> registration(String key);

    ListenerRegistration<?> putRegistration(ListenerRegistration<?> reg);

    ListenerRegistration<?> removeRegistration(String key);

    /** 快照当前全部注册（遍历期间并发注册安全）。 */
    List<ListenerRegistration<?>> snapshotRegistrations();

    // ===================== per-consumer 处理器 =====================

    RetryAndDlqHandler handler(String key);

    void putHandler(String key, RetryAndDlqHandler handler);

    Collection<RetryAndDlqHandler> handlers();

    void removeHandler(String key);

    // ===================== 组管理器 =====================

    ConsumerGroupManager groupManager(String key);

    void putGroupManager(String key, ConsumerGroupManager manager);

    Collection<ConsumerGroupManager> groupManagers();

    /** 注销并移除指定组管理器（注销失败仅记录）。 */
    void removeAndUnregisterGroupManager(String key);

    /** 注销全部组管理器（容器 stop）。 */
    void clearGroupManagers();

    // ===================== per-consumer 过滤器链缓存 =====================

    List<ConsumerFilter> filters(String key);

    void putFilters(String key, List<ConsumerFilter> chain);

    void removeFilters(String key);
}
