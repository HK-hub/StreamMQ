/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.policy.RebalanceStrategy;

/**
 * per-consumer SPI 解析器。
 *
 * <p><b>设计模式：Factory Method + Null Object。</b>注解属性以 SPI 接口本身作为 "使用全局默认"的 marker（Null
 * Object），否则无参实例化自定义实现。
 *
 * <p><b>SPI：</b>容器仅依赖本接口；默认实现 {@link DefaultPerConsumerSpiResolver}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface PerConsumerSpiResolver {

    /** 注册期解析全部 per-consumer SPI 并写入 store / 注册模型。关闭时为 no-op。 */
    void resolveInto(ListenerRegistration<?> reg, RegistrationStore store);

    /** 重建指定注册的过滤器缓存（全局过滤器变更时调用）。 */
    void rebuildFilters(ListenerRegistration<?> reg, RegistrationStore store);

    /** 解析重平衡策略实例（组管理器工厂使用）。 */
    RebalanceStrategy resolveRebalanceStrategy(ListenerRegistration<?> reg);

    boolean isEnabled();
}
