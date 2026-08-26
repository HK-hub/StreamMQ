/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.policy.ConsumerGroupManager;

/**
 * 消费者组管理器工厂。
 *
 * <p><b>设计模式：Factory Method。</b>
 *
 * <p><b>SPI：</b>容器仅依赖本接口；默认实现 {@link DefaultConsumerGroupManagerFactory}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ConsumerGroupManagerFactory {

    /** 创建并注册到组的组管理器（含僵尸组清理）。 */
    ConsumerGroupManager createAndRegister(ListenerRegistration<?> reg);
}
