/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.annotation.StreamMQDlqConsumer;
import io.github.streammq.core.consumer.DlqMessageConsumer;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;

/**
 * 监听器注册服务。
 *
 * <p><b>设计模式：Factory Method（三类注册构建工厂）+ 轻量 Template Method （统一收尾管线：命名空间回填 → SPI 解析 → 入库 → 动态接线）。</b>
 *
 * <p><b>SPI：</b>容器仅依赖本接口；默认实现 {@link DefaultListenerRegistrar}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ListenerRegistrar {

    <T> void registerConcurrent(
            StreamMessageConcurrentlyConsumer<T> consumer, StreamMQConsumer annotation);

    <T> void registerOrderly(StreamMessageOrderlyConsumer<T> consumer, StreamMQConsumer annotation);

    <T> void registerDlq(DlqMessageConsumer<T> consumer, StreamMQDlqConsumer annotation);
}
