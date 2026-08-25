/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.listener;

import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;

/**
 * Consumer 类型枚举。
 *
 * <p>区分两种消费回调接口的注册类型，用于消费循环中分发处理逻辑。 消费结果统一由 {@code onMessage} 返回值表达，不再区分手动/自动 ACK 模式。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum ListenerType {
    /**
     * 并发消费：实现 {@link StreamMessageConcurrentlyConsumer}，返回 {@link
     * io.github.streammq.core.enums.ConsumeAction} 后由容器 ACK
     */
    AUTO_ACK,
    /** 顺序消费：实现 {@link StreamMessageOrderlyConsumer}，按 shardingKey 分片加锁串行消费 */
    ORDERLY
}
