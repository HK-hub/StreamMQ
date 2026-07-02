package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.consumer.StreamMessageManualAckConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;

/**
 * Consumer 类型枚举。
 *
 * <p>区分三种消费回调接口的注册类型，用于消费循环中分发处理逻辑。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum ListenerType {
    /** 自动 ACK：实现 {@link StreamMessageConcurrentlyConsumer}，返回 Action 后由容器 ACK */
    AUTO_ACK,
    /** 手动 ACK：实现 {@link StreamMessageManualAckConsumer}，由 consumer 通过 ctx.acknowledge() 控制 */
    MANUAL_ACK,
    /** 顺序消费：实现 {@link StreamMessageOrderlyConsumer}，按 shardingKey 分片加锁串行消费 */
    ORDERLY
}
