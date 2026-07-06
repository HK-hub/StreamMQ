package io.github.streammq.core.listener;

import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;

/**
 * Consumer 类型枚举。
 *
 * <p>区分两种消费回调接口的注册类型，用于消费循环中分发处理逻辑。
 * 手动 ACK 由 {@link io.github.streammq.core.enums.AcknowledgeMode#MANUAL} 配置驱动，
 * 不再单独设立类型，仍走 {@link #AUTO_ACK} 或 {@link #ORDERLY} 分支。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum ListenerType {
    /** 并发消费：实现 {@link StreamMessageConcurrentlyConsumer}，返回 {@link io.github.streammq.core.enums.ConsumeAction} 后由容器 ACK */
    AUTO_ACK,
    /** 顺序消费：实现 {@link StreamMessageOrderlyConsumer}，按 shardingKey 分片加锁串行消费 */
    ORDERLY
}
