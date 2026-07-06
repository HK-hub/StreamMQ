package io.github.streammq.core.enums;

/**
 * nack 之后的重试模式。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum NackRetryMode {
    /** 主动写入 retry ZSet + ACK 原消息，由 RetryScheduler 调度重投（对齐 RocketMQ） */
    RETRY_ZSET,
    /** 不 ACK 留 PEL，依赖 XAUTOCLAIM 自动重投；超过 fastRetryCount 后可选转入 RETRY_ZSET */
    STREAM_AUTO
}
