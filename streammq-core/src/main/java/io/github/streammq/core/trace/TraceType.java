/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.trace;

/**
 * 追踪事件类型。
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
public enum TraceType {
    /** 消息发送事件 */
    SEND,
    /** 消息消费事件 */
    CONSUME
}
