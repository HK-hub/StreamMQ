/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.enums;

/**
 * 拦截器异常触发的时机。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum InvokeTiming {
    /** 发送/消费前的准备阶段（如参数校验、上下文初始化） */
    BEFORE,
    /** 发送/消费执行中（如网络调用、Redis 操作） */
    EXECUTING,
    /** 发送/消费后的处理阶段（如结果处理、清理） */
    AFTER
}
