/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics.model;

/**
 * 诊断严重级别枚举，用于标识问题严重程度。
 *
 * <p>级别从低到高：
 *
 * <ul>
 *   <li>{@link #INFO} - 信息级别，积压低于警告阈值，无需处理
 *   <li>{@link #WARNING} - 警告级别，积压介于警告阈值与严重阈值之间，建议关注
 *   <li>{@link #CRITICAL} - 严重级别，积压超过严重阈值，需要立即处理
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum Severity {
    /** 信息级别 */
    INFO,
    /** 警告级别 */
    WARNING,
    /** 严重级别 */
    CRITICAL
}
