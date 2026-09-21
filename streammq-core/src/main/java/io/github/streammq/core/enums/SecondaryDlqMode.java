/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.enums;

/**
 * 二级死信队列开关的注解级三态声明。
 *
 * <p>{@code boolean} 注解属性无法表达"未声明"——注解默认值（{@code false}）与用户显式写的 {@code false} 不可区分， 会让 「注解 &gt;
 * 全局配置 &gt; 框架默认」的优先级在 {@code secondary-dlq-enabled} 上退化为"注解默认值静默覆盖全局配置"。 因此改用三态枚举：
 *
 * <ul>
 *   <li>{@link #INHERIT}（默认）：跟随全局配置 {@code streammq.dlq.secondary-dlq-enabled}
 *   <li>{@link #ENABLED}：本消费者显式启用二级 DLQ
 *   <li>{@link #DISABLED}：本消费者显式关闭二级 DLQ（即使全局已开启也不生效）
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
public enum SecondaryDlqMode {

    /** 未声明：跟随全局配置（默认）。 */
    INHERIT,

    /** 显式启用二级死信队列。 */
    ENABLED,

    /** 显式关闭二级死信队列。 */
    DISABLED
}
