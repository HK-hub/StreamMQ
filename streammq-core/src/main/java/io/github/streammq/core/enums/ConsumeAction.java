/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.enums;

import java.time.Duration;
import java.util.Objects;

/**
 * 并发消费回调返回动作（唯一消费结果表达方式，对齐 RocketMQ 返回值语义）。
 *
 * <p>本类型是<b>值对象</b>而非 {@code enum}，因为它需要为 DEFER 携带逐消息的延迟 （{@link #deferDelay()}）——纯 {@code enum}
 * 常量无法持有每实例状态。需要 {@code switch} 时， 请对 {@link #type()} 返回的 {@link Type} 枚举分支：
 *
 * <pre>{@code
 * return switch (action.type()) {
 *     case SUCCESS -> ...;
 *     case RECONSUME_LATER -> ...;
 *     case DEFER -> ...;
 * };
 * }</pre>
 *
 * <p>Listener 在 {@code onMessage} 中返回本实例控制后续流程，框架以返回值为唯一标准， 不再支持手动 {@code
 * context.acknowledge()/nack()/defer()} 调用，避免双模式冲突：
 *
 * <ul>
 *   <li>{@link #SUCCESS} - 消费成功，自动 ACK，从 PEL 移除
 *   <li>{@link #RECONSUME_LATER} - 消费失败，按 RetryPolicy 计算延迟后写入 retry ZSet 重投
 *   <li>{@code DEFER} - 消费失败，按 {@link #defer(Duration)} 指定的延迟重投； DEFER 动作<b>只能</b>由 {@link
 *       #defer(Duration)} 创建（携带正延迟）
 * </ul>
 *
 * <p><b>API 变更（0.1.2，首个公开发布）：</b>删除了 {@code ConsumeAction.DEFER} 常量。 该常量历史上携带 {@code null}
 * 延迟，业务返回它会让框架拿不到延迟而静默失败（既不 ACK 也不重投）， 属于"看起来能用、实际必错"的 API。需要延迟重投请改用 {@link #defer(Duration)}；
 * 需要普通重试请用 {@link #RECONSUME_LATER}。
 *
 * <p>当 Listener 抛出 {@link RuntimeException} 时，框架将其视为 {@link #RECONSUME_LATER}。
 *
 * <p>使用示例：
 *
 * <pre>{@code
 * public ConsumeAction onMessage(Message<Order> msg, ConsumeContext ctx) {
 *     try {
 *         process(msg.getBody());
 *         return ConsumeAction.SUCCESS;
 *     } catch (RetryableException ex) {
 *         return ConsumeAction.RECONSUME_LATER;
 *     } catch (BusyException ex) {
 *         return ConsumeAction.defer(Duration.ofSeconds(30));
 *     }
 * }
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class ConsumeAction {
    /** 动作类型枚举，用于 switch 分支（本类因需携带逐消息延迟而为值对象，非 enum）。 */
    public enum Type {
        /** 消费成功，自动 ACK，从 PEL 移除 */
        SUCCESS,
        /** 消费失败，按 RetryPolicy 重试 */
        RECONSUME_LATER,
        /** 消费失败，按指定延迟重试 */
        DEFER
    }

    public static final ConsumeAction SUCCESS = new ConsumeAction(Type.SUCCESS, null);
    public static final ConsumeAction RECONSUME_LATER =
            new ConsumeAction(Type.RECONSUME_LATER, null);

    private final Type type;
    private final Duration deferDelay;

    /**
     * 私有构造器（唯一创建路径）：{@link Type#DEFER} 必须携带正延迟，否则构造即失败。
     *
     * <p><b>为什么不变量内置在构造器里：</b>此前 {@code DEFER} 允许 {@code null} 延迟的实例存在（如已删除的 {@code
     * ConsumeAction.DEFER} 常量），业务返回它时框架取延迟处 NPE，被吞掉后消息既不 ACK 也不重投。 把"DEFER
     * 必须带正延迟"作为构造期不变量，可从根上杜绝这类静默失效。
     *
     * @param type 动作类型，不可为 null
     * @param deferDelay DEFER 动作的延迟；非 DEFER 时必须为 null（由静态工厂保证）
     * @throws NullPointerException 如果 type 为 null
     * @throws IllegalArgumentException 如果 type 为 DEFER 且延迟为 null 或非正
     */
    private ConsumeAction(Type type, Duration deferDelay) {
        this.type = Objects.requireNonNull(type, "type");
        if (type == Type.DEFER
                && (Objects.isNull(deferDelay) || deferDelay.isNegative() || deferDelay.isZero())) {
            throw new IllegalArgumentException(
                    "DEFER action requires a positive non-null delay, got: " + deferDelay);
        }
        this.deferDelay = deferDelay;
    }

    /**
     * 返回一个 DEFER 动作，按指定延迟重投。
     *
     * @param delay 延迟时长，必须非 null 且为正
     * @return DEFER 动作（携带 delay）
     * @throws NullPointerException 如果 delay 为 null
     * @throws IllegalArgumentException 如果 delay 非正
     */
    public static ConsumeAction defer(Duration delay) {
        Objects.requireNonNull(delay, "delay");
        if (delay.isNegative() || delay.isZero()) {
            throw new IllegalArgumentException("defer delay must be positive: " + delay);
        }
        return new ConsumeAction(Type.DEFER, delay);
    }

    /** 动作类型（用于 switch）。 */
    public Type type() {
        return type;
    }

    /**
     * DEFER 动作的延迟；非 DEFER 时为 null。DEFER 动作该值<b>永不为 null 且恒为正</b>。
     *
     * <p>命名与 {@link io.github.streammq.core.policy.DlqFailureDecision#retryDelay()} 对齐（值对象访问器统一无
     * {@code get} 前缀）。
     *
     * @return 延迟时长
     * @since 0.1.2
     */
    public Duration deferDelay() {
        return deferDelay;
    }

    /**
     * DEFER 动作的延迟；非 DEFER 时为 null。
     *
     * @return 延迟时长
     * @deprecated 命名与 {@link io.github.streammq.core.policy.DlqFailureDecision#retryDelay()} 不一致；改用
     *     {@link #deferDelay()}，本方法将于 0.2.0 移除
     */
    @Deprecated(since = "0.1.2", forRemoval = false)
    public Duration getDeferDelay() {
        return deferDelay;
    }

    public boolean isSuccess() {
        return type == Type.SUCCESS;
    }

    public boolean isReconsumeLater() {
        return type == Type.RECONSUME_LATER;
    }

    public boolean isDefer() {
        return type == Type.DEFER;
    }

    public String name() {
        return type.name();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConsumeAction)) {
            return false;
        }
        ConsumeAction that = (ConsumeAction) o;
        return type == that.type && Objects.equals(deferDelay, that.deferDelay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, deferDelay);
    }

    @Override
    public String toString() {
        return "ConsumeAction{type="
                + type
                + (deferDelay != null ? ", deferDelay=" + deferDelay : "")
                + '}';
    }
}
