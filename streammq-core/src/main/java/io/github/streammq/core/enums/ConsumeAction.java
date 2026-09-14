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
 * <p>本类型是<b>值对象</b>而非 {@code enum}，因为它需要为 {@link #DEFER} 携带逐消息的延迟 （{@link #getDeferDelay()}）——纯
 * {@code enum} 常量无法持有每实例状态。需要 {@code switch} 时， 请对 {@link #type()} 返回的 {@link Type} 枚举分支：
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
 *   <li>{@link #DEFER} - 消费失败，按 {@link #defer(Duration)} 指定的延迟重投
 * </ul>
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
    public static final ConsumeAction DEFER = new ConsumeAction(Type.DEFER, null);

    private final Type type;
    private final Duration deferDelay;

    private ConsumeAction(Type type, Duration deferDelay) {
        this.type = Objects.requireNonNull(type, "type");
        this.deferDelay = deferDelay;
    }

    /**
     * 返回一个 {@link #DEFER} 动作，按指定延迟重投。
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

    /** DEFER 动作的延迟；非 DEFER 时为 null。 */
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
