package io.github.streammq.core.enums;

import java.time.Duration;
import java.util.Objects;
import lombok.Getter;

/**
 * 并发消费回调返回动作（唯一消费结果表达方式，对齐 RocketMQ 返回值语义）。
 *
 * <p>Listener 在 {@code onMessage} 中返回本类实例控制后续流程，框架以返回值为唯一标准， 不再支持手动 {@code
 * context.acknowledge()/nack()/defer()} 调用，避免双模式冲突：
 *
 * <ul>
 *   <li>{@link #SUCCESS} - 消费成功，自动 ACK，从 PEL 移除
 *   <li>{@link #RECONSUME_LATER} - 消费失败，按 {@code RetryPolicy} 计算延迟后写入 retry ZSet 重投
 *   <li>{@link #defer(Duration)} - 消费失败，使用业务指定的延迟写入 retry ZSet 重投（覆盖 RetryPolicy）
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
 * <p>线程安全：{@link #SUCCESS} 与 {@link #RECONSUME_LATER} 为不可变单例常量，可安全共享； {@link #defer(Duration)}
 * 每次返回新实例，仅供单条消息消费使用。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class ConsumeAction {

    /** 动作类型 */
    public enum Type {
        /** 消费成功 */
        SUCCESS,
        /** 消费失败，按 RetryPolicy 重试 */
        RECONSUME_LATER,
        /** 消费失败，按指定延迟重试 */
        DEFER
    }

    /** 消费成功（单例常量） */
    public static final ConsumeAction SUCCESS = new ConsumeAction(Type.SUCCESS, null);

    /** 消费失败，按 RetryPolicy 重试（单例常量） */
    public static final ConsumeAction RECONSUME_LATER =
            new ConsumeAction(Type.RECONSUME_LATER, null);

    private final Type type;

    /**
     * -- GETTER -- 返回 DEFER 动作的延迟时长。
     *
     * @return 延迟时长；非 DEFER 动作返回 {@code null}
     */
    @Getter private final Duration deferDelay;

    private ConsumeAction(Type type, Duration deferDelay) {
        this.type = type;
        this.deferDelay = deferDelay;
    }

    /**
     * 返回一个 DEFER 动作，按指定延迟重投。
     *
     * @param delay 延迟时长，必须非 null 且为正
     * @return DEFER 动作
     * @throws IllegalArgumentException 如果 delay 为 null 或非正
     */
    public static ConsumeAction defer(Duration delay) {
        Objects.requireNonNull(delay, "delay");
        if (delay.isNegative() || delay.isZero()) {
            throw new IllegalArgumentException("defer delay must be positive: " + delay);
        }
        return new ConsumeAction(Type.DEFER, delay);
    }

    /**
     * 返回动作类型。
     *
     * @return 类型
     */
    public Type type() {
        return type;
    }

    /**
     * 返回动作类型名称（{@link Type#name()}），便于日志/追踪输出。
     *
     * @return 类型名称，如 {@code "SUCCESS"} / {@code "RECONSUME_LATER"} / {@code "DEFER"}
     */
    public String name() {
        return type.name();
    }

    /**
     * 是否为 SUCCESS。
     *
     * @return true 表示成功
     */
    public boolean isSuccess() {
        return type == Type.SUCCESS;
    }

    /**
     * 是否为 RECONSUME_LATER。
     *
     * @return true 表示按策略重试
     */
    public boolean isReconsumeLater() {
        return type == Type.RECONSUME_LATER;
    }

    /**
     * 是否为 DEFER。
     *
     * @return true 表示按指定延迟重试
     */
    public boolean isDefer() {
        return type == Type.DEFER;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConsumeAction that)) {
            return false;
        }
        return type == that.type && Objects.equals(deferDelay, that.deferDelay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, deferDelay);
    }

    @Override
    public String toString() {
        return Objects.nonNull(deferDelay)
                ? "ConsumeAction{" + type + ", defer=" + deferDelay + "}"
                : "ConsumeAction{" + type + "}";
    }
}
