package io.github.streammq.core.enums;

import java.time.Duration;
import java.util.Objects;

/**
 * 延时消息级别（18 级，对齐 RocketMQ 兼容）。
 *
 * <p>每个级别对应固定的延时时间，{@link #toMillis()} 返回毫秒数。 底层基于 Redis ZSet + {@code DelayMessageScheduler}
 * 定时轮询投递实现。
 *
 * <p><b>自定义延时：</b>18 级固定延时为 RocketMQ 兼容设计。如需任意毫秒级延时， 请使用 {@link
 * io.github.streammq.core.message.Message#delayTimeMillis} 字段， 支持 1ms ~ 任意时长的精确定时投递。
 *
 * <p>级别清单：
 *
 * <pre>
 * SECOND_1  = 1s     SECOND_5  = 5s     SECOND_10 = 10s    SECOND_30 = 30s
 * MINUTE_1  = 1m     MINUTE_2  = 2m     MINUTE_3  = 3m     MINUTE_4  = 4m
 * MINUTE_5  = 5m     MINUTE_6  = 6m     MINUTE_7  = 7m     MINUTE_8  = 8m
 * MINUTE_9  = 9m     MINUTE_10 = 10m    MINUTE_20 = 20m    MINUTE_30 = 30m
 * HOUR_1    = 1h     HOUR_2    = 2h
 * </pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum DelayLevel {

    /** 1 秒 */
    SECOND_1(Duration.ofSeconds(1)),
    /** 5 秒 */
    SECOND_5(Duration.ofSeconds(5)),
    /** 10 秒 */
    SECOND_10(Duration.ofSeconds(10)),
    /** 30 秒 */
    SECOND_30(Duration.ofSeconds(30)),
    /** 1 分钟 */
    MINUTE_1(Duration.ofMinutes(1)),
    /** 2 分钟 */
    MINUTE_2(Duration.ofMinutes(2)),
    /** 3 分钟 */
    MINUTE_3(Duration.ofMinutes(3)),
    /** 4 分钟 */
    MINUTE_4(Duration.ofMinutes(4)),
    /** 5 分钟 */
    MINUTE_5(Duration.ofMinutes(5)),
    /** 6 分钟 */
    MINUTE_6(Duration.ofMinutes(6)),
    /** 7 分钟 */
    MINUTE_7(Duration.ofMinutes(7)),
    /** 8 分钟 */
    MINUTE_8(Duration.ofMinutes(8)),
    /** 9 分钟 */
    MINUTE_9(Duration.ofMinutes(9)),
    /** 10 分钟 */
    MINUTE_10(Duration.ofMinutes(10)),
    /** 20 分钟 */
    MINUTE_20(Duration.ofMinutes(20)),
    /** 30 分钟 */
    MINUTE_30(Duration.ofMinutes(30)),
    /** 1 小时 */
    HOUR_1(Duration.ofHours(1)),
    /** 2 小时 */
    HOUR_2(Duration.ofHours(2));

    private final Duration duration;

    DelayLevel(Duration duration) {
        this.duration = Objects.requireNonNull(duration, "duration");
    }

    /**
     * 返回该级别对应的 {@link Duration}。
     *
     * @return 延时时长
     */
    public Duration getDuration() {
        return duration;
    }

    /**
     * 返回该级别对应的毫秒数。
     *
     * @return 延时毫秒数
     */
    public long toMillis() {
        return duration.toMillis();
    }

    /**
     * 返回该级别对应的秒数。
     *
     * @return 延时秒数
     */
    public long toSeconds() {
        return duration.toSeconds();
    }

    /**
     * 根据下标获取延时级别（按声明顺序）。
     *
     * @param index 下标，0-based
     * @return 对应的延时级别
     * @throws IndexOutOfBoundsException 如果下标越界
     */
    public static DelayLevel ofIndex(int index) {
        DelayLevel[] values = values();
        if (index < 0 || index >= values.length) {
            throw new IndexOutOfBoundsException(
                    "DelayLevel index out of bounds: "
                            + index
                            + ", valid range [0, "
                            + (values.length - 1)
                            + "]");
        }
        return values[index];
    }

    /**
     * 根据毫秒数查找最接近的延时级别（向上取整）。 若 millis 超过最大级别则返回 {@link #HOUR_2}。
     *
     * @param millis 目标毫秒数
     * @return 最接近的延时级别
     */
    public static DelayLevel closestAbove(long millis) {
        for (DelayLevel level : values()) {
            if (level.toMillis() >= millis) {
                return level;
            }
        }
        return HOUR_2;
    }
}
