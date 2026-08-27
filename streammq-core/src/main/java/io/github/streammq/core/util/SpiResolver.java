/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.util;

import java.util.Objects;
import lombok.experimental.UtilityClass;

/**
 * SPI 实例解析工具：按注解中声明的实现类无参实例化，未声明（marker）时回退到全局默认实例。
 *
 * <p>用于 per-consumer 策略实例化，如 {@code RetryPolicy} / {@code DlqFailureStrategy} / {@code
 * MessageSerializer} / {@code RebalanceStrategy} 等。
 *
 * <p>约定：注解属性以 SPI 接口本身（如 {@code RetryPolicy.class}）作为"使用全局"的 marker。 当 {@code clazz == spiType} 时返回
 * {@code globalDefault}，否则以无参构造器实例化 {@code clazz}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@UtilityClass
public class SpiResolver {

    /**
     * 解析或实例化 SPI 实现。
     *
     * @param clazz 注解中声明的实现类；为 {@code null} 或等于 {@code spiType}（marker）时回退全局默认
     * @param spiType SPI 接口类型（marker 比较基准）
     * @param globalDefault 全局默认实例
     * @param <T> SPI 类型
     * @return 实例
     */
    public static <T> T resolveOrInstantiate(
            Class<? extends T> clazz, Class<T> spiType, T globalDefault) {
        if (Objects.isNull(clazz) || clazz == spiType) {
            return globalDefault;
        }
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    "Failed to instantiate "
                            + clazz.getName()
                            + " as "
                            + spiType.getName()
                            + " (requires public no-arg constructor)",
                    e);
        }
    }
}
