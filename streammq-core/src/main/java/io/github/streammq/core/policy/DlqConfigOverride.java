/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.policy;

import java.util.Objects;

/**
 * per-consumer 的 DLQ 配置覆盖（部分字段，未声明的字段为 {@code null}）。
 *
 * <p>存在的意义：{@code @StreamMQDlqConsumer} 的数值属性若直接以"框架默认值"作注解默认值，框架就无法区分"用户没写" 与"用户写了恰好等于默认值"——
 * 前者应当<b>跟随全局配置</b>、后者应当<b>覆盖全局配置</b>。注解侧因此统一改用哨兵（见 {@link
 * io.github.streammq.core.StreamMQConstants#ANNOTATION_UNSET_INT} 等），在装配期把哨兵折算为 {@code null}， 运行期再由
 * {@link #applyTo(DlqConfig)} 合并到全局配置上。
 *
 * <p>合并规则：本对象中非 {@code null} 的字段覆盖 {@code base} 的对应字段；{@code null} 字段保持 {@code base} 原值。
 *
 * @param maxDlqRetryAttempts 最大 DLQ 重试次数；{@code null} = 跟随全局（必须 {@code >= 0}）
 * @param dlqRetryDelayMs DLQ 重试延迟（毫秒）；{@code null} = 跟随全局（必须 {@code >= 0}）
 * @param secondaryDlqEnabled 是否启用二级 DLQ；{@code null} = 跟随全局
 * @param secondaryDlqKeyPrefix 二级 DLQ Key 前缀段；{@code null} = 跟随全局
 * @param dlqAlertThreshold DLQ 告警阈值；{@code null} = 跟随全局（必须 {@code >= 1}）
 * @param dlqRetryBackoffMultiplier 重试退避倍数；{@code null} = 跟随全局（必须 {@code >= 1.0}）
 * @param dlqRetryMaxDelayMs 重试延迟上限（毫秒）；{@code null} = 跟随全局（必须 {@code > 0}）
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
public record DlqConfigOverride(
        Integer maxDlqRetryAttempts,
        Long dlqRetryDelayMs,
        Boolean secondaryDlqEnabled,
        String secondaryDlqKeyPrefix,
        Integer dlqAlertThreshold,
        Double dlqRetryBackoffMultiplier,
        Long dlqRetryMaxDelayMs) {

    /** 构造期契约校验：非 null 的字段必须落在各自合法值域内（与全局配置校验口径一致）。 */
    public DlqConfigOverride {
        if (Objects.nonNull(maxDlqRetryAttempts) && maxDlqRetryAttempts < 0) {
            throw new IllegalArgumentException(
                    "maxDlqRetryAttempts must be >= 0, got: " + maxDlqRetryAttempts);
        }
        if (Objects.nonNull(dlqRetryDelayMs) && dlqRetryDelayMs < 0) {
            throw new IllegalArgumentException(
                    "dlqRetryDelayMs must be >= 0, got: " + dlqRetryDelayMs);
        }
        if (Objects.nonNull(dlqAlertThreshold) && dlqAlertThreshold < 1) {
            throw new IllegalArgumentException(
                    "dlqAlertThreshold must be >= 1, got: " + dlqAlertThreshold);
        }
        if (Objects.nonNull(dlqRetryBackoffMultiplier) && dlqRetryBackoffMultiplier < 1.0d) {
            throw new IllegalArgumentException(
                    "dlqRetryBackoffMultiplier must be >= 1.0, got: " + dlqRetryBackoffMultiplier);
        }
        if (Objects.nonNull(dlqRetryMaxDelayMs) && dlqRetryMaxDelayMs <= 0) {
            throw new IllegalArgumentException(
                    "dlqRetryMaxDelayMs must be > 0, got: " + dlqRetryMaxDelayMs);
        }
    }

    /** 是否所有字段都未声明（此时无需覆盖，可直接复用全局配置对象）。 */
    public boolean isEmpty() {
        return Objects.isNull(maxDlqRetryAttempts)
                && Objects.isNull(dlqRetryDelayMs)
                && Objects.isNull(secondaryDlqEnabled)
                && Objects.isNull(secondaryDlqKeyPrefix)
                && Objects.isNull(dlqAlertThreshold)
                && Objects.isNull(dlqRetryBackoffMultiplier)
                && Objects.isNull(dlqRetryMaxDelayMs);
    }

    /**
     * 把本覆盖合并到基线配置，返回一个新实例（不修改 {@code base}）。
     *
     * @param base 全局基线配置；{@code null} 时以框架默认值为基线
     * @return 合并后的生效配置
     */
    public DlqConfig applyTo(DlqConfig base) {
        DlqConfig effective = Objects.isNull(base) ? DlqConfig.builder().build() : base;
        if (isEmpty()) {
            return effective;
        }
        DlqConfig merged =
                DlqConfig.builder()
                        .failureStrategyClass(effective.getFailureStrategyClass())
                        .maxDlqRetryAttempts(
                                orDefault(maxDlqRetryAttempts, effective.getMaxDlqRetryAttempts()))
                        .dlqRetryDelayMs(orDefault(dlqRetryDelayMs, effective.getDlqRetryDelayMs()))
                        .secondaryDlqEnabled(
                                orDefault(secondaryDlqEnabled, effective.isSecondaryDlqEnabled()))
                        .secondaryDlqKeyPrefix(
                                orDefault(
                                        secondaryDlqKeyPrefix,
                                        effective.getSecondaryDlqKeyPrefix()))
                        .dlqAlertThreshold(
                                orDefault(dlqAlertThreshold, effective.getDlqAlertThreshold()))
                        .dlqRetryBackoffMultiplier(
                                orDefault(
                                        dlqRetryBackoffMultiplier,
                                        effective.getDlqRetryBackoffMultiplier()))
                        .dlqRetryMaxDelayMs(
                                orDefault(dlqRetryMaxDelayMs, effective.getDlqRetryMaxDelayMs()))
                        .minRetryDelayMs(effective.getMinRetryDelayMs())
                        .streamMaxLen(effective.getStreamMaxLen())
                        .build();
        return merged;
    }

    private static int orDefault(Integer override, int base) {
        return Objects.isNull(override) ? base : override;
    }

    private static long orDefault(Long override, long base) {
        return Objects.isNull(override) ? base : override;
    }

    private static double orDefault(Double override, double base) {
        return Objects.isNull(override) ? base : override;
    }

    private static boolean orDefault(Boolean override, boolean base) {
        return Objects.isNull(override) ? base : override;
    }

    private static String orDefault(String override, String base) {
        return Objects.isNull(override) || override.isEmpty() ? base : override;
    }
}
