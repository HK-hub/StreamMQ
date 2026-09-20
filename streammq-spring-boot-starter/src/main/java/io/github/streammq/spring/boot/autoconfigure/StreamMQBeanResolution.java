/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.core.compression.CompressionCodec;
import io.github.streammq.core.compression.CompressionCodecRegistry;
import java.util.List;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;

/**
 * 自动装配期的「可选依赖候选解析」助手。
 *
 * <p><b>为什么需要它（发布前修复 R6-S3）：</b>{@code ObjectProvider#getIfAvailable()} 在存在多个候选 Bean 且无
 * {@code @Primary} 时直接抛 {@code NoUniqueBeanDefinitionException}——即"注册第二个同类 Bean
 * 就启动失败"，而这恰恰是文档鼓励的用法（例如注册多个自定义 {@link CompressionCodec}）。 语义上分两类：
 *
 * <ul>
 *   <li><b>"至多一个"</b>（指标收集器 / 重试策略 / 转换器等）：用 {@link #uniqueOrNull}——多候选时给出 <b>可定位</b>的异常（列出冲突 Bean
 *       名），而不是 Spring 原生的裸异常或被静默忽略；
 *   <li><b>"允许多个"</b>（{@link CompressionCodec} 注册表）：注入并全量注册，仅在需要"默认 Codec" 时按 {@link
 *       #defaultCompressionCodec} 的选择顺序消歧。
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
final class StreamMQBeanResolution {

    private StreamMQBeanResolution() {}

    /**
     * 解析"至多一个"的注入点：无候选返回 {@code null}，唯一候选直接返回，多候选时优先 {@code @Primary} （{@code @Priority}
     * 次之），仍无法消歧则抛出可定位的异常。
     *
     * @param provider 候选提供者
     * @param role 注入点描述（用于错误信息，如 {@code "StreamMQMetrics"}）
     * @param <T> 候选类型
     * @return 唯一候选，或 null（无候选）
     * @throws IllegalStateException 存在多个候选且无 {@code @Primary} / {@code @Priority}
     */
    static <T> T uniqueOrNull(ObjectProvider<T> provider, String role) {
        List<T> candidates = provider.orderedStream().toList();
        if (candidates.isEmpty()) {
            return null;
        }
        if (candidates.size() == 1) {
            return candidates.get(0);
        }
        // @Primary / @Priority 是显式消歧意图，尊重之（getIfUnique 在无法消歧时返回 null 而不抛异常）
        T primary = provider.getIfUnique();
        if (primary != null) {
            return primary;
        }
        throw new IllegalStateException(
                role
                        + " supports at most ONE bean, but "
                        + candidates.size()
                        + " candidates were found: "
                        + candidateNames(provider)
                        + ". Keep exactly one bean, or mark one of them with @Primary"
                        + " (or @Priority).");
    }

    /**
     * 解析"至多一个"的注入点并给出默认值。
     *
     * @param provider 候选提供者
     * @param role 注入点描述
     * @param defaultValue 无候选时使用的默认值
     * @param <T> 候选类型
     * @return 唯一候选，或 {@code defaultValue}
     */
    static <T> T uniqueOrNull(ObjectProvider<T> provider, String role, T defaultValue) {
        T resolved = uniqueOrNull(provider, role);
        return resolved != null ? resolved : defaultValue;
    }

    /**
     * 选择"用于压缩出站消息 / 兼容旧格式解压"的默认 Codec。
     *
     * <p><b>选择顺序（不得静默取任意候选）：</b>
     *
     * <ol>
     *   <li>配置名精确匹配：{@code streammq.producer.compression-codec}（先查候选 Bean 的 {@code name()}， 再查注册表内置
     *       Codec——{@code gzip} / classpath 存在 lz4-java 时的 {@code lz4}）。显式配置优先于 注解级
     *       {@code @Primary}；配置了却匹配不到<b>即启动失败并列出全部可选名称</b>，绝不回落——否则 用户显式指定的 Codec 会被静默忽略；
     *   <li>{@code @Primary}（或 {@code @Priority}）候选（未显式配置名称时）；
     *   <li>唯一候选；
     *   <li>多候选且无法消歧：需要压缩（{@code compress-threshold > 0}）时启动失败并列出候选；不需要压缩 时返回
     *       null（不静默选中任何一个，注册表仍可按名解压）。
     * </ol>
     *
     * @param candidates 全部 {@link CompressionCodec} Bean（按容器顺序）
     * @param primaryCandidate {@code @Primary} / {@code @Priority} 候选（无则为 null，由调用方用 {@code
     *     ObjectProvider#getIfUnique()} 得到）
     * @param registry Codec 注册表（可为 null）
     * @param configuredName {@code streammq.producer.compression-codec} 配置值（空 = 未配置）
     * @param compressionRequired 本次装配是否必须确定默认 Codec（{@code compress-threshold > 0}）
     * @param injectionPoint 注入点描述（用于错误信息）
     * @return 默认 Codec，或 null（无候选 / 多候选但无需压缩）
     * @throws IllegalStateException 显式配置的名字无法解析，或压缩已启用但候选无法消歧
     */
    static CompressionCodec defaultCompressionCodec(
            List<CompressionCodec> candidates,
            CompressionCodec primaryCandidate,
            CompressionCodecRegistry registry,
            String configuredName,
            boolean compressionRequired,
            String injectionPoint) {
        if (configuredName != null && !configuredName.isBlank()) {
            String name = configuredName.trim();
            for (CompressionCodec candidate : candidates) {
                if (name.equals(candidate.name())) {
                    return candidate;
                }
            }
            CompressionCodec fromRegistry = registry == null ? null : registry.lookup(name);
            if (fromRegistry != null) {
                return fromRegistry;
            }
            throw new IllegalStateException(
                    "streammq.producer.compression-codec="
                            + name
                            + " does not match any compression codec available to "
                            + injectionPoint
                            + ". CompressionCodec bean candidates: "
                            + describeCodecs(candidates)
                            + "; registry codecs: "
                            + (registry == null ? "[]" : registry.availableCodecs())
                            + ". Register a CompressionCodec bean whose name() returns '"
                            + name
                            + "', or configure one of the available names.");
        }
        if (candidates.isEmpty()) {
            return null;
        }
        if (candidates.size() == 1) {
            return candidates.get(0);
        }
        if (primaryCandidate != null) {
            return primaryCandidate;
        }
        if (compressionRequired) {
            throw new IllegalStateException(
                    injectionPoint
                            + ": "
                            + candidates.size()
                            + " CompressionCodec beans are registered ("
                            + describeCodecs(candidates)
                            + ") but none is @Primary, so the default codec for compression is"
                            + " ambiguous and picking one arbitrarily would make the wire format"
                            + " depend on bean registration order. Fix by one of: (a) mark the"
                            + " intended default with @Primary, (b) set"
                            + " streammq.producer.compression-codec=<codec name>, or (c) set"
                            + " streammq.producer.compress-threshold=0 to disable compression");
        }
        // 多候选 + 无需压缩：不选中任何一个（调用方会记录 WARN），注册表仍可按名称解压
        return null;
    }

    /** 用候选 Bean 的类名描述候选集合（同名实现按出现次数去重展示）。 */
    static String describe(List<?> candidates) {
        return candidates.stream()
                .map(candidate -> candidate.getClass().getName())
                .toList()
                .toString();
    }

    /** 描述 Codec 候选：类名 + {@code name()}（配置名匹配的是 {@code name()}，错误信息里必须同时给出两者）。 */
    static String describeCodecs(List<CompressionCodec> candidates) {
        return candidates.stream()
                .map(
                        candidate ->
                                candidate.getClass().getName() + "(name=" + candidate.name() + ")")
                .toList()
                .toString();
    }

    /** 从 Spring 的 {@code NoUniqueBeanDefinitionException} 中提取冲突 Bean 名（拿不到名字时回落说明）。 */
    private static String candidateNames(ObjectProvider<?> provider) {
        try {
            provider.getIfAvailable();
            return "(bean names unavailable)";
        } catch (NoUniqueBeanDefinitionException ex) {
            return String.join(", ", ex.getBeanNamesFound());
        } catch (BeansException ex) {
            return "(bean names unavailable: " + ex.getClass().getSimpleName() + ")";
        }
    }
}
