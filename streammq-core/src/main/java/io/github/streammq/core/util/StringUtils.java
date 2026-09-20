/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.util;

import java.util.Objects;
import lombok.experimental.UtilityClass;

/**
 * 字符串工具类，提供 null 安全的空字符串判断。
 *
 * <p>替代手写 {@code Objects.isNull(str) || str.isEmpty()} 模式，提升可读性。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@UtilityClass
public class StringUtils {

    /** 用户侧命名保留前缀：{@code __} 开头的名称属于框架内部（如 {@code __dlq__} 哨兵），禁止用户占用。 */
    private static final String RESERVED_NAME_PREFIX = "__";

    /**
     * 判断字符序列是否为 null 或空字符串。
     *
     * @param cs 字符序列，可为 null
     * @return true 表示为 null 或空字符串
     */
    public static boolean isEmpty(CharSequence cs) {
        return Objects.isNull(cs) || cs.isEmpty();
    }

    /**
     * 判断字符序列是否非 null 且非空字符串。
     *
     * @param cs 字符序列，可为 null
     * @return true 表示非 null 且非空字符串
     */
    public static boolean isNotEmpty(CharSequence cs) {
        return Objects.nonNull(cs) && !cs.isEmpty();
    }

    /**
     * 校验 StreamMQ 命名（topic / consumerGroup / tag 等）：非 null、非空、不含 {@code ':'}、{@code '*'}、{@code
     * '{'}、{@code '}'}、{@code '|'}、{@code ','} 或空白字符。
     *
     * <p>Redis Stream Key 使用 {@code :} 作为命名空间分隔符、{@code *} 作为通配符，非法字符会破坏 Key 结构或被错误路由； {@code '{}'}
     * 是 Redis Cluster Hash Tag 定界符，会导致整个 Key 家族被强制路由到同一 slot，形成热点； {@code '|'} 与 {@code ','}
     * 是广播租约/注册表的内部编码分隔符，允许它们会让名称在编码时被改写（静默错位）， 例如 {@code a|b} 的 topic 会让「实例租约保护」判定失配。
     *
     * <p><b>topic / consumerGroup 的额外约束：</b>经 {@link #requireValidTopic(String)} / {@link
     * #requireValidGroup(String)} 校验时，还禁止以保留前缀 {@code __} 开头（框架内部哨兵，如 {@code __dlq__}）。
     *
     * @param name 待校验的名称
     * @param field 字段名（用于异常信息，如 {@code "topic"}）
     * @return 去除首尾空白后的合法名称
     * @throws IllegalArgumentException 如果名称为空或包含非法字符
     */
    public static String requireValidName(String name, String field) {
        Objects.requireNonNull(name, field);
        String trimmed = name.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(field + " must not be empty");
        }
        for (int i = 0; i < trimmed.length(); i++) {
            char c = trimmed.charAt(i);
            if (c == ':'
                    || c == '*'
                    || c == '{'
                    || c == '}'
                    || c == '|'
                    || c == ','
                    || Character.isWhitespace(c)) {
                throw new IllegalArgumentException(
                        field
                                + " must not contain ':', '*', '{', '}', '|', ',' or whitespace: "
                                + name);
            }
        }
        return trimmed;
    }

    /**
     * 校验并规范化主题名：非 null / 非空 / 不含 {@code ':'}、{@code '*'}、{@code '{'}、{@code '}'}、{@code '|'}、{@code
     * ','} 或空白字符，且<b>不得以保留前缀 {@code __} 开头</b>（与 {@link #requireValidName(String, String)}
     * 同口径，见该方法的说明）。
     *
     * <p><b>保留名约束（0.1.2 起为不变量）：</b>{@code __} 开头的名称属于框架内部（如 {@code __dlq__} 是 DLQ 重试目标哨兵， 见 {@link
     * io.github.streammq.core.StreamMQConstants#DLQ_RETRY_TARGET_TOPIC_SENTINEL}），
     * 允许用户占用会与内部哨兵冲突/被错误路由，因此一律拒绝。
     *
     * @param topic 主题名
     * @return 去除首尾空白后的主题名
     * @throws IllegalArgumentException 如果主题名为空、包含非法字符或以 {@code __} 开头
     */
    public static String requireValidTopic(String topic) {
        return requireValidNameWithReservedPrefixRule(topic, "topic");
    }

    /**
     * 校验并规范化消费者组名：非 null / 非空 / 不含 {@code ':'}、{@code '*'}、{@code '{'}、{@code '}'}、{@code
     * '|'}、{@code ','} 或空白字符，且<b>不得以保留前缀 {@code __} 开头</b>（与 {@link #requireValidName(String,
     * String)} 同口径，见该方法的说明）。
     *
     * <p><b>保留名约束（0.1.2 起为不变量）：</b>{@code __} 开头的名称属于框架内部（如 {@code __dlq__} 是 DLQ 重试目标哨兵），
     * 允许用户占用会与内部哨兵冲突，因此一律拒绝。
     *
     * @param group 消费者组名
     * @return 去除首尾空白后的消费者组名
     * @throws IllegalArgumentException 如果组名为空、包含非法字符或以 {@code __} 开头
     */
    public static String requireValidGroup(String group) {
        return requireValidNameWithReservedPrefixRule(group, "consumerGroup");
    }

    /**
     * 先按 {@link #requireValidName(String, String)} 校验，再拒绝 {@code __} 开头的保留名（topic / consumerGroup
     * 专用）。
     */
    private static String requireValidNameWithReservedPrefixRule(String name, String field) {
        String valid = requireValidName(name, field);
        if (valid.startsWith(RESERVED_NAME_PREFIX)) {
            throw new IllegalArgumentException(
                    field
                            + " must not start with reserved prefix '__'"
                            + " (internal sentinels like '__dlq__'): "
                            + name);
        }
        return valid;
    }

    /**
     * 校验并规范化命名空间（允许为空字符串；非空时不含 {@code ':'}、{@code '*'} 或空白）。
     *
     * @param namespace 命名空间，可为 null 或空字符串
     * @return 去除首尾空白后的命名空间
     * @throws IllegalArgumentException 如果命名空间非空但包含非法字符
     */
    public static String requireValidNamespace(String namespace) {
        if (isEmpty(namespace)) {
            return "";
        }
        return requireValidName(namespace, "namespace");
    }
}
