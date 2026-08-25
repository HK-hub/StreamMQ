/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.util;

import java.util.Objects;

/**
 * 字符串工具类，提供 null 安全的空字符串判断。
 *
 * <p>替代手写 {@code Objects.isNull(str) || str.isEmpty()} 模式，提升可读性。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class StringUtils {

    private StringUtils() {}

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
     * 校验 StreamMQ 命名（topic / consumerGroup / tag 等）：非 null、非空、不含 {@code ':'}、{@code '*'} 或空白字符。
     *
     * <p>Redis Stream Key 使用 {@code :} 作为命名空间分隔符、{@code *} 作为通配符，非法字符会破坏 Key 结构或被错误路由。
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
            if (c == ':' || c == '*' || Character.isWhitespace(c)) {
                throw new IllegalArgumentException(
                        field + " must not contain ':', '*' or whitespace: " + name);
            }
        }
        return trimmed;
    }

    /**
     * 校验并规范化主题名（非 null / 非空 / 不含 {@code ':'}、{@code '*'} 或空白）。
     *
     * @param topic 主题名
     * @return 去除首尾空白后的主题名
     * @throws IllegalArgumentException 如果主题名为空或包含非法字符
     */
    public static String requireValidTopic(String topic) {
        return requireValidName(topic, "topic");
    }

    /**
     * 校验并规范化消费者组名（非 null / 非空 / 不含 {@code ':'}、{@code '*'} 或空白）。
     *
     * @param group 消费者组名
     * @return 去除首尾空白后的消费者组名
     * @throws IllegalArgumentException 如果组名为空或包含非法字符
     */
    public static String requireValidGroup(String group) {
        return requireValidName(group, "consumerGroup");
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
