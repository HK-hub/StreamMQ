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

    private StringUtils() {
    }

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
}
