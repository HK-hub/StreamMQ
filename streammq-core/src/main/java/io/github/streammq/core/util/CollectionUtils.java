package io.github.streammq.core.util;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * 集合工具类，提供 null 安全的空集合判断。
 *
 * <p>替代手写 {@code Objects.isNull(coll) || coll.isEmpty()} 模式，提升可读性。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class CollectionUtils {

    private CollectionUtils() {}

    /**
     * 判断集合是否为 null 或空集合。
     *
     * @param coll 集合，可为 null
     * @return true 表示为 null 或空集合
     */
    public static boolean isEmpty(Collection<?> coll) {
        return Objects.isNull(coll) || coll.isEmpty();
    }

    /**
     * 判断集合是否非 null 且非空集合。
     *
     * @param coll 集合，可为 null
     * @return true 表示非 null 且非空集合
     */
    public static boolean isNotEmpty(Collection<?> coll) {
        return Objects.nonNull(coll) && !coll.isEmpty();
    }

    /**
     * 判断 Map 是否为 null 或空 Map。
     *
     * @param map Map，可为 null
     * @return true 表示为 null 或空 Map
     */
    public static boolean isEmpty(Map<?, ?> map) {
        return Objects.isNull(map) || map.isEmpty();
    }

    /**
     * 判断 Map 是否非 null 且非空 Map。
     *
     * @param map Map，可为 null
     * @return true 表示非 null 且非空 Map
     */
    public static boolean isNotEmpty(Map<?, ?> map) {
        return Objects.nonNull(map) && !map.isEmpty();
    }
}
