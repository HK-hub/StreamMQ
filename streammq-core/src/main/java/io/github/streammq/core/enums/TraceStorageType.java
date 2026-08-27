/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.enums;

import java.util.Arrays;

/**
 * 追踪存储方式枚举。
 *
 * <p>对应配置项 {@code streammq.trace.storage}：
 *
 * <ul>
 *   <li>{@link #NONE} - 默认，不启用追踪存储
 *   <li>{@link #REDIS} - 使用 Redis Stream 存储追踪数据
 *   <li>{@link #UNKNOWN} - 未知编码兜底值（与 {@link DlqReason} / {@link TransactionScanState} 的 UNKNOWN
 *       兜底约定一致），解析失败时返回而非抛异常或返回 null
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public enum TraceStorageType {

    /** 不启用追踪存储。 */
    NONE("none"),

    /** 使用 Redis Stream 存储追踪数据。 */
    REDIS("redis"),

    /** 未知/无法识别的配置编码（解析失败兜底，避免 null 返回破坏调用方判空契约）。 */
    UNKNOWN("unknown");

    private final String code;

    TraceStorageType(String code) {
        this.code = code;
    }

    /**
     * 返回配置文件中使用的编码。
     *
     * @return 配置编码
     */
    public String getCode() {
        return code;
    }

    /**
     * 根据配置编码解析枚举。
     *
     * @param code 配置编码
     * @return 匹配的枚举；未匹配时返回 {@link #UNKNOWN} 兜底
     */
    public static TraceStorageType ofCode(String code) {
        return Arrays.stream(values())
                .filter(t -> t != UNKNOWN && t.code.equalsIgnoreCase(code))
                .findFirst()
                .orElse(UNKNOWN);
    }
}
